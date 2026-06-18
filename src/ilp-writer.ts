import * as net from "net";

const FLUSH_INTERVAL_MS = 500;
const FLUSH_BATCH_SIZE = 1000;
const INITIAL_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;

// A connection that closes sooner than this never carried useful data — QuestDB
// accepted the TCP handshake but dropped us almost immediately (the signature of
// an OOM-throttled / wedged QuestDB). We treat such a close as a *failed* attempt
// so the backoff grows, instead of resetting the delay and hammering QuestDB
// roughly once a second forever.
const STABLE_CONNECTION_MS = 5000;

// After this many consecutive instant-drops we stop assuming it's transient and
// surface a visible plugin error, so a flapping QuestDB is not invisible behind
// a permanently-green "Recording" status.
const UNHEALTHY_AFTER_FLAPS = 5;

// Upper bound on lines retained while disconnected. QuestDB being down must not
// grow the buffer without limit on a Pi/Cerbo. When full we drop the OLDEST
// lines — for a live history feed the most recent samples are the ones worth
// keeping. At ~80 bytes/line this caps the buffer near ~8 MB.
const MAX_BUFFER_LINES = 100_000;

function escapeTag(s: string): string {
  return s.replace(/[,= \n\\]/g, (c) => `\\${c}`);
}

function escapeFieldString(s: string): string {
  return s.replace(/["\\]/g, (c) => `\\${c}`);
}

export class ILPWriter {
  private socket: net.Socket | null = null;
  private buffer: string[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectDelay = INITIAL_RECONNECT_DELAY_MS;
  private connected = false;
  private connecting = false;
  private stopped = false;
  // Wall-clock ms when the current socket's connect callback fired. Read in the
  // `close` handler to decide whether the connection was stable (reset backoff)
  // or an instant flap (grow backoff). Reset to 0 on every close.
  private connectedAt = 0;
  // Consecutive instant-drops since the last stable connection. Drives the
  // transition to a visible "unhealthy" plugin error and clears once a
  // connection survives past STABLE_CONNECTION_MS.
  private consecutiveFlaps = 0;
  // Lines dropped from the head of the buffer because it hit MAX_BUFFER_LINES
  // while disconnected. Reported in the unhealthy message so silent data loss
  // is at least counted.
  private droppedLines = 0;
  private unhealthy = false;
  private debug: (msg: string) => void;
  private onUnhealthy: (msg: string) => void;
  private onHealthy: () => void;
  // Timing knobs, defaulted from the module constants. Overridable only so the
  // reconnect/flap/backoff tests don't have to wait real seconds; production
  // never passes `timing`.
  private readonly initialReconnectDelay: number;
  private readonly maxReconnectDelay: number;
  private readonly stableConnectionMs: number;
  private readonly unhealthyAfterFlaps: number;

  constructor(
    private host: string,
    private port: number,
    debug?: (msg: string) => void,
    callbacks?: {
      onUnhealthy?: (msg: string) => void;
      onHealthy?: () => void;
      timing?: {
        initialReconnectDelay?: number;
        maxReconnectDelay?: number;
        stableConnectionMs?: number;
        unhealthyAfterFlaps?: number;
      };
    },
  ) {
    this.debug = debug ?? (() => {});
    this.onUnhealthy = callbacks?.onUnhealthy ?? (() => {});
    this.onHealthy = callbacks?.onHealthy ?? (() => {});
    const t = callbacks?.timing ?? {};
    this.initialReconnectDelay =
      t.initialReconnectDelay ?? INITIAL_RECONNECT_DELAY_MS;
    this.maxReconnectDelay = t.maxReconnectDelay ?? MAX_RECONNECT_DELAY_MS;
    this.stableConnectionMs = t.stableConnectionMs ?? STABLE_CONNECTION_MS;
    this.unhealthyAfterFlaps = t.unhealthyAfterFlaps ?? UNHEALTHY_AFTER_FLAPS;
    this.reconnectDelay = this.initialReconnectDelay;
  }

  async connect(): Promise<void> {
    if (this.connected || this.connecting) return;
    this.connecting = true;
    this.stopped = false;

    return new Promise((resolve, reject) => {
      const socket = new net.Socket();
      this.socket = socket;

      socket.connect(this.port, this.host, () => {
        this.connected = true;
        this.connecting = false;
        this.connectedAt = Date.now();
        // NOTE: do NOT reset reconnectDelay here. A successful TCP connect is
        // not proof the connection is usable — QuestDB accepts the handshake
        // then drops us when it is overloaded. The delay is only reset once the
        // connection proves stable (survives STABLE_CONNECTION_MS), handled in
        // the `close` branch below.
        this.debug(`ILP connected to ${this.host}:${this.port}`);
        this.startFlushTimer();
        resolve();
      });

      socket.on("error", (err) => {
        if (this.connecting) {
          this.connecting = false;
          reject(err);
        }
        this.debug(`ILP socket error: ${err.message}`);
      });

      // `close` is the single source of truth for a connection ending. Node
      // always emits it after a failed connect (following `error`) and after a
      // live socket drops, so all backoff / flap accounting / rescheduling
      // lives here — the connect() rejection path deliberately does NOT also
      // reschedule, which would double-count flaps and run two timers.
      socket.on("close", () => {
        const wasConnected = this.connected;
        const upForMs = this.connectedAt ? Date.now() - this.connectedAt : 0;
        this.connected = false;
        this.connecting = false;
        this.connectedAt = 0;
        this.stopFlushTimer();
        if (this.stopped) return;

        // A connection that survived past the stability threshold is treated as
        // a healthy session that happened to end: reset the backoff and flap
        // counter so an occasional restart reconnects promptly. Anything
        // shorter (including a never-fully-connected socket) is a flap — grow
        // the backoff so we stop hammering an unhealthy QuestDB.
        if (wasConnected && upForMs >= this.stableConnectionMs) {
          this.reconnectDelay = this.initialReconnectDelay;
          this.consecutiveFlaps = 0;
          this.markHealthy();
        } else {
          this.consecutiveFlaps++;
          this.reconnectDelay = Math.min(
            this.reconnectDelay * 2,
            this.maxReconnectDelay,
          );
          this.debug(
            `ILP connection dropped after ${upForMs}ms (flap #${this.consecutiveFlaps}), retrying in ${this.reconnectDelay}ms`,
          );
          if (this.consecutiveFlaps >= this.unhealthyAfterFlaps) {
            this.markUnhealthy();
          }
        }
        this.scheduleReconnect();
      });
    });
  }

  private markUnhealthy(): void {
    this.unhealthy = true;
    const dropped =
      this.droppedLines > 0
        ? ` (${this.droppedLines} buffered samples dropped)`
        : "";
    this.onUnhealthy(
      `QuestDB keeps dropping the write connection — the container may be unhealthy or out of memory${dropped}.`,
    );
  }

  private markHealthy(): void {
    if (!this.unhealthy) return;
    this.unhealthy = false;
    this.droppedLines = 0;
    this.onHealthy();
  }

  private scheduleReconnect(): void {
    // Guard against two reconnect timers running at once. `connect()` rejecting
    // and the socket's `close` event can both fire for one failed attempt; only
    // the first scheduling wins until the timer runs.
    if (this.reconnectTimer || this.stopped) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.stopped) return;
      // connect() rejects on a failed attempt; the socket's `close` handler has
      // already done the backoff/flap accounting and will reschedule. Swallow
      // the rejection here so it doesn't surface as an unhandled rejection.
      this.connect()
        .then(() => {
          if (this.buffer.length > 0) this.flush();
        })
        .catch(() => {
          /* `close` handles backoff + reschedule */
        });
    }, this.reconnectDelay);
  }

  write(path: string, context: string, value: number, timestamp: Date): void {
    const ts = BigInt(timestamp.getTime()) * 1000000n;
    this.enqueue(
      `signalk,path=${escapeTag(path)},context=${escapeTag(context)} value=${value} ${ts}\n`,
    );
  }

  writeString(
    path: string,
    context: string,
    value: string,
    timestamp: Date,
  ): void {
    const ts = BigInt(timestamp.getTime()) * 1000000n;
    this.enqueue(
      `signalk_str,path=${escapeTag(path)},context=${escapeTag(context)} value_str="${escapeFieldString(value)}" ${ts}\n`,
    );
  }

  writePosition(
    path: string,
    context: string,
    position: { latitude: number; longitude: number },
    timestamp: Date,
  ): void {
    const ts = BigInt(timestamp.getTime()) * 1000000n;
    this.enqueue(
      `signalk_position,context=${escapeTag(context)} lat=${position.latitude},lon=${position.longitude} ${ts}\n`,
    );
  }

  private enqueue(line: string): void {
    this.buffer.push(line);
    // Bound the buffer so a long QuestDB outage can't exhaust memory. Drop from
    // the head (oldest samples) — the newest data is the most useful to retain
    // for a live history feed. Count drops so markUnhealthy can report them.
    if (this.buffer.length > MAX_BUFFER_LINES) {
      const overflow = this.buffer.length - MAX_BUFFER_LINES;
      this.buffer.splice(0, overflow);
      this.droppedLines += overflow;
    }
    if (this.buffer.length >= FLUSH_BATCH_SIZE) {
      this.flush();
    }
  }

  private flush(): void {
    if (!this.connected || !this.socket || this.buffer.length === 0) return;

    const data = this.buffer.join("");
    this.buffer = [];

    // The write callback reports failure (e.g. ERR_STREAM_DESTROYED when
    // QuestDB drops the connection mid-flush). Re-prepend the batch so the next
    // flush retries it on the reconnected socket instead of silently losing it.
    // QuestDB sorts/dedups by the designated `ts`, so the resulting out-of-order
    // ingestion is harmless.
    const canWrite = this.socket.write(data, (err) => {
      if (err) {
        this.buffer.unshift(data);
        this.debug(`ILP write failed, re-queued batch: ${err.message}`);
      }
    });
    if (!canWrite) {
      this.socket.once("drain", () => {
        this.debug("ILP socket drained, resuming writes");
      });
    }
  }

  private startFlushTimer(): void {
    this.stopFlushTimer();
    this.flushTimer = setInterval(() => this.flush(), FLUSH_INTERVAL_MS);
  }

  private stopFlushTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  async disconnect(): Promise<void> {
    this.stopped = true;
    this.stopFlushTimer();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.buffer.length > 0 && this.connected) {
      this.flush();
    }

    return new Promise((resolve) => {
      if (!this.socket) {
        resolve();
        return;
      }
      this.socket.end(() => {
        this.socket?.destroy();
        this.socket = null;
        this.connected = false;
        resolve();
      });
    });
  }
}

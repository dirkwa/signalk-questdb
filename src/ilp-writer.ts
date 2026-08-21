import * as net from "net";

// Every flush that carries lines becomes one WAL transaction per touched
// table, and with DEDUP tables each transaction pays a dedup pass on apply
// whose cost grows with partition size. Flushing twice a second produced
// ~173K tiny (~30-row) transactions per table per day in the field — enough
// that by day two the WAL apply on a Pi could no longer keep up with the
// commit rate and tables stalled. Fewer, larger transactions are dramatically
// cheaper to apply; QuestDB's own guidance is >100 rows per transaction. The
// trade-off is bounded staleness: at most this many ms of buffered samples
// are lost on a hard crash (a normal stop flushes first).
export const DEFAULT_FLUSH_INTERVAL_MS = 5000;
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
  // \r is escaped alongside \n for the same reason: ILP is a line protocol,
  // and a bare carriage return in a tag corrupts the row on the wire. The
  // escape is the two-character form so the value round-trips as text rather
  // than as a control character.
  return s.replace(/[,= \n\r\\]/g, (c) => {
    if (c === "\n") return "\\n";
    if (c === "\r") return "\\r";
    return `\\${c}`;
  });
}

function escapeFieldString(s: string): string {
  // Newlines and carriage returns MUST be encoded, not passed through. ILP is
  // a line protocol: a raw \n inside a quoted value splits the row into two
  // malformed lines and QuestDB silently stores NEITHER — verified against a
  // live instance, which recorded 0 rows for such a write. Signal K string
  // values do carry newlines (notification messages, free-text fields), so
  // this is reachable, and the failure is invisible: no error, no row.
  return s.replace(/[\\"\n\r]/g, (c) => {
    if (c === "\n") return "\\n";
    if (c === "\r") return "\\r";
    return `\\${c}`;
  });
}

export class ILPWriter {
  private socket: net.Socket | null = null;
  private buffer: string[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  // Fires once a fresh connection has stayed up for stableConnectionMs. That is
  // what marks the connection healthy and resets the backoff — NOT the close
  // handler, so a connection that recovers and simply stays connected (never
  // closing again) still clears the unhealthy state.
  private stableTimer: NodeJS.Timeout | null = null;
  private reconnectDelay = INITIAL_RECONNECT_DELAY_MS;
  private connected = false;
  private connecting = false;
  private stopped = false;
  // Last timestamp handed out, in nanoseconds. Rows are stamped at write time
  // with the server clock (see the delta handler), but `Date` is only
  // millisecond-resolution while the `signalk`/`signalk_str` tables dedup on
  // KEYS(ts, path, context, source). Two writes for the same path within one
  // millisecond would collide and the later would upsert over the earlier —
  // silent data loss for unthrottled (samplingRate 0) or bursty paths. QuestDB
  // stores microsecond resolution, so advancing this counter by at least 1µs
  // per write keeps every row's `ts` distinct without moving the visible
  // millisecond. It only ratchets forward: a same/earlier wall-clock reading
  // is bumped past the last one, so ingestion also stays strictly ordered.
  private lastNanos = 0n;
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
  private totalDroppedLines = 0;
  private readonly maxBufferLines: number;
  private unhealthy = false;
  // True between a backpressured write and the socket's next drain. Guards
  // against stacking one drain listener per flush while the socket is full.
  private awaitingDrain = false;
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
  // How often buffered lines are committed. Unlike `timing`, this is a real
  // production knob (wired to the plugin's ilpFlushIntervalMs config) — see
  // the DEFAULT_FLUSH_INTERVAL_MS comment for the batching rationale.
  private readonly flushIntervalMs: number;

  constructor(
    private host: string,
    private port: number,
    debug?: (msg: string) => void,
    callbacks?: {
      onUnhealthy?: (msg: string) => void;
      onHealthy?: () => void;
      flushIntervalMs?: number;
      // Test knob only: shrinks the disconnected-buffer cap so overflow
      // behaviour is exercisable without 100k writes. Production never
      // passes it.
      maxBufferLines?: number;
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
    this.flushIntervalMs =
      callbacks?.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.maxBufferLines = callbacks?.maxBufferLines ?? MAX_BUFFER_LINES;
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
        // then drops us when it is overloaded. The delay is reset, and the
        // connection marked healthy, only once it survives stableConnectionMs
        // (the stability timer below) — independent of any later `close`.
        this.debug(`ILP connected to ${this.host}:${this.port}`);
        this.startStabilityTimer(socket);
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
        const wasStable = this.connected && !this.stableTimer;
        const upForMs = this.connectedAt ? Date.now() - this.connectedAt : 0;
        this.connected = false;
        this.connecting = false;
        this.connectedAt = 0;
        // The next socket starts un-backpressured, so a stale flag here would
        // permanently suppress the drain listener on it.
        this.awaitingDrain = false;
        this.stopStabilityTimer();
        this.stopFlushTimer();
        if (this.stopped) return;

        // If the stability timer had already fired (wasStable), this was a
        // healthy session that simply ended — backoff/flaps were already reset
        // by the timer, so just reconnect promptly. Otherwise the connection
        // dropped before proving stable: a flap — grow the backoff so we stop
        // hammering an unhealthy QuestDB.
        if (!wasStable) {
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

  // Mark the connection healthy once it has stayed up for stableConnectionMs.
  // Guarded on socket identity so a stale timer from a previous socket can't
  // clear the state of a newer connection.
  private startStabilityTimer(socket: net.Socket): void {
    this.stopStabilityTimer();
    this.stableTimer = setTimeout(() => {
      this.stableTimer = null;
      if (this.stopped || !this.connected || this.socket !== socket) return;
      this.reconnectDelay = this.initialReconnectDelay;
      this.consecutiveFlaps = 0;
      this.markHealthy();
    }, this.stableConnectionMs);
  }

  private stopStabilityTimer(): void {
    if (this.stableTimer) {
      clearTimeout(this.stableTimer);
      this.stableTimer = null;
    }
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

  // Strictly-increasing nanosecond timestamp for the next row. Starts from the
  // server clock but never repeats or goes backwards within one writer, so
  // same-millisecond writes to a dedup table don't collide (see `lastNanos`).
  // An explicit `Date` (used by tests that assert exact ILP lines) is honoured
  // verbatim, but still advances the floor so it can't collide with a later
  // omitted-timestamp write.
  private nextNanos(explicit?: Date): bigint {
    if (explicit) {
      const explicitNanos = BigInt(explicit.getTime()) * 1_000_000n;
      if (explicitNanos > this.lastNanos) this.lastNanos = explicitNanos;
      return explicitNanos;
    }
    const nowNanos = BigInt(Date.now()) * 1_000_000n;
    // Advance by at least 1µs (QuestDB's storage resolution) past the last
    // value so the microsecond QuestDB persists is always distinct.
    this.lastNanos =
      nowNanos > this.lastNanos ? nowNanos : this.lastNanos + 1_000n;
    return this.lastNanos;
  }

  // Backfill variants (`*AtNanos`) take the source system's own nanosecond
  // timestamp verbatim instead of deriving one from the server clock.
  //
  // The live `write*` path stamps rows as they arrive and uses `lastNanos` to
  // keep every row distinct. Backfill cannot use that: historical rows must
  // land at their ORIGINAL instant, and they arrive out of order relative to
  // whatever the live feed is writing concurrently, so a monotonic ratchet
  // would shift them. Passing a `Date` is not enough either — `Date` is
  // millisecond-resolution, while InfluxDB stores nanoseconds and the
  // signalk/signalk_str tables dedup on KEYS(ts, path, context, source). Two
  // source points 200µs apart would truncate to the same millisecond and the
  // second would silently UPSERT over the first. Taking the caller's bigint
  // preserves the sub-millisecond spread that keeps them distinct rows.
  //
  // These deliberately do NOT touch `lastNanos`: a backfill of last year's
  // data must not drag the live writer's floor backwards (it only ratchets
  // up, so an old timestamp would be ignored anyway) nor push it forward.
  writeAtNanos(
    path: string,
    context: string,
    value: number,
    tsNanos: bigint,
    source?: string,
  ): void {
    const sourceTag = source ? `,source=${escapeTag(source)}` : "";
    this.enqueue(
      `signalk,path=${escapeTag(path)},context=${escapeTag(context)}${sourceTag} value=${value} ${tsNanos}\n`,
    );
  }

  writeStringAtNanos(
    path: string,
    context: string,
    value: string,
    tsNanos: bigint,
    kind?: "boolean" | "identity",
    source?: string,
  ): void {
    const sourceTag = source ? `,source=${escapeTag(source)}` : "";
    const kindTag = kind ? `,value_kind=${escapeTag(kind)}` : "";
    this.enqueue(
      `signalk_str,path=${escapeTag(path)},context=${escapeTag(context)}${sourceTag}${kindTag} value_str="${escapeFieldString(value)}" ${tsNanos}\n`,
    );
  }

  writePositionAtNanos(
    context: string,
    position: { latitude: number; longitude: number },
    tsNanos: bigint,
    source?: string,
  ): void {
    const sourceTag = source ? `,source=${escapeTag(source)}` : "";
    this.enqueue(
      `signalk_position,context=${escapeTag(context)}${sourceTag} lat=${position.latitude},lon=${position.longitude} ${tsNanos}\n`,
    );
  }

  /**
   * Roughly how much is still in flight, so a bulk importer can apply
   * backpressure instead of queueing an entire InfluxDB into memory.
   *
   * Counts BOTH the unflushed lines and what is still sitting inside the
   * socket. flush() clears `buffer` even when `socket.write()` returns false
   * (the data is handed to the kernel queue, not yet on the wire), so counting
   * only `buffer` would report an idle writer while the socket backs up — and
   * the importer would keep reading until MAX_BUFFER_LINES started dropping
   * the OLDEST lines, silently losing the history being copied.
   *
   * The socket side is an estimate: `writableLength` is bytes, converted at a
   * nominal line size. The importer only needs an order of magnitude to decide
   * whether to pause, not an exact count.
   */
  get pendingLines(): number {
    const NOMINAL_LINE_BYTES = 80;
    const queued = this.socket?.writableLength ?? 0;
    return this.buffer.length + Math.ceil(queued / NOMINAL_LINE_BYTES);
  }

  // `source` is the delta's sourceRef, a tag (SYMBOL) like the other
  // dimensions. Omitted when the delta carried none, leaving the column null —
  // exactly how rows written before the column existed read back.
  write(
    path: string,
    context: string,
    value: number,
    timestamp?: Date,
    source?: string,
  ): void {
    const ts = this.nextNanos(timestamp);
    const sourceTag = source ? `,source=${escapeTag(source)}` : "";
    this.enqueue(
      `signalk,path=${escapeTag(path)},context=${escapeTag(context)}${sourceTag} value=${value} ${ts}\n`,
    );
  }

  // `kind` records what the value ORIGINALLY was, so a boolean recorded as
  // "true" stays distinguishable from a path whose text value is the word
  // "true". It is a tag (SYMBOL) because it has two values and is filtered
  // on, never aggregated. Omitted for plain text, leaving value_kind null,
  // which is exactly how rows written before the column existed read back.
  writeString(
    path: string,
    context: string,
    value: string,
    timestamp?: Date,
    kind?: "boolean" | "identity",
    source?: string,
  ): void {
    const ts = this.nextNanos(timestamp);
    const sourceTag = source ? `,source=${escapeTag(source)}` : "";
    const kindTag = kind ? `,value_kind=${escapeTag(kind)}` : "";
    this.enqueue(
      `signalk_str,path=${escapeTag(path)},context=${escapeTag(context)}${sourceTag}${kindTag} value_str="${escapeFieldString(value)}" ${ts}\n`,
    );
  }

  // signalk_position has no path column — it holds navigation.position rows
  // exclusively (the caller enforces that), keyed on KEYS(ts, context, source).
  writePosition(
    context: string,
    position: { latitude: number; longitude: number },
    timestamp?: Date,
    source?: string,
  ): void {
    const ts = this.nextNanos(timestamp);
    const sourceTag = source ? `,source=${escapeTag(source)}` : "";
    this.enqueue(
      `signalk_position,context=${escapeTag(context)}${sourceTag} lat=${position.latitude},lon=${position.longitude} ${ts}\n`,
    );
  }

  private enqueue(line: string): void {
    this.buffer.push(line);
    this.enforceBufferCap();
    if (this.buffer.length >= FLUSH_BATCH_SIZE) {
      this.flush();
    }
  }

  // Put a failed flush's lines back at the FRONT (they precede anything enqueued
  // since) and re-apply the cap. Going through the same per-line cap as
  // enqueue() keeps MAX_BUFFER_LINES honest across repeated flap cycles —
  // re-queuing the whole batch as one element would let the buffer grow without
  // bound and undercount drops.
  private requeueFront(lines: string[]): void {
    if (lines.length === 0) return;
    this.buffer.unshift(...lines);
    this.enforceBufferCap();
  }

  // Bound the buffer so a long QuestDB outage can't exhaust memory. Drop from
  // the head (oldest samples) — the newest data is the most useful to retain
  // for a live history feed. Count drops so markUnhealthy can report them.
  private enforceBufferCap(): void {
    if (this.buffer.length > this.maxBufferLines) {
      const overflow = this.buffer.length - this.maxBufferLines;
      this.buffer.splice(0, overflow);
      this.droppedLines += overflow;
      this.totalDroppedLines += overflow;
    }
  }

  // Monotonic drop counter, NEVER reset (droppedLines above is consumed and
  // cleared by the health reporting). Callers holding write-once state — the
  // recorder's vessel-name dedupe — compare it between writes: an advance
  // means enqueued lines may have been discarded, so anything deduplicated
  // as "already written" has to be considered unwritten again.
  get droppedLineCount(): number {
    return this.totalDroppedLines;
  }

  private flush(): void {
    if (!this.connected || !this.socket || this.buffer.length === 0) return;

    const data = this.buffer.join("");
    this.buffer = [];

    // The write callback reports failure (e.g. ERR_STREAM_DESTROYED when
    // QuestDB drops the connection mid-flush). Re-queue the batch's lines so the
    // next flush retries them on the reconnected socket instead of silently
    // losing them. QuestDB sorts/dedups by the designated `ts`, so the resulting
    // out-of-order ingestion is harmless. Splitting back into lines (keeping the
    // trailing newline on each) means the re-queued data counts toward
    // MAX_BUFFER_LINES per line, preserving the bound across flap cycles.
    const lines = data.length > 0 ? data.split(/(?<=\n)/) : [];
    const canWrite = this.socket.write(data, (err) => {
      if (err) {
        this.requeueFront(lines);
        this.debug(`ILP write failed, re-queued batch: ${err.message}`);
      }
    });
    if (!canWrite && !this.awaitingDrain) {
      // ONE listener at a time. `once` still accumulates while the socket
      // stays backpressured: each flush that returns false adds another, and
      // none are removed until a drain finally fires. A bulk import keeps the
      // socket saturated for minutes, so they pile up — Node warned
      // "MaxListenersExceededWarning: 11 drain listeners added to [Socket]"
      // during a 65M-row import. Harmless in effect (they only log) but it is
      // a real leak, and it masks the warning's value for genuine ones.
      this.awaitingDrain = true;
      this.socket.once("drain", () => {
        this.awaitingDrain = false;
        this.debug("ILP socket drained, resuming writes");
      });
    }
  }

  private startFlushTimer(): void {
    this.stopFlushTimer();
    this.flushTimer = setInterval(() => this.flush(), this.flushIntervalMs);
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
    this.stopStabilityTimer();
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
        this.awaitingDrain = false;
        resolve();
      });
    });
  }
}

import * as net from "net";

const FLUSH_INTERVAL_MS = 100;
const FLUSH_BATCH_SIZE = 500;
const MAX_RECONNECT_DELAY_MS = 30000;

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
  private reconnectDelay = 1000;
  private connected = false;
  private connecting = false;
  private stopped = false;
  private debug: (msg: string) => void;

  constructor(
    private host: string,
    private port: number,
    debug?: (msg: string) => void,
  ) {
    this.debug = debug ?? (() => {});
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
        this.reconnectDelay = 1000;
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

      socket.on("close", () => {
        this.connected = false;
        this.connecting = false;
        this.stopFlushTimer();
        if (!this.stopped) {
          this.scheduleReconnect();
        }
      });
    });
  }

  private scheduleReconnect(): void {
    setTimeout(async () => {
      if (this.stopped) return;
      try {
        await this.connect();
        if (this.buffer.length > 0) {
          this.flush();
        }
      } catch {
        this.reconnectDelay = Math.min(
          this.reconnectDelay * 2,
          MAX_RECONNECT_DELAY_MS,
        );
        this.debug(
          `ILP reconnect failed, retrying in ${this.reconnectDelay}ms`,
        );
      }
    }, this.reconnectDelay);
  }

  write(path: string, context: string, value: number, timestamp: Date): void {
    const ts = BigInt(timestamp.getTime()) * 1000000n;
    const line = `signalk,path=${escapeTag(path)},context=${escapeTag(context)} value=${value} ${ts}\n`;
    this.buffer.push(line);
    if (this.buffer.length >= FLUSH_BATCH_SIZE) {
      this.flush();
    }
  }

  writeString(
    path: string,
    context: string,
    value: string,
    timestamp: Date,
  ): void {
    const ts = BigInt(timestamp.getTime()) * 1000000n;
    const line = `signalk_str,path=${escapeTag(path)},context=${escapeTag(context)} value_str="${escapeFieldString(value)}" ${ts}\n`;
    this.buffer.push(line);
    if (this.buffer.length >= FLUSH_BATCH_SIZE) {
      this.flush();
    }
  }

  writePosition(
    path: string,
    context: string,
    position: { latitude: number; longitude: number },
    timestamp: Date,
  ): void {
    const ts = BigInt(timestamp.getTime()) * 1000000n;
    const line = `signalk_position,context=${escapeTag(context)} lat=${position.latitude},lon=${position.longitude} ${ts}\n`;
    this.buffer.push(line);
    if (this.buffer.length >= FLUSH_BATCH_SIZE) {
      this.flush();
    }
  }

  private flush(): void {
    if (!this.connected || !this.socket || this.buffer.length === 0) return;

    const data = this.buffer.join("");
    this.buffer = [];

    const canWrite = this.socket.write(data);
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

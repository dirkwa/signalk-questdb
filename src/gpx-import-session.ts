// The upload side of a GPX import: the file arrives as numbered slices over
// several requests, because one request carrying 1.5 GB outlasts Node's
// five-minute request timeout on a boat's Wi-Fi and is refused by any proxy
// with a body limit. Each slice names the byte offset it starts at, so a
// client that lost a reply can ask where to resume instead of guessing, and a
// slice sent twice is refused rather than imported twice.
//
// A slice's reply waits until its points are with the writer and the writer
// has room again. That is the whole flow control: the browser sends the next
// slice only when the server is ready for it.

import { randomUUID } from "node:crypto";
import type { IncomingHttpHeaders } from "node:http";
import type { GpxImportState, GpxImportStatus } from "./api-contract.js";
import type { GpxImport } from "./gpx-import.js";

/** The largest slice one request may carry. */
export const MAX_SLICE_BYTES = 8 * 1024 * 1024;

/** An import nobody has sent a slice to for this long is abandoned. */
export const IDLE_TIMEOUT_MS = 15 * 60 * 1000;

/** The request does not fit the import's state; carries where it stands. */
export class GpxImportConflict extends Error {
  constructor(
    message: string,
    readonly status: GpxImportStatus | null,
  ) {
    super(message);
  }
}

export class SliceTooLarge extends Error {}

export class GpxImportSession {
  readonly id = randomUUID();
  private state: GpxImportState = "receiving";
  private received = 0;
  private error: string | undefined;
  private busy = false;
  private readonly startedAt: string;
  private updatedAt: string;
  private lastActivity: number;

  constructor(
    readonly filename: string,
    readonly size: number,
    private readonly importer: GpxImport,
    /** Resolves once QuestDB has applied everything the import wrote. */
    private readonly applied: () => Promise<void>,
    private readonly now: () => number = Date.now,
  ) {
    this.lastActivity = now();
    this.startedAt = new Date(this.lastActivity).toISOString();
    this.updatedAt = this.startedAt;
  }

  get status(): GpxImportStatus {
    return {
      id: this.id,
      filename: this.filename,
      size: this.size,
      received: this.received,
      state: this.state,
      summary: { ...this.importer.summary },
      ...(this.error !== undefined ? { error: this.error } : {}),
      startedAt: this.startedAt,
      updatedAt: this.updatedAt,
    };
  }

  get active(): boolean {
    return this.state === "receiving" || this.state === "finishing";
  }

  /** Receiving, but nothing has arrived for IDLE_TIMEOUT_MS. */
  get idle(): boolean {
    return (
      this.state === "receiving" &&
      !this.busy &&
      this.now() - this.lastActivity > IDLE_TIMEOUT_MS
    );
  }

  async append(offset: number, bytes: Uint8Array): Promise<GpxImportStatus> {
    this.expectReceiving();
    if (offset !== this.received) {
      throw new GpxImportConflict(
        `The next slice starts at byte ${this.received}, not ${offset}`,
        this.status,
      );
    }
    if (this.received + bytes.length > this.size) {
      throw new GpxImportConflict(
        `The slice runs past the announced size of ${this.size} bytes`,
        this.status,
      );
    }
    this.busy = true;
    try {
      await this.importer.write(bytes);
      this.received += bytes.length;
    } catch (err) {
      this.fail(err);
      throw err;
    } finally {
      this.busy = false;
      this.touch();
    }
    return this.status;
  }

  async finish(): Promise<GpxImportStatus> {
    this.expectReceiving();
    if (this.received !== this.size) {
      throw new GpxImportConflict(
        `Only ${this.received} of ${this.size} bytes have arrived`,
        this.status,
      );
    }
    this.state = "finishing";
    this.busy = true;
    this.touch();
    try {
      await this.importer.finish();
      await this.applied();
      // An abort while finishing has the last word.
      if (this.state === "finishing") this.state = "finished";
    } catch (err) {
      this.fail(err);
      throw err;
    } finally {
      this.busy = false;
      this.touch();
    }
    return this.status;
  }

  /** End the import. What it already wrote stays; dedup absorbs a re-run. */
  abort(reason = "Aborted"): void {
    if (!this.active) return;
    this.importer.cancel();
    this.state = "aborted";
    this.error = reason;
    this.touch();
  }

  private expectReceiving(): void {
    if (this.state !== "receiving") {
      throw new GpxImportConflict(`The import is ${this.state}`, this.status);
    }
    if (this.busy) {
      throw new GpxImportConflict(
        "The previous slice is still being imported",
        this.status,
      );
    }
  }

  private fail(err: unknown): void {
    // An abort already set the final state and its reason.
    if (this.state === "aborted") return;
    this.state = "failed";
    this.error = err instanceof Error ? err.message : String(err);
  }

  private touch(): void {
    this.lastActivity = this.now();
    this.updatedAt = new Date(this.lastActivity).toISOString();
  }
}

/**
 * One import at a time: they share the ILP writer, and two progress bars
 * against one writer would tell the user nothing. The last import's status
 * stays readable after it ends, so a panel opened later can show its result.
 */
export class GpxImports {
  private current: GpxImportSession | null = null;

  start(create: () => GpxImportSession): GpxImportSession {
    this.reapIdle();
    if (this.current?.active) {
      throw new GpxImportConflict(
        "A GPX import is already running",
        this.current.status,
      );
    }
    this.current = create();
    return this.current;
  }

  get(id: string): GpxImportSession | null {
    this.reapIdle();
    return this.current?.id === id ? this.current : null;
  }

  latest(): GpxImportStatus | null {
    this.reapIdle();
    return this.current?.status ?? null;
  }

  /** An import holds the writer: receiving, or finishing what it received. */
  get active(): boolean {
    this.reapIdle();
    return this.current?.active ?? false;
  }

  /** The plugin is stopping: the writer the import uses is going away. */
  abortAll(): void {
    this.current?.abort("The plugin stopped");
  }

  private reapIdle(): void {
    if (this.current?.idle) {
      this.current.abort(
        `No slice arrived for ${IDLE_TIMEOUT_MS / 60000} minutes`,
      );
    }
  }
}

/**
 * The body of a slice request, refused past `limit` bytes. Read here rather
 * than by a body parser: signalk-server parses JSON and form bodies only, so
 * an octet-stream arrives untouched.
 */
export function readSlice(
  req: NodeJS.ReadableStream & {
    headers: IncomingHttpHeaders;
    readableEnded?: boolean;
  },
  limit = MAX_SLICE_BYTES,
): Promise<Buffer> {
  if (Number(req.headers["content-length"]) > limit) {
    return Promise.reject(
      new SliceTooLarge(`A slice is at most ${limit} bytes`),
    );
  }
  if (req.readableEnded) {
    return Promise.reject(new Error("The request body was already read"));
  }
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > limit) {
        reject(new SliceTooLarge(`A slice is at most ${limit} bytes`));
        req.removeAllListeners("data");
        req.resume();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

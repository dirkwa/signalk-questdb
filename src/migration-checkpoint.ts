// Where an InfluxDB import has got to, kept on disk so it survives a restart.
//
// An import runs for hours, measurement by measurement, and a re-run begins
// again at the first one. Interrupted at the same point each time, it never
// reaches the measurements late in the alphabet — `navigation.position` among
// them. The checkpoint is what lets a re-run pick up where the last one stopped.
//
// The one rule it must never break: a checkpoint may not run ahead of the
// data. Rows the importer has "written" sit in the ILP writer's buffer, then in
// the socket, then in QuestDB's page cache, before they are on disk. A
// checkpoint claiming them done while they are still in any of those would,
// after a crash, make the resumed run skip rows that were never stored — a
// silent hole in the history. CheckpointTracker is the guard against that.

import { mkdir, open, readFile, rename, rm } from "node:fs/promises";
import path from "node:path";
import type { MigrationRequest } from "./migration.js";

/**
 * What makes two imports the same import. Never holds credentials: they are
 * asked for again when an import is resumed.
 */
export interface MigrationIdentity {
  url: string;
  type: string;
  bucket: string;
  from: string;
  to: string;
  context: string;
  sourceLabel: string;
  /** An explicit measurement selection, sorted; absent means all of them. */
  measurements?: string[];
  windowMs: number;
}

export interface MigrationCheckpoint {
  version: 1;
  identity: MigrationIdentity;
  /**
   * Measurements imported in full. A set of names, not a position in the
   * list: the list is re-discovered on resume, and a measurement that has
   * appeared since must be imported, not skipped for sorting early.
   */
  done: string[];
  /** The measurement under way and the first window not yet imported. */
  current?: { measurement: string; windowStart: number };
  /** Counters as they stood at this position, so a resumed run shows totals. */
  progress: { read: number; written: number; skipped: number };
  updatedAt: string;
}

export function migrationIdentity(
  req: MigrationRequest,
  windowMs: number,
): MigrationIdentity {
  return {
    url: req.url,
    type: req.type,
    bucket: req.bucket,
    // Normalised, so the same instant written two ways is the same import.
    from: new Date(req.from).toISOString(),
    to: new Date(req.to).toISOString(),
    context: req.context,
    sourceLabel: req.sourceLabel ?? "influxdb-import",
    measurements:
      req.measurements && req.measurements.length > 0
        ? [...req.measurements].sort()
        : undefined,
    windowMs,
  };
}

export function sameIdentity(
  a: MigrationIdentity,
  b: MigrationIdentity,
): boolean {
  return (
    a.url === b.url &&
    a.type === b.type &&
    a.bucket === b.bucket &&
    a.from === b.from &&
    a.to === b.to &&
    a.context === b.context &&
    a.sourceLabel === b.sourceLabel &&
    a.windowMs === b.windowMs &&
    JSON.stringify(a.measurements ?? null) ===
      JSON.stringify(b.measurements ?? null)
  );
}

/** What runMigration needs of a store; the file store below is the real one. */
export interface CheckpointStore {
  save(checkpoint: MigrationCheckpoint): Promise<void>;
  clear(): Promise<void>;
}

/** The checkpoint of the one import this plugin runs at a time, as a file. */
export class FileCheckpointStore implements CheckpointStore {
  constructor(private readonly file: string) {}

  /**
   * The stored checkpoint, or null. A file that cannot be read or does not
   * hold a checkpoint is treated as absent: the cost is an import starting
   * over, which is always safe, where trusting a damaged one is not.
   */
  async load(): Promise<MigrationCheckpoint | null> {
    let text: string;
    try {
      text = await readFile(this.file, "utf8");
    } catch {
      return null;
    }
    try {
      const parsed: unknown = JSON.parse(text);
      return isCheckpoint(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }

  /**
   * Written beside the target, synced, and renamed over it, so a crash or a
   * power cut leaves either the previous checkpoint or the new one — never a
   * truncated one. The directory is synced too, so the rename itself is on
   * disk; where a directory cannot be opened for that, the rename is left to
   * the file system's own ordering.
   */
  async save(checkpoint: MigrationCheckpoint): Promise<void> {
    const dir = path.dirname(this.file);
    await mkdir(dir, { recursive: true });
    const tmp = `${this.file}.tmp`;
    const fh = await open(tmp, "w");
    try {
      await fh.writeFile(JSON.stringify(checkpoint), "utf8");
      await fh.sync();
    } finally {
      await fh.close();
    }
    await rename(tmp, this.file);
    await syncDirectory(dir);
  }

  /** Synced like a save: a cleared checkpoint must not come back after a power cut. */
  async clear(): Promise<void> {
    await rm(this.file, { force: true });
    await syncDirectory(path.dirname(this.file));
  }
}

/**
 * Put a directory's entries on disk, so a rename or removal in it survives a
 * power cut. A directory that does not exist has nothing to sync, and not
 * every platform lets one be opened and synced; any other failure is a real
 * one and is not hidden.
 */
async function syncDirectory(dir: string): Promise<void> {
  try {
    const dh = await open(dir, "r");
    try {
      await dh.sync();
    } finally {
      await dh.close();
    }
  } catch (err) {
    const code =
      typeof err === "object" &&
      err !== null &&
      "code" in err &&
      typeof err.code === "string"
        ? err.code
        : "";
    if (code !== "ENOENT" && !DIRECTORY_SYNC_UNSUPPORTED.has(code)) throw err;
  }
}

/**
 * Errors that mean "this platform or file system does not sync directories":
 * Windows refuses to open one, and some file systems refuse to fsync one.
 */
const DIRECTORY_SYNC_UNSUPPORTED: ReadonlySet<string> = new Set([
  "EISDIR",
  "EPERM",
  "EINVAL",
  "ENOTSUP",
  "EOPNOTSUPP",
]);

function isCheckpoint(value: unknown): value is MigrationCheckpoint {
  if (typeof value !== "object" || value === null) return false;
  const cp = value as Partial<MigrationCheckpoint>;
  const id = cp.identity as Partial<MigrationIdentity> | undefined;
  const progress = cp.progress;
  return (
    cp.version === 1 &&
    typeof id === "object" &&
    id !== null &&
    typeof id.url === "string" &&
    typeof id.type === "string" &&
    typeof id.bucket === "string" &&
    typeof id.from === "string" &&
    typeof id.to === "string" &&
    typeof id.context === "string" &&
    typeof id.sourceLabel === "string" &&
    typeof id.windowMs === "number" &&
    (id.measurements === undefined ||
      (Array.isArray(id.measurements) &&
        id.measurements.every((m) => typeof m === "string"))) &&
    Array.isArray(cp.done) &&
    cp.done.every((m) => typeof m === "string") &&
    (cp.current === undefined ||
      (typeof cp.current === "object" &&
        cp.current !== null &&
        typeof cp.current.measurement === "string" &&
        typeof cp.current.windowStart === "number")) &&
    typeof progress === "object" &&
    progress !== null &&
    typeof progress.read === "number" &&
    typeof progress.written === "number" &&
    typeof progress.skipped === "number" &&
    typeof cp.updatedAt === "string"
  );
}

/**
 * How old a position must be before it is persisted.
 *
 * Leaving the writer is not the same as being stored. QuestDB takes ILP rows
 * into memory and commits them on an interval of seconds, so a crash of either
 * process loses the rows of the last interval. The managed container runs
 * QuestDB with cairo.commit.mode=sync, which puts a commit on disk; an external
 * one may be on QuestDB's default nosync, where what survives a power cut is
 * what the kernel had already written back — with default settings anything
 * older than about 35 s (30 s dirty-page expiry plus the 5 s flusher period).
 * A minute clears both with room to spare, on the assumption of those
 * defaults: an external QuestDB is the operator's, and the README asks them to
 * run it with sync, which is the real answer for a power cut. The price is
 * that a resumed import redoes up to a minute of work, which costs nothing but
 * the time: rows upsert.
 */
export const CHECKPOINT_LAG_MS = 60_000;

/**
 * The writer counters the tracker reads; ILPWriter provides all three. A
 * writer that cannot report them gets no checkpoints at all: without them
 * there is no telling what has left it, and guessing is how a checkpoint ends
 * up ahead of the data.
 */
export interface SettlementSource {
  readonly enqueuedLineCount?: number;
  readonly settledLineCount?: number;
  readonly droppedLineCount?: number;
}

/**
 * Persists positions only once they are safe to resume from.
 *
 * A position offered now is held as the candidate. It is written later, by a
 * subsequent offer, once two things hold: every line enqueued up to it has
 * left the writer, and it is at least `lagMs` old. Positions offered while a
 * candidate is waiting are passed over, so what reaches disk is always a
 * position that was reached at least `lagMs` ago.
 *
 * If the writer drops lines the tracker stops for good. The cap discards the
 * OLDEST buffered lines, which may predate any position taken since, so none
 * can be trusted; the last one persisted before the drop still can. The run
 * itself fails at its end for the same drops, and the re-run resumes from
 * there.
 */
export class CheckpointTracker {
  private candidate: {
    checkpoint: MigrationCheckpoint;
    mark: number;
    takenAt: number;
  } | null = null;
  private readonly droppedAtStart: number;

  constructor(
    private readonly store: CheckpointStore,
    private readonly writer: SettlementSource,
    private readonly lagMs: number = CHECKPOINT_LAG_MS,
    private readonly now: () => number = Date.now,
  ) {
    this.droppedAtStart = writer.droppedLineCount ?? 0;
  }

  /**
   * Offer the position just reached. `snapshot` is only called if the position
   * is taken up, so passing over one costs nothing.
   */
  async offer(snapshot: () => MigrationCheckpoint): Promise<void> {
    const dropped = this.writer.droppedLineCount ?? 0;
    if (dropped !== this.droppedAtStart) {
      this.candidate = null;
      return;
    }
    const enqueued = this.writer.enqueuedLineCount;
    const settled = this.writer.settledLineCount;
    if (enqueued === undefined || settled === undefined) return;
    if (this.candidate) {
      // Lines leave the writer in the order they came, so the ones that have
      // left — accepted by the socket, or dropped before this run began — are
      // the first `settled + dropped` of the sequence.
      const left = settled + dropped;
      const ripe = this.now() - this.candidate.takenAt >= this.lagMs;
      if (!ripe || left < this.candidate.mark) return;
      await this.store.save(this.candidate.checkpoint);
      this.candidate = null;
    }
    this.candidate = {
      checkpoint: snapshot(),
      mark: enqueued,
      takenAt: this.now(),
    };
  }
}

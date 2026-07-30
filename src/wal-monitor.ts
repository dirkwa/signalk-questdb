// WAL suspension monitor: detection, one-shot automatic lossless recovery,
// and the data needed for a guided (bounded, explicit) skip of an unreadable
// segment.
//
// Background — how a WAL table dies silently: QuestDB ingestion is two-stage.
// The sequencer durably commits every ILP batch (sequencerTxn advances, the
// client sees success), then a background apply job merges transactions into
// the partitions (writerTxn advances). When the apply job hits an error it
// SUSPENDS the table: sequencerTxn keeps climbing, writerTxn freezes, queries
// serve stale data, and no client-visible error is raised anywhere. Observed
// in the field for 17 days straight before anyone noticed.
//
// Causes split into two classes with different remedies:
//   - Transient (disk full since cleared, OOM, fd exhaustion): a plain
//     `RESUME WAL` replays every pending transaction — lossless, done.
//   - Unreadable segment (an unclean container stop mid-write corrupts the
//     in-flight segment's column files; QuestDB then throws e.g. an
//     AssertionError from VarcharTypeDriver on EVERY apply attempt): plain
//     resume re-suspends within milliseconds, forever. The only way forward
//     is `RESUME WAL FROM TXN <n>` past the unreadable data — lossy, so it
//     must be explicit, minimal, and quantified before a human triggers it.
//
// The monitor auto-applies the lossless remedy (once per stall point) and
// prepares — but never auto-applies — the lossy one.
//
// A trap this design dodges: errorTag/errorMessage from wal_tables() often
// cannot explain a suspension. A raw Java error (the corrupt-segment case!)
// sets no errorTag, and the detail lives only in engine memory — a container
// recreate wipes it while the suspended flag itself persists. Hence
// `extractApplyError` scrapes the engine log for the real reason while it is
// still there, and the caller persists the snapshot to the server log.

export interface SuspendedTable {
  name: string;
  writerTxn: number;
  sequencerTxn: number;
  txnLag: number;
  errorTag: string;
  errorMessage: string;
}

// One row of the pending-transaction segment map: a (walId, segmentId) group
// of not-yet-applied transactions, with its sequencer-txn span and the
// wall-clock commit-time window it covers.
export interface PendingSegment {
  walId: number;
  segmentId: number;
  txns: number;
  minTxn: number;
  maxTxn: number;
  minTimestamp: string;
  maxTimestamp: string;
}

export interface SkipPlan {
  // First transaction of the segment AFTER the one the writer is stuck in —
  // the minimal `RESUME WAL FROM TXN` target that gets past the unreadable
  // data. Skips are segment-granular because corruption is physical (the
  // segment's column files), not logical: every transaction in the segment
  // reads from the same broken files.
  skipToTxn: number;
  skippedTxns: number;
  // Commit-time window of the skipped transactions (server wall-clock at
  // ingestion, not data timestamps) — what the operator is agreeing to lose.
  skipWindowStart: string;
  skipWindowEnd: string;
  walId: number;
  segmentId: number;
  // The stuck segment is the newest one — skipping it drops the ENTIRE
  // pending backlog, not a slice out of the middle. The UI must escalate.
  tailSkip: boolean;
}

// Pending-segment map for a suspended table, oldest first. The WHERE clause
// restricts to unapplied transactions, so the first group is exactly the
// stuck segment's unreadable tail and its min(timestamp) is the freeze time.
export function buildPendingSegmentsSQL(
  table: string,
  writerTxn: number,
): string {
  const safeTable = table.replace(/'/g, "''");
  const txn = Math.max(0, Math.floor(writerTxn));
  return (
    "SELECT walId, segmentId, count() txns, " +
    "min(sequencerTxn) minTxn, max(sequencerTxn) maxTxn, " +
    "min(timestamp) minTimestamp, max(timestamp) maxTimestamp " +
    `FROM wal_transactions('${safeTable}') ` +
    `WHERE sequencerTxn > ${txn} ` +
    "GROUP BY walId, segmentId ORDER BY minTxn"
  );
}

// True when the operator-confirmed plan (untrusted request payload) matches
// the freshly computed one in EVERY field. The skip is destructive, so a
// confirmation must prove the operator saw exactly what is about to be
// skipped — not just the same target txn, which can coincide across plans
// with different loss windows.
export function skipPlansEqual(plan: SkipPlan, confirmed: unknown): boolean {
  if (typeof confirmed !== "object" || confirmed === null) return false;
  const c = confirmed as Record<string, unknown>;
  return (
    Number(c.skipToTxn) === plan.skipToTxn &&
    Number(c.skippedTxns) === plan.skippedTxns &&
    Number(c.walId) === plan.walId &&
    Number(c.segmentId) === plan.segmentId &&
    String(c.skipWindowStart) === plan.skipWindowStart &&
    String(c.skipWindowEnd) === plan.skipWindowEnd &&
    c.tailSkip === plan.tailSkip
  );
}

export function computeSkipPlan(segments: PendingSegment[]): SkipPlan | null {
  if (segments.length === 0) return null;
  const [stuck, ...rest] = segments;
  return {
    skipToTxn: rest.length > 0 ? rest[0].minTxn : stuck.maxTxn + 1,
    skippedTxns: stuck.txns,
    skipWindowStart: stuck.minTimestamp,
    skipWindowEnd: stuck.maxTimestamp,
    walId: stuck.walId,
    segmentId: stuck.segmentId,
    tailSkip: rest.length === 0,
  };
}

// Pull the apply job's failure out of the engine log. QuestDB logs it as a
// single multi-line entry:
//
//   2026-07-17T22:22:51Z C i.q.c.w.ApplyWal2TableJob job failed, table
//       suspended [table=signalk_str~8, seqTxn=..., error=java.lang.AssertionError
//   \tat io.questdb.cairo.VarcharTypeDriver.getDataVectorSize(...)
//   ...
//   ]
//
// but arrives from the container-log API as separate lines. Capture the last
// failure line plus its stack, stopping at the closing bracket or the next
// timestamped entry.
const APPLY_FAILED = /ApplyWal2TableJob job failed/;
const LOG_ENTRY_START = /^\d{4}-\d{2}-\d{2}T/;
const MAX_ERROR_LINES = 15;

export function extractApplyError(
  lines: string[],
  tableName?: string,
): string | null {
  // The failure line names the table as its on-disk dir name,
  // `table=<name>~<id>` — scope to it so one table's diagnosis can't show
  // another suspended table's stack trace.
  const tableMatcher = tableName
    ? new RegExp(
        `table=${tableName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}[~,\\]]`,
      )
    : null;
  let start = -1;
  for (let i = lines.length - 1; i >= 0; i--) {
    if (
      APPLY_FAILED.test(lines[i]) &&
      (!tableMatcher || tableMatcher.test(lines[i]))
    ) {
      start = i;
      break;
    }
  }
  if (start === -1) return null;
  const collected = [lines[start]];
  for (
    let i = start + 1;
    i < lines.length && collected.length < MAX_ERROR_LINES;
    i++
  ) {
    const line = lines[i];
    if (LOG_ENTRY_START.test(line)) break;
    collected.push(line);
    if (line.trim() === "]") break;
  }
  return collected.join("\n");
}

// Frames that only appear when the writer dies OPENING the table's partition
// (or reading the commit record that describes it), rather than while reading
// a pending WAL segment.
//
// Anchored to a stack-trace line inside QuestDB's own package
// (`\tat io.questdb.cairo.TableReader.openPartition(TableReader.java:12)`).
// Both halves of that anchor matter, and each closes a real false positive:
// the captured text also contains QuestDB's `[table=<name>, error=<message>]`
// header, so an unanchored word matches a user's table name or error prose;
// and without the namespace, an unrelated library's `openPartition` frame
// matches too. A false positive silently withholds a skip that would have
// repaired the table, so this errs toward not classifying.
//
// The class name is left open (rather than whitelisting exact pairs) so an
// upstream refactor that moves these methods still matches; the namespace
// plus the method set is specific enough that nothing else realistically
// collides.
const PARTITION_OPEN_FRAME =
  /^\s*at\s+io\.questdb\.[\w.$]*\b(?:openPartition|openLastPartition|unsafeLoadAll|readUnsafe)\s*\(/m;

// True when the apply failure happened before any transaction could be
// applied, i.e. while opening the existing partition data.
//
// This distinguishes the two corruption classes, which look identical in
// wal_tables() but have opposite remedies:
//
//   - unreadable WAL SEGMENT — the pending data is bad; skipping past it with
//     RESUME WAL FROM TXN gets the table running again at a bounded cost.
//   - torn APPLIED PARTITION (power loss under the default nosync commit
//     mode: the `_txn` commit record reached disk claiming rows whose column
//     data did not) — the writer asserts while opening that partition, before
//     it looks at any transaction. Every RESUME WAL variant re-hits it no
//     matter which txn it starts from, so offering the lossy skip here
//     destroys the pending backlog and still leaves the table suspended.
//     Repair means restoring `_txn` to the durable on-disk prefix, which is
//     surgery on QuestDB internals, not something to drive from a button.
export function isPartitionOpenFailure(applyError: string | null): boolean {
  if (!applyError) return false;
  return PARTITION_OPEN_FRAME.test(applyError);
}

export type AutoResumeOutcome = "pending" | "failed";

export interface WalMonitorDeps {
  listSuspended(): Promise<SuspendedTable[]>;
  resumeTable(name: string): Promise<void>;
  // Called on every check that finds suspensions, with the current set and
  // whether any table's automatic resume has already failed (=> the alert
  // should escalate from "recovering" to "needs operator action").
  onSuspended(tables: SuspendedTable[], anyAutoResumeFailed: boolean): void;
  // Called once when a previously-suspended state fully clears.
  onResolved(): void;
  debug(msg: string): void;
  error(msg: string): void;
}

export interface WalMonitorOptions {
  intervalMs?: number;
  firstCheckDelayMs?: number;
  // Minimum gap before re-attempting an automatic resume at the SAME stall
  // point. Guards against a resume/re-suspend flap when re-failure is slow
  // enough to fall between checks (a fast re-failure is caught by the
  // same-writerTxn episode and never retried at all).
  retryMinMs?: number;
  now?: () => number;
}

const DEFAULT_INTERVAL_MS = 60_000;
const DEFAULT_FIRST_CHECK_DELAY_MS = 10_000;
const DEFAULT_RETRY_MIN_MS = 10 * 60_000;

interface Episode {
  writerTxn: number;
  outcome: AutoResumeOutcome;
  // True once the lossless RESUME WAL for this episode actually executed
  // (the request succeeded). Only then is a re-suspension at the same
  // writerTxn evidence of unreadable data.
  resumeIssued: boolean;
}

// Per-table suspension tracking. An episode is keyed by (table, writerTxn):
// the same table re-suspending at a HIGHER writerTxn means the resume made
// real progress and hit a different failure — that deserves a fresh automatic
// attempt. Re-suspension at the SAME writerTxn means replay hit the same
// unreadable data — retrying is the storm the QuestDB web console is known
// for (it parks a busy ALTER and retries forever), so the episode locks to
// `failed` and only an operator action moves things forward.
export class WalMonitor {
  private episodes = new Map<string, Episode>();
  // Survives episode teardown so a resume→clear→re-suspend flap at one stall
  // point cannot trigger more than one automatic resume per retryMinMs.
  private lastAttempts = new Map<string, { writerTxn: number; at: number }>();
  private timer: NodeJS.Timeout | null = null;
  private firstTimer: NodeJS.Timeout | null = null;
  private checking = false;
  // Latched by stop(). A check that was already in flight when the plugin
  // tore down re-reads this after every await so it can't issue an ALTER
  // against — or emit status for — a QuestDB the teardown is removing.
  private stopped = false;
  private readonly intervalMs: number;
  private readonly firstCheckDelayMs: number;
  private readonly retryMinMs: number;
  private readonly now: () => number;

  constructor(
    private deps: WalMonitorDeps,
    options: WalMonitorOptions = {},
  ) {
    this.intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
    this.firstCheckDelayMs =
      options.firstCheckDelayMs ?? DEFAULT_FIRST_CHECK_DELAY_MS;
    this.retryMinMs = options.retryMinMs ?? DEFAULT_RETRY_MIN_MS;
    this.now = options.now ?? Date.now;
  }

  start(): void {
    this.stopped = false;
    this.firstTimer = setTimeout(() => {
      void this.check();
    }, this.firstCheckDelayMs);
    this.timer = setInterval(() => {
      void this.check();
    }, this.intervalMs);
  }

  stop(): void {
    this.stopped = true;
    if (this.firstTimer) {
      clearTimeout(this.firstTimer);
      this.firstTimer = null;
    }
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.episodes.clear();
    this.lastAttempts.clear();
  }

  outcomeFor(table: string): AutoResumeOutcome | null {
    return this.episodes.get(table)?.outcome ?? null;
  }

  async check(): Promise<void> {
    if (this.checking || this.stopped) return;
    this.checking = true;
    try {
      await this.runCheck();
    } catch (err) {
      // Transient query failures (container restarting, plugin mid-update)
      // must not kill the interval.
      this.deps.debug(
        `WAL monitor check failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.checking = false;
    }
  }

  private async runCheck(): Promise<void> {
    const suspended = await this.deps.listSuspended();
    if (this.stopped) return;
    const suspendedNames = new Set(suspended.map((t) => t.name));

    const hadEpisodes = this.episodes.size > 0;
    for (const name of [...this.episodes.keys()]) {
      if (!suspendedNames.has(name)) this.episodes.delete(name);
    }

    if (suspended.length === 0) {
      if (hadEpisodes) {
        this.deps.debug("WAL suspension resolved — all tables applying again");
        this.deps.onResolved();
      }
      return;
    }

    for (const table of suspended) {
      const episode = this.episodes.get(table.name);
      if (!episode || episode.writerTxn !== table.writerTxn) {
        // New suspension, or the writer advanced to a new stall point.
        // Persist the full snapshot NOW: the engine-side error detail is
        // in-memory only and a container recreate erases it while the
        // suspension itself persists.
        this.deps.error(
          `WAL suspended on ${table.name}: writerTxn=${table.writerTxn} ` +
            `sequencerTxn=${table.sequencerTxn} lag=${table.txnLag} ` +
            `errorTag=${JSON.stringify(table.errorTag)} ` +
            `errorMessage=${JSON.stringify(table.errorMessage)}`,
        );
        const episode: Episode = {
          writerTxn: table.writerTxn,
          outcome: "pending",
          resumeIssued: false,
        };
        this.episodes.set(table.name, episode);
        episode.resumeIssued = await this.attemptResume(table);
      } else if (episode.outcome === "pending") {
        if (episode.resumeIssued) {
          // Our resume actually executed and the table is suspended again at
          // the same writerTxn: replay re-hit the same failure. Lock the
          // episode.
          episode.outcome = "failed";
          this.deps.error(
            `Automatic lossless resume failed on ${table.name} — WAL apply ` +
              `fails again at txn ${table.writerTxn + 1}. The segment data ` +
              `at the stall point is likely unreadable (corrupt, typically ` +
              `after an unclean shutdown). A bounded skip is required: open ` +
              `the QuestDB History plugin panel to repair.`,
          );
        } else {
          // The resume REQUEST failed (busy writer, transport error) or was
          // rate-limit deferred — nothing was diagnosed, so this must not
          // escalate to "failed" (which the panel reads as corruption).
          // Keep trying, still bounded by the per-stall-point rate limit.
          episode.resumeIssued = await this.attemptResume(table);
        }
      }
      if (this.stopped) return;
    }

    const anyFailed = suspended.some(
      (t) => this.episodes.get(t.name)?.outcome === "failed",
    );
    this.deps.onSuspended(suspended, anyFailed);
  }

  // Issues the lossless resume unless one already ran recently at this same
  // stall point. Returns true only when the ALTER actually executed — a
  // deferred or failed request must not count as a diagnostic attempt.
  private async attemptResume(table: SuspendedTable): Promise<boolean> {
    const last = this.lastAttempts.get(table.name);
    if (
      last &&
      last.writerTxn === table.writerTxn &&
      this.now() - last.at < this.retryMinMs
    ) {
      // Same stall point, attempted recently — e.g. the episode map lost the
      // earlier attempt to an intermediate "resolved" flap. Don't pile on.
      return false;
    }
    this.lastAttempts.set(table.name, {
      writerTxn: table.writerTxn,
      at: this.now(),
    });
    try {
      await this.deps.resumeTable(table.name);
      this.deps.debug(
        `attempted automatic lossless RESUME WAL on ${table.name}`,
      );
      return true;
    } catch (err) {
      this.deps.error(
        `automatic RESUME WAL on ${table.name} failed: ` +
          `${err instanceof Error ? err.message : String(err)}`,
      );
      return false;
    }
  }
}

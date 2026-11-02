// QuestDB watchdog: restarts a managed QuestDB container that has run out of
// memory or stopped answering, and says so before and after — in the server
// log with QuestDB's own message, on the plugin status, and as a Signal K
// notification.
//
// Background — how QuestDB runs out of memory without dying: QuestDB caps the
// native memory it tracks (90 % of the container limit by default) and fails
// any allocation past the cap with "global RSS memory limit exceeded". When a
// leak holds usage at the cap, every query that needs memory fails, the WAL
// apply job then suspends the tables, and in the end the HTTP server stops
// accepting connections. The JVM never exits, so the container's restart
// policy never acts, and nothing recovers short of a restart. Observed twice
// on QuestDB 9.4.3, whose HTTP query cache kept every cached GROUP BY's
// buffers at their peak (see QDB_HTTP_QUERY_CACHE_ENABLED in
// container-env.ts): 17 hours, then 4 hours, of recording lost before
// someone restarted it by hand.
//
// What counts:
//   - out of memory: QuestDB reported its global limit, and its native memory
//     is still at that limit at two checks in a row. A single large query that
//     hits the limit frees what it held when it ends, and so does a client
//     that keeps asking for one; a leak keeps the memory held. The per-query
//     limit ("query memory limit exceeded") is not this: it fails one query
//     and leaves the instance healthy.
//   - unresponsive: the container is running but QuestDB has answered nothing
//     for minutes, at `unresponsiveChecks` checks in a row. A busy QuestDB is
//     not this: with one worker, a long statement holds up every query behind
//     it, so a health check can time out while QuestDB is merely working.
//   A container someone else stopped is left alone, unless the watchdog's
//   own stop may be what keeps it down (watchdog-checks.ts).
//
// Restarts are bounded: at most one per `minRestartGapMs`, and at most
// `maxRestarts` within `restartWindowMs`. A restart that does not help would
// otherwise turn into a loop that takes recording down every few minutes.
// A restart that failed (its stop or start threw) counts against neither: it
// may have left the container stopped, and recording stays off until the
// container runs, so it is tried again after `failedRetryGapMs`.

export type WatchdogReason = "out-of-memory" | "unresponsive";

export type WatchdogVerdict =
  | { state: "ok" }
  // Not a check: the container is not running, or the plugin is busy with
  // it. Starts the count over and leaves an alert as it is.
  | { state: "skip"; why: string }
  // `message` is QuestDB's own text where there is one.
  | { state: WatchdogReason; message: string };

export type RestartOutcome =
  | { state: "answering" }
  // Restarted, but QuestDB does not answer yet.
  | { state: "not-answering" }
  // Nothing was restarted: the container was stopped meanwhile, or the
  // plugin is stopping.
  | { state: "called-off"; why: string };

export interface WatchdogDeps {
  // Runs `fn` under the plugin's lifecycle lock, so a probe and the restart
  // that follows it see one container: an update or purge cannot land between
  // them.
  exclusive<T>(fn: () => Promise<T>): Promise<T>;
  // One look at QuestDB.
  probe(): Promise<WatchdogVerdict>;
  // QuestDB's recent error lines for the server log; best effort.
  recentErrors(): Promise<string[]>;
  // Stops and starts the container and waits for QuestDB to answer. Throws
  // when the restart itself failed.
  restart(): Promise<RestartOutcome>;
  // Server log, at error level so it shows without debug enabled.
  log(msg: string): void;
  // Plugin status line, as an error.
  status(msg: string): void;
  // The incident is over: the plugin may show its recording line again.
  recovered(): void;
  notify(state: "alert" | "normal", message: string): void;
  debug(msg: string): void;
}

export interface WatchdogOptions {
  intervalMs?: number;
  unresponsiveChecks?: number;
  outOfMemoryChecks?: number;
  minRestartGapMs?: number;
  maxRestarts?: number;
  restartWindowMs?: number;
  failedRetryGapMs?: number;
  // Monotonic milliseconds for the spans; wall-clock milliseconds for the
  // times the messages name. A Pi without a clock battery steps its wall
  // clock at boot, which must not move a restart.
  now?: () => number;
  wallNow?: () => number;
}

const DEFAULT_INTERVAL_MS = 60_000;
const DEFAULT_UNRESPONSIVE_CHECKS = 3;
const DEFAULT_OUT_OF_MEMORY_CHECKS = 2;
const DEFAULT_MIN_RESTART_GAP_MS = 15 * 60_000;
const DEFAULT_MAX_RESTARTS = 3;
const DEFAULT_RESTART_WINDOW_MS = 6 * 3_600_000;
const DEFAULT_FAILED_RETRY_GAP_MS = 5 * 60_000;

// QuestDB's message when the instance as a whole is at its memory cap
// (io.questdb.std.Unsafe). The same text in 9.x and 10.x.
const GLOBAL_LIMIT_RE = /global RSS memory limit exceeded[^"\n]*/;

// How close to the limit counts as held at it. A leak pins usage within a
// few hundred KiB of the limit (724,566,016 of 724,775,670 bytes on the
// boat); memory that a finished query freed drops far below.
const AT_LIMIT_FRACTION = 0.95;

// The part of a QuestDB error that names the global memory limit, or null.
// Errors reach the plugin wrapped ("QuestDB query failed (400): {...}"), so
// this cuts QuestDB's own sentence out of whatever surrounds it.
export function globalMemoryLimitMessage(text: string): string | null {
  const match = GLOBAL_LIMIT_RE.exec(text);
  return match ? match[0].trim() : null;
}

// The limit in bytes that a global-limit message names (`RSS_MEM_LIMIT=`),
// or null.
export function memoryLimitBytes(message: string): number | null {
  const match = /RSS_MEM_LIMIT=(\d+)/.exec(message);
  const bytes = match ? Number(match[1]) : NaN;
  return Number.isSafeInteger(bytes) && bytes > 0 ? bytes : null;
}

// Whether native memory in use is still held at the limit.
export function heldAtLimit(usedBytes: number, limitBytes: number): boolean {
  return usedBytes >= limitBytes * AT_LIMIT_FRACTION;
}

// Error and critical lines of the engine log, newest last. With the stdout
// level at ERROR (container-env.ts) the log holds little else, but advisory
// lines still come through and say nothing about a failure.
export function questdbErrorLines(lines: readonly string[], max = 5): string[] {
  return lines
    .filter((line) => /^\S+\s+[EC]\s/.test(line))
    .slice(-max)
    .map((line) => (line.length > 300 ? `${line.slice(0, 300)}…` : line));
}

const LABEL: Record<WatchdogReason, string> = {
  "out-of-memory": "out of memory",
  unresponsive: "not answering",
};

export class QuestdbWatchdog {
  private timer: NodeJS.Timeout | null = null;
  private checking = false;
  private stopped = false;
  // Checks in a row that found QuestDB failing, for either reason: a memory
  // leak typically ends with QuestDB not answering at all, and the count
  // must not start over when it does.
  private failing = 0;
  private restarts: number[] = [];
  // When a restart last failed.
  private failedAt: number | null = null;
  // Set once a held-back restart has been reported, so a long incident does
  // not repeat the same report every minute.
  private heldBack = false;
  // An alert is out that nobody has cleared yet.
  private alerting = false;
  private readonly intervalMs: number;
  private readonly unresponsiveChecks: number;
  private readonly outOfMemoryChecks: number;
  private readonly minRestartGapMs: number;
  private readonly maxRestarts: number;
  private readonly restartWindowMs: number;
  private readonly failedRetryGapMs: number;
  private readonly now: () => number;
  private readonly wallNow: () => number;

  constructor(
    private deps: WatchdogDeps,
    options: WatchdogOptions = {},
  ) {
    this.intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
    this.unresponsiveChecks = Math.max(
      1,
      options.unresponsiveChecks ?? DEFAULT_UNRESPONSIVE_CHECKS,
    );
    this.outOfMemoryChecks = Math.max(
      1,
      options.outOfMemoryChecks ?? DEFAULT_OUT_OF_MEMORY_CHECKS,
    );
    this.minRestartGapMs =
      options.minRestartGapMs ?? DEFAULT_MIN_RESTART_GAP_MS;
    this.maxRestarts = Math.max(1, options.maxRestarts ?? DEFAULT_MAX_RESTARTS);
    this.restartWindowMs = options.restartWindowMs ?? DEFAULT_RESTART_WINDOW_MS;
    this.failedRetryGapMs =
      options.failedRetryGapMs ?? DEFAULT_FAILED_RETRY_GAP_MS;
    this.now = options.now ?? (() => performance.now());
    this.wallNow = options.wallNow ?? Date.now;
  }

  // The wall-clock time of a monotonic instant.
  private iso(at: number): string {
    return new Date(this.wallNow() - (this.now() - at)).toISOString();
  }

  start(): void {
    this.stopped = false;
    this.timer = setInterval(() => {
      void this.check();
    }, this.intervalMs);
  }

  // Ends monitoring. An alert that is still out is the caller's to clear (see
  // `alertOutstanding`): this runs inside the plugin's teardown, which knows
  // what to say.
  stop(): void {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.failing = 0;
  }

  get alertOutstanding(): boolean {
    return this.alerting;
  }

  async check(): Promise<void> {
    if (this.checking || this.stopped) return;
    this.checking = true;
    try {
      await this.deps.exclusive(() => this.runCheck());
    } catch (err) {
      // A failed probe (QuestDB mid-restart, the runtime busy) must not end
      // the interval.
      this.deps.debug(
        `QuestDB watchdog check failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.checking = false;
    }
  }

  private async runCheck(): Promise<void> {
    if (this.stopped) return;
    const verdict = await this.deps.probe();
    if (this.stopped) return;
    if (verdict.state === "skip") {
      this.failing = 0;
      this.deps.debug(`QuestDB watchdog: check skipped: ${verdict.why}`);
      return;
    }
    if (verdict.state === "ok") {
      this.failing = 0;
      this.heldBack = false;
      // QuestDB came back by itself: while a restart was held back, or after
      // one that it did not answer at first.
      this.clearAlert("QuestDB answers again.");
      return;
    }
    const count = ++this.failing;
    const needed =
      verdict.state === "out-of-memory"
        ? this.outOfMemoryChecks
        : this.unresponsiveChecks;
    this.deps.debug(
      `QuestDB watchdog: ${LABEL[verdict.state]} (${count}/${needed}): ${verdict.message}`,
    );
    if (count < needed) return;
    await this.restartFor(verdict.state, verdict.message);
  }

  private async restartFor(
    reason: WatchdogReason,
    message: string,
  ): Promise<void> {
    const now = this.now();
    if (this.failedAt !== null && now - this.failedAt < this.failedRetryGapMs) {
      // Reported with the failure; not a hold-back.
      this.deps.debug(
        `QuestDB watchdog: ${LABEL[reason]}; the failed restart is tried ` +
          `again at ${this.iso(this.failedAt + this.failedRetryGapMs)}`,
      );
      return;
    }
    this.restarts = this.restarts.filter(
      (at) => now - at < this.restartWindowMs,
    );
    const last = this.restarts.at(-1);
    const nextAllowed = Math.max(
      last !== undefined ? last + this.minRestartGapMs : now,
      this.restarts.length >= this.maxRestarts
        ? this.restarts[this.restarts.length - this.maxRestarts] +
            this.restartWindowMs
        : now,
    );
    if (nextAllowed > now) {
      if (!this.heldBack) {
        this.heldBack = true;
        const text =
          `QuestDB is ${LABEL[reason]} again (${message}), but it was ` +
          `restarted ${this.restarts.length} time(s) in the last ` +
          `${Math.round(this.restartWindowMs / 3_600_000)} h; the next ` +
          `restart is held back until ${this.iso(nextAllowed)}.`;
        this.deps.log(`QuestDB watchdog: ${text}`);
        this.deps.status(`QuestDB ${LABEL[reason]}; restart held back`);
        this.alerting = true;
        this.deps.notify("alert", text);
      }
      return;
    }
    this.heldBack = false;

    let lines: string[] = [];
    try {
      lines = await this.deps.recentErrors();
    } catch {
      // Best effort: the restart does not wait on the log.
    }
    if (this.stopped) return;
    this.deps.log(
      `QuestDB watchdog: QuestDB is ${LABEL[reason]}: ${message}. ` +
        `Restarting the QuestDB container.` +
        (lines.length > 0
          ? ` QuestDB's last errors: ${lines.join(" | ")}`
          : ""),
    );
    this.deps.status(`QuestDB ${LABEL[reason]}: restarting the container`);
    this.alerting = true;
    this.deps.notify(
      "alert",
      `QuestDB is ${LABEL[reason]} (${message}). Restarting the QuestDB ` +
        `container; history is not recorded meanwhile.`,
    );

    this.restarts.push(now);
    this.failing = 0;
    let outcome: RestartOutcome;
    try {
      outcome = await this.deps.restart();
    } catch (err) {
      this.restarts.pop();
      this.failedAt = now;
      if (this.stopped) return;
      const text =
        `restarting the QuestDB container failed: ` +
        `${err instanceof Error ? err.message : String(err)}; trying again ` +
        `in ${Math.round(this.failedRetryGapMs / 60_000)} min`;
      this.deps.log(`QuestDB watchdog: ${text}`);
      this.deps.status(`QuestDB ${LABEL[reason]}: ${text}`);
      this.deps.notify("alert", `QuestDB is ${LABEL[reason]}; ${text}.`);
      return;
    }
    if (this.stopped) return;
    if (outcome.state === "called-off") {
      // Nothing was restarted, so nothing counts against the limits. The
      // alert stays: QuestDB is still failing, and the next check that finds
      // it answering clears the alert and the status with it.
      this.restarts.pop();
      this.deps.log(`QuestDB watchdog: restart called off: ${outcome.why}.`);
      this.deps.notify(
        "alert",
        `QuestDB is ${LABEL[reason]} (${message}); the restart was called ` +
          `off: ${outcome.why}.`,
      );
      return;
    }
    const seconds = Math.round((this.now() - now) / 1000);
    if (outcome.state === "not-answering") {
      const text =
        `restarted the QuestDB container after it was ${LABEL[reason]}, ` +
        `but it does not answer yet (${seconds} s)`;
      this.deps.log(`QuestDB watchdog: ${text}.`);
      this.deps.status(`QuestDB ${LABEL[reason]}: ${text}`);
      this.deps.notify("alert", `QuestDB ${text}.`);
      return;
    }
    this.deps.log(
      `QuestDB watchdog: restarted the QuestDB container after it was ` +
        `${LABEL[reason]}; it answers again after ${seconds} s.`,
    );
    this.clearAlert(
      `QuestDB was restarted at ${this.iso(now)} after it was ` +
        `${LABEL[reason]} (${message}).`,
    );
  }

  private clearAlert(message: string): void {
    if (!this.alerting) return;
    this.alerting = false;
    this.deps.recovered();
    this.deps.notify("normal", message);
  }
}

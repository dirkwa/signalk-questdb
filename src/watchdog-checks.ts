// The watchdog's look at a managed QuestDB and its restart of the container
// (questdb-watchdog.ts decides when). The plugin's state comes in through
// `deps`, so both can run against a fake container manager in tests.

import type { ContainerManagerApi } from "signalk-container-helper";
import type { QueryClient } from "./query-client.js";
import {
  globalMemoryLimitMessage,
  heldAtLimit,
  memoryLimitBytes,
  type RestartOutcome,
  type WatchdogVerdict,
} from "./questdb-watchdog.js";

export interface WatchdogChecksDeps {
  containers: Pick<ContainerManagerApi, "getState" | "stop" | "start">;
  name: string;
  client(): Pick<QueryClient, "health" | "exec"> | null;
  // Why a check cannot judge QuestDB right now, such as a parquet export
  // that keeps its one worker busy for minutes; null when it can.
  busy(): string | null;
  // signalk-questdb's containerEpoch: bumped when an update recreates the
  // container, and by restart() here.
  epoch(): number;
  bumpEpoch(): void;
  // The plugin stopped or purged since the watchdog was armed.
  ended(): boolean;
  // "host:port" of QuestDB's HTTP endpoint, for messages.
  endpoint(): string | null;
  // Reconnects the ILP writer at once rather than at the end of its backoff.
  reconnectWriter(): void;
  // The watchdog's own container stops that have not settled yet, shared by
  // every instance the plugin creates: a stop an earlier start's watchdog
  // issued can still take the container down after a plugin restart.
  stops: { pending: number };
  now?: () => number;
  timing?: Partial<WatchdogTiming>;
}

export interface WatchdogTiming {
  // How long QuestDB must have answered nothing before a failed health check
  // counts as not answering. Longer than any statement QuestDB runs for the
  // plugin: the retention copy may take 120 s, and QuestDB stops other
  // queries at its own 60 s default.
  silenceMs: number;
  // How long a global-limit message counts as evidence. A leak keeps
  // producing them: every read that needs memory fails.
  evidenceMs: number;
  // With such evidence, how long QuestDB must have answered nothing before
  // that counts as out of memory. Shorter than `silenceMs`, but longer than
  // the longest statement the plugin runs, the 120 s retention copy: a
  // client resending a read that hits the limit keeps the worker busy too.
  limitSilenceMs: number;
  // Deadline for each container call. The container API takes no abort
  // signal, and the checks run under the plugin's lifecycle lock: a stalled
  // runtime must cost one failed check, not every start and update queued
  // behind it.
  containerCallMs: number;
  // How long a restart waits for QuestDB to answer again. QuestDB starts in
  // seconds; replaying a large WAL backlog can take a minute or two on a Pi.
  answerMs: number;
  pollMs: number;
}

const DEFAULT_TIMING: WatchdogTiming = {
  silenceMs: 5 * 60_000,
  evidenceMs: 10 * 60_000,
  limitSilenceMs: 150_000,
  containerCallMs: 60_000,
  answerMs: 120_000,
  pollMs: 1000,
};

// Native memory in use, as QuestDB's limit counts it.
const NATIVE_MEMORY_SQL =
  "SELECT sum(bytes) FROM memory_metrics() WHERE memory_tag LIKE 'NATIVE%'";

class Deadline extends Error {}

// Rejects once `ms` passed; the call itself runs on, and its late failure is
// swallowed.
function bounded<T>(call: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  return Promise.race([
    call,
    new Promise<never>((_, reject) => {
      timer = setTimeout(
        () =>
          reject(
            new Deadline(
              `${what} did not finish within ${Math.round(ms / 1000)} s`,
            ),
          ),
        ms,
      );
    }),
  ]).finally(() => {
    clearTimeout(timer);
    call.catch(() => {});
  });
}

const mib = (bytes: number) => Math.round(bytes / 1048576);

export class WatchdogChecks {
  private limit: { message: string; bytes: number; at: number } | null = null;
  private answeredAt: number;
  private seenEpoch: number;
  // Set while the container may be down because of a stop of the
  // watchdog's: from the moment restart() stops it until the container is
  // seen running with none of those stops pending. While it is set, a
  // stopped container is the watchdog's to start again, not someone else's
  // to respect: a stop that outlives its deadline can still take the
  // container down minutes later, and recording must not then stay off.
  private leftStopped: boolean;
  private readonly now: () => number;
  private readonly timing: WatchdogTiming;

  constructor(private deps: WatchdogChecksDeps) {
    this.now = deps.now ?? (() => performance.now());
    this.timing = { ...DEFAULT_TIMING, ...deps.timing };
    this.answeredAt = this.now();
    this.seenEpoch = deps.epoch();
    this.leftStopped = deps.stops.pending > 0;
  }

  // A container stop of the watchdog's own, counted until it settles.
  private ownStop(): Promise<void> {
    const stops = this.deps.stops;
    stops.pending++;
    const stop = this.deps.containers.stop(this.deps.name);
    stop
      .catch(() => {})
      .finally(() => {
        stops.pending--;
      });
    return stop;
  }

  // QueryClient observers (QueryClientOptions).
  noteResponse(): void {
    this.answeredAt = this.now();
  }

  noteErrorResponse(body: string): void {
    const message = globalMemoryLimitMessage(body);
    if (message) this.noteLimit(message);
  }

  // From the WAL monitor: a table suspended at the limit names it even when
  // no read of the plugin's hit it.
  noteSuspended(tables: readonly { name: string; errorMessage: string }[]) {
    for (const table of tables) {
      const message = globalMemoryLimitMessage(table.errorMessage);
      if (message) this.noteLimit(`${table.name}: ${message}`);
    }
  }

  private noteLimit(message: string): void {
    const bytes = memoryLimitBytes(message) ?? this.limit?.bytes;
    if (bytes !== undefined) this.limit = { message, bytes, at: this.now() };
  }

  private recentLimit() {
    const limit = this.limit;
    return limit && this.now() - limit.at <= this.timing.evidenceMs
      ? limit
      : null;
  }

  private async readState(): Promise<string> {
    try {
      return await bounded(
        this.deps.containers.getState(this.deps.name),
        this.timing.containerCallMs,
        "reading the container state",
      );
    } catch {
      return "unknown";
    }
  }

  // Whether native memory is still held at the limit, with what was read;
  // held is false where it cannot be told.
  private async memoryHeld(
    client: Pick<QueryClient, "exec">,
    limitBytes: number,
  ): Promise<{ held: boolean; detail: string }> {
    try {
      const result = await client.exec(NATIVE_MEMORY_SQL, 10_000);
      const used = Number(result.dataset[0]?.[0]);
      return Number.isFinite(used)
        ? {
            held: heldAtLimit(used, limitBytes),
            detail: `native memory ${mib(used)} of ${mib(limitBytes)} MiB`,
          }
        : { held: false, detail: "" };
    } catch (err) {
      // The read itself hit the limit: as held as it gets. Anything else,
      // such as an older QuestDB without memory_metrics(), tells nothing.
      const message = err instanceof Error ? err.message : String(err);
      return globalMemoryLimitMessage(message)
        ? { held: true, detail: "reading native memory hit the limit too" }
        : { held: false, detail: "" };
    }
  }

  async probe(): Promise<WatchdogVerdict> {
    const busy = this.deps.busy();
    if (busy) return { state: "skip", why: busy };
    const client = this.deps.client();
    if (!client) return { state: "skip", why: "not connected" };
    // Stops are only issued under the lock this check holds too (bar a
    // restart a purge overran, see restart()), so the count can only fall
    // during the read; one that fell may be a stop that ended just after the
    // container read as running.
    const pendingBefore = this.deps.stops.pending;
    const state = await this.readState();
    if (
      state === "running" &&
      pendingBefore === 0 &&
      this.deps.stops.pending === 0
    ) {
      // Running, and no stop of ours left to take it down: a later stop is
      // someone else's.
      this.leftStopped = false;
    }
    if (this.deps.epoch() !== this.seenEpoch) {
      // An update recreated the container: the checks before it were about
      // the old one, and the new one gets its full grace.
      this.seenEpoch = this.deps.epoch();
      this.answeredAt = this.now();
      this.limit = null;
      return { state: "skip", why: "the container was just recreated" };
    }
    if (state === "stopped" && this.leftStopped) {
      return {
        state: "unresponsive",
        message: "the container is stopped since the watchdog stopped it",
      };
    }
    if (state !== "running") {
      return { state: "skip", why: `the container is ${state}` };
    }

    const health = await client.health();
    const healthy = health.ok;
    // Noted by the observer too, so recentLimit() below has it.
    const healthHitLimit =
      health.error !== undefined &&
      globalMemoryLimitMessage(health.error) !== null;
    const limit = this.recentLimit();
    const silentMs = () => this.now() - this.answeredAt;
    if (limit) {
      if (!healthy) {
        if (healthHitLimit) {
          // QuestDB answered the health check itself with the limit.
          return {
            state: "out-of-memory",
            message: `${limit.message}; the health check hit it too`,
          };
        }
        // How a leak ends: QuestDB stops answering altogether.
        if (silentMs() >= this.timing.limitSilenceMs) {
          return {
            state: "out-of-memory",
            message: `${limit.message}; QuestDB no longer answers`,
          };
        }
      } else {
        const memory = await this.memoryHeld(client, limit.bytes);
        if (memory.held) {
          return {
            state: "out-of-memory",
            message: `${limit.message}; ${memory.detail}`,
          };
        }
      }
    }
    if (healthy) return { state: "ok" };
    const silent = silentMs();
    if (silent < this.timing.silenceMs) {
      return {
        state: "skip",
        why: `QuestDB is busy: it answered ${Math.round(silent / 1000)} s ago`,
      };
    }
    const endpoint = this.deps.endpoint();
    return {
      state: "unresponsive",
      message:
        (endpoint ? `no answer at ${endpoint}` : "no answer") +
        ` for ${Math.round(silent / 60_000)} min`,
    };
  }

  async restart(): Promise<RestartOutcome> {
    const pluginStopping = {
      state: "called-off",
      why: "the plugin is stopping",
    } as const;
    if (this.deps.ended()) return pluginStopping;
    const { containers, name } = this.deps;
    const state = await this.readState();
    // A purge forces its teardown through once it has waited its while for
    // the lock, and a new start can then run beside this restart: past this
    // point a stop would hit that start's container.
    if (this.deps.ended()) return pluginStopping;
    if (state !== "running" && !(state === "stopped" && this.leftStopped)) {
      return { state: "called-off", why: `the container is ${state}` };
    }
    if (state === "running" && this.deps.stops.pending > 0) {
      // Another stop would only queue behind the one that hangs, and hold the
      // lifecycle lock for its deadline each time this is tried again.
      throw new Error(
        "a stop sent earlier has not finished, and no other is sent until it has",
      );
    }
    // A WAL skip validated against the container before the restart must
    // not run against the one after it.
    this.deps.bumpEpoch();
    this.seenEpoch = this.deps.epoch();
    if (state === "running") {
      this.leftStopped = true;
      await bounded(
        this.ownStop(),
        this.timing.containerCallMs,
        "stopping the container",
      );
    }
    if (this.deps.ended()) return pluginStopping;
    // The instance this start brings up gets its grace from here, also when
    // the start misses its deadline and lands later; a limit the old
    // instance hit is no evidence against it.
    this.answeredAt = this.now();
    this.limit = null;
    await bounded(
      containers.start(name),
      this.timing.containerCallMs,
      "starting the container",
    );
    // Running now, by this start: only a stop of ours still pending can
    // take it down again.
    this.leftStopped = this.deps.stops.pending > 0;
    if (this.deps.ended()) {
      // The teardown stopped the container while this start was in flight;
      // leave it stopped as the teardown meant it. Counted like any stop of
      // ours, for the watchdog of the plugin's next start.
      await bounded(
        this.ownStop(),
        this.timing.containerCallMs,
        "stopping the container",
      ).catch(() => {});
      return pluginStopping;
    }
    // And from here, once the start is done.
    this.answeredAt = this.now();
    this.limit = null;
    const deadline = this.now() + this.timing.answerMs;
    while (this.now() < deadline) {
      if (this.deps.ended()) return pluginStopping;
      const client = this.deps.client();
      if (client && (await client.health()).ok) {
        // The ILP writer lost its connection with the old instance and is
        // backing off; it need not wait out the backoff.
        this.deps.reconnectWriter();
        return { state: "answering" };
      }
      await new Promise((resolve) => setTimeout(resolve, this.timing.pollMs));
    }
    return { state: "not-answering" };
  }
}

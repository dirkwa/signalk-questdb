import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  QuestdbWatchdog,
  globalMemoryLimitMessage,
  heldAtLimit,
  memoryLimitBytes,
  questdbErrorLines,
  type RestartOutcome,
  type WatchdogOptions,
  type WatchdogVerdict,
} from "../questdb-watchdog.js";

// QuestDB 9.4.3's error response when the instance is at its cap, as the
// plugin's QueryClient wraps it.
const LIMIT_ERROR =
  'QuestDB query failed (400): {"query":"SELECT 1","error":"global RSS ' +
  "memory limit exceeded [usage=724566016, RSS_MEM_LIMIT=724775670, " +
  'size=262144, memoryTag=36]","position":0}';

const OOM: WatchdogVerdict = {
  state: "out-of-memory",
  message: "global RSS memory limit exceeded [usage=1]",
};
const DOWN: WatchdogVerdict = { state: "unresponsive", message: "no answer" };
const OK: WatchdogVerdict = { state: "ok" };

function makeHarness(options: WatchdogOptions = {}) {
  let clock = Date.parse("2026-11-01T20:00:00Z");
  let verdict: WatchdogVerdict = OK;
  let restartResult: RestartOutcome | Error = { state: "answering" };
  let restartGate: Promise<void> | null = null;
  let errorsGate: Promise<void> | null = null;
  let probeGate: Promise<void> | null = null;
  let lockGate: Promise<void> | null = null;
  const h = {
    restarts: 0,
    logs: [] as string[],
    statuses: [] as string[],
    notes: [] as { state: string; message: string }[],
    recovered: 0,
    exclusiveCalls: 0,
    errorLines: [
      "2026-11-01T20:00:00.000000Z E i.q.c.h.p.JsonQueryProcessor boom",
    ],
    set(v: WatchdogVerdict) {
      verdict = v;
    },
    restartWith(r: RestartOutcome | Error) {
      restartResult = r;
    },
    holdRestart(gate: Promise<void>) {
      restartGate = gate;
    },
    holdErrors(gate: Promise<void>) {
      errorsGate = gate;
    },
    holdProbe(gate: Promise<void>) {
      probeGate = gate;
    },
    holdLock(gate: Promise<void>) {
      lockGate = gate;
    },
    advance(ms: number) {
      clock += ms;
    },
    watchdog: null as unknown as QuestdbWatchdog,
  };
  h.watchdog = new QuestdbWatchdog(
    {
      exclusive: async (fn) => {
        h.exclusiveCalls++;
        if (lockGate) await lockGate;
        return fn();
      },
      probe: async () => {
        if (probeGate) await probeGate;
        return verdict;
      },
      recentErrors: async () => {
        if (errorsGate) await errorsGate;
        return h.errorLines;
      },
      restart: async () => {
        h.restarts++;
        if (restartGate) await restartGate;
        clock += 12_000;
        if (restartResult instanceof Error) throw restartResult;
        return restartResult;
      },
      log: (msg) => h.logs.push(msg),
      status: (msg) => h.statuses.push(msg),
      recovered: () => h.recovered++,
      notify: (state, message) => h.notes.push({ state, message }),
      debug: () => {},
    },
    { now: () => clock, wallNow: () => clock, ...options },
  );
  return h;
}

// One check a minute, as the timer would run them.
async function checks(h: ReturnType<typeof makeHarness>, n: number) {
  for (let i = 0; i < n; i++) {
    await h.watchdog.check();
    h.advance(60_000);
  }
}

describe("globalMemoryLimitMessage", () => {
  it("cuts QuestDB's sentence out of a wrapped error", () => {
    assert.equal(
      globalMemoryLimitMessage(LIMIT_ERROR),
      "global RSS memory limit exceeded [usage=724566016, " +
        "RSS_MEM_LIMIT=724775670, size=262144, memoryTag=36]",
    );
  });

  it("ignores the per-query limit and other errors", () => {
    // One query over its own budget leaves the instance healthy; restarting
    // for it would only cost recording.
    assert.equal(
      globalMemoryLimitMessage(
        '{"error":"query memory limit exceeded [workload=HTTP, limit=1]"}',
      ),
      null,
    );
    assert.equal(globalMemoryLimitMessage("mmap failed: out of memory"), null);
    assert.equal(globalMemoryLimitMessage(""), null);
  });
});

describe("memoryLimitBytes", () => {
  it("reads RSS_MEM_LIMIT from the global-limit message", () => {
    assert.equal(
      memoryLimitBytes(globalMemoryLimitMessage(LIMIT_ERROR) ?? ""),
      724775670,
    );
    assert.equal(memoryLimitBytes("global RSS memory limit exceeded"), null);
    assert.equal(memoryLimitBytes("RSS_MEM_LIMIT=0"), null);
  });
});

describe("heldAtLimit", () => {
  it("tells memory held at the limit from memory a query freed", () => {
    // Usage on the boat while the leak held it.
    assert.equal(heldAtLimit(724566016, 724775670), true);
    // Right after a large query ended: back near the 40 MiB baseline.
    assert.equal(heldAtLimit(42 * 1048576, 724775670), false);
    assert.equal(heldAtLimit(0.9 * 724775670, 724775670), false);
  });
});

describe("questdbErrorLines", () => {
  it("keeps the last error and critical lines, bounded", () => {
    const lines = [
      "2026-11-01T20:00:00.000000Z A server-main enjoy",
      "2026-11-01T20:00:01.000000Z E one",
      "2026-11-01T20:00:02.000000Z C two",
      "2026-11-01T20:00:03.000000Z I three",
      "2026-11-01T20:00:04.000000Z E " + "x".repeat(400),
    ];
    const kept = questdbErrorLines(lines, 2);
    assert.equal(kept.length, 2);
    assert.match(kept[0], / C two$/);
    assert.ok(kept[1].endsWith("…"));
    assert.ok(kept[1].length <= 301);
  });
});

describe("QuestdbWatchdog", () => {
  it("does nothing while QuestDB is fine", async () => {
    const h = makeHarness();
    await checks(h, 5);
    assert.equal(h.restarts, 0);
    assert.deepEqual(h.logs, []);
    assert.deepEqual(h.notes, []);
  });

  it("runs every check under the caller's lock", async () => {
    const h = makeHarness();
    await checks(h, 3);
    assert.equal(h.exclusiveCalls, 3);
  });

  it("restarts once out of memory persists into a second check", async () => {
    const h = makeHarness();
    h.set(OOM);
    await checks(h, 1);
    assert.equal(h.restarts, 0, "one failing check can be a single big query");
    await checks(h, 1);
    assert.equal(h.restarts, 1);
  });

  it("does not restart for an out-of-memory check that clears", async () => {
    const h = makeHarness();
    h.set(OOM);
    await checks(h, 1);
    h.set(OK);
    await checks(h, 1);
    h.set(OOM);
    await checks(h, 1);
    assert.equal(h.restarts, 0);
  });

  it("restarts a QuestDB that stopped answering after three checks", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 2);
    assert.equal(h.restarts, 0);
    await checks(h, 1);
    assert.equal(h.restarts, 1);
  });

  it("keeps counting when out of memory turns into no answer", async () => {
    // How the leak ended on the boat: queries failed for an hour, then the
    // HTTP server stopped accepting connections. The count must not start
    // over at the switch.
    const h = makeHarness();
    h.set(OOM);
    await checks(h, 1);
    h.set(DOWN);
    await checks(h, 2);
    assert.equal(h.restarts, 1);
  });

  it("starts over on a skipped check and leaves alerts alone", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 2);
    h.set({ state: "skip", why: "the container is stopped" });
    await checks(h, 1);
    h.set(DOWN);
    await checks(h, 2);
    assert.equal(h.restarts, 0);
  });

  it("logs QuestDB's message, sets the status and alerts before restarting", async () => {
    const h = makeHarness();
    h.set(OOM);
    await checks(h, 2);
    assert.match(h.logs[0], /^QuestDB watchdog: QuestDB is out of memory: /);
    assert.match(h.logs[0], /global RSS memory limit exceeded \[usage=1\]/);
    assert.match(h.logs[0], /Restarting the QuestDB container/);
    assert.match(
      h.logs[0],
      /QuestDB's last errors: .* E i\.q\.c\.h\.p\.JsonQueryProcessor boom/,
    );
    assert.equal(
      h.statuses[0],
      "QuestDB out of memory: restarting the container",
    );
    assert.equal(h.notes[0].state, "alert");
    assert.match(h.notes[0].message, /global RSS memory limit exceeded/);
  });

  it("logs the restart and clears the alert once QuestDB answers", async () => {
    const h = makeHarness();
    h.set(OOM);
    await checks(h, 2);
    assert.match(
      h.logs[1],
      /^QuestDB watchdog: restarted the QuestDB container after it was out of memory; it answers again after 12 s\.$/,
    );
    assert.equal(h.recovered, 1);
    assert.equal(h.notes.length, 2);
    assert.equal(h.notes[1].state, "normal");
    // The record of what happened stays on the notification.
    assert.match(
      h.notes[1].message,
      /^QuestDB was restarted at 2026-11-01T20:01:00\.000Z after it was out of memory \(global RSS/,
    );
    assert.equal(h.watchdog.alertOutstanding, false);
  });

  it("keeps the alert when the restart fails", async () => {
    const h = makeHarness();
    h.set(DOWN);
    h.restartWith(new Error("runtime gone"));
    await checks(h, 3);
    assert.match(
      h.logs[1],
      /restarting the QuestDB container failed: runtime gone/,
    );
    assert.match(h.statuses[1], /runtime gone/);
    assert.equal(h.notes.at(-1)?.state, "alert");
    assert.equal(h.recovered, 0);
    assert.equal(h.watchdog.alertOutstanding, true);
  });

  it("keeps the alert when QuestDB does not answer after the restart, and clears it once it does", async () => {
    const h = makeHarness();
    h.set(DOWN);
    h.restartWith({ state: "not-answering" });
    await checks(h, 3);
    assert.match(h.logs[1], /does not answer yet \(12 s\)/);
    assert.equal(h.notes.at(-1)?.state, "alert");
    h.set(OK);
    await checks(h, 1);
    assert.equal(h.notes.at(-1)?.state, "normal");
    assert.equal(h.notes.at(-1)?.message, "QuestDB answers again.");
    assert.equal(h.recovered, 1);
  });

  it("waits the minimum gap before restarting again", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 3);
    assert.equal(h.restarts, 1);
    await checks(h, 6);
    assert.equal(h.restarts, 1, "held back within 15 minutes");
    const heldBack = h.logs.filter((l) => /held back/.test(l));
    assert.equal(heldBack.length, 1, "a long incident is reported once");
    assert.match(heldBack[0], /restarted 1 time\(s\) in the last 6 h/);
    await checks(h, 10);
    assert.equal(h.restarts, 2);
  });

  it("allows at most three restarts in six hours", async () => {
    const h = makeHarness();
    h.set(DOWN);
    // Three restarts, each after the minimum gap.
    for (let i = 0; i < 3; i++) {
      await checks(h, 3);
      h.advance(16 * 60_000);
    }
    assert.equal(h.restarts, 3);
    await checks(h, 60);
    assert.equal(h.restarts, 3, "the fourth waits for the window");
    assert.match(
      h.logs.find((l) => /held back/.test(l)) ?? "",
      /held back until 2026-11-02T02:02:00\.000Z/,
    );
    h.advance(6 * 3_600_000);
    await checks(h, 3);
    assert.equal(h.restarts, 4);
  });

  it("clears a held-back alert when QuestDB recovers by itself", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 3);
    await checks(h, 3);
    assert.equal(h.notes.at(-1)?.state, "alert");
    h.set(OK);
    await checks(h, 1);
    assert.equal(h.notes.at(-1)?.message, "QuestDB answers again.");
    // The next incident is reported again rather than staying quiet.
    h.set(DOWN);
    await checks(h, 3);
    assert.equal(h.logs.filter((l) => /held back/.test(l)).length, 2);
  });

  it("reports nothing about a restart that a stop overtook", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 2);
    let release!: () => void;
    h.holdRestart(new Promise<void>((resolve) => (release = resolve)));
    const pending = h.watchdog.check();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(h.restarts, 1);
    h.watchdog.stop();
    release();
    await pending;
    assert.equal(h.logs.length, 1, "only the pre-restart line");
    assert.equal(h.recovered, 0);
    // The teardown clears it: see stopWatchdog in index.ts.
    assert.equal(h.watchdog.alertOutstanding, true);
  });

  it("never overlaps checks", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 2);
    let release!: () => void;
    h.holdRestart(new Promise<void>((resolve) => (release = resolve)));
    const first = h.watchdog.check();
    await new Promise((resolve) => setImmediate(resolve));
    await h.watchdog.check();
    release();
    await first;
    assert.equal(h.restarts, 1);
    assert.equal(h.exclusiveCalls, 3);
  });

  it("keeps the alert, and the limits free, when a restart is called off", async () => {
    const h = makeHarness();
    h.set(DOWN);
    h.restartWith({ state: "called-off", why: "the container is stopped" });
    await checks(h, 3);
    assert.equal(h.restarts, 1);
    assert.match(
      h.logs.at(-1) ?? "",
      /restart called off: the container is stopped\.$/,
    );
    // QuestDB is still failing: the alert stays, with what happened.
    assert.equal(h.notes.at(-1)?.state, "alert");
    assert.match(
      h.notes.at(-1)?.message ?? "",
      /called off: the container is stopped/,
    );
    assert.equal(h.watchdog.alertOutstanding, true);
    assert.equal(h.statuses.length, 1, "no status beyond the first");
    // Nothing was restarted, so the next restart is not held back.
    h.restartWith({ state: "answering" });
    await checks(h, 3);
    assert.equal(h.restarts, 2);
    assert.equal(h.logs.filter((l) => /held back/.test(l)).length, 0);
    // And the alert clears with the restart that did help.
    assert.equal(h.notes.at(-1)?.state, "normal");
    assert.equal(h.recovered, 1);
  });

  it("reports a second hold-back after a restart in between", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 3); // restart 1
    await checks(h, 3); // held back
    await checks(h, 13); // gap over at 20:17: restart 2
    assert.equal(h.restarts, 2);
    await checks(h, 3); // held back again, with no answer in between
    assert.equal(h.logs.filter((l) => /held back/.test(l)).length, 2);
  });

  it("does not probe when stopped while waiting for the lock", async () => {
    const h = makeHarness();
    h.set(DOWN);
    let probes = 0;
    h.watchdog = new QuestdbWatchdog(
      {
        exclusive: async (fn) => {
          await gate;
          return fn();
        },
        probe: async () => {
          probes++;
          return DOWN;
        },
        recentErrors: async () => [],
        restart: async () => ({ state: "answering" }),
        log: () => {},
        status: () => {},
        recovered: () => {},
        notify: () => {},
        debug: () => {},
      },
      {},
    );
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const pending = h.watchdog.check();
    h.watchdog.stop();
    release();
    await pending;
    assert.equal(probes, 0);
  });

  it("lets a restart older than the window count no more", async () => {
    const h = makeHarness({ maxRestarts: 1, minRestartGapMs: 0 });
    h.set(DOWN);
    await checks(h, 3);
    assert.equal(h.restarts, 1);
    // The restart ran at the third check, 2 min in, and took 12 s; checks()
    // then moved on a minute. The next round's third check lands exactly one
    // window after that restart, which then no longer counts.
    h.advance(6 * 3_600_000 - 192_000);
    await checks(h, 3);
    assert.equal(h.restarts, 2);
  });

  it("does nothing more once stopped during the probe's error fetch", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 2);
    let release!: () => void;
    h.holdErrors(new Promise<void>((resolve) => (release = resolve)));
    const pending = h.watchdog.check();
    await new Promise((resolve) => setImmediate(resolve));
    h.watchdog.stop();
    release();
    await pending;
    assert.equal(h.restarts, 0);
    assert.deepEqual(h.logs, []);
    assert.deepEqual(h.notes, []);
  });

  it("reports no failure for a restart a stop overtook", async () => {
    const h = makeHarness();
    h.set(DOWN);
    await checks(h, 2);
    let release!: () => void;
    h.holdRestart(new Promise<void>((resolve) => (release = resolve)));
    h.restartWith(new Error("plugin stopped"));
    const pending = h.watchdog.check();
    await new Promise((resolve) => setImmediate(resolve));
    h.watchdog.stop();
    release();
    await pending;
    assert.equal(h.logs.length, 1, "only the pre-restart line");
    assert.equal(h.statuses.length, 1);
  });

  it("starts the count over after a stop, and checks again after a start", async () => {
    const h = makeHarness({ intervalMs: 1_000_000_000 });
    h.set(DOWN);
    await checks(h, 2);
    h.watchdog.stop();
    h.watchdog.start();
    try {
      await checks(h, 2);
      assert.equal(h.restarts, 0, "the two checks before the stop are gone");
      await checks(h, 1);
      assert.equal(h.restarts, 1, "and the checks after the start count");
    } finally {
      h.watchdog.stop();
    }
  });

  it("acts on no verdict that arrives after a stop", async () => {
    const h = makeHarness();
    h.set(DOWN);
    h.restartWith({ state: "not-answering" });
    await checks(h, 3);
    assert.equal(h.watchdog.alertOutstanding, true);
    const notes = h.notes.length;
    let release!: () => void;
    h.holdProbe(new Promise<void>((resolve) => (release = resolve)));
    h.set(OK);
    const pending = h.watchdog.check();
    h.watchdog.stop();
    release();
    await pending;
    // The teardown clears the alert with its own words; a recovery reported
    // after the stop would race it.
    assert.equal(h.notes.length, notes);
    assert.equal(h.recovered, 0);
    assert.equal(h.watchdog.alertOutstanding, true);
  });

  it("survives a probe that throws", async () => {
    const h = makeHarness();
    h.watchdog = new QuestdbWatchdog(
      {
        exclusive: (fn) => fn(),
        probe: async () => {
          throw new Error("runtime busy");
        },
        recentErrors: async () => [],
        restart: async () => ({ state: "answering" }),
        log: (msg) => h.logs.push(msg),
        status: () => {},
        recovered: () => {},
        notify: () => {},
        debug: () => {},
      },
      {},
    );
    await h.watchdog.check();
    await h.watchdog.check();
    assert.deepEqual(h.logs, []);
  });
});

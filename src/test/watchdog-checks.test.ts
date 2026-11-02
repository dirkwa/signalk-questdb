import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { WatchdogChecks } from "../watchdog-checks.js";
import type { QuestDBResult } from "../query-client.js";

const LIMIT_BODY =
  '{"error":"global RSS memory limit exceeded [usage=724566016, ' +
  'RSS_MEM_LIMIT=724775670, size=262144, memoryTag=36]"}';
const LIMIT = 724775670;

function memoryResult(bytes: number): QuestDBResult {
  return {
    columns: [{ name: "sum", type: "LONG" }],
    dataset: [[bytes]],
    count: 1,
    timestamp: -1,
  };
}

// A fake container manager and QuestDB. The clock only moves when a test
// moves it, or by `healthCost` per health check (so a restart's wait for an
// answer ends without real time passing).
function makeHarness() {
  const h = {
    clock: 0,
    healthCost: 0,
    state: "running" as string,
    healthy: true,
    memory: (40 * 1048576) as number | Error,
    calls: [] as string[],
    epoch: 0,
    ended: false,
    busy: null as string | null,
    stopImpl: null as (() => Promise<void>) | null,
    startImpl: null as (() => Promise<void>) | null,
    reconnects: 0,
    stops: { pending: 0 },
    onGetState: null as (() => Promise<void>) | null,
    restartPlugin: () => {},
    onHealth: null as (() => void) | null,
    // An error answer to the health check, as QueryClient reports one.
    healthError: null as string | null,
    checks: null as unknown as WatchdogChecks,
  };
  const client = {
    health: async () => {
      h.calls.push("health");
      h.clock += h.healthCost;
      h.onHealth?.();
      if (h.healthy) {
        h.checks.noteResponse();
        return { ok: true };
      }
      if (h.healthError !== null) {
        h.checks.noteResponse();
        h.checks.noteErrorResponse(h.healthError);
        return { ok: false, error: h.healthError };
      }
      return { ok: false };
    },
    exec: async (sql: string) => {
      h.calls.push(`exec ${sql.slice(0, 30)}`);
      if (h.memory instanceof Error) throw h.memory;
      return memoryResult(h.memory);
    },
  };
  const deps = {
    containers: {
      getState: async () => {
        h.calls.push("getState");
        // Read first, answered after the hook: a state read in flight.
        const state = h.state;
        await h.onGetState?.();
        return state as "running";
      },
      stop: async () => {
        h.calls.push("stop");
        if (h.stopImpl) return h.stopImpl();
        h.state = "stopped";
      },
      start: async () => {
        h.calls.push("start");
        if (h.startImpl) return h.startImpl();
        h.state = "running";
      },
    },
    name: "signalk-questdb",
    client: () => client,
    busy: () => h.busy,
    epoch: () => h.epoch,
    bumpEpoch: () => {
      h.epoch++;
    },
    ended: () => h.ended,
    endpoint: () => "questdb:9000",
    reconnectWriter: () => {
      h.reconnects++;
    },
    stops: h.stops,
    now: () => h.clock,
    timing: { pollMs: 1, answerMs: 10_000, containerCallMs: 50 },
  };
  h.checks = new WatchdogChecks(deps);
  // A plugin restart: a new instance over the same container and stops.
  h.restartPlugin = () => {
    h.checks = new WatchdogChecks(deps);
  };
  return h;
}

describe("WatchdogChecks.probe", () => {
  it("finds a healthy QuestDB ok", async () => {
    const h = makeHarness();
    assert.deepEqual(await h.checks.probe(), { state: "ok" });
  });

  it("skips while the plugin is busy with QuestDB", async () => {
    const h = makeHarness();
    h.busy = "an export is running";
    h.healthy = false;
    h.clock = 60 * 60_000;
    assert.deepEqual(await h.checks.probe(), {
      state: "skip",
      why: "an export is running",
    });
    assert.deepEqual(h.calls, [], "not even a health check");
  });

  it("counts a busy QuestDB as busy until it has answered nothing for 5 min", async () => {
    const h = makeHarness();
    h.healthy = false;
    h.clock = 4 * 60_000;
    assert.equal((await h.checks.probe()).state, "skip");
    h.clock = 5 * 60_000;
    assert.deepEqual(await h.checks.probe(), {
      state: "unresponsive",
      message: "no answer at questdb:9000 for 5 min",
    });
  });

  it("starts the silence over at any answer, error answers included", async () => {
    const h = makeHarness();
    h.healthy = false;
    h.clock = 4 * 60_000;
    h.checks.noteResponse();
    h.clock = 8 * 60_000;
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("leaves a container someone else stopped alone", async () => {
    const h = makeHarness();
    h.state = "stopped";
    h.clock = 60 * 60_000;
    assert.deepEqual(await h.checks.probe(), {
      state: "skip",
      why: "the container is stopped",
    });
  });

  it("gives a container an update just recreated its full grace", async () => {
    const h = makeHarness();
    // The old container hit the limit; the update is the operator's remedy.
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.healthy = false;
    h.clock = 60_000;
    h.epoch++;
    assert.equal((await h.checks.probe()).state, "skip");
    h.clock += 4 * 60_000;
    assert.equal(
      (await h.checks.probe()).state,
      "skip",
      "silence counts from the recreate, and the old limit is no evidence",
    );
  });

  it("finds out of memory while native memory is held at the limit", async () => {
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.memory = 724566016;
    const verdict = await h.checks.probe();
    assert.equal(verdict.state, "out-of-memory");
    assert.match(
      verdict.state === "out-of-memory" ? verdict.message : "",
      /^global RSS memory limit exceeded \[usage=724566016.*\]; native memory 691 of 691 MiB$/,
    );
  });

  it("does not count a limit hit whose memory was freed", async () => {
    // A single large query, or a client that keeps sending one: each frees
    // what it held when it ends.
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.memory = 42 * 1048576;
    assert.deepEqual(await h.checks.probe(), { state: "ok" });
  });

  it("forgets a limit hit after 10 min", async () => {
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.memory = LIMIT;
    h.clock = 10 * 60_000 + 1;
    assert.deepEqual(await h.checks.probe(), { state: "ok" });
  });

  it("does not count a limit it cannot confirm", async () => {
    // memory_metrics() missing on an older QuestDB, or the read failed.
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.memory = new Error("unknown function memory_metrics");
    assert.deepEqual(await h.checks.probe(), { state: "ok" });
  });

  it("keeps out of memory when QuestDB then stops answering", async () => {
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.healthy = false;
    h.clock = 150_000;
    const verdict = await h.checks.probe();
    assert.equal(verdict.state, "out-of-memory");
    assert.match(
      verdict.state === "out-of-memory" ? verdict.message : "",
      /; QuestDB no longer answers$/,
    );
  });

  it("counts a limit hit and a busy worker as busy", async () => {
    // A client resending a read that hits the limit holds the worker for
    // as long as each read runs; that is no leak.
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.healthy = false;
    h.clock = 120_000;
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("counts a health check answered with the limit as held", async () => {
    // QuestDB still answers, but fails even SELECT 1 at the cap: that is no
    // busy worker.
    const h = makeHarness();
    h.clock = 60_000;
    h.healthy = false;
    h.healthError = LIMIT_BODY;
    const verdict = await h.checks.probe();
    assert.equal(verdict.state, "out-of-memory");
    assert.match(
      verdict.state === "out-of-memory" ? verdict.message : "",
      /; the health check hit it too$/,
    );
  });

  it("does not blame the health check for another request's limit", async () => {
    // The health check times out behind a busy worker while a client's
    // resent read gets the limit answer: the observers see that answer, but
    // it is not the health check's.
    const h = makeHarness();
    h.healthy = false;
    h.onHealth = () => {
      h.checks.noteResponse();
      h.checks.noteErrorResponse(LIMIT_BODY);
    };
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("counts a memory read that hits the limit as held", async () => {
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.memory = new Error(
      `QuestDB query failed (400): ${LIMIT_BODY.replace("usage=724566016", "usage=724700000")}`,
    );
    const verdict = await h.checks.probe();
    assert.equal(verdict.state, "out-of-memory");
    assert.match(
      verdict.state === "out-of-memory" ? verdict.message : "",
      /; reading native memory hit the limit too$/,
    );
  });

  it("learns the limit from a suspended table", async () => {
    const h = makeHarness();
    h.checks.noteSuspended([
      { name: "signalk", errorMessage: "" },
      {
        name: "signalk_str",
        errorMessage:
          "global RSS memory limit exceeded [usage=1, RSS_MEM_LIMIT=1000]",
      },
    ]);
    h.memory = 999;
    const verdict = await h.checks.probe();
    assert.equal(verdict.state, "out-of-memory");
    assert.match(
      verdict.state === "out-of-memory" ? verdict.message : "",
      /^signalk_str: global RSS memory limit exceeded/,
    );
  });

  it("ignores the per-query limit", async () => {
    const h = makeHarness();
    h.checks.noteErrorResponse(
      '{"error":"query memory limit exceeded [workload=HTTP, limit=1]"}',
    );
    h.memory = LIMIT;
    assert.deepEqual(await h.checks.probe(), { state: "ok" });
  });
});

describe("WatchdogChecks.restart", () => {
  it("stops, starts and waits for an answer", async () => {
    const h = makeHarness();
    h.healthCost = 1000;
    assert.deepEqual(await h.checks.restart(), { state: "answering" });
    assert.deepEqual(
      h.calls.filter((c) => c !== "health"),
      ["getState", "stop", "start"],
    );
    assert.equal(h.epoch, 1, "a WAL skip must not span the restart");
    assert.equal(h.reconnects, 1);
  });

  it("is not answering when QuestDB does not answer in time", async () => {
    const h = makeHarness();
    h.healthy = false;
    h.healthCost = 1000;
    assert.deepEqual(await h.checks.restart(), { state: "not-answering" });
    assert.equal(h.reconnects, 0);
  });

  it("calls off a restart of a container someone else stopped meanwhile", async () => {
    const h = makeHarness();
    h.state = "stopped";
    assert.deepEqual(await h.checks.restart(), {
      state: "called-off",
      why: "the container is stopped",
    });
    assert.ok(!h.calls.includes("start"));
    assert.equal(h.epoch, 0);
  });

  it("calls off a restart once the plugin is stopping", async () => {
    const h = makeHarness();
    h.ended = true;
    assert.deepEqual(await h.checks.restart(), {
      state: "called-off",
      why: "the plugin is stopping",
    });
    assert.deepEqual(h.calls, []);
  });

  it("leaves the container stopped when a teardown lands during the start", async () => {
    const h = makeHarness();
    h.startImpl = async () => {
      h.state = "running";
      h.ended = true;
    };
    assert.equal((await h.checks.restart()).state, "called-off");
    assert.deepEqual(h.calls, ["getState", "stop", "start", "stop"]);
  });

  it("starts again a container it stopped and could not start", async () => {
    const h = makeHarness();
    h.startImpl = async () => {
      throw new Error("port in use");
    };
    await assert.rejects(h.checks.restart(), /port in use/);
    assert.equal(h.state, "stopped");
    // Its own stop is not someone else's to respect.
    h.clock = 60 * 60_000;
    assert.equal((await h.checks.probe()).state, "unresponsive");
    h.startImpl = null;
    h.healthCost = 1000;
    assert.deepEqual(await h.checks.restart(), { state: "answering" });
    assert.ok(
      h.calls.lastIndexOf("start") > h.calls.lastIndexOf("stop"),
      "started without another stop",
    );
  });

  it("hands back a container it could not start once it runs again", async () => {
    const h = makeHarness();
    h.startImpl = async () => {
      throw new Error("port in use");
    };
    await assert.rejects(h.checks.restart());
    h.clock = 60 * 60_000;
    assert.equal(
      (await h.checks.probe()).state,
      "unresponsive",
      "its own stop is not someone else's to respect",
    );
    // Started by hand, later stopped by hand: that stop is the operator's.
    h.state = "running";
    await h.checks.probe();
    h.state = "stopped";
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("hands back a container only on a read that says running", async () => {
    // A read that tells nothing, such as one past its deadline, must not
    // hand back a container the watchdog's stop keeps down.
    const h = makeHarness();
    h.startImpl = async () => {
      throw new Error("port in use");
    };
    await assert.rejects(h.checks.restart());
    h.state = "no-runtime";
    assert.equal((await h.checks.probe()).state, "skip");
    h.state = "stopped";
    h.clock = 60 * 60_000;
    assert.equal((await h.checks.probe()).state, "unresponsive");
  });

  it("calls off a restart once a teardown landed during the state read", async () => {
    const h = makeHarness();
    h.onGetState = async () => {
      h.onGetState = null;
      h.ended = true;
    };
    assert.deepEqual(await h.checks.restart(), {
      state: "called-off",
      why: "the plugin is stopping",
    });
    assert.ok(!h.calls.includes("stop"));
  });

  it("keeps a container its own when an update fails to start it", async () => {
    // The update bumps the epoch before its ensureRunning, which then fails:
    // the container is still down because of the watchdog's stop.
    const h = makeHarness();
    h.startImpl = async () => {
      throw new Error("port in use");
    };
    await assert.rejects(h.checks.restart());
    h.epoch++;
    assert.equal((await h.checks.probe()).state, "skip");
    h.clock = 60 * 60_000;
    assert.equal((await h.checks.probe()).state, "unresponsive");
  });

  it("hands back a container whose stop failed once it runs", async () => {
    // The stop never took the container down: nothing of the watchdog's is
    // keeping it down, so a later stop is someone else's.
    const h = makeHarness();
    h.stopImpl = async () => {
      throw new Error("runtime busy");
    };
    await assert.rejects(h.checks.restart(), /runtime busy/);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(h.stops.pending, 0);
    await h.checks.probe();
    h.state = "stopped";
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("does not hand back a container on a read the late stop overtook", async () => {
    // The state read says running, but the watchdog's late stop ends while
    // the read is in flight: the container is down because of it.
    const h = makeHarness();
    let finish!: () => void;
    h.stopImpl = () =>
      new Promise<void>((resolve) => {
        finish = () => {
          h.state = "stopped";
          resolve();
        };
      });
    await assert.rejects(h.checks.restart(), /did not finish/);
    h.onGetState = async () => {
      h.onGetState = null;
      finish();
      await new Promise((resolve) => setImmediate(resolve));
    };
    await h.checks.probe();
    h.clock = 60 * 60_000;
    assert.equal((await h.checks.probe()).state, "unresponsive");
  });

  it("counts the stop after a teardown for the next plugin start", async () => {
    const h = makeHarness();
    let finish!: () => void;
    h.startImpl = async () => {
      h.state = "running";
      h.ended = true;
      // The teardown's own stop then hangs.
      h.stopImpl = () =>
        new Promise<void>((resolve) => {
          finish = () => {
            h.state = "stopped";
            resolve();
          };
        });
    };
    assert.equal((await h.checks.restart()).state, "called-off");
    h.ended = false;
    h.stopImpl = null;
    h.startImpl = null;
    h.restartPlugin();
    assert.equal((await h.checks.probe()).state, "ok");
    finish();
    await new Promise((resolve) => setImmediate(resolve));
    h.clock = 60 * 60_000;
    assert.equal((await h.checks.probe()).state, "unresponsive");
  });

  it("keeps a stop of the previous plugin start its own", async () => {
    const h = makeHarness();
    let finish!: () => void;
    h.stopImpl = () =>
      new Promise<void>((resolve) => {
        finish = () => {
          h.state = "stopped";
          resolve();
        };
      });
    await assert.rejects(h.checks.restart(), /did not finish/);
    h.restartPlugin();
    assert.equal((await h.checks.probe()).state, "ok", "still running");
    finish();
    await new Promise((resolve) => setImmediate(resolve));
    h.clock = 60 * 60_000;
    assert.equal((await h.checks.probe()).state, "unresponsive");
  });

  it("hands a container back once an update recreated it", async () => {
    const h = makeHarness();
    h.startImpl = async () => {
      throw new Error("port in use");
    };
    await assert.rejects(h.checks.restart());
    h.state = "running";
    h.epoch++;
    assert.equal((await h.checks.probe()).state, "skip");
    h.state = "stopped";
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("gives up on a container call that never finishes", async () => {
    const h = makeHarness();
    h.stopImpl = () => new Promise<void>(() => {});
    await assert.rejects(
      h.checks.restart(),
      /stopping the container did not finish within 0 s/,
    );
    assert.ok(!h.calls.includes("start"));
  });

  it("starts a container whose stop finished after its deadline", async () => {
    const h = makeHarness();
    let finish!: () => void;
    h.stopImpl = () =>
      new Promise<void>((resolve) => {
        finish = () => {
          h.state = "stopped";
          resolve();
        };
      });
    await assert.rejects(h.checks.restart(), /did not finish/);
    finish();
    h.clock = 60 * 60_000;
    assert.deepEqual(await h.checks.probe(), {
      state: "unresponsive",
      message: "the container is stopped since the watchdog stopped it",
    });
  });

  it("keeps a stop that is still running its own", async () => {
    // The container reads as running until a late stop ends; a probe in
    // that window must not hand the stop to someone else.
    const h = makeHarness();
    let finish!: () => void;
    h.stopImpl = () =>
      new Promise<void>((resolve) => {
        finish = () => {
          h.state = "stopped";
          resolve();
        };
      });
    await assert.rejects(h.checks.restart(), /did not finish/);
    assert.equal(h.state, "running");
    await h.checks.probe();
    finish();
    await new Promise((resolve) => setImmediate(resolve));
    h.clock = 60 * 60_000;
    assert.equal((await h.checks.probe()).state, "unresponsive");
  });

  it("keeps a late stop its own across a later restart", async () => {
    // The first restart's stop hangs; a later restart stops and starts the
    // container; then the first stop ends after all and stops the new one.
    const h = makeHarness();
    let finish!: () => void;
    h.stopImpl = () =>
      new Promise<void>((resolve) => {
        finish = () => {
          h.state = "stopped";
          resolve();
        };
      });
    await assert.rejects(h.checks.restart(), /did not finish/);
    h.stopImpl = null;
    h.healthCost = 1000;
    assert.equal((await h.checks.restart()).state, "answering");
    finish();
    await new Promise((resolve) => setImmediate(resolve));
    h.clock += 60 * 60_000;
    assert.deepEqual(await h.checks.probe(), {
      state: "unresponsive",
      message: "the container is stopped since the watchdog stopped it",
    });
  });

  it("starts a container whose stop ended long after its deadline", async () => {
    const h = makeHarness();
    let finish!: () => void;
    h.stopImpl = () =>
      new Promise<void>((resolve) => {
        finish = () => {
          h.state = "stopped";
          resolve();
        };
      });
    await assert.rejects(h.checks.restart(), /did not finish/);
    // Long past its deadline the container still runs…
    assert.equal((await h.checks.probe()).state, "ok");
    // …until it ends after all: the container is down because of it.
    finish();
    await new Promise((resolve) => setImmediate(resolve));
    h.clock = 60 * 60_000;
    assert.deepEqual(await h.checks.probe(), {
      state: "unresponsive",
      message: "the container is stopped since the watchdog stopped it",
    });
  });

  it("does not start the container once a teardown landed during the stop", async () => {
    const h = makeHarness();
    h.stopImpl = async () => {
      h.state = "stopped";
      h.ended = true;
    };
    assert.equal((await h.checks.restart()).state, "called-off");
    assert.deepEqual(h.calls, ["getState", "stop"]);
  });

  it("respects an operator's stop after a restart that worked", async () => {
    const h = makeHarness();
    h.healthCost = 1000;
    assert.equal((await h.checks.restart()).state, "answering");
    h.state = "stopped";
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("gives a restarted container that does not answer yet its grace", async () => {
    const h = makeHarness();
    h.clock = 60 * 60_000;
    h.healthy = false;
    h.healthCost = 1000;
    assert.equal((await h.checks.restart()).state, "not-answering");
    h.clock += 4 * 60_000;
    assert.equal((await h.checks.probe()).state, "skip");
  });

  it("stops waiting for an answer once the plugin is stopping", async () => {
    const h = makeHarness();
    h.healthy = false;
    // Without the check, the wait would run to its deadline instead.
    h.healthCost = 1000;
    h.onHealth = () => {
      h.ended = true;
    };
    assert.deepEqual(await h.checks.restart(), {
      state: "called-off",
      why: "the plugin is stopping",
    });
    assert.equal(h.calls.filter((c) => c === "health").length, 1);
  });

  it("gives the restarted container its full grace", async () => {
    const h = makeHarness();
    h.checks.noteErrorResponse(LIMIT_BODY);
    h.healthCost = 1000;
    await h.checks.restart();
    // The limit the old instance hit is no evidence against the new one,
    // and its silence starts now.
    h.memory = LIMIT;
    assert.deepEqual(await h.checks.probe(), { state: "ok" });
    h.healthy = false;
    h.clock += 4 * 60_000;
    assert.equal((await h.checks.probe()).state, "skip");
  });
});

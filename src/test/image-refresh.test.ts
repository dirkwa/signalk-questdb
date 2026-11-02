import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { refreshImage, type ImageRefreshDeps } from "../image-refresh.js";

const REF = "questdb/questdb:latest";

type Leaves = "new" | "old" | "missing" | "stopped";

// A fake container manager whose pulled and running image ids a test sets,
// and a lifecycle lock whose holder the calls record. A recreate moves the
// running container to the pulled image unless the test says what it leaves
// instead; a restore starts the container on what the tag names.
function makeHarness() {
  const h = {
    calls: [] as string[],
    pulledId: "sha256:new" as string | null,
    runningId: "sha256:old" as string | null,
    state: "running" as string,
    pullError: null as Error | null,
    recreateError: null as Error | null,
    restoreError: null as Error | null,
    // Fails the recreate before the container is handed over.
    failBeforeRecreate: false,
    // A wedge or a held-back pull leaves "old"; a half-done recreate leaves
    // the container "stopped" on the old image, or "missing".
    leaves: "new" as Leaves,
    current: true,
    tag: "latest",
    blocked: null as string | null,
    errors: [] as string[],
    down: [] as string[],
    listError: null as Error | null,
    // How many reads of the container list fail before they work.
    listFailures: 0,
    digestError: null as Error | null,
    stateError: null as Error | null,
    onPull: null as (() => void) | null,
    onRecreate: null as (() => void) | null,
    onRestore: null as (() => void) | null,
    abort: new AbortController(),
  };
  const deps: ImageRefreshDeps = {
    containers: {
      pullImage: async (ref) => {
        h.calls.push(`pull ${ref}`);
        h.onPull?.();
        if (h.pullError) throw h.pullError;
      },
      getImageDigest: async (ref) => {
        if (h.digestError) throw h.digestError;
        if (ref === REF) return h.pulledId;
        return ref === "sk-signalk-questdb" && h.state !== "missing"
          ? h.runningId
          : null;
      },
      listContainers: async () => {
        if (h.listError) throw h.listError;
        if (h.listFailures > 0) {
          h.listFailures--;
          throw new Error("runtime busy");
        }
        return h.state === "missing"
          ? []
          : [
              {
                name: "sk-signalk-questdb",
                unprefixedName: "signalk-questdb",
                image: REF,
                state: h.state === "running" ? "running" : "stopped",
              },
            ];
      },
      getState: async () => {
        if (h.stateError) throw h.stateError;
        return h.state as "running";
      },
    },
    name: "signalk-questdb",
    image: "questdb/questdb",
    withLock: async (fn) => {
      h.calls.push("lock");
      try {
        return await fn();
      } finally {
        h.calls.push("unlock");
      }
    },
    current: () => h.current,
    configuredTag: () => h.tag,
    blocked: () => h.blocked,
    recreate: async (tag) => {
      if (h.failBeforeRecreate) throw new Error("no mount");
      h.calls.push(`recreate ${tag}`);
      if (h.leaves === "new") h.runningId = h.pulledId;
      else if (h.leaves !== "old") h.state = h.leaves;
      h.onRecreate?.();
      if (h.recreateError) throw h.recreateError;
    },
    restore: async (tag) => {
      h.calls.push(`restore ${tag}`);
      h.onRestore?.();
      if (h.restoreError) throw h.restoreError;
      // A missing container is created on what the tag names now.
      if (h.state === "missing") h.runningId = h.pulledId;
      h.state = "running";
    },
    debug: () => {},
    error: (msg) => h.errors.push(msg),
    reportDown: (msg) => h.down.push(msg),
  };
  return { h, run: () => refreshImage(deps, "latest", h.abort.signal) };
}

describe("refreshImage", () => {
  it("pulls outside the lock, then recreates under it", async () => {
    const { h, run } = makeHarness();
    assert.equal(await run(), true);
    assert.deepEqual(h.calls, [
      `pull ${REF}`,
      "lock",
      "recreate latest",
      "unlock",
    ]);
  });

  it("changes nothing where the pull brought the image QuestDB runs", async () => {
    // Docker Hub may carry a release later than GitHub announces it.
    const { h, run } = makeHarness();
    h.runningId = "sha256:new";
    assert.equal(await run(), false);
    assert.ok(!h.calls.includes("recreate latest"));
  });

  it("does not count a recreate where the old image is unknown", async () => {
    // The next start finds QuestDB current, or the pulled image running.
    const { h, run } = makeHarness();
    h.runningId = null;
    assert.equal(await run(), false);
    assert.ok(h.calls.includes("recreate latest"));
  });

  it("does not count a recreate that did not land where the old image was unknown", async () => {
    // A wedge after a first read that failed: the old image stays, and it
    // is not the one pulled.
    const { h, run } = makeHarness();
    h.listFailures = 1;
    h.leaves = "old";
    assert.equal(await run(), false);
    assert.ok(h.calls.includes("recreate latest"));
  });

  it("starts a container left stopped without an error", async () => {
    const { h, run } = makeHarness();
    h.leaves = "stopped";
    assert.equal(await run(), false);
    assert.ok(h.calls.includes("restore latest"));
    assert.equal(h.state, "running");
  });

  it("does not count a recreate it cannot tell apart", async () => {
    // Recorded, it would end the check without a change.
    const { h, run } = makeHarness();
    h.pulledId = null;
    h.runningId = null;
    assert.equal(await run(), false);
    assert.ok(h.calls.includes("recreate latest"));
  });

  it("does not count a recreate where the container list cannot be read", async () => {
    const { h, run } = makeHarness();
    h.listError = new Error("runtime busy");
    assert.equal(await run(), false);
    assert.ok(h.calls.includes("recreate latest"));
  });

  it("rejects without a recreate where the pulled image cannot be read", async () => {
    const { h, run } = makeHarness();
    h.digestError = new Error("inspect failed");
    await assert.rejects(run(), /inspect failed/);
    assert.ok(!h.calls.includes("recreate latest"));
  });

  it("leaves a tag the button changed since the check began", async () => {
    const { h, run } = makeHarness();
    // The button pinned a release while the pull ran.
    h.onPull = () => {
      h.tag = "9.4.3";
    };
    assert.equal(await run(), false);
    assert.ok(!h.calls.includes("recreate latest"));
  });

  it("does not recreate while something blocks it", async () => {
    const { h, run } = makeHarness();
    h.blocked = "the string table is being rebuilt";
    assert.equal(await run(), false);
    assert.ok(!h.calls.includes("recreate latest"));
  });

  it("leaves a container someone else stopped", async () => {
    const { h, run } = makeHarness();
    h.state = "stopped";
    assert.equal(await run(), false);
    assert.ok(!h.calls.includes("recreate latest"));
    assert.ok(!h.calls.includes("restore latest"));
  });

  it("does not pull for a start that has ended", async () => {
    const { h, run } = makeHarness();
    h.current = false;
    assert.equal(await run(), false);
    assert.deepEqual(h.calls, []);
  });

  it("does not recreate once the plugin stopped during the pull", async () => {
    const { h, run } = makeHarness();
    h.onPull = () => h.abort.abort();
    assert.equal(await run(), false);
    assert.ok(!h.calls.includes("recreate latest"));
  });

  it("rejects on a failed pull, which changed nothing", async () => {
    const { h, run } = makeHarness();
    h.pullError = new Error("registry unreachable");
    await assert.rejects(run(), /registry unreachable/);
    assert.ok(!h.calls.includes("lock"));
  });

  it("rejects on a failure before the recreate", async () => {
    const { h, run } = makeHarness();
    h.failBeforeRecreate = true;
    await assert.rejects(run(), /no mount/);
  });

  it("rejects on a failure before the recreate where the old image was unknown", async () => {
    // The container already ran the pulled image, which the first read
    // missed: it reads as the new image, but nothing tells it changed.
    const { h, run } = makeHarness();
    h.runningId = "sha256:new";
    h.listFailures = 1;
    h.failBeforeRecreate = true;
    await assert.rejects(run(), /no mount/);
  });

  it("counts a recreate that failed later as applied", async () => {
    // Unrecorded, it would recreate the container at every start.
    const { h, run } = makeHarness();
    h.recreateError = new Error("writer did not reconnect");
    assert.equal(await run(), true);
    assert.equal(h.errors.length, 1);
    assert.match(h.errors[0], /writer did not reconnect/);
    assert.deepEqual(h.down, []);
  });

  it("does not count a recreate that left the old image running", async () => {
    // A wedged container defers the recreate without an error.
    const { h, run } = makeHarness();
    h.leaves = "old";
    assert.equal(await run(), false);
  });

  it("rejects on a recreate that failed and left the old image running", async () => {
    // As where the runtime refused to remove the container.
    const { h, run } = makeHarness();
    h.leaves = "old";
    h.recreateError = new Error("remove refused");
    await assert.rejects(run(), /remove refused/);
    assert.deepEqual(h.errors, []);
  });

  it("starts a container a failed recreate left stopped, and rejects", async () => {
    // Stopped gracefully, then the remove failed: the old image stays.
    const { h, run } = makeHarness();
    h.leaves = "stopped";
    h.recreateError = new Error("device busy");
    await assert.rejects(run(), /device busy/);
    assert.ok(h.calls.includes("restore latest"));
    assert.equal(h.state, "running");
  });

  it("creates a container a failed recreate removed, and counts it", async () => {
    const { h, run } = makeHarness();
    h.leaves = "missing";
    h.recreateError = new Error("port in use");
    assert.equal(await run(), true);
    assert.ok(h.calls.includes("restore latest"));
    assert.equal(h.runningId, "sha256:new");
  });

  it("reports QuestDB down where starting it again fails too", async () => {
    const { h, run } = makeHarness();
    h.leaves = "missing";
    h.recreateError = new Error("port in use");
    h.restoreError = new Error("port still in use");
    await assert.rejects(run(), /port in use/);
    assert.equal(h.down.length, 1);
    assert.match(h.down[0], /port still in use/);
  });

  it("does not report QuestDB down to a start that came after a stop", async () => {
    // The stop lands while QuestDB is being started again.
    const { h, run } = makeHarness();
    h.leaves = "missing";
    h.recreateError = new Error("port in use");
    h.restoreError = new Error("plugin stopped while the update was running");
    h.onRestore = () => {
      h.current = false;
    };
    await assert.rejects(run(), /port in use/);
    assert.deepEqual(h.down, []);
  });

  it("does not count a failed recreate whose outcome cannot be read", async () => {
    // A runtime gone between the reads looks like no container at all.
    const { h, run } = makeHarness();
    h.recreateError = new Error("No container runtime available");
    h.onRecreate = () => {
      h.stateError = new Error("no runtime");
    };
    await assert.rejects(run(), /No container runtime/);
    assert.ok(!h.calls.includes("restore latest"));
  });

  it("stays quiet about a recreate cut short by a stop", async () => {
    const { h, run } = makeHarness();
    // The stop lands during the recreate, and the rest fails on it.
    h.onRecreate = () => {
      h.current = false;
    };
    h.recreateError = new Error("plugin stopped while the update was running");
    assert.equal(await run(), true);
    assert.deepEqual(h.errors, []);
  });

  it("leaves a container a stop interrupted to the stop", async () => {
    const { h, run } = makeHarness();
    h.leaves = "stopped";
    h.onRecreate = () => {
      h.current = false;
    };
    h.recreateError = new Error("plugin stopped while the update was running");
    await assert.rejects(run(), /plugin stopped/);
    assert.ok(!h.calls.includes("restore latest"));
  });
});

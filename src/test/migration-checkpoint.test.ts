import { test, describe } from "node:test";
import assert from "node:assert";
import { mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  CheckpointTracker,
  FileCheckpointStore,
  migrationIdentity,
  sameIdentity,
  type CheckpointStore,
  type MigrationCheckpoint,
  type WrittenTail,
} from "../migration-checkpoint.js";

const DAY = 24 * 60 * 60 * 1000;

const request = (over: Record<string, unknown> = {}) => ({
  url: "http://influx:8086",
  type: "influxdb1",
  bucket: "signalk",
  from: "2024-03-01T00:00:00Z",
  to: "2024-03-11T00:00:00Z",
  context: "self",
  ...over,
});

/** As it reads back from disk: JSON keeps no key whose value is undefined. */
const stored = (cp: MigrationCheckpoint): unknown =>
  JSON.parse(JSON.stringify(cp));

const checkpoint = (
  over: Partial<MigrationCheckpoint> = {},
): MigrationCheckpoint => ({
  version: 1,
  identity: migrationIdentity(request(), DAY),
  done: ["environment.depth.belowKeel"],
  current: {
    measurement: "navigation.position",
    windowStart: Date.parse("2024-03-04T00:00:00Z"),
  },
  progress: { read: 10, written: 9, skipped: 1 },
  updatedAt: "2024-06-01T00:00:00.000Z",
  ...over,
});

describe("import identity", () => {
  test("the same instant written two ways is the same import", () => {
    assert.ok(
      sameIdentity(
        migrationIdentity(request(), DAY),
        migrationIdentity(
          request({
            from: "2024-03-01T02:00:00+02:00",
            to: "2024-03-11T00:00:00.000Z",
          }),
          DAY,
        ),
      ),
    );
  });

  // Resuming a different range from this position would skip windows the new
  // range has never imported.
  test("a different range, source or context is a different import", () => {
    const base = migrationIdentity(request(), DAY);
    for (const over of [
      { to: "2024-03-12T00:00:00Z" },
      { from: "2024-02-01T00:00:00Z" },
      { bucket: "other" },
      { url: "http://elsewhere:8086" },
      { type: "influxdb2" },
      { context: "vessels.urn:mrn:imo:mmsi:123456789" },
      { sourceLabel: "second-import" },
    ]) {
      assert.ok(
        !sameIdentity(base, migrationIdentity(request(over), DAY)),
        JSON.stringify(over),
      );
    }
    assert.ok(!sameIdentity(base, migrationIdentity(request(), DAY / 2)));
  });

  test("a measurement selection is compared regardless of order", () => {
    assert.ok(
      sameIdentity(
        migrationIdentity(request({ measurements: ["b", "a"] }), DAY),
        migrationIdentity(request({ measurements: ["a", "b"] }), DAY),
      ),
    );
    assert.ok(
      !sameIdentity(
        migrationIdentity(request({ measurements: ["a"] }), DAY),
        migrationIdentity(request(), DAY),
      ),
    );
  });

  test("credentials are never part of it", () => {
    const identity = migrationIdentity(
      request({ auth: { token: "s3cret", password: "hunter2" } }),
      DAY,
    );
    const text = JSON.stringify(identity);
    assert.ok(!text.includes("s3cret") && !text.includes("hunter2"), text);
  });
});

describe("checkpoint file", () => {
  const withDir = async (fn: (dir: string) => Promise<void>) => {
    const dir = mkdtempSync(path.join(tmpdir(), "sk-questdb-cp-"));
    try {
      await fn(dir);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  };

  test("what is saved is what is loaded", () =>
    withDir(async (dir) => {
      const store = new FileCheckpointStore(path.join(dir, "cp.json"));
      assert.strictEqual(await store.load(), null);
      await store.save(checkpoint());
      assert.deepStrictEqual(await store.load(), stored(checkpoint()));
      // Renamed into place: nothing half-written is left beside it.
      assert.deepStrictEqual(readdirSync(dir), ["cp.json"]);
    }));

  test("a missing data directory is created", () =>
    withDir(async (dir) => {
      const store = new FileCheckpointStore(
        path.join(dir, "not", "yet", "cp.json"),
      );
      await store.save(checkpoint());
      assert.deepStrictEqual(await store.load(), stored(checkpoint()));
    }));

  test("clearing removes it, and clearing nothing is not an error", () =>
    withDir(async (dir) => {
      const store = new FileCheckpointStore(path.join(dir, "cp.json"));
      await store.clear();
      await store.save(checkpoint());
      await store.clear();
      assert.strictEqual(await store.load(), null);
      await new FileCheckpointStore(
        path.join(dir, "never", "made", "cp.json"),
      ).clear();
    }));

  // Starting over is always safe; resuming from a position that was never
  // real is not. Anything doubtful reads as "no checkpoint".
  test("a damaged or foreign file reads as no checkpoint", () =>
    withDir(async (dir) => {
      const file = path.join(dir, "cp.json");
      const store = new FileCheckpointStore(file);
      const bad: string[] = [
        "",
        "{not json",
        "null",
        "[]",
        JSON.stringify({ ...checkpoint(), version: 2 }),
        JSON.stringify({ ...checkpoint(), done: "all" }),
        JSON.stringify({ ...checkpoint(), done: [1, 2] }),
        JSON.stringify({ ...checkpoint(), identity: { url: "http://x" } }),
        JSON.stringify({
          ...checkpoint(),
          current: { measurement: "m", windowStart: "yesterday" },
        }),
        JSON.stringify({ ...checkpoint(), progress: { read: "many" } }),
      ];
      for (const text of bad) {
        writeFileSync(file, text);
        assert.strictEqual(await store.load(), null, text.slice(0, 60));
      }
    }));
});

describe("checkpoint tracker", () => {
  class MemoryStore implements CheckpointStore {
    saved: MigrationCheckpoint[] = [];
    async save(cp: MigrationCheckpoint) {
      this.saved.push(cp);
    }
    async clear() {
      this.saved = [];
    }
  }

  const position = (n: number) => () =>
    checkpoint({ progress: { read: n, written: n, skipped: 0 } });
  const TAIL: WrittenTail = {
    context: "self",
    source: "influxdb-import",
    numeric: {
      path: "environment.depth.belowKeel",
      tsNanos: 1_700_000_000_000_000_000n,
    },
  };

  const LAG = 60_000;
  const setup = () => {
    const store = new MemoryStore();
    // Settled keeps pace with enqueued unless a test holds it back.
    const writer = {
      droppedLineCount: 0,
      enqueuedLineCount: 0,
      held: 0,
      get settledLineCount() {
        return this.enqueuedLineCount - this.held;
      },
    };
    const clock = { t: 1_000_000 };
    const tracker = new CheckpointTracker(store, writer, LAG, () => clock.t);
    return { store, writer, clock, tracker };
  };

  test("a position is not persisted when it is reached", async () => {
    const { store, tracker } = setup();
    await tracker.offer(position(1), TAIL);
    assert.strictEqual(store.saved.length, 0);
  });

  // QuestDB does not fsync what it commits. After a power cut, what survives is
  // what the kernel had written back — so a position has to be older than that
  // before it is trusted.
  test("it is persisted only once it is old enough", async () => {
    const { store, writer, clock, tracker } = setup();
    writer.enqueuedLineCount = 100;
    await tracker.offer(position(1), TAIL);

    clock.t += LAG - 1;
    writer.enqueuedLineCount = 200;
    await tracker.offer(position(2), TAIL);
    assert.strictEqual(store.saved.length, 0);

    clock.t += 1;
    writer.enqueuedLineCount = 300;
    await tracker.offer(position(3), TAIL);
    // The OLD position: the one that has aged, not the one just reached.
    assert.deepStrictEqual(
      store.saved.map((cp) => cp.progress.read),
      [1],
    );
  });

  // The rows behind a position are first in the writer's buffer, then in the
  // socket. Until they have left, a crash loses them — and a checkpoint past
  // them would make the resumed run skip rows that were never stored.
  test("it is not persisted while its rows are still in the writer", async () => {
    const { store, writer, clock, tracker } = setup();
    writer.enqueuedLineCount = 1000;
    await tracker.offer(position(1), TAIL);

    clock.t += LAG * 10;
    writer.enqueuedLineCount = 1400;
    writer.held = 401; // line 1000 is among those not yet accepted
    await tracker.offer(position(2), TAIL);
    assert.strictEqual(store.saved.length, 0);

    writer.held = 400; // exactly the lines after it
    await tracker.offer(position(3), TAIL);
    assert.deepStrictEqual(
      store.saved.map((cp) => cp.progress.read),
      [1],
    );
  });

  // Without the counters there is no telling what has left the writer.
  // Guessing is how a checkpoint ends up ahead of the data, so: none at all.
  test("a writer that cannot say what has left it gets no checkpoints", async () => {
    for (const writer of [
      {},
      { enqueuedLineCount: 5000 },
      { settledLineCount: 5000 },
    ]) {
      const store = new MemoryStore();
      const clock = { t: 0 };
      const tracker = new CheckpointTracker(store, writer, LAG, () => clock.t);
      for (let i = 1; i < 6; i++) {
        clock.t += LAG;
        await tracker.offer(position(i), TAIL);
      }
      assert.strictEqual(store.saved.length, 0, JSON.stringify(writer));
    }
  });

  test("positions reached while one is waiting are passed over, unbuilt", async () => {
    const { writer, clock, tracker } = setup();
    await tracker.offer(position(1), TAIL);
    let built = 0;
    for (let i = 0; i < 1000; i++) {
      clock.t += 10;
      writer.enqueuedLineCount += 50;
      await tracker.offer(() => {
        built++;
        return checkpoint();
      }, TAIL);
    }
    assert.strictEqual(built, 0);
  });

  // The cap discards the OLDEST buffered lines, which may predate a position
  // taken since. None of those can be trusted; the one already on disk can.
  test("after the writer drops lines nothing more is persisted", async () => {
    const { store, writer, clock, tracker } = setup();
    await tracker.offer(position(1), TAIL);
    clock.t += LAG;
    writer.enqueuedLineCount = 500;
    await tracker.offer(position(2), TAIL);
    assert.strictEqual(store.saved.length, 1);

    writer.droppedLineCount = 7;
    for (let i = 3; i < 10; i++) {
      clock.t += LAG;
      writer.enqueuedLineCount += 500;
      await tracker.offer(position(i), TAIL);
    }
    assert.deepStrictEqual(
      store.saved.map((cp) => cp.progress.read),
      [1],
    );
  });

  // The writer's counters and the lag are local knowledge. Only QuestDB can
  // say the rows are committed and applied, and a stalled server can hold
  // them unread for longer than any lag.
  test("with a confirmation, a position is saved only once QuestDB has its rows", async () => {
    const { store, writer, clock } = setup();
    const asked: WrittenTail[] = [];
    let stored = false;
    const tracker = new CheckpointTracker(
      store,
      writer,
      LAG,
      () => clock.t,
      async (tail) => {
        asked.push(tail);
        return stored;
      },
    );
    await tracker.offer(position(1), TAIL);
    clock.t += LAG;
    writer.enqueuedLineCount = 500;
    await tracker.offer(position(2), TAIL);
    // Ripe and settled, but QuestDB says no: it waits, and keeps waiting.
    assert.strictEqual(store.saved.length, 0);
    assert.deepStrictEqual(asked, [TAIL]);
    await tracker.offer(position(3), TAIL);
    assert.strictEqual(store.saved.length, 0);
    assert.strictEqual(asked.length, 2);

    stored = true;
    await tracker.offer(position(4), TAIL);
    // The FIRST position, once its rows are there — not a later one.
    assert.deepStrictEqual(
      store.saved.map((cp) => cp.progress.read),
      [1],
    );
  });

  test("the confirmation is not asked before a position is ripe and settled", async () => {
    const { store, writer, clock } = setup();
    let asked = 0;
    const tracker = new CheckpointTracker(
      store,
      writer,
      LAG,
      () => clock.t,
      async () => {
        asked++;
        return true;
      },
    );
    writer.enqueuedLineCount = 100;
    await tracker.offer(position(1), TAIL);
    clock.t += LAG - 1;
    await tracker.offer(position(2), TAIL);
    clock.t += 1;
    writer.held = 1;
    await tracker.offer(position(3), TAIL);
    assert.strictEqual(asked, 0);
    assert.strictEqual(store.saved.length, 0);
  });

  test("a confirmation that fails to answer is a no", async () => {
    const { store, writer, clock } = setup();
    const tracker = new CheckpointTracker(
      store,
      writer,
      LAG,
      () => clock.t,
      async () => {
        throw new Error("ECONNREFUSED");
      },
    );
    await tracker.offer(position(1), TAIL);
    clock.t += LAG;
    await tracker.offer(position(2), TAIL);
    assert.strictEqual(store.saved.length, 0);
  });

  // The tail is what the position is confirmed against, so it must be the
  // one the position was offered with, not whatever was written since.
  test("a position is confirmed against the rows written before it", async () => {
    const { store, writer, clock } = setup();
    const asked: WrittenTail[] = [];
    const tracker = new CheckpointTracker(
      store,
      writer,
      LAG,
      () => clock.t,
      async (tail) => {
        asked.push(tail);
        return true;
      },
    );
    const first: WrittenTail = { ...TAIL, numeric: { path: "a", tsNanos: 1n } };
    await tracker.offer(position(1), first);
    first.numeric = { path: "b", tsNanos: 2n }; // mutated later, as the live tail is
    clock.t += LAG;
    await tracker.offer(position(2), {
      ...TAIL,
      numeric: { path: "c", tsNanos: 3n },
    });
    assert.deepStrictEqual(asked[0].numeric, { path: "a", tsNanos: 1n });
    assert.strictEqual(store.saved.length, 1);
  });

  // Drops from before this run say nothing about it.
  test("drops that predate the run do not stop it", async () => {
    const store = new MemoryStore();
    // 9000 enqueued over the writer's life: 40 dropped back then, the rest sent.
    const writer = {
      droppedLineCount: 40,
      enqueuedLineCount: 9000,
      settledLineCount: 8960,
    };
    const clock = { t: 0 };
    const tracker = new CheckpointTracker(store, writer, LAG, () => clock.t);
    await tracker.offer(position(1), TAIL);
    clock.t += LAG;
    // Nothing more enqueued: everything up to the position has still left.
    await tracker.offer(position(2), TAIL);
    assert.deepStrictEqual(
      store.saved.map((cp) => cp.progress.read),
      [1],
    );
  });
});

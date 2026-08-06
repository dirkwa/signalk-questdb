import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  WalMonitor,
  buildPendingSegmentsSQL,
  computeSkipPlan,
  extractApplyError,
  isPartitionOpenFailure,
  skipPlansEqual,
  type PendingSegment,
  type SkipPlan,
  type SuspendedTable,
  type WalMonitorDeps,
} from "../wal-monitor.js";

function segment(overrides: Partial<PendingSegment> = {}): PendingSegment {
  return {
    walId: 33,
    segmentId: 20,
    txns: 6,
    minTxn: 4672112,
    maxTxn: 4672117,
    minTimestamp: "2026-07-01T04:22:14.701496Z",
    maxTimestamp: "2026-07-01T04:22:17.204514Z",
    ...overrides,
  };
}

function suspendedTable(
  overrides: Partial<SuspendedTable> = {},
): SuspendedTable {
  return {
    name: "signalk_str",
    writerTxn: 4672111,
    sequencerTxn: 5456609,
    txnLag: 784498,
    errorTag: "",
    errorMessage: "",
    ...overrides,
  };
}

describe("buildPendingSegmentsSQL", () => {
  it("targets wal_transactions of the table with a strict txn lower bound", () => {
    const sql = buildPendingSegmentsSQL("signalk_str", 4672111);
    assert.match(sql, /FROM wal_transactions\('signalk_str'\)/);
    assert.match(sql, /WHERE sequencerTxn > 4672111 /);
    assert.match(sql, /GROUP BY walId, segmentId ORDER BY minTxn/);
  });

  it("escapes single quotes in the table name", () => {
    const sql = buildPendingSegmentsSQL("bad'name", 1);
    assert.match(sql, /wal_transactions\('bad''name'\)/);
  });

  it("floors and clamps the writer txn", () => {
    assert.match(buildPendingSegmentsSQL("t", 12.9), /> 12 /);
    assert.match(buildPendingSegmentsSQL("t", -5), /> 0 /);
  });
});

describe("computeSkipPlan", () => {
  it("returns null when there are no pending segments", () => {
    assert.equal(computeSkipPlan([]), null);
  });

  it("skips to the first txn of the next segment", () => {
    const plan = computeSkipPlan([
      segment(),
      segment({
        walId: 34,
        segmentId: 0,
        txns: 41499,
        minTxn: 4672118,
        maxTxn: 4713616,
        minTimestamp: "2026-07-01T04:50:02.387389Z",
        maxTimestamp: "2026-07-01T12:22:51.865153Z",
      }),
    ]);
    assert.ok(plan);
    assert.equal(plan.skipToTxn, 4672118);
    assert.equal(plan.skippedTxns, 6);
    assert.equal(plan.skipWindowStart, "2026-07-01T04:22:14.701496Z");
    assert.equal(plan.skipWindowEnd, "2026-07-01T04:22:17.204514Z");
    assert.equal(plan.walId, 33);
    assert.equal(plan.segmentId, 20);
    assert.equal(plan.tailSkip, false);
  });

  it("flags a tail skip when the stuck segment is the newest", () => {
    const plan = computeSkipPlan([segment()]);
    assert.ok(plan);
    assert.equal(plan.skipToTxn, 4672118);
    assert.equal(plan.tailSkip, true);
  });
});

describe("skipPlansEqual", () => {
  const plan: SkipPlan = {
    skipToTxn: 4672118,
    skippedTxns: 6,
    skipWindowStart: "2026-07-01T04:22:14.701496Z",
    skipWindowEnd: "2026-07-01T04:22:17.204514Z",
    walId: 33,
    segmentId: 20,
    tailSkip: false,
  };

  it("accepts an identical confirmation (including a JSON round-trip)", () => {
    assert.equal(skipPlansEqual(plan, { ...plan }), true);
    assert.equal(skipPlansEqual(plan, JSON.parse(JSON.stringify(plan))), true);
  });

  it("rejects non-object confirmations", () => {
    assert.equal(skipPlansEqual(plan, undefined), false);
    assert.equal(skipPlansEqual(plan, null), false);
    assert.equal(skipPlansEqual(plan, 4672118), false);
  });

  it("rejects when any field diverges — same target txn is not enough", () => {
    assert.equal(skipPlansEqual(plan, { ...plan, skippedTxns: 12000 }), false);
    assert.equal(
      skipPlansEqual(plan, { ...plan, skipWindowEnd: "2026-07-05T00:00:00Z" }),
      false,
    );
    assert.equal(skipPlansEqual(plan, { ...plan, tailSkip: true }), false);
    assert.equal(skipPlansEqual(plan, { ...plan, walId: 34 }), false);
    assert.equal(skipPlansEqual(plan, { ...plan, segmentId: 21 }), false);
    assert.equal(skipPlansEqual(plan, { ...plan, skipToTxn: 4672119 }), false);
  });
});

describe("extractApplyError", () => {
  const failure = [
    "2026-07-17T22:22:51.774951Z C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=signalk_str~8, seqTxn=4672112, error=java.lang.AssertionError",
    "\tat io.questdb.cairo.VarcharTypeDriver.getDataVectorSize(VarcharTypeDriver.java:175)",
    "\tat io.questdb.cairo.TableWriter.processWalCommitBlock(TableWriter.java:9965)",
    "]",
  ];

  it("returns null when no failure is present", () => {
    assert.equal(
      extractApplyError(["2026-07-17T22:22:51Z I job finished [table=x]"]),
      null,
    );
  });

  it("captures the failure line and its stack up to the closing bracket", () => {
    const lines = [
      "2026-07-17T22:22:50Z I i.q.c.TableWriter open 'signalk_str~8'",
      ...failure,
      "2026-07-17T22:22:56Z I i.q.c.w.ApplyWal2TableJob job finished [table=signalk~7]",
    ];
    const error = extractApplyError(lines);
    assert.ok(error);
    assert.match(error, /job failed, table suspended/);
    assert.match(error, /VarcharTypeDriver.getDataVectorSize/);
    assert.ok(error.endsWith("]"));
    assert.doesNotMatch(error, /job finished/);
  });

  it("uses the LAST failure when several are present", () => {
    const older = failure.map((l) => l.replace("AssertionError", "OldError"));
    const error = extractApplyError([...older, ...failure]);
    assert.ok(error);
    assert.match(error, /AssertionError/);
    assert.doesNotMatch(error, /OldError/);
  });

  it("stops at the next timestamped entry when no bracket closes the stack", () => {
    const error = extractApplyError([
      failure[0],
      failure[1],
      "2026-07-17T22:22:52Z I unrelated next entry",
    ]);
    assert.ok(error);
    assert.doesNotMatch(error, /unrelated/);
  });

  it("scopes to the requested table when several tables have failures", () => {
    const otherFailure = failure.map((l) =>
      l
        .replace("signalk_str~8", "signalk~7")
        .replace("AssertionError", "OtherTableError"),
    );
    // The other table failed LAST — an unscoped scrape would return it.
    const lines = [...failure, ...otherFailure];
    const strError = extractApplyError(lines, "signalk_str");
    assert.ok(strError);
    assert.match(strError, /AssertionError/);
    assert.doesNotMatch(strError, /OtherTableError/);
    const skError = extractApplyError(lines, "signalk");
    assert.ok(skError);
    assert.match(skError, /OtherTableError/);
    assert.equal(extractApplyError(lines, "signalk_position"), null);
  });
});

interface Harness {
  monitor: WalMonitor;
  resumeCalls: string[];
  suspendedCalls: { names: string[]; anyFailed: boolean }[];
  resolvedCalls: number;
  errors: string[];
  setSuspended(tables: SuspendedTable[]): void;
  setResumeFailing(error: Error | null): void;
  advance(ms: number): void;
}

function makeHarness(
  options: { retryMinMs?: number; failListWith?: Error } = {},
): Harness {
  let suspended: SuspendedTable[] = [];
  let resumeFailure: Error | null = null;
  let clock = 1_000_000;
  const harness: Partial<Harness> = {
    resumeCalls: [],
    suspendedCalls: [],
    resolvedCalls: 0,
    errors: [],
  };
  const deps: WalMonitorDeps = {
    listSuspended: async () => {
      if (options.failListWith) throw options.failListWith;
      return suspended;
    },
    resumeTable: async (name) => {
      harness.resumeCalls!.push(name);
      if (resumeFailure) throw resumeFailure;
    },
    onSuspended: (tables, anyFailed) => {
      harness.suspendedCalls!.push({
        names: tables.map((t) => t.name),
        anyFailed,
      });
    },
    onResolved: () => {
      harness.resolvedCalls!++;
    },
    debug: () => {},
    error: (msg) => {
      harness.errors!.push(msg);
    },
  };
  harness.monitor = new WalMonitor(deps, {
    retryMinMs: options.retryMinMs ?? 10 * 60_000,
    now: () => clock,
  });
  harness.setSuspended = (tables) => {
    suspended = tables;
  };
  harness.setResumeFailing = (error) => {
    resumeFailure = error;
  };
  harness.advance = (ms) => {
    clock += ms;
  };
  return harness as Harness;
}

describe("WalMonitor", () => {
  it("attempts one lossless resume on a new suspension and reports pending", async () => {
    const h = makeHarness();
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    assert.deepEqual(h.resumeCalls, ["signalk_str"]);
    assert.equal(h.monitor.outcomeFor("signalk_str"), "pending");
    assert.deepEqual(h.suspendedCalls, [
      { names: ["signalk_str"], anyFailed: false },
    ]);
    // The snapshot (writerTxn/sequencerTxn/error columns) must be persisted
    // to the log the moment the suspension is seen — the engine-side detail
    // does not survive a container recreate.
    assert.match(h.errors[0], /writerTxn=4672111/);
  });

  it("locks the episode to failed when re-suspended at the same writerTxn", async () => {
    const h = makeHarness();
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    await h.monitor.check();
    assert.equal(h.monitor.outcomeFor("signalk_str"), "failed");
    assert.deepEqual(h.resumeCalls, ["signalk_str"]);
    assert.equal(h.suspendedCalls[1].anyFailed, true);
    // Still failed on later checks; never resumes again on its own.
    await h.monitor.check();
    assert.deepEqual(h.resumeCalls, ["signalk_str"]);
    assert.equal(h.monitor.outcomeFor("signalk_str"), "failed");
  });

  it("starts a fresh episode when the writer advances to a new stall point", async () => {
    const h = makeHarness();
    h.setSuspended([suspendedTable({ writerTxn: 100 })]);
    await h.monitor.check();
    await h.monitor.check();
    assert.equal(h.monitor.outcomeFor("signalk_str"), "failed");
    h.setSuspended([suspendedTable({ writerTxn: 200 })]);
    await h.monitor.check();
    assert.deepEqual(h.resumeCalls, ["signalk_str", "signalk_str"]);
    assert.equal(h.monitor.outcomeFor("signalk_str"), "pending");
  });

  it("reports resolution exactly once when the suspension clears", async () => {
    const h = makeHarness();
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    h.setSuspended([]);
    await h.monitor.check();
    await h.monitor.check();
    assert.equal(h.resolvedCalls, 1);
    assert.equal(h.monitor.outcomeFor("signalk_str"), null);
  });

  it("rate-limits resumes across a resume/clear/re-suspend flap at one stall point", async () => {
    const h = makeHarness();
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    // Replay makes the table look healthy for one check, then it re-suspends
    // at the same writerTxn (a slow re-failure).
    h.setSuspended([]);
    await h.monitor.check();
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    assert.deepEqual(h.resumeCalls, ["signalk_str"]);
    // After the gap passes, one more automatic attempt is allowed.
    h.advance(11 * 60_000);
    h.setSuspended([]);
    await h.monitor.check();
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    assert.deepEqual(h.resumeCalls, ["signalk_str", "signalk_str"]);
  });

  it("does not classify a failed resume REQUEST as unreadable data", async () => {
    const h = makeHarness();
    h.setResumeFailing(new Error("table busy"));
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    assert.equal(h.monitor.outcomeFor("signalk_str"), "pending");
    // The request never executed, so the next check must NOT escalate to
    // "failed" (the panel reads that as corruption) — it stays pending.
    await h.monitor.check();
    assert.equal(h.monitor.outcomeFor("signalk_str"), "pending");
    // Once the retry gap passes and a resume actually executes, a
    // re-suspension at the same writerTxn is real evidence — now it fails.
    h.setResumeFailing(null);
    h.advance(11 * 60_000);
    await h.monitor.check();
    assert.equal(h.monitor.outcomeFor("signalk_str"), "pending");
    await h.monitor.check();
    assert.equal(h.monitor.outcomeFor("signalk_str"), "failed");
    assert.deepEqual(h.resumeCalls, ["signalk_str", "signalk_str"]);
  });

  it("does nothing after stop()", async () => {
    const h = makeHarness();
    h.setSuspended([suspendedTable()]);
    h.monitor.stop();
    await h.monitor.check();
    assert.deepEqual(h.resumeCalls, []);
    assert.deepEqual(h.suspendedCalls, []);
  });

  it("survives a failing wal_tables() probe", async () => {
    const h = makeHarness({ failListWith: new Error("connection refused") });
    await h.monitor.check();
    assert.deepEqual(h.suspendedCalls, []);
    assert.equal(h.resolvedCalls, 0);
  });

  it("handles multiple tables independently", async () => {
    const h = makeHarness();
    h.setSuspended([
      suspendedTable(),
      suspendedTable({ name: "signalk", writerTxn: 900 }),
    ]);
    await h.monitor.check();
    assert.deepEqual(h.resumeCalls, ["signalk_str", "signalk"]);
    // signalk recovers, signalk_str stays stuck.
    h.setSuspended([suspendedTable()]);
    await h.monitor.check();
    assert.equal(h.monitor.outcomeFor("signalk_str"), "failed");
    assert.equal(h.monitor.outcomeFor("signalk"), null);
    // Not fully resolved while one table is still suspended.
    assert.equal(h.resolvedCalls, 0);
  });
});

describe("isPartitionOpenFailure", () => {
  // Field report (issue #81): power loss left the `_txn` commit record
  // claiming 1,403,951 rows while the column files held 1,402,837. The writer
  // asserts while OPENING the partition, before it reads any transaction, so
  // every RESUME WAL variant re-hits it.
  const partitionOpenStack = [
    "2026-07-21T07:35:50.072000Z C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=signalk_str~8, seqTxn=57606, error=java.lang.AssertionError",
    "\tat io.questdb.cairo.VarcharTypeDriver.getDataVectorSize(VarcharTypeDriver.java:175)",
    "\tat io.questdb.cairo.TableReader.openPartition(TableReader.java:1234)",
    "]",
  ];

  // The class we shipped the guided skip for: the failure is in the pending
  // WAL segment's own files, which a bounded skip really can get past.
  const segmentStack = [
    "2026-07-17T22:22:51.000000Z C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=signalk_str~8, seqTxn=4672118, error=java.lang.AssertionError",
    "\tat io.questdb.cairo.VarcharTypeDriver.getDataVectorSize(VarcharTypeDriver.java:175)",
    "\tat io.questdb.cairo.wal.TableWriterSegmentFileCache.mmapSegments(TableWriterSegmentFileCache.java:200)",
    "]",
  ];

  it("detects a failure while opening the applied partition", () => {
    assert.equal(
      isPartitionOpenFailure(extractApplyError(partitionOpenStack)),
      true,
    );
  });

  it("does not flag an unreadable WAL segment, which a skip can repair", () => {
    assert.equal(
      isPartitionOpenFailure(extractApplyError(segmentStack)),
      false,
    );
  });

  it("detects every partition-open frame the classifier accepts", () => {
    // Each alternative in the pattern is a distinct way the writer reports
    // dying before it reads a transaction; an untested one can silently stop
    // matching.
    const frames = [
      "io.questdb.cairo.TableReader.openPartition(TableReader.java:1234)",
      "io.questdb.cairo.TableWriter.openLastPartition(TableWriter.java:4242)",
      "io.questdb.cairo.TxReader.unsafeLoadAll(TxReader.java:311)",
      "io.questdb.cairo.ColumnVersionReader.readUnsafe(ColumnVersionReader.java:98)",
    ];
    for (const frame of frames) {
      const log = [
        "2026-07-21T07:35:50.072000Z C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=signalk_str~8, error=java.lang.AssertionError",
        `\tat ${frame}`,
        "]",
      ];
      assert.equal(
        isPartitionOpenFailure(extractApplyError(log)),
        true,
        `expected ${frame} to classify as a partition-open failure`,
      );
    }
  });

  it("does not classify on the table name or error prose", () => {
    // The captured entry includes QuestDB's `[table=..., error=...]` header.
    // Matching a bare word anywhere in it would misfire on a user's table
    // name or on wording inside an unrelated error — and a false positive
    // withholds a skip that would actually have repaired the table.
    const tableNamed = [
      "2026-07-21T07:35:50.072000Z C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=openPartition~3, error=io.questdb.cairo.CairoException: could not open, out of disk space",
      "]",
    ];
    const prose = [
      "2026-07-21T07:35:50.072000Z C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=signalk~3, error=io.questdb.cairo.CairoException: openPartition budget exceeded",
      "]",
    ];
    assert.equal(isPartitionOpenFailure(extractApplyError(tableNamed)), false);
    assert.equal(isPartitionOpenFailure(extractApplyError(prose)), false);
  });

  it("does not classify a same-named frame from another library", () => {
    // The method names are not unique to QuestDB; without the namespace
    // anchor an unrelated dependency's frame would suppress a valid skip.
    const foreign = [
      "2026-07-21T07:35:50.072000Z C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=signalk~3, error=java.lang.AssertionError",
      "\tat com.example.storage.Writer.openPartition(Writer.java:88)",
      "]",
    ];
    assert.equal(isPartitionOpenFailure(extractApplyError(foreign)), false);
  });

  it("treats a missing diagnosis as not-this-class", () => {
    // No engine log (container recreated, log API unavailable): must not
    // withhold the skip on speculation.
    assert.equal(isPartitionOpenFailure(null), false);
    assert.equal(isPartitionOpenFailure(""), false);
  });
});

import { after, describe, it } from "node:test";
import assert from "node:assert/strict";
import { waitForQuestdb } from "../questdb-ready.js";

// Short enough for a test, the same shape as the defaults.
const fast = { attemptMs: 5, pollMs: 1, minDelayMs: 2, maxDelayMs: 8 };

describe("waitForQuestdb", () => {
  // retryForever sleeps between attempts on an unref'd timer, which alone
  // would let the process exit while a test still waits on it.
  const keepAlive = setInterval(() => {}, 1_000);
  after(() => clearInterval(keepAlive));

  it("resolves at once when QuestDB answers", async () => {
    const failed: number[] = [];
    await waitForQuestdb(async () => true, {
      ...fast,
      signal: new AbortController().signal,
      onAttemptFailed: (_err, next) => failed.push(next),
    });
    assert.deepEqual(failed, []);
  });

  it("keeps trying past an attempt without an answer, backing off", async () => {
    let up = false;
    const failed: number[] = [];
    await waitForQuestdb(async () => up, {
      ...fast,
      signal: new AbortController().signal,
      onAttemptFailed: (_err, next, attempt) => {
        failed.push(next);
        if (attempt === 4) up = true;
      },
    });
    assert.deepEqual(failed, [2, 4, 8, 8]);
  });

  it("asks again where it is before each attempt after the first", async () => {
    // a container recreated during the wait, back on another port
    let port = 9000;
    let refreshed = 0;
    const asked: number[] = [];
    await waitForQuestdb(
      async () => {
        asked.push(port);
        return refreshed === 3;
      },
      {
        ...fast,
        signal: new AbortController().signal,
        onAttemptFailed: () => {},
        beforeRetry: async () => {
          refreshed++;
          port = 9000 + refreshed;
        },
      },
    );
    assert.equal(refreshed, 3);
    assert.equal(asked[0], 9000);
    assert.equal(asked[asked.length - 1], 9003);
  });

  it("takes a failure to find it for a failed attempt, and tries again", async () => {
    let refreshed = 0;
    const errors: string[] = [];
    await waitForQuestdb(async () => refreshed === 2, {
      ...fast,
      signal: new AbortController().signal,
      onAttemptFailed: (err) => errors.push((err as Error).message),
      beforeRetry: async () => {
        if (++refreshed === 1) throw new Error("no container");
      },
    });
    assert.deepEqual(errors, ["QuestDB did not answer", "no container"]);
  });

  it("stops asking and rejects once the plugin stops between attempts", async () => {
    const abort = new AbortController();
    let asked = 0;
    const waiting = waitForQuestdb(
      async () => {
        asked++;
        return false;
      },
      {
        ...fast,
        signal: abort.signal,
        onAttemptFailed: (_err, _next, attempt) => {
          if (attempt === 2) abort.abort();
        },
      },
    );

    await assert.rejects(waiting);
    const before = asked;
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(asked, before);
  });

  it("stops asking within an attempt once the plugin stops", async () => {
    const abort = new AbortController();
    let asked = 0;
    const waiting = waitForQuestdb(
      async () => {
        if (++asked === 3) abort.abort();
        return false;
      },
      {
        ...fast,
        attemptMs: 60_000,
        signal: abort.signal,
        onAttemptFailed: () => {},
      },
    );

    await assert.rejects(waiting);
    assert.equal(asked, 3);
  });
});

import { retryForever } from "signalk-container-helper";

/** How long one attempt waits for QuestDB to answer, and how often it asks. */
const ATTEMPT_MS = 30_000;
const POLL_MS = 500;

export interface WaitForQuestdbOptions {
  /** Ends the wait; the plugin stopping, or a purge taking over. */
  signal: AbortSignal;
  /** After each attempt QuestDB did not answer, with the wait before the next. */
  onAttemptFailed: (err: unknown, nextDelayMs: number, attempt: number) => void;
  /** Before each attempt after the first; one that throws fails the attempt. */
  beforeRetry?: () => Promise<void>;
  attemptMs?: number;
  pollMs?: number;
  minDelayMs?: number;
  maxDelayMs?: number;
}

/**
 * Resolves once QuestDB answers. Each attempt asks every `pollMs` for up to
 * `attemptMs`, and one without an answer is tried again after a backoff, 15 s
 * doubling to 120 s, for as long as the plugin runs: a QuestDB that comes up
 * late, replaying its WAL say, or is restarted while the plugin starts, must
 * not leave recording off until someone restarts the plugin. Rejects only
 * when `signal` aborts.
 */
export async function waitForQuestdb(
  isHealthy: () => Promise<boolean>,
  options: WaitForQuestdbOptions,
): Promise<void> {
  const { signal, attemptMs = ATTEMPT_MS, pollMs = POLL_MS } = options;
  let attempts = 0;
  await retryForever(
    async () => {
      if (attempts++ > 0) await options.beforeRetry?.();
      const deadline = Date.now() + attemptMs;
      while (!signal.aborted) {
        if (await isHealthy()) return;
        if (Date.now() >= deadline) break;
        await new Promise((resolve) => setTimeout(resolve, pollMs));
      }
      throw new Error("QuestDB did not answer");
    },
    {
      signal,
      minDelayMs: options.minDelayMs,
      maxDelayMs: options.maxDelayMs,
      onAttemptFailed: options.onAttemptFailed,
    },
  );
}

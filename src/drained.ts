import type { ServerResponse } from "node:http";

// After a write the response refused, waits until it takes more; false once
// the client is gone, or once `signal` (the export's deadline) aborts. A
// response whose client disconnected never drains, nor does one whose client
// stays connected but stops reading, and a stream loop waiting for it would
// hold its QuestDB export open for good, and with it the watchdog's export
// count (questdbExportsRunning in index.ts).
export function drained(
  res: ServerResponse,
  signal?: AbortSignal,
): Promise<boolean> {
  if (res.destroyed || signal?.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    const settle = () => {
      res.off("drain", settle);
      res.off("close", settle);
      signal?.removeEventListener("abort", settle);
      resolve(!res.destroyed && !signal?.aborted);
    };
    res.once("drain", settle);
    res.once("close", settle);
    signal?.addEventListener("abort", settle, { once: true });
  });
}

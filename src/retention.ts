import { QueryClient } from "./query-client";

const TABLES = ["signalk", "signalk_str", "signalk_position"];
const DAY_MS = 24 * 60 * 60 * 1000;

export function startRetention(
  queryClient: QueryClient,
  retentionDays: number,
  debug: (msg: string) => void,
): NodeJS.Timeout | null {
  if (retentionDays <= 0) return null;

  async function dropOldPartitions() {
    for (const table of TABLES) {
      try {
        await queryClient.exec(
          `ALTER TABLE ${table} DROP PARTITION WHERE ts < dateadd('d', -${Math.floor(retentionDays)}, now())`,
        );
      } catch (err) {
        debug(
          `Retention drop for ${table} failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
    debug(`Retention check complete (keeping ${retentionDays} days)`);
  }

  dropOldPartitions();

  return setInterval(dropOldPartitions, DAY_MS);
}

// The archive: what retention keeps of the own vessel once it drops the rest.
//
// Retention drops whole daily partitions, because QuestDB has no row-level
// DELETE, so "drop everything but the own track" cannot be a filter on the
// drop. Before a partition goes, the rows worth keeping are copied, thinned,
// into a table of their own that retention never touches. Reads add that
// table back in wherever it covers the asked range.
//
// The archive tables mirror their base tables' columns and dedup keys, so a
// copy that is repeated — a run interrupted between the copy and the drop —
// upserts onto itself instead of doubling rows. They are partitioned by month:
// an archive is sparse and lives for years, and a partition per day would
// leave thousands of near-empty ones behind.

import type { QueryClient } from "./query-client.js";
import { isMissingTable } from "./schema-errors.js";

export const ARCHIVE_TABLES = {
  signalk: "signalk_archive",
  signalk_position: "signalk_position_archive",
} as const;

export type ArchivedTable = keyof typeof ARCHIVE_TABLES;

const COLUMNS: Record<ArchivedTable, string> = {
  signalk: "ts, path, context, source, value",
  signalk_position: "ts, context, source, lat, lon",
};

/** Whether the archive holds rows of a table that a read must include. */
export interface ArchiveReader {
  covers(table: ArchivedTable, from: string, to: string): boolean;
}

export const NO_ARCHIVE: ArchiveReader = { covers: () => false };

/**
 * What a read selects from: the base table alone, or the base table and its
 * archive merged in time order. The base table alone keeps the common case —
 * a recent window, which the archive never reaches — on exactly the query it
 * has always been.
 *
 * The merge has to declare `ts` its designated timestamp again, or SAMPLE BY
 * and LATEST ON reject it, and that declaration is only true because of the
 * ORDER BY. A time filter on the outer query still reaches both tables as an
 * interval scan (verified with EXPLAIN on QuestDB 9.4 and 10.0), but the
 * ORDER BY then sorts every row in range, so its memory grows with the
 * range. And SAMPLE BY … FROM … TO … FILL(NULL) over it adds a bucket past
 * TO that repeats the last one (QuestDB 9.4). The v2 provider's sampled and
 * raw reads therefore read each table on its own and merge the rows
 * themselves; this union is for reads that need one order across both
 * tables, or that a short window bounds.
 */
export function readFrom(table: ArchivedTable, withArchive: boolean): string {
  if (!withArchive) return table;
  const columns = COLUMNS[table];
  return (
    `((SELECT ${columns} FROM ${table} UNION ALL ` +
    `SELECT ${columns} FROM ${ARCHIVE_TABLES[table]}) ORDER BY ts) TIMESTAMP(ts)`
  );
}

/**
 * The time span each archive table holds, cached so that deciding whether a
 * read needs it costs no query. Refreshed after anything writes to the
 * archive; until the first refresh nothing is archived as far as reads know.
 */
export class ArchiveSpans implements ArchiveReader {
  private readonly spans = new Map<ArchivedTable, [number, number]>();
  private read = false;

  constructor(private readonly queryClient: Pick<QueryClient, "exec">) {}

  /** Whether a refresh has read every archive table at least once. */
  get known(): boolean {
    return this.read;
  }

  async refresh(): Promise<void> {
    for (const table of Object.keys(ARCHIVE_TABLES) as ArchivedTable[]) {
      try {
        const result = await this.queryClient.exec(
          `SELECT min(ts), max(ts) FROM ${ARCHIVE_TABLES[table]}`,
        );
        const [from, to] = result.dataset[0] ?? [];
        if (typeof from === "string" && typeof to === "string") {
          this.spans.set(table, [Date.parse(from), Date.parse(to)]);
        } else {
          this.spans.delete(table);
        }
      } catch (err) {
        // A missing table archives nothing. Any other failure keeps the span
        // already known: forgetting it would hide archived history from
        // every read until the next refresh.
        if (isMissingTable(err)) this.spans.delete(table);
        else throw err;
      }
    }
    this.read = true;
  }

  covers(table: ArchivedTable, from: string, to: string): boolean {
    const span = this.spans.get(table);
    if (!span) return false;
    return Date.parse(from) <= span[1] && Date.parse(to) >= span[0];
  }
}

/**
 * Wait until QuestDB has applied everything committed to `table`'s WAL when
 * this was called. A row is readable only once applied, and a suspended
 * table never gets there, so that fails at once rather than at the deadline.
 */
export async function waitForApply(
  queryClient: Pick<QueryClient, "exec">,
  table: string,
  timeoutMs: number,
  pollMs = 1000,
): Promise<void> {
  const state = async () => {
    const result = await queryClient.exec(
      `SELECT suspended, writerTxn, sequencerTxn FROM wal_tables() WHERE name = '${table}'`,
    );
    const [suspended, writerTxn, sequencerTxn] = result.dataset[0] ?? [];
    if (typeof writerTxn !== "number" || typeof sequencerTxn !== "number") {
      throw new Error(`${table} is not a WAL table`);
    }
    if (suspended === true) {
      throw new Error(`${table} is suspended, so the rows cannot be applied`);
    }
    return { writerTxn, sequencerTxn };
  };
  const target = (await state()).sequencerTxn;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if ((await state()).writerTxn >= target) return;
    if (Date.now() > deadline) {
      throw new Error(`${table} did not apply the rows within the timeout`);
    }
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}

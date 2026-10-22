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
 * interval scan (verified with EXPLAIN on QuestDB 9.4 and 10.0).
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

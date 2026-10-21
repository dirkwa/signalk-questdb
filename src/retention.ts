import {
  QueryClient,
  isSafeIdentifier,
  validateTimestamp,
} from "./query-client.js";
import { ARCHIVE_TABLES, type ArchivedTable } from "./archive.js";
import type { PathMatcher } from "./path-matcher.js";

const TABLES = ["signalk", "signalk_str", "signalk_position"] as const;
type Table = (typeof TABLES)[number];

const DAY_MS = 24 * 60 * 60 * 1000;

// A partition is named by its day ("2026-08-01"); the name goes into the DROP
// statement as a literal, so anything else is refused rather than quoted.
const PARTITION_NAME = /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/;

// The copy into the archive reads a whole day of a base table, which on a
// busy install is millions of rows and runs on QuestDB's single worker.
const COPY_TIMEOUT_MS = 120_000;
const APPLY_TIMEOUT_MS = 120_000;
const APPLY_POLL_MS = 1000;

export interface RetentionOptions {
  retentionDays: number;
  /** Copy the own vessel's positions to the archive before they are dropped. */
  archiveOwnTrack: boolean;
  /** Own-vessel numeric paths to copy to the archive; null copies none. */
  archivePaths: PathMatcher | null;
  /** One archived row per path and source per this many seconds; 0 keeps all. */
  archiveResolutionSeconds: number;
}

export interface Retention {
  stop(): void;
}

interface Partition {
  name: string;
  /** Midnight of the partition's day: thinned copies are stamped on bucket
   * starts, and the first can lie before the partition's earliest row. */
  day: string;
  to: string;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Drop every daily partition that lies wholly before the retention cutoff,
 * once a day. A partition still holding a row inside the window is kept
 * whole until it ages out completely, so retention keeps at least the
 * configured number of days, never fewer.
 *
 * Before a partition of `signalk_position` or `signalk` is dropped, the own
 * vessel's rows that the options ask to keep are copied, thinned, into the
 * archive, and the drop waits until QuestDB has applied that copy. A copy
 * that fails leaves the partition in place for the next run; a run stopped
 * between the copy and the drop repeats the copy, which the archive's dedup
 * keys absorb.
 */
export function startRetention(
  queryClient: Pick<QueryClient, "exec">,
  options: RetentionOptions,
  debug: (msg: string) => void,
  onArchived: () => void = () => {},
): Retention | null {
  if (options.retentionDays <= 0) return null;

  let stopped = false;
  // A first run over an install that recorded for years can take longer than
  // the interval; a second run must not start on the same partitions.
  let running = false;

  async function expiredPartitions(
    table: Table,
    cutoff: string,
  ): Promise<Partition[]> {
    const result = await queryClient.exec(
      `SELECT name, maxTimestamp FROM table_partitions('${table}') ` +
        `WHERE maxTimestamp < '${cutoff}' ORDER BY name`,
    );
    return result.dataset.flatMap((row) => {
      const [name, to] = row;
      if (typeof name !== "string" || !PARTITION_NAME.test(name)) return [];
      if (typeof to !== "string") return [];
      return [
        {
          name,
          day: validateTimestamp(`${name}T00:00:00.000Z`),
          to: validateTimestamp(to),
        },
      ];
    });
  }

  async function count(table: string, where: string): Promise<number> {
    const result = await queryClient.exec(
      `SELECT count() FROM ${table} WHERE ${where}`,
      COPY_TIMEOUT_MS,
    );
    const n = result.dataset[0]?.[0];
    return typeof n === "number" ? n : 0;
  }

  /** Wait until QuestDB has applied `table`'s WAL up to `txn`. */
  async function applied(table: string, txn: number): Promise<void> {
    const deadline = Date.now() + APPLY_TIMEOUT_MS;
    for (;;) {
      const result = await queryClient.exec(
        `SELECT suspended, writerTxn FROM wal_tables() WHERE name = '${table}'`,
      );
      const [suspended, writerTxn] = result.dataset[0] ?? [];
      if (suspended === true) {
        throw new Error(`${table} is suspended, so the copy cannot be applied`);
      }
      if (typeof writerTxn === "number" && writerTxn >= txn) return;
      if (Date.now() > deadline) {
        throw new Error(`${table} did not apply the copy within the timeout`);
      }
      await sleep(APPLY_POLL_MS);
    }
  }

  async function sequencerTxn(table: string): Promise<number> {
    const result = await queryClient.exec(
      `SELECT sequencerTxn FROM wal_tables() WHERE name = '${table}'`,
    );
    const txn = result.dataset[0]?.[0];
    if (typeof txn !== "number") {
      throw new Error(`${table} is not a WAL table`);
    }
    return txn;
  }

  /** The own-vessel rows of `partition` that the options keep, or null. */
  async function keptWhere(
    table: ArchivedTable,
    partition: Partition,
  ): Promise<string | null> {
    const where =
      `ts >= '${partition.day}' AND ts <= '${partition.to}' ` +
      `AND context = 'self'`;
    if (table === "signalk_position") {
      return options.archiveOwnTrack ? where : null;
    }
    const matcher = options.archivePaths;
    if (!matcher || matcher.isEmpty) return null;
    const result = await queryClient.exec(
      `SELECT DISTINCT path FROM signalk WHERE ${where}`,
      COPY_TIMEOUT_MS,
    );
    // A stored path the identifier guard rejects cannot be named in the
    // filter; it is left out of the copy rather than failing every run.
    const paths = result.dataset
      .map((row) => row[0])
      .filter((path): path is string => typeof path === "string")
      .filter((path) => matcher.matches(path))
      .filter((path) => {
        if (isSafeIdentifier(path)) return true;
        debug(
          `Retention: ${path} cannot be archived, its name is not a safe identifier`,
        );
        return false;
      })
      .map((path) => `'${path}'`);
    return paths.length > 0
      ? `${where} AND path IN (${paths.join(", ")})`
      : null;
  }

  // first() rather than an average: the archive keeps samples the vessel
  // actually recorded, so a position is one it was at and an angle is never
  // averaged across north.
  function thinnedSelect(table: ArchivedTable, where: string): string {
    const resolution = Math.floor(options.archiveResolutionSeconds);
    const values = table === "signalk_position" ? ["lat", "lon"] : ["value"];
    const keys =
      table === "signalk_position"
        ? "context, source"
        : "path, context, source";
    if (resolution <= 0) {
      return `SELECT ts, ${keys}, ${values.join(", ")} FROM ${table} WHERE ${where}`;
    }
    const firsts = values.map((v) => `first(${v}) ${v}`).join(", ");
    return (
      `SELECT ts, ${keys}, ${firsts} FROM ${table} WHERE ${where} ` +
      `SAMPLE BY ${resolution}s ALIGN TO CALENDAR`
    );
  }

  /** Copy what is kept of `partition` to the archive; true if anything was. */
  async function archive(table: Table, partition: Partition): Promise<boolean> {
    if (table === "signalk_str") return false;
    const where = await keptWhere(table, partition);
    if (where === null) return false;
    if ((await count(table, where)) === 0) return false;

    const target = ARCHIVE_TABLES[table];
    await queryClient.exec(
      `INSERT INTO ${target} ${thinnedSelect(table, where)}`,
      COPY_TIMEOUT_MS,
    );
    await applied(target, await sequencerTxn(target));
    if ((await count(target, where)) === 0) {
      throw new Error(`the copy of ${table} ${partition.name} is missing`);
    }
    return true;
  }

  async function run() {
    if (running || stopped) return;
    running = true;
    let archived = false;
    try {
      const cutoff = new Date(
        Date.now() - Math.floor(options.retentionDays) * DAY_MS,
      ).toISOString();
      for (const table of TABLES) {
        // One table failing leaves the others to run. Within a table, the
        // first failure stops it: the later partitions would hit the same
        // cause, and none of them is dropped without its copy.
        try {
          for (const partition of await expiredPartitions(table, cutoff)) {
            if (stopped) return;
            if (await archive(table, partition)) archived = true;
            if (stopped) return;
            await queryClient.exec(
              `ALTER TABLE ${table} DROP PARTITION LIST '${partition.name}'`,
            );
          }
        } catch (err) {
          debug(
            `Retention for ${table} failed: ${err instanceof Error ? err.message : String(err)}`,
          );
        }
      }
      debug(`Retention check complete (keeping ${options.retentionDays} days)`);
    } finally {
      running = false;
      if (archived) onArchived();
    }
  }

  void run();
  const timer = setInterval(() => void run(), DAY_MS);

  return {
    stop() {
      stopped = true;
      clearInterval(timer);
    },
  };
}

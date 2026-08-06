// Wire shapes for the plugin's own REST endpoints under
// /plugins/signalk-questdb/api/.
//
// Both surfaces import these: the handlers in index.ts assert their responses
// against them with `satisfies`, and the config panel casts fetch results to
// them. That makes a field rename a compile error on both sides instead of an
// "undefined" that only shows up as a blank spot in the Admin UI.
//
// The panel is a browser bundle, so this file must stay type-only — no
// imports that pull runtime code (fs, typebox) into webpack's graph.

import type { MaxMapCountStatus } from "./host-limits";
import type { SkipPlan, SuspendedTable } from "./wal-monitor";

/**
 * Every handler answers failures with `{ error }`, and the action endpoints
 * answer success with `{ message }`. The panel reads both off one object and
 * renders whichever is present, so both are optional everywhere rather than
 * split into a union the callers would have to narrow at ten sites.
 */
export interface ApiError {
  error?: string;
  message?: string;
}

/**
 * Mirror of signalk-container's `UlimitClamp` event, redeclared (not
 * imported) for the same reason index.ts redeclares it: signalk-container is
 * an optional peer reached through globalThis, so there is no type to import.
 */
export interface UlimitClampStatus {
  ulimit: string;
  requested: number;
  granted: number;
  reason: string;
}

export type SuspendedTableStatus = SuspendedTable & {
  /** WAL monitor's verdict for the current stall point: "pending" | "failed". */
  autoResume: string | null;
};

/**
 * GET /api/status.
 *
 * Deliberately flat rather than a union discriminated on `status`: the panel
 * gates the whole running-state block behind one `isRunning` alias, and TS
 * does not narrow through an alias. Optional fields let the panel read
 * `dbStatus?.totalRows` at the leaves instead of restructuring the render.
 */
export interface DbStatus extends ApiError {
  status: "running" | "unhealthy" | "not_running";
  totalRows?: number;
  activePathsToday?: number;
  walSuspended?: boolean;
  suspendedTables?: SuspendedTableStatus[];
  schemaMismatch?: boolean;
  ulimitClamp?: UlimitClampStatus | null;
  hostMaxMapCount?: MaxMapCountStatus | null;
  endpoint?: string | null;
}

/** GET /api/versions — QuestDB releases, drafts filtered out. */
export interface QuestdbVersion {
  tag: string;
  prerelease: boolean;
}

/** GET /api/update/check. */
export interface UpdateInfo extends ApiError {
  currentVersion: string;
  latestVersion: string;
  updateAvailable: boolean;
}

/**
 * POST /api/update/apply. `newVersion` is the tag the container was actually
 * recreated with, so the panel can sync its version field to reality.
 */
export interface UpdateApplyResponse extends ApiError {
  newVersion?: string;
}

export type WalDiagnosisTable = SuspendedTable & {
  autoResume: string | null;
  /** Commit-time of the first unapplied txn = when apply froze. */
  suspendedSince: string | null;
  pendingSegments: number;
  /** Null when the failure is a partition-open error, where skipping cannot help. */
  skipPlan: SkipPlan | null;
  partitionOpenFailure: boolean;
  segmentError: string | null;
  applyError: string | null;
};

/** GET /api/wal-diagnosis. */
export interface WalDiagnosis extends ApiError {
  tables: WalDiagnosisTable[];
}

export interface ResumeWalResult {
  table: string;
  ok: boolean;
  error?: string;
  writerTxn: number;
  sequencerTxn: number;
}

/** POST /api/resume-wal. */
export interface ResumeWalResponse extends ApiError {
  results?: ResumeWalResult[];
  /** How many of `results` succeeded. */
  resumed?: number;
}

/**
 * POST /api/resume-wal/skip — the lossy repair. Carries only a human-readable
 * outcome; the skip target itself is computed server-side.
 */
export type SkipWalResponse = ApiError;

export interface MigrationSource {
  type: string;
  url: string;
  status: string;
  version?: string;
}

/** GET /api/migration/detect. */
export interface MigrationDetectResponse extends ApiError {
  sources: MigrationSource[];
}

import React, { useState, useEffect, useCallback, useRef } from "react";
// Type-only, always: this file is bundled for the browser, so a value import
// reaching out of src/configpanel/ would pull the server module's runtime
// dependencies (typebox, fs/promises) into the panel bundle.
import type { Config } from "../config/schema.js";
import type {
  ApiError,
  DbStatus,
  MigrationSource,
  ResumeWalResponse,
  SkipWalResponse,
  UpdateApplyResponse,
  UpdateInfo,
  WalDiagnosis,
} from "../api-contract.js";
import type { SkipPlan } from "../wal-monitor.js";
import {
  ActionStatus,
  Button,
  CollapsibleSection,
  formatNumber,
  StatusCard,
  useStatusPoll,
  useVersions,
  VersionSelect,
} from "signalk-container-helper/ui";
import { S } from "./styles.js";
import { toMigrationSources } from "./responses.js";

type FilterMode = Config["pathFilter"]["mode"];
type Compression = Config["compression"];

/** `catch` binds `unknown` under strict mode; mirrors the idiom in index.ts. */
function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/**
 * Both fields are optional on every response — a body that failed to parse,
 * or an error shape from a proxy rather than the plugin, carries neither.
 * Interpolating them raw renders the literal string "undefined" at the user.
 */
function apiText(value: string | undefined, fallback: string): string {
  return value ?? fallback;
}

const COMPRESSIONS = ["none", "lz4", "zstd"] as const;

// The update row's buttons sit inline with status text, so they run smaller
// than a form button. Not the helper's `small` (4px 10px / 11px) — that is a
// shade tighter than what this row was built around.
const UPDATE_BTN = { padding: "4px 12px", fontSize: 12 } as const;

/** A <select> value is `string` to the type system; narrow it back. */
function toCompression(value: string): Compression {
  return (COMPRESSIONS as readonly string[]).includes(value)
    ? (value as Compression)
    : "lz4";
}

interface PluginConfigurationPanelProps {
  configuration?: Config;
  // Partial<Config>, not Config: the panel spreads the stored configuration
  // and overwrites only the keys it manages, so what it hands back is
  // whatever was stored plus this panel's fields — a config predating a
  // schema addition still misses that key. That is the contract
  // normalizeConfig() in config/schema.ts is written to accept; claiming
  // Config here would assert a completeness neither side believes in.
  save: (config: Partial<Config>) => void;
}

export default function PluginConfigurationPanel({
  configuration,
  save,
}: PluginConfigurationPanelProps) {
  const cfg: Partial<Config> = configuration || {};

  const [questdbHost, setQuestdbHost] = useState(
    cfg.questdbHost || "127.0.0.1",
  );
  const [questdbIlpPort, setQuestdbIlpPort] = useState(
    cfg.questdbIlpPort || 9009,
  );
  const [questdbHttpPort, setQuestdbHttpPort] = useState(
    cfg.questdbHttpPort || 9000,
  );
  const [questdbPgPort, setQuestdbPgPort] = useState(cfg.questdbPgPort || 8812);
  const [questdbVersion, setQuestdbVersion] = useState(
    cfg.questdbVersion || "latest",
  );
  const [managedContainer, setManagedContainer] = useState(
    cfg.managedContainer !== false,
  );
  const [recordSelf, setRecordSelf] = useState(cfg.recordSelf !== false);
  const [recordOthers, setRecordOthers] = useState(cfg.recordOthers !== false);
  // Restore is opt-in, so a missing key means off — the inverse of the
  // recording toggles above.
  const [restoreOnStart, setRestoreOnStart] = useState(
    cfg.restoreOnStart === true,
  );
  // On unless explicitly disabled — matches normalizeConfig, so a save from
  // this panel cannot silently flip the documented default.
  const [enableConsole, setEnableConsole] = useState(
    cfg.enableConsole !== false,
  );
  const [restoreMaxAgeMinutes, setRestoreMaxAgeMinutes] = useState(
    (cfg.restoreMaxAgeMinutes ?? 0) > 0 ? cfg.restoreMaxAgeMinutes! : 9,
  );
  const [defaultSamplingRate, setDefaultSamplingRate] = useState(
    cfg.defaultSamplingRate ?? 2000,
  );
  // Kept as the raw input string: coercing per keystroke turns a cleared
  // field into 0 and partial input into NaN mid-edit. doSave parses and
  // validates once.
  const [ilpFlushIntervalMs, setIlpFlushIntervalMs] = useState(
    String(cfg.ilpFlushIntervalMs ?? 5000),
  );
  const [retentionDays, setRetentionDays] = useState(cfg.retentionDays || 0);
  // Hydrate defensively: a hand-edited or corrupted config could carry a bad
  // mode or a non-array `paths`, and an unguarded `.join()` would crash the
  // whole panel render.
  const [filterMode, setFilterMode] = useState<FilterMode>(
    cfg.pathFilter?.mode === "include" ? "include" : "exclude",
  );
  // One glob per line in the textarea; round-tripped to/from the schema's
  // pathFilter.paths string array.
  const [filterPaths, setFilterPaths] = useState(
    (Array.isArray(cfg.pathFilter?.paths) ? cfg.pathFilter.paths : []).join(
      "\n",
    ),
  );
  const [compression, setCompression] = useState<Compression>(
    cfg.compression || "lz4",
  );
  const [compressionLevel, setCompressionLevel] = useState(
    cfg.compressionLevel || 3,
  );
  const [networkName, setNetworkName] = useState(
    cfg.networkName || "sk-network",
  );
  const [exposeToContainers, setExposeToContainers] = useState(
    cfg.exposeToContainers || false,
  );

  // useVersions keeps the last good list when a fetch fails. The local
  // version wiped it: a 200 carrying non-JSON set `versions` to [], and an
  // empty dropdown is what lets a Save silently change the running image.
  const {
    versions,
    versionsError,
    loading: versionsLoading,
    refresh: fetchVersions,
  } = useVersions("/plugins/signalk-questdb/api/versions");
  // Self-scheduling rather than setInterval: a response slower than the
  // period cannot stack overlapping requests, and a generation counter drops
  // stale responses that land after newer ones. The panel already did this
  // for WAL diagnosis but not for status.
  const {
    status: dbStatus,
    loading: statusLoading,
    refresh: fetchStatus,
  } = useStatusPoll<DbStatus>("/plugins/signalk-questdb/api/status", {
    // Parsed even on non-2xx: the unhealthy 503 carries fields the panel
    // must still surface (hostMaxMapCount — mmap exhaustion is one of the
    // ways QuestDB becomes unhealthy).
    fallback: { status: "not_running" },
    intervalMs: 5000,
  });
  const [migrationSources, setMigrationSources] = useState<
    MigrationSource[] | null
  >(null);
  const [migrationDetecting, setMigrationDetecting] = useState(false);
  const [migrationUrl, setMigrationUrl] = useState("");
  const [actionStatus, setActionStatus] = useState("");
  const [statusError, setStatusError] = useState(false);
  const [updateInfo, setUpdateInfo] = useState<UpdateInfo | null>(null);
  const [checkingUpdate, setCheckingUpdate] = useState(false);
  const [updating, setUpdating] = useState(false);
  const [purging, setPurging] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [resumeOutcome, setResumeOutcome] = useState("");
  const [walDiag, setWalDiag] = useState<WalDiagnosis | null>(null);
  // True when the most recent diagnosis refresh failed: the displayed data
  // is kept (better than flapping controls away) but anything destructive
  // derived from it — the manual FROM TXN statement, the skip button — is
  // withheld until a refresh confirms the plan is current.
  const [walDiagStale, setWalDiagStale] = useState(false);
  // Monotonic request id so a slow diagnosis response that lands after a
  // newer one (poll cycle vs. post-action refresh) is dropped instead of
  // overwriting fresher data.
  const walDiagGen = useRef(0);
  const [skipping, setSkipping] = useState(false);

  const fetchWalDiagnosis = useCallback(async (signal?: AbortSignal) => {
    const gen = ++walDiagGen.current;
    try {
      const res = await fetch("/plugins/signalk-questdb/api/wal-diagnosis", {
        signal,
      });
      if (gen !== walDiagGen.current) return;
      if (res.ok) {
        const data = (await res.json()) as WalDiagnosis;
        if (gen !== walDiagGen.current) return;
        setWalDiag(data);
        setWalDiagStale(false);
      } else {
        setWalDiagStale(true);
      }
    } catch {
      // diagnosis is best-effort; the banner still renders from status,
      // but flag the kept data as stale so destructive actions pause.
      if (gen === walDiagGen.current) setWalDiagStale(true);
    }
  }, []);

  const suspendedNow = Boolean(
    dbStatus &&
    dbStatus.status === "running" &&
    (dbStatus.suspendedTables || []).length > 0,
  );

  useEffect(() => {
    if (!suspendedNow) {
      setWalDiag(null);
      setWalDiagStale(false);
      return undefined;
    }
    // Keep polling while suspended: a table can join the suspension, the
    // monitor's auto-resume verdict can arrive, or the skip plan can change
    // — and the first fetch may simply have failed. On a failed refresh the
    // last good diagnosis is kept (the skip endpoint re-validates the plan
    // server-side, so acting on a stale one is rejected, not dangerous).
    // Self-scheduling rather than setInterval: the endpoint aggregates
    // several engine queries plus a log fetch, and on a slow host one
    // response can take longer than the poll period — the next request
    // must only start after the previous one settled. Cleanup aborts the
    // in-flight request so a banner unmount doesn't leave it dangling.
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const controller = new AbortController();
    const poll = async () => {
      await fetchWalDiagnosis(controller.signal);
      if (!cancelled) {
        timer = setTimeout(poll, 15000);
      }
    };
    poll();
    return () => {
      cancelled = true;
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [suspendedNow, fetchWalDiagnosis]);

  const handleSkipWal = async (tableName: string, plan: SkipPlan) => {
    const backlogWarning = plan.tailSkip
      ? "\n\nWARNING: this is the newest pending segment — skipping it drops the ENTIRE pending backlog."
      : "";
    if (
      !window.confirm(
        `Skip the unreadable WAL segment on ${tableName}?\n\n` +
          `This permanently loses ${formatNumber(plan.skippedTxns)} transaction(s) ` +
          `recorded between ${plan.skipWindowStart} and ${plan.skipWindowEnd}. ` +
          `Everything after the skipped segment is replayed without loss.` +
          backlogWarning,
      )
    ) {
      return;
    }
    setSkipping(true);
    setResumeOutcome("");
    try {
      const res = await fetch("/plugins/signalk-questdb/api/resume-wal/skip", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        // The full plan, not just the target txn: the server 409s unless
        // every field matches its freshly recomputed plan, so the operator
        // can only ever confirm exactly the loss they were shown.
        body: JSON.stringify({ table: tableName, confirmPlan: plan }),
      });
      const data = (await res
        .json()
        .catch(() => ({ error: res.statusText }))) as SkipWalResponse;
      if (res.ok) {
        setResumeOutcome(data.message ?? "");
      } else {
        setResumeOutcome(
          `Skip failed: ${apiText(data.error, "unknown error")}`,
        );
      }
      fetchStatus();
      fetchWalDiagnosis();
    } catch (e) {
      setResumeOutcome(`Skip failed: ${errorMessage(e)}`);
    }
    setSkipping(false);
  };

  const checkForUpdate = async () => {
    setCheckingUpdate(true);
    try {
      const res = await fetch("/plugins/signalk-questdb/api/update/check");
      if (res.ok) {
        setUpdateInfo((await res.json()) as UpdateInfo);
      }
    } catch {
      // silently fail
    }
    setCheckingUpdate(false);
  };

  const applyUpdate = async () => {
    setUpdating(true);
    setActionStatus("Pulling new image and restarting...");
    setStatusError(false);
    try {
      const res = await fetch("/plugins/signalk-questdb/api/update/apply", {
        method: "POST",
      });
      if (res.ok) {
        const data = (await res.json()) as UpdateApplyResponse;
        setActionStatus(data.message ?? "");
        setUpdateInfo(null);
        if (data.newVersion) {
          setQuestdbVersion(data.newVersion);
        }
        fetchStatus();
      } else {
        const data = (await res
          .json()
          .catch(() => ({ error: res.statusText }))) as UpdateApplyResponse;
        setActionStatus(
          `Update failed: ${apiText(data.error, "unknown error")}`,
        );
        setStatusError(true);
      }
    } catch (e) {
      setActionStatus(`Update failed: ${errorMessage(e)}`);
      setStatusError(true);
    }
    setUpdating(false);
  };

  const purgeData = async () => {
    if (
      !window.confirm(
        "Remove the QuestDB container and DELETE ALL recorded data? This cannot be undone.",
      )
    ) {
      return;
    }
    setPurging(true);
    setActionStatus("Removing QuestDB container and data...");
    setStatusError(false);
    try {
      const res = await fetch("/plugins/signalk-questdb/api/purge-data", {
        method: "POST",
      });
      const data = (await res
        .json()
        .catch(() => ({ error: res.statusText }))) as ApiError;
      if (res.ok) {
        setActionStatus(data.message ?? "");
        fetchStatus();
      } else {
        setActionStatus(
          `Remove failed: ${apiText(data.error, "unknown error")}`,
        );
        setStatusError(true);
      }
    } catch (e) {
      setActionStatus(`Remove failed: ${errorMessage(e)}`);
      setStatusError(true);
    }
    setPurging(false);
  };

  const handleResumeWal = async () => {
    setResuming(true);
    setResumeOutcome("");
    try {
      const res = await fetch("/plugins/signalk-questdb/api/resume-wal", {
        method: "POST",
      });
      const data = (await res
        .json()
        .catch(() => ({ error: res.statusText }))) as ResumeWalResponse;
      if (res.ok) {
        const failed = (data.results || []).filter((r) => !r.ok);
        setResumeOutcome(
          failed.length === 0
            ? `${apiText(data.message, "Resume requested")}. Recording resumes as the backlog drains — the banner clears once tables catch up.`
            : `${apiText(data.message, "Resume requested")}. ` +
                failed.map((r) => `${r.table}: ${r.error}`).join(" · "),
        );
        fetchStatus();
        fetchWalDiagnosis();
      } else {
        setResumeOutcome(
          `Resume failed: ${apiText(data.error, "unknown error")}`,
        );
      }
    } catch (e) {
      setResumeOutcome(`Resume failed: ${errorMessage(e)}`);
    }
    setResuming(false);
  };

  const detectMigration = async () => {
    setMigrationDetecting(true);
    setActionStatus("");
    try {
      const params = migrationUrl
        ? `?url=${encodeURIComponent(migrationUrl)}`
        : "";
      const res = await fetch(
        `/plugins/signalk-questdb/api/migration/detect${params}`,
      );
      if (res.ok) {
        // A 200 carrying HTML (captive portal, auth redirect) rejects in
        // json() before any shape check runs, and the catch below would
        // report the parse error as "Detection failed" rather than the
        // honest "nothing detected".
        const sources = toMigrationSources(await res.json().catch(() => null));
        setMigrationSources(sources);
        if (sources.length === 0) {
          setActionStatus(
            migrationUrl
              ? `No InfluxDB found at ${migrationUrl}.`
              : "No InfluxDB instances detected on localhost:8086.",
          );
          setStatusError(false);
        }
      }
    } catch (e) {
      setActionStatus("Detection failed: " + errorMessage(e));
      setStatusError(true);
    }
    setMigrationDetecting(false);
  };

  const [exportFrom, setExportFrom] = useState("");
  const [exportTo, setExportTo] = useState("");
  const [exportFormat, setExportFormat] = useState("parquet");
  const [exporting, setExporting] = useState(false);

  const doSave = () => {
    // The min/max attributes only hint, and the state holds the raw input
    // string — parse and gate here so a bad interval can never reach the
    // flush timer.
    const batchIntervalMs = Number(ilpFlushIntervalMs);
    if (
      ilpFlushIntervalMs === "" ||
      !Number.isFinite(batchIntervalMs) ||
      batchIntervalMs < 500 ||
      batchIntervalMs > 300000
    ) {
      setActionStatus("Write batch interval must be 500–300000 ms.");
      setStatusError(true);
      return;
    }
    // `satisfies` rather than a bare argument: it rejects a key that is not
    // in the schema, so a rename or typo here becomes a compile error
    // instead of a value silently written under a key nothing reads.
    const next = {
      // Spread the stored configuration first so keys this panel does not
      // manage (questdbMemoryLimit, questdbCpuLimit, hand-edited extras)
      // survive a save — replacing the config wholesale silently stripped
      // them, including the schema's default memory cap (issue #98).
      ...cfg,
      questdbHost,
      questdbIlpPort,
      questdbHttpPort,
      questdbPgPort,
      questdbVersion,
      managedContainer,
      defaultSamplingRate,
      ilpFlushIntervalMs: batchIntervalMs,
      recordSelf,
      recordOthers,
      restoreOnStart,
      enableConsole,
      restoreMaxAgeMinutes,
      retentionDays,
      compression,
      compressionLevel,
      networkName,
      exposeToContainers,
      pathFilter: {
        mode: filterMode,
        paths: filterPaths
          .split("\n")
          .map((s) => s.trim())
          .filter(Boolean),
      },
      samplingRates: cfg.samplingRates || {},
    } satisfies Partial<Config>;
    save(next);
    setActionStatus("Saved! Plugin will restart with new configuration.");
    setStatusError(false);
  };

  const doExport = async () => {
    if (!exportFrom || !exportTo) {
      setActionStatus("Set both from and to dates for export.");
      setStatusError(true);
      return;
    }
    setExporting(true);
    setActionStatus(`Exporting ${exportFormat.toUpperCase()}...`);
    setStatusError(false);
    try {
      const url = `/plugins/signalk-questdb/api/export?from=${encodeURIComponent(exportFrom)}&to=${encodeURIComponent(exportTo)}&format=${exportFormat}`;
      const res = await fetch(url);
      if (!res.ok) {
        const data = (await res
          .json()
          .catch(() => ({ error: res.statusText }))) as ApiError;
        setActionStatus(
          `Export failed: ${apiText(data.error, "unknown error")}`,
        );
        setStatusError(true);
        setExporting(false);
        return;
      }
      const blob = await res.blob();
      const ext = exportFormat === "parquet" ? "parquet" : "csv";
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `signalk-export-${exportFrom.slice(0, 10)}.${ext}`;
      a.click();
      URL.revokeObjectURL(a.href);
      setActionStatus(`Exported ${(blob.size / 1024 / 1024).toFixed(1)} MB`);
    } catch (e) {
      setActionStatus(`Export failed: ${errorMessage(e)}`);
      setStatusError(true);
    }
    setExporting(false);
  };

  const isRunning = dbStatus && dbStatus.status === "running";
  const suspendedTables = (isRunning && dbStatus.suspendedTables) || [];
  const walSuspended = suspendedTables.length > 0;
  const schemaMismatch = (isRunning && dbStatus.schemaMismatch) || false;
  const ulimitClamp = (isRunning && dbStatus.ulimitClamp) || null;
  // Only surfaced when too low — a healthy limit needs no banner. NOT gated
  // on isRunning: mmap exhaustion is one of the ways QuestDB becomes
  // unhealthy, so the hint must stay visible while it is down (the status
  // endpoint reports the field on the unhealthy response too). The endpoint
  // re-reads /proc on every poll, so this clears by itself once the operator
  // applies the sysctl (no container restart needed).
  const maxMapCount =
    (dbStatus?.hostMaxMapCount?.tooLow && dbStatus.hostMaxMapCount) || null;

  return (
    <div style={S.root}>
      {/* QuestDB Status */}
      <div style={S.sectionTitle}>QuestDB Status</div>

      {/* Rendered outside the running/not-running branches: the status
          endpoint reports hostMaxMapCount even on the unhealthy response,
          and the remediation hint matters MOST while QuestDB is down. */}
      {maxMapCount && (
        <div style={S.infoBanner}>
          <div style={S.warnBannerTitle}>
            Host vm.max_map_count is low — {formatNumber(maxMapCount.current)}{" "}
            (QuestDB recommends {formatNumber(maxMapCount.recommended)})
          </div>
          QuestDB memory-maps every partition and WAL segment it touches. As the
          database grows, this kernel limit can run out — mmap then fails with
          out-of-memory errors even though RAM is free, which shows up as slow
          or failing history queries, can suspend recording, and can take
          QuestDB down entirely. Run this in a shell on the host machine — not
          inside the QuestDB container, which can neither change the
          kernel-global limit nor has sudo (takes effect immediately, no restart
          needed):
          <code style={S.warnBannerCode}>
            {`echo 'vm.max_map_count=${maxMapCount.recommended}' | sudo tee /etc/sysctl.d/99-questdb.conf && sudo sysctl --system`}
          </code>
        </div>
      )}

      {statusLoading ? (
        <div style={S.empty}>Checking QuestDB...</div>
      ) : isRunning ? (
        <>
          {walSuspended && (
            <div style={S.warnBanner}>
              <div style={S.warnBannerTitle}>
                QuestDB WAL suspended — recording stalled
              </div>
              Rows are arriving but no longer commit, so the counts below have
              stopped advancing. This follows an error while QuestDB applied the
              write-ahead log — see the reason reported per table below.
              Resuming replays every pending transaction, so no data is lost.
              <code style={S.warnBannerCode}>
                {suspendedTables
                  .map((t) => {
                    // Show QuestDB's own reason when it reports one, so the
                    // operator can tell a transient resource error from a
                    // poison transaction. Flatten any control characters —
                    // the reason renders on one line per table.
                    const reason = String(
                      t.errorMessage || t.errorTag || "reason not reported",
                    ).replace(/[\r\n]+/g, " ");
                    const diag = (walDiag?.tables || []).find(
                      (d) => d.name === t.name,
                    );
                    const since = diag?.suspendedSince
                      ? ` — frozen since ${diag.suspendedSince}`
                      : "";
                    return `${t.name}: ${formatNumber(t.txnLag)} txns behind — ${reason}${since}`;
                  })
                  .join("\n")}
              </code>
              {(walDiag?.tables || [])
                .filter((d) => d.applyError)
                .map((d) => (
                  <div key={d.name} style={{ marginTop: 8, fontSize: 12 }}>
                    QuestDB&apos;s own log reports the apply failure on{" "}
                    <code>{d.name}</code> as:
                    <code style={S.warnBannerCode}>{d.applyError}</code>
                  </div>
                ))}
              <div style={{ marginTop: 10 }}>
                <Button
                  variant="primary"
                  busy={resuming}
                  busyLabel="Resuming..."
                  disabled={skipping}
                  onClick={handleResumeWal}
                >
                  Resume recording
                </Button>
              </div>
              {suspendedTables
                .filter((t) => t.autoResume === "failed")
                .map((t) => {
                  const diag = (walDiag?.tables || []).find(
                    (d) => d.name === t.name,
                  );
                  // Torn applied partition: the writer dies opening the
                  // partition, so no RESUME WAL target helps. Say so instead
                  // of offering a skip that loses the backlog for nothing.
                  if (diag?.partitionOpenFailure) {
                    return (
                      <div key={t.name} style={{ marginTop: 12 }}>
                        <div style={S.warnBannerTitle}>
                          {t.name}: table data damaged — automatic repair not
                          possible
                        </div>
                        QuestDB fails while opening this table&apos;s stored
                        data, before it reaches any pending transaction. That
                        means the commit record on disk describes rows whose
                        data never made it to storage — the signature of a power
                        loss during a write. Skipping transactions cannot fix
                        this (the failure happens before them) and would discard
                        the {formatNumber(t.txnLag)} pending transactions for
                        nothing, so no repair button is offered. Recovering this
                        table means repairing its commit record by hand; please
                        open an issue with the error above. Recording continues
                        into the other tables, and &ldquo;Resume
                        recording&rdquo; will re-suspend this one within
                        milliseconds — it is safe to press, but it cannot repair
                        this damage.
                      </div>
                    );
                  }
                  const plan = diag?.skipPlan;
                  if (!plan) return null;
                  return (
                    <div key={t.name} style={{ marginTop: 12 }}>
                      <div style={S.warnBannerTitle}>
                        {t.name}: automatic resume failed — WAL segment
                        unreadable
                      </div>
                      A lossless resume was attempted and the table re-suspended
                      at the same transaction: the write-ahead-log segment at
                      the stall point cannot be read back (typically corrupted
                      by an unclean shutdown mid-write). Replaying can never get
                      past it. The only repair is to skip the unreadable
                      segment, losing{" "}
                      <b>{formatNumber(plan.skippedTxns)} transaction(s)</b>{" "}
                      recorded between <b>{plan.skipWindowStart}</b> and{" "}
                      <b>{plan.skipWindowEnd}</b>
                      {plan.tailSkip
                        ? " — and this is the newest pending segment, so skipping it drops the ENTIRE pending backlog"
                        : ` — everything after the skipped segment (${formatNumber(
                            Math.max(0, t.txnLag - plan.skippedTxns),
                          )} txns) is replayed without loss`}
                      .
                      <div style={{ marginTop: 8 }}>
                        <Button
                          variant="danger"
                          busy={skipping}
                          busyLabel="Skipping..."
                          disabled={resuming || walDiagStale}
                          onClick={() => handleSkipWal(t.name, plan)}
                        >
                          Skip unreadable segment (loses data)
                        </Button>
                        {walDiagStale && (
                          <div style={{ marginTop: 6, fontSize: 12 }}>
                            The diagnosis could not be refreshed — the numbers
                            above may be stale. The skip re-enables once a
                            refresh succeeds.
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              <div style={{ marginTop: 8, fontSize: 12 }}>
                Manual fallback in the QuestDB SQL console (<code>:9000</code>
                ), one statement at a time — avoid running it from several tabs,
                the console retries a busy statement forever:
                <code style={S.warnBannerCode}>
                  {suspendedTables
                    .map((t) => {
                      const quoted = `"${String(t.name).replace(/"/g, '""')}"`;
                      const diag = (walDiag?.tables || []).find(
                        (d) => d.name === t.name,
                      );
                      const lines = [`ALTER TABLE ${quoted} RESUME WAL;`];
                      // The FROM TXN statement bypasses the server-side plan
                      // re-validation, so only print it while the displayed
                      // diagnosis is confirmed current.
                      if (
                        t.autoResume === "failed" &&
                        diag?.skipPlan &&
                        !walDiagStale
                      ) {
                        lines.push(
                          `-- if the resume re-suspends: skip the unreadable segment (loses ${formatNumber(diag.skipPlan.skippedTxns)} txns)`,
                          `ALTER TABLE ${quoted} RESUME WAL FROM TXN ${diag.skipPlan.skipToTxn};`,
                        );
                      }
                      return lines.join("\n");
                    })
                    .join("\n")}
                </code>
              </div>
            </div>
          )}
          {/* Rendered outside the suspended banner: a successful resume clears
              walSuspended on the very next status fetch, which unmounts the
              banner — the confirmation must survive that. */}
          {resumeOutcome && <div style={S.infoBanner}>{resumeOutcome}</div>}
          {schemaMismatch && (
            <div style={S.warnBanner}>
              <div style={S.warnBannerTitle}>
                QuestDB table schema mismatch — data not readable
              </div>
              A QuestDB table was re-created by ILP ingestion with the wrong
              timestamp column, so rows are being stored but reads filtering on
              time (history, Grafana, and the counts above) return nothing. The
              plugin rebuilds the affected table with the correct schema
              automatically within a minute; if this persists, restart the
              plugin. (The few rows written into the wrong-schema table are lost
              — they were unreadable anyway.)
            </div>
          )}
          {ulimitClamp && (
            <div style={S.infoBanner}>
              <div style={S.warnBannerTitle}>
                QuestDB open-files limit capped by the host
              </div>
              QuestDB requested a <code>{ulimitClamp.ulimit}</code> limit of{" "}
              {formatNumber(ulimitClamp.requested)}, but this host only allows{" "}
              {formatNumber(ulimitClamp.granted)}, so it was capped. QuestDB is
              running on the lower limit. To grant the full value, raise the
              host limit for the user running the container runtime — under
              rootless Podman that means a systemd <code>user@.service</code>{" "}
              <code>LimitNOFILE</code> drop-in (editing{" "}
              <code>/etc/security/limits.conf</code> alone is usually not
              enough) — then restart the Signal K server so the plugin can
              re-create the QuestDB container with the raised limit
              (signalk-container 1.25.3+; on older versions remove the container
              once with <code>podman rm -f sk-signalk-questdb</code> instead).
              This warning clears itself once the container is running with the
              full value.{" "}
              <a
                href="https://github.com/dirkwa/signalk-container#raising-the-open-files-limit-nofile"
                target="_blank"
                rel="noreferrer"
                style={{ color: "#92400e", textDecoration: "underline" }}
              >
                Step-by-step instructions
              </a>
              .
            </div>
          )}
          <StatusCard
            icon="Q"
            iconBackground="#7c3aed"
            iconColor="#fff"
            title="QuestDB"
            meta={
              <>
                {dbStatus?.endpoint || `${questdbHost}:${questdbHttpPort}`}{" "}
                &middot; {walSuspended ? "WAL suspended" : "Recording"}
              </>
            }
            state={walSuspended ? "error" : "ok"}
            stateTitle={walSuspended ? "WAL suspended" : "Running"}
          />

          <div style={S.statsGrid}>
            <div style={S.statCard}>
              <div style={S.statValue}>{formatNumber(dbStatus.totalRows)}</div>
              <div style={S.statLabel}>Total Rows</div>
            </div>
            <div style={S.statCard}>
              <div style={S.statValue}>
                {formatNumber(dbStatus.activePathsToday)}
              </div>
              <div style={S.statLabel}>Active Paths Today</div>
            </div>
            {/* Only when a restore actually ran. Absent means the feature is
                off, or the query is still in flight — showing "0" for either
                would read as "it ran and found nothing". */}
            {dbStatus.restore && (
              <div style={S.statCard}>
                <div style={S.statValue}>
                  {dbStatus.restore.failed
                    ? "—"
                    : formatNumber(dbStatus.restore.contexts)}
                </div>
                <div style={S.statLabel}>
                  {dbStatus.restore.failed
                    ? "Restore failed"
                    : "Vessels Restored"}
                </div>
              </div>
            )}
          </div>

          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 10,
              marginBottom: 12,
            }}
          >
            {updateInfo && updateInfo.updateAvailable ? (
              <>
                <span style={{ fontSize: 13 }}>
                  v{updateInfo.currentVersion} &rarr;{" "}
                  <strong>v{updateInfo.latestVersion}</strong> available
                </span>
                <Button
                  variant="primary"
                  busy={updating}
                  busyLabel="Updating..."
                  onClick={applyUpdate}
                  style={UPDATE_BTN}
                >
                  Update QuestDB
                </Button>
              </>
            ) : updateInfo && !updateInfo.updateAvailable ? (
              <span style={{ fontSize: 12, color: "#888" }}>
                v{updateInfo.currentVersion} (up to date)
              </span>
            ) : (
              <Button
                variant="secondary"
                busy={checkingUpdate}
                busyLabel="Checking..."
                onClick={checkForUpdate}
                style={UPDATE_BTN}
              >
                Check for updates
              </Button>
            )}
          </div>
        </>
      ) : (
        // No explicit icon colours: the error state supplies the shared
        // muted-red treatment, which is where the local #fef2f2/#ef4444 pair
        // came from in the first place.
        <StatusCard
          icon="Q"
          title="QuestDB"
          meta={
            <>
              {dbStatus?.status === "unhealthy"
                ? "Not responding"
                : "Not running"}
              {managedContainer ? " — enable plugin to start container" : ""}
            </>
          }
          state="error"
        />
      )}

      {/* Image Version */}
      <div style={S.sectionTitle}>Image Version</div>

      <div style={S.fieldRow}>
        <span style={S.label}>QuestDB version</span>
        {/* The hand-rolled <select> had no option for a value outside the
            listed releases. A pinned tag that aged out of the top 3 — or any
            tag at all when GitHub rate-limited the listing and `versions` came
            back empty — rendered the box blank on "latest", and doSave writes
            questdbVersion unconditionally, so the next Save silently changed
            the running image. VersionSelect injects a synthetic
            "<tag> (running)" option instead. */}
        <VersionSelect
          value={questdbVersion}
          onChange={setQuestdbVersion}
          versions={versions}
          loading={versionsLoading}
          onRefresh={() => void fetchVersions()}
          error={versionsError}
          stableCount={3}
          preCount={2}
        />
      </div>

      {/* Connection Settings */}
      <div style={S.sectionTitle}>Connection</div>

      <div style={S.fieldRow}>
        <span style={S.label}>Managed container</span>
        <input
          type="checkbox"
          style={S.checkbox}
          checked={managedContainer}
          onChange={(e) => setManagedContainer(e.target.checked)}
        />
        <span style={S.hint}>
          {managedContainer
            ? "signalk-container manages QuestDB"
            : "Connect to external QuestDB"}
        </span>
      </div>

      {/* External mode: the user points the plugin at their own QuestDB, so
          host + connect ports are what we use. In managed mode the address is
          resolved automatically (signalk-container), so these are hidden. */}
      {!managedContainer && (
        <>
          <div style={S.fieldRow}>
            <span style={S.label}>QuestDB host</span>
            <input
              style={S.input}
              value={questdbHost}
              onChange={(e) => setQuestdbHost(e.target.value)}
            />
          </div>

          <div style={S.fieldRow}>
            <span style={S.label}>HTTP port (queries)</span>
            <input
              style={S.inputSmall}
              type="number"
              value={questdbHttpPort}
              onChange={(e) => setQuestdbHttpPort(Number(e.target.value))}
            />
          </div>

          <div style={S.fieldRow}>
            <span style={S.label}>ILP port (writes)</span>
            <input
              style={S.inputSmall}
              type="number"
              value={questdbIlpPort}
              onChange={(e) => setQuestdbIlpPort(Number(e.target.value))}
            />
          </div>
        </>
      )}

      {/* Managed mode: connectivity is automatic. "Bind to 0.0.0.0" is the one
          switch that matters — turning it on publishes QuestDB on the LAN and
          reveals the host-port/network knobs that path uses. */}
      {managedContainer && (
        <div style={S.fieldRow}>
          <span style={S.label}>Bind to 0.0.0.0</span>
          <input
            type="checkbox"
            style={S.checkbox}
            checked={exposeToContainers}
            onChange={(e) => setExposeToContainers(e.target.checked)}
          />
          <span
            style={{
              ...S.hint,
              color: exposeToContainers ? "#ef4444" : undefined,
            }}
          >
            {exposeToContainers
              ? "Caution! This exposes your data to the network"
              : "Signal K reaches QuestDB automatically — only enable to reach it from another machine or a separate-Docker Grafana"}
          </span>
        </div>
      )}

      {managedContainer && exposeToContainers && (
        <>
          <div style={S.fieldRow}>
            <span style={S.label}>HTTP port (queries)</span>
            <input
              style={S.inputSmall}
              type="number"
              value={questdbHttpPort}
              onChange={(e) => setQuestdbHttpPort(Number(e.target.value))}
            />
          </div>

          <div style={S.fieldRow}>
            <span style={S.label}>ILP port (writes)</span>
            <input
              style={S.inputSmall}
              type="number"
              value={questdbIlpPort}
              onChange={(e) => setQuestdbIlpPort(Number(e.target.value))}
            />
          </div>

          <div style={S.fieldRow}>
            <span style={S.label}>PostgreSQL wire port</span>
            <input
              style={S.inputSmall}
              type="number"
              value={questdbPgPort}
              onChange={(e) => setQuestdbPgPort(Number(e.target.value))}
            />
            <span style={S.hint}>for Grafana</span>
          </div>

          <div style={S.fieldRow}>
            <span style={S.label}>Container network</span>
            <input
              style={S.input}
              value={networkName}
              onChange={(e) => setNetworkName(e.target.value)}
            />
            <span style={S.hint}>shared with signalk-grafana</span>
          </div>
        </>
      )}

      {/* Recording */}
      <div style={S.sectionTitle}>Recording</div>

      <div style={S.fieldRow}>
        <span style={S.label}>Default sampling rate (ms)</span>
        <input
          style={S.inputSmall}
          type="number"
          value={defaultSamplingRate}
          onChange={(e) => setDefaultSamplingRate(Number(e.target.value))}
        />
        <span style={S.hint}>
          1000 = max 1 write/sec per path (0 = every update)
        </span>
      </div>

      <div style={S.fieldRow}>
        <span style={S.label}>Write batch interval (ms)</span>
        <input
          style={S.inputSmall}
          type="number"
          min={500}
          max={300000}
          aria-label="Write batch interval in milliseconds"
          value={ilpFlushIntervalMs}
          onChange={(e) => setIlpFlushIntervalMs(e.target.value)}
        />
        <span style={S.hint}>
          one QuestDB transaction per table per interval; longer = bigger
          batches, but up to this many ms lost on a hard crash (default 5000)
        </span>
      </div>

      <div style={S.fieldRow}>
        <span style={S.label}>Record own vessel</span>
        <input
          type="checkbox"
          style={S.checkbox}
          checked={recordSelf}
          onChange={(e) => setRecordSelf(e.target.checked)}
        />
      </div>

      <div style={S.fieldRow}>
        <span style={S.label}>Record AIS targets</span>
        <input
          type="checkbox"
          style={S.checkbox}
          checked={recordOthers}
          onChange={(e) => setRecordOthers(e.target.checked)}
        />
      </div>

      <div style={S.fieldRow}>
        <span style={S.label}>Restore vessels on startup</span>
        <input
          type="checkbox"
          style={S.checkbox}
          aria-label="Restore vessels on startup"
          checked={restoreOnStart}
          onChange={(e) => setRestoreOnStart(e.target.checked)}
        />
        <span style={S.hint}>
          replay each vessel&apos;s last recorded position after a restart, so
          AIS targets appear at once instead of only when they next transmit
        </span>
      </div>

      <div style={S.fieldRow}>
        <span style={S.label}>QuestDB console webapp</span>
        <input
          type="checkbox"
          style={S.checkbox}
          aria-label="QuestDB console webapp"
          checked={enableConsole}
          onChange={(e) => setEnableConsole(e.target.checked)}
        />
        <span style={S.hint}>
          serve QuestDB&apos;s own console inside the Signal K admin UI (admin
          only). It is a full SQL client — unlike the read-only query endpoint
          it can modify and delete recorded data
        </span>
      </div>

      {restoreOnStart && (
        <div style={S.fieldRow}>
          <span style={S.label}>Restore max age (minutes)</span>
          <input
            style={S.inputSmall}
            type="number"
            min={1}
            aria-label="Restore max age (minutes)"
            value={restoreMaxAgeMinutes}
            onChange={(e) => {
              // Clearing the field yields "" → NaN, which would be saved and
              // then silently replaced by the default. Hold the last valid
              // number instead.
              const next = Number(e.target.value);
              if (Number.isFinite(next) && next >= 1) {
                setRestoreMaxAgeMinutes(next);
              }
            }}
          />
          <span style={S.hint}>
            only replay values this recent (default 9, matching Freeboard&apos;s
            AIS expiry); higher puts staler positions on the chart
          </span>
        </div>
      )}

      <div style={S.fieldRow}>
        <span style={S.label}>Retention (days)</span>
        <input
          style={S.inputSmall}
          type="number"
          value={retentionDays}
          onChange={(e) => setRetentionDays(Number(e.target.value))}
        />
        <span style={S.hint}>0 = keep forever</span>
      </div>

      {/* Open on mount when patterns are already configured, so a user who has
          set filtering sees it rather than a collapsed header. Keyed on the
          paths, not the mode: mode defaults to "exclude" and is never empty,
          so keying on it would open this for everyone and say nothing.

          defaultOpen is read ONCE, on mount. That works here only because the
          host has the configuration in hand before the panel renders —
          signalk-server seeds it via useState(plugin.data.configuration) and
          passes it straight down, so there is no render where it is absent
          and then arrives. Deriving this from anything the panel fetches
          itself would seed false forever and lose the behaviour silently. */}
      <CollapsibleSection
        title="Path filtering"
        defaultOpen={filterPaths.trim() !== ""}
      >
        <div style={S.fieldRow}>
          <span style={S.label}>Filter mode</span>
          <select
            style={S.select}
            value={filterMode}
            // Narrow rather than cast, matching how the state is hydrated:
            // a <select> value is only ever `string` to the type system.
            onChange={(e) =>
              setFilterMode(
                e.target.value === "include" ? "include" : "exclude",
              )
            }
          >
            <option value="exclude">Exclude matching paths</option>
            <option value="include">Include only matching paths</option>
          </select>
        </div>
        <div style={{ marginBottom: 10 }}>
          <div style={{ ...S.label, width: "auto", marginBottom: 6 }}>
            Path patterns (one per line, glob supported) — optional
          </div>
          {/* The examples match the schema's own description rather than
              inventing copy. navigation.position used to lead here, and as
              grey placeholder text inside an empty box it read as a value:
              a user concluded their position was excluded by default when
              nothing is filtered at all (issue #123). */}
          <textarea
            style={S.textarea}
            value={filterPaths}
            onChange={(e) => setFilterPaths(e.target.value)}
            placeholder={"notifications.*\nenvironment.wind.*"}
          />
          <div style={S.fieldHelp}>
            Empty = record everything (the default). Nothing is filtered out
            unless you list a pattern here.
          </div>
          <div style={{ ...S.hint, marginLeft: 0 }}>
            {filterMode === "exclude"
              ? "These paths are NOT recorded; everything else is."
              : "ONLY these paths are recorded; everything else is dropped."}
          </div>
        </div>
      </CollapsibleSection>

      <CollapsibleSection title="Compression (on-disk)">
        <div style={S.fieldRow}>
          <span style={S.label}>Compression codec</span>
          <select
            style={S.select}
            value={compression}
            onChange={(e) => setCompression(toCompression(e.target.value))}
          >
            <option value="none">None</option>
            <option value="lz4">LZ4 (fast)</option>
            <option value="zstd">ZSTD (smaller)</option>
          </select>
          <span style={S.hint}>applies to new data after save</span>
        </div>

        {compression === "zstd" && (
          <div style={S.fieldRow}>
            <span style={S.label}>ZSTD level</span>
            <input
              style={S.inputSmall}
              type="number"
              min="1"
              max="22"
              value={compressionLevel}
              onChange={(e) => setCompressionLevel(Number(e.target.value))}
            />
            <span style={S.hint}>1 (fast) to 22 (smallest)</span>
          </div>
        )}
      </CollapsibleSection>

      <CollapsibleSection title="InfluxDB Migration">
        <div
          style={{
            display: "flex",
            gap: 10,
            alignItems: "center",
            marginBottom: 10,
          }}
        >
          <Button
            variant="primary"
            busy={migrationDetecting}
            busyLabel="Detecting..."
            onClick={detectMigration}
          >
            Detect InfluxDB
          </Button>
          <span style={S.hint}>
            Checks localhost:8086 for InfluxDB 1.x and 2.x
          </span>
        </div>

        {migrationSources && migrationSources.length > 0 && (
          <div>
            {migrationSources.map((src, i) => (
              <div key={i} style={S.migrationItem}>
                <div
                  style={{
                    ...S.cardIcon,
                    width: 36,
                    height: 36,
                    fontSize: 16,
                    background:
                      src.type === "influxdb2" ? "#020a47" : "#22adf6",
                    color: "#fff",
                  }}
                >
                  {src.type === "influxdb2" ? "2" : "1"}
                </div>
                <div style={S.cardInfo}>
                  <div style={S.cardTitle}>
                    InfluxDB {src.type === "influxdb2" ? "2.x" : "1.x"}
                    <span style={{ ...S.tag, ...S.tagLatest }}>
                      {src.status}
                    </span>
                  </div>
                  <div style={S.cardMeta}>
                    {src.url} &middot; v{src.version}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        <div style={{ ...S.fieldRow, marginTop: 10 }}>
          <span style={S.label}>Manual InfluxDB URL</span>
          <input
            style={S.input}
            placeholder="http://192.168.1.100:8086"
            value={migrationUrl}
            onChange={(e) => setMigrationUrl(e.target.value)}
          />
          <span style={S.hint}>for remote instances</span>
        </div>

        {migrationSources &&
          migrationSources.length === 0 &&
          !migrationDetecting && (
            <div style={{ ...S.empty, padding: "16px", textAlign: "left" }}>
              No InfluxDB found on localhost. Use manual URL above for remote
              instances.
            </div>
          )}
      </CollapsibleSection>

      <CollapsibleSection title="Data Export">
        <div style={S.fieldRow}>
          <span style={S.label}>From</span>
          <input
            style={S.input}
            type="datetime-local"
            value={exportFrom}
            onChange={(e) => setExportFrom(e.target.value)}
          />
        </div>

        <div style={S.fieldRow}>
          <span style={S.label}>To</span>
          <input
            style={S.input}
            type="datetime-local"
            value={exportTo}
            onChange={(e) => setExportTo(e.target.value)}
          />
        </div>

        <div style={S.fieldRow}>
          <span style={S.label}>Format</span>
          <select
            style={S.select}
            value={exportFormat}
            onChange={(e) => setExportFormat(e.target.value)}
          >
            <option value="parquet">Parquet</option>
            <option value="csv">CSV</option>
          </select>
        </div>

        <Button
          variant="primary"
          busy={exporting}
          busyLabel="Exporting..."
          onClick={doExport}
        >
          Export Data
        </Button>
      </CollapsibleSection>

      {cfg.managedContainer !== false && (
        <CollapsibleSection title="Danger zone">
          <div style={{ fontSize: 13, color: "#555", marginBottom: 10 }}>
            Remove the QuestDB container and permanently delete all recorded
            data. Use this to fully reset QuestDB — Signal K's plugin-uninstall
            cannot delete this data on rootless Podman. This cannot be undone.
          </div>
          <Button
            variant="danger"
            busy={purging}
            busyLabel="Removing..."
            onClick={purgeData}
          >
            Remove container &amp; all data
          </Button>
        </CollapsibleSection>
      )}

      <ActionStatus
        message={actionStatus}
        error={statusError}
        style={{ marginTop: 16 }}
      />

      <div style={{ marginTop: 24 }}>
        <Button onClick={doSave}>Save Configuration</Button>
      </div>
    </div>
  );
}

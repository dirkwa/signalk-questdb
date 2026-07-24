import React, { useState, useEffect, useCallback, useRef } from "react";

const S = {
  root: {
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    color: "#333",
    padding: "16px 0",
  },
  sectionTitle: {
    fontSize: 13,
    fontWeight: 600,
    color: "#888",
    textTransform: "uppercase",
    letterSpacing: "0.05em",
    marginBottom: 10,
    marginTop: 24,
  },
  btn: {
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    padding: "8px 16px",
    border: "none",
    borderRadius: 6,
    fontSize: 13,
    fontWeight: 600,
    cursor: "pointer",
  },
  btnPrimary: { background: "#3b82f6", color: "#fff" },
  btnSave: { background: "#3b82f6", color: "#fff" },
  btnDanger: {
    background: "#ef4444",
    color: "#fff",
    padding: "6px 12px",
    fontSize: 12,
  },
  btnDisabled: { opacity: 0.5, cursor: "not-allowed" },
  status: { marginTop: 8, fontSize: 12, minHeight: 18 },
  warnBanner: {
    padding: "12px 16px",
    background: "#fef2f2",
    border: "1px solid #fecaca",
    borderRadius: 10,
    marginBottom: 12,
    fontSize: 13,
    color: "#991b1b",
    lineHeight: 1.5,
  },
  warnBannerTitle: { fontWeight: 700, marginBottom: 4 },
  infoBanner: {
    padding: "12px 16px",
    background: "#fffbeb",
    border: "1px solid #fde68a",
    borderRadius: 10,
    marginBottom: 12,
    fontSize: 13,
    color: "#92400e",
    lineHeight: 1.5,
  },
  warnBannerCode: {
    display: "block",
    marginTop: 8,
    padding: "8px 10px",
    background: "#fff",
    border: "1px solid #fecaca",
    borderRadius: 6,
    fontFamily: "monospace",
    fontSize: 12,
    color: "#7f1d1d",
    whiteSpace: "pre-wrap",
    wordBreak: "break-word",
  },
  card: {
    display: "flex",
    alignItems: "center",
    gap: 14,
    padding: "14px 18px",
    background: "#f8f9fa",
    border: "1px solid #e0e0e0",
    borderRadius: 10,
    marginBottom: 12,
  },
  cardIcon: {
    width: 44,
    height: 44,
    borderRadius: 10,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: 20,
    fontWeight: 700,
    flexShrink: 0,
  },
  cardInfo: { flex: 1 },
  cardTitle: { fontSize: 15, fontWeight: 600, color: "#333" },
  cardMeta: { fontSize: 12, color: "#888" },
  stateIndicator: {
    width: 10,
    height: 10,
    borderRadius: "50%",
    flexShrink: 0,
  },
  fieldRow: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    marginBottom: 10,
  },
  label: {
    fontSize: 13,
    fontWeight: 500,
    color: "#555",
    width: 180,
    flexShrink: 0,
  },
  select: {
    padding: "6px 10px",
    borderRadius: 6,
    border: "1px solid #ccc",
    fontSize: 13,
    background: "#fff",
    color: "#333",
    minWidth: 200,
  },
  input: {
    padding: "6px 10px",
    borderRadius: 6,
    border: "1px solid #ccc",
    fontSize: 13,
    background: "#fff",
    color: "#333",
    width: 200,
  },
  inputSmall: {
    padding: "6px 10px",
    borderRadius: 6,
    border: "1px solid #ccc",
    fontSize: 13,
    background: "#fff",
    color: "#333",
    width: 80,
  },
  checkbox: { width: 16, height: 16, accentColor: "#3b82f6" },
  hint: { fontSize: 11, color: "#aaa", marginLeft: 8 },
  textarea: {
    padding: "6px 10px",
    borderRadius: 6,
    border: "1px solid #ccc",
    fontSize: 13,
    fontFamily: "monospace",
    background: "#fff",
    color: "#333",
    width: "100%",
    minHeight: 70,
    boxSizing: "border-box",
    resize: "vertical",
  },
  empty: {
    textAlign: "center",
    padding: "30px 16px",
    color: "#999",
    fontSize: 13,
  },
  tag: {
    display: "inline-block",
    padding: "2px 8px",
    borderRadius: 4,
    fontSize: 10,
    fontWeight: 600,
    marginLeft: 8,
  },
  tagPre: { background: "#fef3c7", color: "#92400e" },
  tagLatest: { background: "#dcfce7", color: "#166534" },
  migrationItem: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "12px 16px",
    background: "#f8f9fa",
    border: "1px solid #e0e0e0",
    borderRadius: 10,
    marginBottom: 8,
  },
  migrationActions: {
    display: "flex",
    gap: 8,
    alignItems: "center",
  },
  statsGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
    gap: 10,
    marginBottom: 12,
  },
  statCard: {
    padding: "12px 16px",
    background: "#f8f9fa",
    border: "1px solid #e0e0e0",
    borderRadius: 10,
    textAlign: "center",
  },
  statValue: { fontSize: 22, fontWeight: 700, color: "#333" },
  statLabel: { fontSize: 11, color: "#888", marginTop: 2 },
};

function CollapsibleSection({ title, children }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      {/* A real <button> so keyboard users can toggle with Enter/Space and
          screen readers announce expanded state; the style resets the default
          button chrome so it still reads as a section title. */}
      <button
        type="button"
        aria-expanded={open}
        style={{
          ...S.sectionTitle,
          cursor: "pointer",
          userSelect: "none",
          display: "flex",
          alignItems: "center",
          gap: 6,
          width: "100%",
          textAlign: "left",
          background: "none",
          border: "none",
          padding: 0,
        }}
        onClick={() => setOpen(!open)}
      >
        <span
          style={{
            fontSize: 10,
            transition: "transform 0.15s",
            transform: open ? "rotate(90deg)" : "rotate(0deg)",
          }}
        >
          {"\u25b6"}
        </span>
        {title}
      </button>
      {open && <div style={{ marginBottom: 16 }}>{children}</div>}
    </div>
  );
}

function formatNumber(n) {
  if (n === null || n === undefined) return "—";
  if (n >= 1000000) return (n / 1000000).toFixed(1) + "M";
  if (n >= 1000) return (n / 1000).toFixed(1) + "K";
  return String(n);
}

export default function PluginConfigurationPanel({ configuration, save }) {
  const cfg = configuration || {};

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
  const [recordOthers, setRecordOthers] = useState(cfg.recordOthers || false);
  const [defaultSamplingRate, setDefaultSamplingRate] = useState(
    cfg.defaultSamplingRate ?? 2000,
  );
  const [retentionDays, setRetentionDays] = useState(cfg.retentionDays || 0);
  // Hydrate defensively: a hand-edited or corrupted config could carry a bad
  // mode or a non-array `paths`, and an unguarded `.join()` would crash the
  // whole panel render.
  const [filterMode, setFilterMode] = useState(
    cfg.pathFilter?.mode === "include" ? "include" : "exclude",
  );
  // One glob per line in the textarea; round-tripped to/from the schema's
  // pathFilter.paths string array.
  const [filterPaths, setFilterPaths] = useState(
    (Array.isArray(cfg.pathFilter?.paths) ? cfg.pathFilter.paths : []).join(
      "\n",
    ),
  );
  const [compression, setCompression] = useState(cfg.compression || "lz4");
  const [compressionLevel, setCompressionLevel] = useState(
    cfg.compressionLevel || 3,
  );
  const [networkName, setNetworkName] = useState(
    cfg.networkName || "sk-network",
  );
  const [exposeToContainers, setExposeToContainers] = useState(
    cfg.exposeToContainers || false,
  );

  const [versions, setVersions] = useState([]);
  const [versionsLoading, setVersionsLoading] = useState(false);
  const [dbStatus, setDbStatus] = useState(null);
  const [statusLoading, setStatusLoading] = useState(true);
  const [migrationSources, setMigrationSources] = useState(null);
  const [migrationDetecting, setMigrationDetecting] = useState(false);
  const [migrationUrl, setMigrationUrl] = useState("");
  const [actionStatus, setActionStatus] = useState("");
  const [statusError, setStatusError] = useState(false);
  const [updateInfo, setUpdateInfo] = useState(null);
  const [checkingUpdate, setCheckingUpdate] = useState(false);
  const [updating, setUpdating] = useState(false);
  const [purging, setPurging] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [resumeOutcome, setResumeOutcome] = useState("");
  const [walDiag, setWalDiag] = useState(null);
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

  const fetchVersions = useCallback(async () => {
    setVersionsLoading(true);
    try {
      const res = await fetch("/plugins/signalk-questdb/api/versions");
      if (res.ok) {
        const data = await res.json();
        setVersions(data);
      }
    } catch {
      // offline or error
    }
    setVersionsLoading(false);
  }, []);

  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch("/plugins/signalk-questdb/api/status");
      // Parse the body on non-2xx too: the unhealthy 503 carries fields the
      // panel must still surface (hostMaxMapCount — mmap exhaustion is one
      // of the ways QuestDB becomes unhealthy). Unparseable bodies fall
      // through to the plain not_running shape.
      const body = await res.json().catch(() => null);
      setDbStatus(
        body && typeof body === "object" ? body : { status: "not_running" },
      );
    } catch {
      setDbStatus({ status: "not_running" });
    }
    setStatusLoading(false);
  }, []);

  const fetchWalDiagnosis = useCallback(async (signal) => {
    const gen = ++walDiagGen.current;
    try {
      const res = await fetch("/plugins/signalk-questdb/api/wal-diagnosis", {
        signal,
      });
      if (gen !== walDiagGen.current) return;
      if (res.ok) {
        const data = await res.json();
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
    let timer = null;
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

  const handleSkipWal = async (tableName, plan) => {
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
      const data = await res.json().catch(() => ({ error: res.statusText }));
      if (res.ok) {
        setResumeOutcome(data.message);
      } else {
        setResumeOutcome(`Skip failed: ${data.error}`);
      }
      fetchStatus();
      fetchWalDiagnosis();
    } catch (e) {
      setResumeOutcome(`Skip failed: ${e.message}`);
    }
    setSkipping(false);
  };

  const checkForUpdate = async () => {
    setCheckingUpdate(true);
    try {
      const res = await fetch("/plugins/signalk-questdb/api/update/check");
      if (res.ok) {
        setUpdateInfo(await res.json());
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
        const data = await res.json();
        setActionStatus(data.message);
        setUpdateInfo(null);
        if (data.newVersion) {
          setQuestdbVersion(data.newVersion);
        }
        fetchStatus();
      } else {
        const data = await res.json().catch(() => ({ error: res.statusText }));
        setActionStatus(`Update failed: ${data.error}`);
        setStatusError(true);
      }
    } catch (e) {
      setActionStatus(`Update failed: ${e.message}`);
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
      const data = await res.json().catch(() => ({ error: res.statusText }));
      if (res.ok) {
        setActionStatus(data.message);
        fetchStatus();
      } else {
        setActionStatus(`Remove failed: ${data.error}`);
        setStatusError(true);
      }
    } catch (e) {
      setActionStatus(`Remove failed: ${e.message}`);
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
      const data = await res.json().catch(() => ({ error: res.statusText }));
      if (res.ok) {
        const failed = (data.results || []).filter((r) => !r.ok);
        setResumeOutcome(
          failed.length === 0
            ? `${data.message}. Recording resumes as the backlog drains — the banner clears once tables catch up.`
            : `${data.message}. ` +
                failed.map((r) => `${r.table}: ${r.error}`).join(" · "),
        );
        fetchStatus();
        fetchWalDiagnosis();
      } else {
        setResumeOutcome(`Resume failed: ${data.error}`);
      }
    } catch (e) {
      setResumeOutcome(`Resume failed: ${e.message}`);
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
        const data = await res.json();
        setMigrationSources(data.sources);
        if (data.sources.length === 0) {
          setActionStatus(
            migrationUrl
              ? `No InfluxDB found at ${migrationUrl}.`
              : "No InfluxDB instances detected on localhost:8086.",
          );
          setStatusError(false);
        }
      }
    } catch (e) {
      setActionStatus("Detection failed: " + e.message);
      setStatusError(true);
    }
    setMigrationDetecting(false);
  };

  useEffect(() => {
    fetchVersions();
    fetchStatus();
    const interval = setInterval(fetchStatus, 5000);
    return () => clearInterval(interval);
  }, [fetchVersions, fetchStatus]);

  const [exportFrom, setExportFrom] = useState("");
  const [exportTo, setExportTo] = useState("");
  const [exportFormat, setExportFormat] = useState("parquet");
  const [exporting, setExporting] = useState(false);

  const doSave = () => {
    save({
      questdbHost,
      questdbIlpPort,
      questdbHttpPort,
      questdbPgPort,
      questdbVersion,
      managedContainer,
      defaultSamplingRate,
      recordSelf,
      recordOthers,
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
    });
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
        const data = await res.json().catch(() => ({ error: res.statusText }));
        setActionStatus(`Export failed: ${data.error}`);
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
      setActionStatus(`Export failed: ${e.message}`);
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

  // Build version options: latest first, then pre-releases, then stable
  const stableVersions = versions.filter((v) => !v.prerelease).slice(0, 3);
  const preVersions = versions.filter((v) => v.prerelease).slice(0, 2);

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
                <button
                  style={{
                    ...S.btn,
                    ...S.btnPrimary,
                    ...(resuming || skipping ? S.btnDisabled : {}),
                  }}
                  disabled={resuming || skipping}
                  onClick={handleResumeWal}
                >
                  {resuming ? "Resuming..." : "Resume recording"}
                </button>
              </div>
              {suspendedTables
                .filter((t) => t.autoResume === "failed")
                .map((t) => {
                  const diag = (walDiag?.tables || []).find(
                    (d) => d.name === t.name,
                  );
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
                        <button
                          style={{
                            ...S.btn,
                            ...S.btnDanger,
                            ...(skipping || resuming || walDiagStale
                              ? S.btnDisabled
                              : {}),
                          }}
                          disabled={skipping || resuming || walDiagStale}
                          onClick={() => handleSkipWal(t.name, plan)}
                        >
                          {skipping
                            ? "Skipping..."
                            : "Skip unreadable segment (loses data)"}
                        </button>
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
              enough) — then restart the QuestDB container.{" "}
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
          <div style={S.card}>
            <div
              style={{ ...S.cardIcon, background: "#7c3aed", color: "#fff" }}
            >
              Q
            </div>
            <div style={S.cardInfo}>
              <div style={S.cardTitle}>QuestDB</div>
              <div style={S.cardMeta}>
                {dbStatus?.endpoint || `${questdbHost}:${questdbHttpPort}`}{" "}
                &middot; {walSuspended ? "WAL suspended" : "Recording"}
              </div>
            </div>
            <div
              style={{
                ...S.stateIndicator,
                background: walSuspended ? "#ef4444" : "#10b981",
              }}
              title={walSuspended ? "WAL suspended" : "Running"}
            />
          </div>

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
                <button
                  style={{
                    ...S.btn,
                    ...S.btnPrimary,
                    padding: "4px 12px",
                    fontSize: 12,
                    ...(updating ? S.btnDisabled : {}),
                  }}
                  onClick={applyUpdate}
                  disabled={updating}
                >
                  {updating ? "Updating..." : "Update QuestDB"}
                </button>
              </>
            ) : updateInfo && !updateInfo.updateAvailable ? (
              <span style={{ fontSize: 12, color: "#888" }}>
                v{updateInfo.currentVersion} (up to date)
              </span>
            ) : (
              <button
                style={{
                  ...S.btn,
                  padding: "4px 12px",
                  fontSize: 12,
                  background: "#f1f5f9",
                  color: "#475569",
                  border: "1px solid #e2e8f0",
                  ...(checkingUpdate ? S.btnDisabled : {}),
                }}
                onClick={checkForUpdate}
                disabled={checkingUpdate}
              >
                {checkingUpdate ? "Checking..." : "Check for updates"}
              </button>
            )}
          </div>
        </>
      ) : (
        <div style={S.card}>
          <div
            style={{ ...S.cardIcon, background: "#fef2f2", color: "#ef4444" }}
          >
            Q
          </div>
          <div style={S.cardInfo}>
            <div style={S.cardTitle}>QuestDB</div>
            <div style={S.cardMeta}>
              {dbStatus?.status === "unhealthy"
                ? "Not responding"
                : "Not running"}
              {managedContainer ? " — enable plugin to start container" : ""}
            </div>
          </div>
          <div style={{ ...S.stateIndicator, background: "#ef4444" }} />
        </div>
      )}

      {/* Image Version */}
      <div style={S.sectionTitle}>Image Version</div>

      <div style={S.fieldRow}>
        <span style={S.label}>QuestDB version</span>
        <select
          style={S.select}
          value={questdbVersion}
          onChange={(e) => setQuestdbVersion(e.target.value)}
        >
          <option value="latest">latest (recommended)</option>
          {preVersions.map((v) => (
            <option key={v.tag} value={v.tag}>
              {v.tag} (pre-release)
            </option>
          ))}
          {stableVersions.map((v, i) => (
            <option key={v.tag} value={v.tag}>
              {v.tag}
              {i === 0 ? " (current stable)" : ""}
            </option>
          ))}
        </select>
        {versionsLoading && <span style={S.hint}>loading releases...</span>}
        <button
          style={{
            ...S.btn,
            ...S.btnPrimary,
            padding: "4px 10px",
            fontSize: 11,
          }}
          onClick={fetchVersions}
        >
          ↻
        </button>
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
        <span style={S.label}>Retention (days)</span>
        <input
          style={S.inputSmall}
          type="number"
          value={retentionDays}
          onChange={(e) => setRetentionDays(Number(e.target.value))}
        />
        <span style={S.hint}>0 = keep forever</span>
      </div>

      <CollapsibleSection title="Path filtering">
        <div style={S.fieldRow}>
          <span style={S.label}>Filter mode</span>
          <select
            style={S.select}
            value={filterMode}
            onChange={(e) => setFilterMode(e.target.value)}
          >
            <option value="exclude">Exclude matching paths</option>
            <option value="include">Include only matching paths</option>
          </select>
        </div>
        <div style={{ marginBottom: 10 }}>
          <div style={{ ...S.label, width: "auto", marginBottom: 6 }}>
            Path patterns (one per line, glob supported)
          </div>
          <textarea
            style={S.textarea}
            value={filterPaths}
            onChange={(e) => setFilterPaths(e.target.value)}
            placeholder={"navigation.position\nnavigation.*\nnotifications.*"}
          />
          <div style={S.hint}>
            {filterMode === "exclude"
              ? "These paths are NOT recorded; everything else is."
              : "ONLY these paths are recorded; everything else is dropped."}{" "}
            Leave empty to record everything.
          </div>
        </div>
      </CollapsibleSection>

      <CollapsibleSection title="Compression (on-disk)">
        <div style={S.fieldRow}>
          <span style={S.label}>Compression codec</span>
          <select
            style={S.select}
            value={compression}
            onChange={(e) => setCompression(e.target.value)}
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
          <button
            style={{
              ...S.btn,
              ...S.btnPrimary,
              ...(migrationDetecting ? S.btnDisabled : {}),
            }}
            onClick={detectMigration}
            disabled={migrationDetecting}
          >
            {migrationDetecting ? "Detecting..." : "Detect InfluxDB"}
          </button>
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

        <button
          style={{
            ...S.btn,
            ...S.btnPrimary,
            ...(exporting ? S.btnDisabled : {}),
          }}
          onClick={doExport}
          disabled={exporting}
        >
          {exporting ? "Exporting..." : "Export Data"}
        </button>
      </CollapsibleSection>

      {cfg.managedContainer !== false && (
        <CollapsibleSection title="Danger zone">
          <div style={{ fontSize: 13, color: "#555", marginBottom: 10 }}>
            Remove the QuestDB container and permanently delete all recorded
            data. Use this to fully reset QuestDB — Signal K's plugin-uninstall
            cannot delete this data on rootless Podman. This cannot be undone.
          </div>
          <button
            style={{
              ...S.btn,
              ...S.btnDanger,
              ...(purging ? S.btnDisabled : {}),
            }}
            onClick={purgeData}
            disabled={purging}
          >
            {purging ? "Removing..." : "Remove container & all data"}
          </button>
        </CollapsibleSection>
      )}

      {/* Status */}
      {actionStatus && (
        <div
          style={{
            ...S.status,
            color: statusError ? "#ef4444" : "#10b981",
            marginTop: 16,
          }}
        >
          {actionStatus}
        </div>
      )}

      {/* Save */}
      <div style={{ marginTop: 24 }}>
        <button style={{ ...S.btn, ...S.btnSave }} onClick={doSave}>
          Save Configuration
        </button>
      </div>
    </div>
  );
}

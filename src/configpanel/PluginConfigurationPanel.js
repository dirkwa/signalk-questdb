import React, { useState, useEffect, useCallback } from "react";

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

function formatNumber(n) {
  if (n === null || n === undefined) return "—";
  if (n >= 1000000) return (n / 1000000).toFixed(1) + "M";
  if (n >= 1000) return (n / 1000).toFixed(1) + "K";
  return String(n);
}

export default function PluginConfigurationPanel({ configuration, save }) {
  const cfg = configuration || {};

  const [questdbHost, setQuestdbHost] = useState(cfg.questdbHost || "127.0.0.1");
  const [questdbIlpPort, setQuestdbIlpPort] = useState(cfg.questdbIlpPort || 9009);
  const [questdbHttpPort, setQuestdbHttpPort] = useState(cfg.questdbHttpPort || 9000);
  const [questdbPgPort, setQuestdbPgPort] = useState(cfg.questdbPgPort || 8812);
  const [questdbVersion, setQuestdbVersion] = useState(cfg.questdbVersion || "latest");
  const [managedContainer, setManagedContainer] = useState(cfg.managedContainer !== false);
  const [recordSelf, setRecordSelf] = useState(cfg.recordSelf !== false);
  const [recordOthers, setRecordOthers] = useState(cfg.recordOthers || false);
  const [retentionDays, setRetentionDays] = useState(cfg.retentionDays || 0);
  const [compression, setCompression] = useState(cfg.compression || "lz4");
  const [compressionLevel, setCompressionLevel] = useState(cfg.compressionLevel || 3);

  const [versions, setVersions] = useState([]);
  const [versionsLoading, setVersionsLoading] = useState(false);
  const [dbStatus, setDbStatus] = useState(null);
  const [statusLoading, setStatusLoading] = useState(true);
  const [migrationSources, setMigrationSources] = useState(null);
  const [migrationDetecting, setMigrationDetecting] = useState(false);
  const [migrationUrl, setMigrationUrl] = useState("");
  const [actionStatus, setActionStatus] = useState("");
  const [statusError, setStatusError] = useState(false);

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
      if (res.ok) {
        setDbStatus(await res.json());
      } else {
        setDbStatus({ status: "not_running" });
      }
    } catch {
      setDbStatus({ status: "not_running" });
    }
    setStatusLoading(false);
  }, []);

  const detectMigration = async () => {
    setMigrationDetecting(true);
    setActionStatus("");
    try {
      const res = await fetch("/plugins/signalk-questdb/api/migration/detect");
      if (res.ok) {
        const data = await res.json();
        setMigrationSources(data.sources);
        if (data.sources.length === 0) {
          setActionStatus("No InfluxDB instances detected on localhost:8086.");
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
      recordSelf,
      recordOthers,
      retentionDays,
      compression,
      compressionLevel,
      pathFilter: cfg.pathFilter || { mode: "exclude", paths: [] },
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

  // Build version options: latest first, then pre-releases, then stable
  const stableVersions = versions.filter((v) => !v.prerelease).slice(0, 3);
  const preVersions = versions.filter((v) => v.prerelease).slice(0, 2);

  return (
    <div style={S.root}>
      {/* QuestDB Status */}
      <div style={S.sectionTitle}>QuestDB Status</div>

      {statusLoading ? (
        <div style={S.empty}>Checking QuestDB...</div>
      ) : isRunning ? (
        <>
          <div style={S.card}>
            <div style={{ ...S.cardIcon, background: "#7c3aed", color: "#fff" }}>Q</div>
            <div style={S.cardInfo}>
              <div style={S.cardTitle}>QuestDB</div>
              <div style={S.cardMeta}>
                {questdbHost}:{questdbHttpPort} &middot; Recording
              </div>
            </div>
            <div style={{ ...S.stateIndicator, background: "#10b981" }} title="Running" />
          </div>

          <div style={S.statsGrid}>
            <div style={S.statCard}>
              <div style={S.statValue}>{formatNumber(dbStatus.totalRows)}</div>
              <div style={S.statLabel}>Total Rows</div>
            </div>
            <div style={S.statCard}>
              <div style={S.statValue}>{formatNumber(dbStatus.activePathsToday)}</div>
              <div style={S.statLabel}>Active Paths Today</div>
            </div>
          </div>
        </>
      ) : (
        <div style={S.card}>
          <div style={{ ...S.cardIcon, background: "#fef2f2", color: "#ef4444" }}>Q</div>
          <div style={S.cardInfo}>
            <div style={S.cardTitle}>QuestDB</div>
            <div style={S.cardMeta}>
              {dbStatus?.status === "unhealthy" ? "Not responding" : "Not running"}
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
          style={{ ...S.btn, ...S.btnPrimary, padding: "4px 10px", fontSize: 11 }}
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
          {managedContainer ? "signalk-container manages QuestDB" : "Connect to external QuestDB"}
        </span>
      </div>

      {!managedContainer && (
        <div style={S.fieldRow}>
          <span style={S.label}>QuestDB host</span>
          <input
            style={S.input}
            value={questdbHost}
            onChange={(e) => setQuestdbHost(e.target.value)}
          />
        </div>
      )}

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

      {/* Recording */}
      <div style={S.sectionTitle}>Recording</div>

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

      {/* Migration */}
      <div style={S.sectionTitle}>InfluxDB Migration</div>

      <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 10 }}>
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
        <span style={S.hint}>Checks localhost:8086 for InfluxDB 1.x and 2.x</span>
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
                  background: src.type === "influxdb2" ? "#020a47" : "#22adf6",
                  color: "#fff",
                }}
              >
                {src.type === "influxdb2" ? "2" : "1"}
              </div>
              <div style={S.cardInfo}>
                <div style={S.cardTitle}>
                  InfluxDB {src.type === "influxdb2" ? "2.x" : "1.x"}
                  <span style={{ ...S.tag, ...S.tagLatest }}>{src.status}</span>
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

      {migrationSources && migrationSources.length === 0 && !migrationDetecting && (
        <div style={{ ...S.empty, padding: "16px", textAlign: "left" }}>
          No InfluxDB found on localhost. Use manual URL above for remote instances.
        </div>
      )}

      {/* Export */}
      <div style={S.sectionTitle}>Compression &amp; Export</div>

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

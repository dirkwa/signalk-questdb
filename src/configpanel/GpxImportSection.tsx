import React, { useEffect, useRef, useState } from "react";
import { Button, formatNumber } from "signalk-container-helper/ui";
import type { GpxImportResponse, GpxImportStatus } from "../api-contract.js";
import { S } from "./styles.js";
import { scanGpx, type GpxTrackGroup } from "./gpx-scan.js";
import { GPX_IMPORT_API, uploadGpx, GpxUploadError } from "./gpx-upload.js";

const day = (ms: number | null) =>
  ms === null ? "?" : new Date(ms).toISOString().slice(0, 10);

const percent = (part: number, whole: number) =>
  whole > 0 ? Math.min(100, Math.floor((part / whole) * 100)) : 0;

const STATE_LABEL: Record<GpxImportStatus["state"], string> = {
  receiving: "Importing",
  finishing: "Finishing",
  finished: "Import complete",
  failed: "Import failed",
  aborted: "Import aborted",
};

function ProgressBar({ value }: { value: number }) {
  return (
    <div
      role="progressbar"
      aria-valuenow={value}
      aria-valuemin={0}
      aria-valuemax={100}
      style={{
        height: 8,
        background: "#e5e7eb",
        borderRadius: 4,
        overflow: "hidden",
        margin: "8px 0",
      }}
    >
      <div
        style={{ width: `${value}%`, height: "100%", background: "#2563eb" }}
      />
    </div>
  );
}

function ImportSummary({ status }: { status: GpxImportStatus }) {
  const s = status.summary;
  return (
    <div style={{ ...S.empty, padding: "12px", textAlign: "left" }}>
      <div>
        <strong>{STATE_LABEL[status.state]}</strong>
        {status.filename ? ` — ${status.filename}` : ""}
      </div>
      {status.state === "receiving" && (
        <ProgressBar value={percent(status.received, status.size)} />
      )}
      <div style={S.cardMeta}>
        {(status.received / 1e6).toFixed(0)} of {(status.size / 1e6).toFixed(0)}{" "}
        MB read · {formatNumber(s.imported)} rows written ·{" "}
        {formatNumber(s.alreadyRecorded)} points already recorded ·{" "}
        {formatNumber(s.invalid)} invalid
      </div>
      {s.from && s.to && (
        <div style={S.cardMeta}>
          From {s.from.slice(0, 10)} to {s.to.slice(0, 10)}
        </div>
      )}
      {status.error && (
        <div style={{ ...S.cardMeta, color: "#b91c1c" }}>{status.error}</div>
      )}
    </div>
  );
}

/**
 * Import a chartplotter's GPX export into the position archive. The file is
 * looked at in the browser first, so the user picks the own vessel's tracks
 * before a byte is uploaded.
 */
export function GpxImportSection({
  archiveResolutionSeconds,
}: {
  archiveResolutionSeconds: number;
}) {
  const [file, setFile] = useState<File | null>(null);
  const [scanned, setScanned] = useState(0);
  const [groups, setGroups] = useState<GpxTrackGroup[] | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [fillGapsOnly, setFillGapsOnly] = useState(true);
  const [status, setStatus] = useState<GpxImportStatus | null>(null);
  const [busy, setBusy] = useState<"scanning" | "importing" | null>(null);
  const [error, setError] = useState<string | null>(null);
  // AIS targets are most of the names in an export and never the vessel's;
  // they stay one line until asked for.
  const [showOthers, setShowOthers] = useState(false);
  // The upload's, kept apart from the scan's: closing the section unmounts
  // this component, which may stop a scan but must not abort an import.
  const abort = useRef<AbortController | null>(null);
  const scanAbort = useRef<AbortController | null>(null);
  // Set once this panel starts an import: the last-import status read on
  // mount may arrive after that and must not replace the running one's.
  const importStarted = useRef(false);

  // The last import, so a panel opened afterwards still shows its result.
  useEffect(() => {
    let cancelled = false;
    fetch(GPX_IMPORT_API)
      .then((res) => res.json() as Promise<GpxImportResponse>)
      .then((body) => {
        if (!cancelled && !importStarted.current && body.import) {
          setStatus(body.import);
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
      scanAbort.current?.abort();
    };
  }, []);

  // An import this panel is not driving (started before the section was
  // last closed, or in another tab) is followed from the server.
  const following =
    busy !== "importing" &&
    (status?.state === "receiving" || status?.state === "finishing");
  useEffect(() => {
    if (!following) return;
    const timer = setInterval(() => {
      fetch(GPX_IMPORT_API)
        .then((res) => res.json() as Promise<GpxImportResponse>)
        .then((body) => {
          if (body.import) setStatus(body.import);
        })
        .catch(() => undefined);
    }, 2000);
    return () => clearInterval(timer);
  }, [following]);

  const scan = async (picked: File) => {
    scanAbort.current?.abort();
    const controller = new AbortController();
    scanAbort.current = controller;
    setFile(picked);
    setGroups(null);
    setError(null);
    setScanned(0);
    setBusy("scanning");
    try {
      const found = await scanGpx(picked, setScanned, controller.signal);
      setGroups(found);
      setSelected(new Set(found.filter((g) => g.selected).map((g) => g.name)));
    } catch (err) {
      if (!controller.signal.aborted) {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      if (scanAbort.current === controller) scanAbort.current = null;
      setBusy(null);
    }
  };

  const start = async () => {
    if (!file) return;
    importStarted.current = true;
    const controller = new AbortController();
    abort.current = controller;
    setError(null);
    setBusy("importing");
    try {
      await uploadGpx({
        file,
        filename: file.name,
        tracks: [...selected],
        fillGapsOnly,
        onStatus: setStatus,
        signal: controller.signal,
      });
    } catch (err) {
      if (err instanceof GpxUploadError && err.status) setStatus(err.status);
      if (!controller.signal.aborted) {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      if (abort.current === controller) abort.current = null;
      setBusy(null);
    }
    // The server's word on how it ended, whether finished, aborted or failed.
    const last = await fetch(GPX_IMPORT_API)
      .then((res) => res.json() as Promise<GpxImportResponse>)
      .catch(() => null);
    if (last?.import) setStatus(last.import);
  };

  // The local signal is only seen between requests, and a finish can wait
  // minutes on QuestDB, so the server is always told as well, whoever started
  // the import.
  const abortRunning = async () => {
    abort.current?.abort();
    if (status) {
      const res = await fetch(
        `${GPX_IMPORT_API}/${encodeURIComponent(status.id)}`,
        { method: "DELETE" },
      ).catch(() => null);
      const body = (await res
        ?.json()
        .catch(() => null)) as GpxImportResponse | null;
      if (body?.import) setStatus(body.import);
    }
  };

  const toggle = (name: string) =>
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });

  const running =
    status?.state === "receiving" || status?.state === "finishing";
  const others = groups?.filter((g) => g.otherVessel) ?? [];

  return (
    <div>
      <div style={{ fontSize: 13, color: "#555", marginBottom: 10 }}>
        Import a track exported from a chartplotter (GPX) into the archive,
        where retention never drops it. Points are thinned to one per{" "}
        {archiveResolutionSeconds > 0
          ? `${archiveResolutionSeconds} s`
          : "recorded point"}{" "}
        (Archive resolution). The file is read here first, so you choose which
        tracks are your own before anything is uploaded.
      </div>

      <div style={S.fieldRow}>
        <span style={S.label}>GPX file</span>
        <input
          type="file"
          accept=".gpx,application/gpx+xml"
          aria-label="GPX file"
          disabled={busy !== null}
          onChange={(e) => {
            const picked = e.target.files?.[0];
            if (picked) void scan(picked);
          }}
        />
      </div>

      {busy === "scanning" && file && (
        <div style={S.cardMeta}>
          Reading {file.name}…
          <ProgressBar value={percent(scanned, file.size)} />
        </div>
      )}

      {groups && (
        <>
          <div style={{ ...S.label, width: "auto", margin: "10px 0 6px" }}>
            Tracks in the file, by name
          </div>
          <table style={{ width: "100%", fontSize: 13, borderSpacing: 0 }}>
            <thead>
              <tr style={{ textAlign: "left", color: "#666" }}>
                <th />
                <th>Name</th>
                <th>Tracks</th>
                <th>Points</th>
                <th>Period</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {groups
                .filter((g) => !g.otherVessel || showOthers)
                .map((g) => (
                  <tr key={g.name}>
                    <td>
                      <input
                        type="checkbox"
                        style={S.checkbox}
                        aria-label={`Import ${g.name || "(no name)"}`}
                        checked={selected.has(g.name)}
                        disabled={busy !== null}
                        onChange={() => toggle(g.name)}
                      />
                    </td>
                    <td>{g.name || <em>(no name)</em>}</td>
                    <td>{formatNumber(g.tracks)}</td>
                    <td>{formatNumber(g.points)}</td>
                    <td>
                      {day(g.from)} – {day(g.to)}
                    </td>
                    <td style={{ color: "#92400e" }}>
                      {g.otherVessel
                        ? "AIS target"
                        : g.conflict
                          ? `${Math.round(g.conflict.km).toLocaleString()} km from “${g.conflict.name}” on ${day(g.conflict.at)}: another vessel?`
                          : ""}
                    </td>
                  </tr>
                ))}
              {others.length > 0 && (
                <tr>
                  <td />
                  <td colSpan={5}>
                    <button
                      type="button"
                      style={{
                        ...S.link,
                        background: "none",
                        border: 0,
                        padding: 0,
                      }}
                      onClick={() => setShowOthers((shown) => !shown)}
                    >
                      {showOthers ? "Hide" : "Show"} {others.length} AIS target
                      {others.length === 1 ? "" : "s"} (
                      {formatNumber(others.reduce((n, g) => n + g.points, 0))}{" "}
                      points)
                    </button>
                  </td>
                </tr>
              )}
            </tbody>
          </table>

          <div style={{ ...S.fieldRow, marginTop: 10 }}>
            <span style={S.label}>Skip recorded times</span>
            <input
              type="checkbox"
              style={S.checkbox}
              aria-label="Skip times already recorded"
              checked={fillGapsOnly}
              disabled={busy !== null}
              onChange={(e) => setFillGapsOnly(e.target.checked)}
            />
            <span style={S.hint}>
              leave out points where QuestDB already has positions of the vessel
              within the same five minutes
            </span>
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <Button
              variant="primary"
              busy={busy === "importing"}
              busyLabel="Importing…"
              disabled={selected.size === 0 || busy !== null || running}
              onClick={() => void start()}
            >
              {`Import ${selected.size} track name${selected.size === 1 ? "" : "s"}`}
            </Button>
          </div>
        </>
      )}

      {error && (
        <div style={{ ...S.cardMeta, color: "#b91c1c", marginTop: 8 }}>
          {error}
        </div>
      )}

      {status && (
        <div style={{ marginTop: 10 }}>
          <ImportSummary status={status} />
          {running && (
            <Button variant="warn" small onClick={() => void abortRunning()}>
              Abort
            </Button>
          )}
        </div>
      )}
    </div>
  );
}

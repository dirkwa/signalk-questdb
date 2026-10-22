// Sending a GPX file to the plugin's import, slice by slice (see
// src/gpx-import-session.ts for the server side). Each reply waits until the
// server has written the slice's points, so the loop below never runs ahead
// of QuestDB. The file is read one slice at a time, so the browser holds no
// more of it than that.

import type { GpxImportResponse, GpxImportStatus } from "../api-contract.js";

export const GPX_IMPORT_API = "/plugins/signalk-questdb/api/gpx-import";

/** Bytes per request: well under the server's 8 MiB limit. */
export const SLICE_BYTES = 4 * 1024 * 1024;

// A boat's Wi-Fi drops for a moment now and then. A slice that did not get
// through is sent again from wherever the server says it got to.
const RETRIES = 5;
const RETRY_DELAY_MS = 2000;

export interface GpxUpload {
  file: Blob;
  filename: string;
  /** Names of the tracks to import. */
  tracks: string[];
  fillGapsOnly: boolean;
  onStatus: (status: GpxImportStatus) => void;
  /** Aborting it aborts the import on the server too. */
  signal: AbortSignal;
  fetch?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  sliceBytes?: number;
}

export class GpxUploadError extends Error {
  constructor(
    message: string,
    readonly status: GpxImportStatus | null,
  ) {
    super(message);
  }
}

async function read(res: Response): Promise<GpxImportResponse> {
  try {
    return (await res.json()) as GpxImportResponse;
  } catch {
    return { error: `${res.status} ${res.statusText}` };
  }
}

/** Import `file`, resolving with the finished import's status. */
export async function uploadGpx(upload: GpxUpload): Promise<GpxImportStatus> {
  const request = upload.fetch ?? fetch;
  const sleep =
    upload.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const { file, signal } = upload;
  const sliceBytes = upload.sliceBytes ?? SLICE_BYTES;

  const started = await request(GPX_IMPORT_API, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      filename: upload.filename,
      size: file.size,
      tracks: upload.tracks,
      fillGapsOnly: upload.fillGapsOnly,
    }),
  });
  const created = await read(started);
  if (!started.ok || !created.import) {
    throw new GpxUploadError(
      created.error ?? "The import could not start",
      created.import ?? null,
    );
  }
  let status = created.import;
  upload.onStatus(status);
  const url = `${GPX_IMPORT_API}/${encodeURIComponent(status.id)}`;

  const abort = async () => {
    await request(url, { method: "DELETE" }).catch(() => undefined);
    throw new DOMException("The import was aborted", "AbortError");
  };

  let failures = 0;
  while (status.received < file.size) {
    if (signal.aborted) await abort();
    const offset = status.received;
    let res: Response;
    try {
      const slice = await file.slice(offset, offset + sliceBytes).arrayBuffer();
      res = await request(`${url}?offset=${offset}`, {
        method: "PUT",
        headers: { "content-type": "application/octet-stream" },
        body: slice,
      });
    } catch (err) {
      if (signal.aborted) await abort();
      if (++failures > RETRIES) throw err;
      await sleep(RETRY_DELAY_MS);
      // The slice may have arrived even though its reply did not.
      const now = await request(GPX_IMPORT_API).then(read, () => null);
      if (now?.import?.id === status.id) status = now.import;
      continue;
    }
    const body = await read(res);
    if (res.status === 409 && body.import?.state === "receiving") {
      // Out of step with the server: carry on from where it is. An offset
      // that did not move means it is still writing the previous slice, so
      // wait, and give up if it never moves.
      if (body.import.received === offset) {
        if (++failures > RETRIES) {
          throw new GpxUploadError(
            body.error ?? `The slice at byte ${offset} was refused`,
            body.import,
          );
        }
        await sleep(RETRY_DELAY_MS);
      }
      status = body.import;
      continue;
    }
    if (!res.ok || !body.import) {
      throw new GpxUploadError(
        body.error ?? `The slice at byte ${offset} was refused`,
        body.import ?? null,
      );
    }
    failures = 0;
    status = body.import;
    upload.onStatus(status);
  }

  if (signal.aborted) await abort();
  const finished = await request(`${url}/finish`, { method: "POST" });
  const done = await read(finished);
  if (!finished.ok || !done.import) {
    throw new GpxUploadError(
      done.error ?? "The import could not finish",
      done.import ?? null,
    );
  }
  upload.onStatus(done.import);
  return done.import;
}

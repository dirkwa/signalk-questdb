import React, { useEffect, useState } from "react";

/**
 * The embeddable webapp: QuestDB's own console, hosted inside the Signal K
 * admin shell so the navigation sidebar stays put.
 *
 * This must be a Module Federation remote exposing `./AppPanel`, NOT a static
 * page. Signal K's Embedded route (`/admin/#/e/<name>`) resolves that exact
 * module name and renders it as a React component — a `public/index.html`
 * shipped alongside is never loaded, and the admin UI fails with
 * "Module ./AppPanel does not exist in container".
 *
 * The console itself stays in an iframe rather than being reimplemented: it is
 * a large third-party app, and proxying it (see src/console-proxy.ts) is what
 * lets it run under the Signal K origin behind an admin session.
 */

const CONSOLE_URL = "/plugins/signalk-questdb/console/";

export default function AppPanel() {
  // `null` while probing: rendering the frame first and swapping it out on
  // failure makes the panel flash an error page every load.
  const [reachable, setReachable] = useState<boolean | null>(null);

  useEffect(() => {
    let cancelled = false;
    // GET, not HEAD: QuestDB answers HEAD with 405, so a HEAD probe reports a
    // perfectly working console as broken.
    fetch(CONSOLE_URL, { method: "GET" })
      .then((res) => {
        if (!cancelled) setReachable(res.ok);
      })
      .catch(() => {
        if (!cancelled) setReachable(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (reachable === null) {
    return <div style={{ padding: "1rem" }}>Loading QuestDB console…</div>;
  }

  if (!reachable) {
    return (
      <div style={{ padding: "1rem", maxWidth: "40rem" }}>
        <h4>QuestDB console unavailable</h4>
        <p>
          The console could not be loaded. The usual causes are that the QuestDB
          container is not running, or that the{" "}
          <strong>QuestDB console webapp</strong> option is switched off in the
          plugin configuration.
        </p>
      </div>
    );
  }

  return (
    <iframe
      src={CONSOLE_URL}
      title="QuestDB Console"
      // The admin shell gives the panel a sized container; fill it rather than
      // guessing a viewport height, which would double-scroll inside the shell.
      style={{ width: "100%", height: "100%", minHeight: "80vh", border: 0 }}
      // Same list Signal K's own admin uses to embed the server docs
      // (EmbeddedDocs.tsx). `allow-same-origin` is required — the console
      // reads localStorage and calls its API relatively, and dropping it
      // breaks both. It therefore does NOT isolate the frame from the Signal K
      // origin; what the sandbox does buy is everything left OUT of the list:
      // no top-level navigation away from the admin UI, no popups, no form
      // submission, no downloads, no pointer lock.
      //
      // Real isolation needs a separate origin, which a Signal K plugin cannot
      // mint — its routes are mounted under the server's own origin. The
      // console is admin-only for exactly this reason.
      sandbox="allow-scripts allow-same-origin"
    />
  );
}

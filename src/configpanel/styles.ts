import type React from "react";
import { panelStyles } from "signalk-container-helper/ui";

// Panel styles layered on signalk-container-helper's shared set.
//
// The helper already carries 32 of these keys with byte-identical values —
// they were hand-copied between the container plugins before the library
// existed. Only what is genuinely questdb's own is declared here, so a change
// to the shared look reaches every panel at once instead of drifting per
// plugin.
//
// sectionToggle/sectionMarker are gone: the helper carries them as of 0.4.1,
// so the #123 affordance fix now lives in one place for every panel.
//
// btnDanger deliberately takes the HELPER's version rather than this panel's
// former larger padding/fontSize. Converging on the shared sizing is the point
// of adopting panelStyles, and both call sites — skipping a WAL segment and
// purging all data — read fine at the standard size.
export const S = {
  ...panelStyles,

  btnSave: { background: "#3b82f6", color: "#fff" },
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
  // Helper text that has to be READ, not merely available — darker and larger
  // than `hint`. A `hint` sits below a field the user is already looking at;
  // this one has to out-argue a textarea's own grey placeholder, and at
  // hint's #aaa/11px it lost that argument (issue #123).
  fieldHelp: { fontSize: 12, color: "#666", marginTop: 6, lineHeight: 1.5 },
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
  // `satisfies` rather than a type annotation: it checks every value against
  // CSSProperties while keeping each key's literal type, so `textAlign:
  // "center"` stays assignable where a widened `string` would not. An
  // annotation would also make every key name valid and lose typo checking.
} satisfies Record<string, React.CSSProperties>;

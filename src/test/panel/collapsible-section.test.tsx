import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { renderToStaticMarkup } from "react-dom/server";
import PluginConfigurationPanel from "../../configpanel/PluginConfigurationPanel.js";
import type { Config } from "../../config/schema.js";

// Issue #123: a user reported the path-filter field as uneditable on 1.8.3.
// The textarea was a correct controlled input the whole time — it was inside
// a collapsed section whose header was styled as a muted grey caption, so
// nothing suggested clicking it and the settings inside looked absent.
//
// These tests pin the two behaviours that fix carries, both of which are
// invisible to `tsc` and to every other suite in this repo:
//
//   1. a section with configured content is OPEN on mount, and
//   2. the header is a real control — a <button> that reports its state.
//
// Rendering is `renderToStaticMarkup`, which needs no DOM and no jsdom: the
// component's initial render is a pure function of its props, and initial
// render is exactly what "does it start open" is about. Interaction (clicking
// the toggle) is deliberately NOT tested here — that would need a full DOM,
// and the toggle is stock useState whose risk is not in this change.

/** The panel takes Partial<Config>; only pathFilter matters to these tests. */
const configWith = (paths: string[]): Partial<Config> => ({
  pathFilter: { mode: "exclude", paths },
});

const render = (configuration: Partial<Config>) =>
  renderToStaticMarkup(
    <PluginConfigurationPanel
      configuration={configuration as Config}
      save={() => {}}
    />,
  );

/**
 * The rendered markup for one section, from its header button to the start of
 * the next section. Scoping assertions this way keeps a test about Path
 * filtering from passing because some OTHER section happened to be open.
 */
const sectionMarkup = (html: string, title: string): string => {
  const at = html.indexOf(`>${title}</button>`);
  assert.notEqual(at, -1, `section "${title}" not rendered at all`);
  // Back up to the opening <button> so aria-expanded is inside the slice.
  const start = html.lastIndexOf("<button", at);
  const next = html.indexOf("<button", at);
  return html.slice(start, next === -1 ? undefined : next);
};

describe("Path filtering section (issue #123)", () => {
  it("starts open when patterns are already configured", () => {
    const html = render(configWith(["notifications.*"]));
    const section = sectionMarkup(html, "Path filtering");

    assert.match(
      section,
      /aria-expanded="true"/,
      "a user with filtering configured must see it without hunting for it",
    );
    // The body is what actually matters; aria-expanded could be right while
    // the children stayed unrendered.
    assert.ok(
      html.includes("notifications.*"),
      "the configured patterns must be rendered, not just the open state",
    );
  });

  it("starts collapsed when no patterns are configured", () => {
    const html = render(configWith([]));
    const section = sectionMarkup(html, "Path filtering");

    assert.match(
      section,
      /aria-expanded="false"/,
      "an unconfigured section stays collapsed so the panel is not a wall",
    );
  });

  it("treats whitespace-only patterns as unconfigured", () => {
    // doSave() trims and drops empties, so a config carrying only blank
    // entries means nothing is filtered. Opening the section for it would
    // advertise a setting that is not set.
    const html = render(configWith(["   ", ""]));

    assert.match(
      sectionMarkup(html, "Path filtering"),
      /aria-expanded="false"/,
    );
  });

  it("does not open for the default mode alone", () => {
    // pathFilter.mode defaults to "exclude" and is never absent, so keying
    // the open state on it would open this section for every user and tell
    // them nothing. Guards against a future refactor doing exactly that.
    const html = render({ pathFilter: { mode: "exclude", paths: [] } });

    assert.match(
      sectionMarkup(html, "Path filtering"),
      /aria-expanded="false"/,
    );
  });

  it("survives a config with no pathFilter at all", () => {
    // Configs predating the pathFilter key still exist; normalizeConfig fixes
    // them server-side, but the panel renders the raw stored object.
    const html = render({});

    assert.match(
      sectionMarkup(html, "Path filtering"),
      /aria-expanded="false"/,
    );
  });

  it("survives a hand-corrupted pathFilter.paths", () => {
    // The panel hydrates defensively (`Array.isArray(...) ? ... : []`) because
    // an unguarded .join() on a non-array crashes the whole panel render.
    const html = render({
      pathFilter: { paths: "notifications.*" },
    } as unknown as Partial<Config>);

    assert.match(
      sectionMarkup(html, "Path filtering"),
      /aria-expanded="false"/,
    );
  });
});

describe("collapsible section headers are controls, not captions", () => {
  // The defect in #123 was presentational: the header was a <button>, but
  // styled to read as a heading. Colour cannot be asserted meaningfully in
  // markup, so this pins the part that IS load-bearing and machine-checkable —
  // that every section header is a real button reporting its state, which is
  // what keyboard and screen-reader users depend on.
  const SECTIONS = [
    "Path filtering",
    "Compression (on-disk)",
    "InfluxDB Migration",
    "Data Export",
    "Danger zone",
  ];

  it("renders every section header as a button with aria-expanded", () => {
    const html = render(configWith([]));

    for (const title of SECTIONS) {
      const section = sectionMarkup(html, title);
      assert.match(
        section,
        /<button/,
        `"${title}" header must be a real button, not a styled div`,
      );
      assert.match(
        section,
        /aria-expanded="(true|false)"/,
        `"${title}" must report expanded state to assistive tech`,
      );
      assert.match(
        section,
        /type="button"/,
        `"${title}" must not submit — it is a disclosure toggle`,
      );
    }
  });

  it("collapses every section except a configured Path filtering", () => {
    // Guards the scope decision in #123: the restyle applies to all five
    // sections, but auto-open applies ONLY to Path filtering. Compression has
    // a non-empty default (lz4), so keying open state on "has a value" there
    // would leave it permanently expanded.
    const html = render(configWith(["notifications.*"]));

    for (const title of SECTIONS) {
      const expected = title === "Path filtering" ? "true" : "false";
      assert.match(
        sectionMarkup(html, title),
        new RegExp(`aria-expanded="${expected}"`),
        `"${title}" should render aria-expanded="${expected}"`,
      );
    }
  });
});

describe("path filter copy does not imply position is excluded", () => {
  // The second half of #123: the reporter concluded the defaults excluded
  // their boat position. They do not — mode is "exclude" with an empty list.
  // The cause was our own placeholder using navigation.position as its
  // example, which renders as grey text inside an empty box and reads as a
  // value. These assertions are about user-visible copy, so they are worth
  // the brittleness: if someone reintroduces the example, that IS the bug.
  it("never offers navigation.position as the example pattern", () => {
    const html = render(configWith([]));

    assert.ok(
      !html.includes("navigation.position"),
      "navigation.position as placeholder text reads as a configured value",
    );
  });

  it("states that empty means everything is recorded", () => {
    const html = render(configWith(["notifications.*"]));

    assert.match(
      html,
      /Empty = record everything/,
      "the reassurance must be present, and prominent enough to outrank the placeholder",
    );
  });
});

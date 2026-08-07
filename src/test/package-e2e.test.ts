// End-to-end checks on the BUILT plugin, as a consumer receives it.
//
// The other suites test modules in isolation. These test the seams that only
// exist once the package is assembled: that Signal K can load the ESM entry
// point at all, that the federated config panel the Admin UI fetches is
// complete and self-consistent, and that the browser bundle did not swallow
// server-only code. Every one of these has broken at least once during the
// TypeScript/ESM/Vite migration, always silently — the build stayed green
// while the plugin failed to load or the panel rendered blank.
//
// Run with `npm run build:all`, which builds before testing — like every
// suite here, this executes from dist/. A missing panel build is a FAILURE
// here rather than a skip: these assertions exist because the panel can
// break silently, and skipping them on absent output would report green for
// exactly the build that produced nothing.
//
// Where an assertion is about what users RECEIVE rather than what the build
// produced, it reads npm's publish manifest instead of the working tree.
// Those differ: .npmignore can exclude a file that is sitting right there on
// disk, which is exactly how public/assets/ nearly got dropped.

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  readFileSync,
  existsSync,
  readdirSync,
  mkdtempSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);
const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const publicDir = path.join(repoRoot, "public");

/**
 * Fail loudly rather than skip. `npm run build:all` builds before testing,
 * and CI runs `npm run build` first, so an absent panel means the build
 * produced nothing — the case these assertions are here to catch.
 */
const requirePanelBuild = () => {
  assert.ok(
    existsSync(path.join(publicDir, "remoteEntry.js")),
    "public/remoteEntry.js is missing — run `npm run build:all` (or `npm run build`) before the tests",
  );
};

const readPublic = (file: string) =>
  readFileSync(path.join(publicDir, file), "utf8");

const panelChunks = () =>
  existsSync(path.join(publicDir, "assets"))
    ? readdirSync(path.join(publicDir, "assets")).filter((f) =>
        f.endsWith(".js"),
      )
    : [];

/** Every JS file the Admin UI could end up executing. */
const allBundleSources = () =>
  panelChunks()
    .map((f) => readFileSync(path.join(publicDir, "assets", f), "utf8"))
    .concat([readPublic("remoteEntry.js")]);

// Require a path prefix, since a bare "logger.js" also appears inside the
// federation runtime as plain data. remoteEntry.js references chunks as
// "./assets/x.js"; the chunks reference their siblings as "./x.js".
const chunkRefs = (source: string): string[] =>
  [...source.matchAll(/["'`]\.\/(?:assets\/)?([\w.-]+\.js)["'`]/g)].map(
    (m) => m[1],
  );

/**
 * Every chunk reachable from remoteEntry.js, following imports transitively.
 * The entry names only a handful directly — the rest are pulled in by those
 * chunks in turn, and a second-level chunk going missing breaks the panel
 * just as completely as a first-level one.
 */
const reachableChunks = (): string[] => {
  const assetsDir = path.join(publicDir, "assets");
  const seen = new Set<string>();
  const queue = chunkRefs(readPublic("remoteEntry.js"));

  while (queue.length > 0) {
    const chunk = queue.pop() as string;
    if (seen.has(chunk)) continue;
    const file = path.join(assetsDir, chunk);
    if (!existsSync(file)) {
      // Recorded so the assertion below reports it rather than skipping it.
      seen.add(chunk);
      continue;
    }
    seen.add(chunk);
    queue.push(...chunkRefs(readFileSync(file, "utf8")));
  }
  return [...seen];
};

/**
 * Paths npm would actually publish. Reading the working tree proves nothing
 * about the tarball — .npmignore excludes by exact filename, so a rename or
 * a well-meant "make the ignore files consistent" edit can drop a file that
 * is still present on disk and still passes every other check here.
 */
let manifestCache: string[] | null = null;
const publishedFiles = (): string[] => {
  if (manifestCache) return manifestCache;
  // `--cache` into a temp dir, and `--offline` so nothing is fetched.
  //
  // npm writes to its cache even for a dry-run pack. The Signal K plugin
  // registry scores plugins by running `npm test` inside
  // `firejail --net=none --read-only=/home`, so the default cache under
  // $HOME fails with EROFS and every assertion here errored — costing the
  // plugin 30 points with a `tests-failing` badge while the suite passed
  // everywhere else. Keep this sandbox-safe.
  const cacheDir = mkdtempSync(path.join(tmpdir(), "sk-questdb-pack-"));
  let out: string;
  try {
    out = execFileSync(
      "npm",
      ["pack", "--dry-run", "--json", "--offline", "--cache", cacheDir],
      {
        cwd: repoRoot,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      },
    );
  } finally {
    rmSync(cacheDir, { recursive: true, force: true });
  }
  // npm has shipped both shapes for `pack --json`: an array of results, and
  // an object keyed by package name (npm 12 under a lifecycle script). Take
  // whichever came back rather than pinning the test to one npm version.
  const parsed: unknown = JSON.parse(out);
  const [result] = (
    Array.isArray(parsed) ? parsed : Object.values(parsed as object)
  ) as { files?: { path: string }[] }[];
  assert.ok(
    result?.files,
    `unexpected npm pack --json output: ${out.slice(0, 200)}`,
  );
  manifestCache = result.files.map((f) => f.path);
  return manifestCache;
};

describe("plugin entry point (what Signal K loads)", () => {
  // Signal K's importOrRequire() does `require(dir)` and takes
  // `mod.default ?? mod`. The package is ESM, so this only works because
  // Node >= 20.19 can require an ES module — the exact combination that
  // broke with "type": "module" plus CommonJS-emitted output.
  it("is require()-able and yields the plugin factory", () => {
    const mod = require(repoRoot);
    const factory = mod.default ?? mod;
    assert.equal(
      typeof factory,
      "function",
      "plugin must export a factory function",
    );
  });

  it("constructs a plugin with the lifecycle Signal K calls", () => {
    const mod = require(repoRoot);
    const factory = mod.default ?? mod;
    const plugin = factory({
      debug: () => {},
      error: () => {},
      setPluginStatus: () => {},
    });

    assert.equal(plugin.id, "signalk-questdb");
    assert.equal(typeof plugin.name, "string");
    assert.equal(typeof plugin.start, "function");
    assert.equal(typeof plugin.stop, "function");
  });

  it("exposes a config schema the Admin UI form can render", () => {
    const mod = require(repoRoot);
    const plugin = (mod.default ?? mod)({
      debug: () => {},
      error: () => {},
      setPluginStatus: () => {},
    });
    const schema =
      typeof plugin.schema === "function" ? plugin.schema() : plugin.schema;

    assert.equal(schema.type, "object");
    // Options the panel reads back by name; a schema rename here silently
    // empties the corresponding field in the form.
    for (const key of [
      "questdbHost",
      "questdbMemoryLimit",
      "questdbCpuLimit",
      "pathFilter",
      "samplingRates",
      "compression",
      "ilpFlushIntervalMs",
    ]) {
      assert.ok(schema.properties[key], `schema must declare ${key}`);
    }
  });

  it("keeps the compression codecs the panel offers", () => {
    const mod = require(repoRoot);
    const plugin = (mod.default ?? mod)({
      debug: () => {},
      error: () => {},
      setPluginStatus: () => {},
    });
    const schema =
      typeof plugin.schema === "function" ? plugin.schema() : plugin.schema;
    const codecs = schema.properties.compression.anyOf.map(
      (v: { const: string }) => v.const,
    );

    // The panel's <select> renders exactly these, and toCompression() falls
    // back to "lz4" for anything else — so a codec added to the schema but
    // not to the panel would be silently coerced on save.
    assert.deepEqual(codecs, ["none", "lz4", "zstd"]);
  });
});

describe("federated config panel (what the Admin UI fetches)", () => {
  it("emits a remote entry", () => {
    requirePanelBuild();
    assert.ok(readPublic("remoteEntry.js").length > 0);
  });

  it("exports the get/init pair the Admin UI's ESM loader requires", async () => {
    requirePanelBuild();
    // dynamicutilities.ts does `await import(remoteEntryUrl)` and only
    // accepts the module as a container when BOTH are functions — so
    // import it the same way rather than pattern-matching the text, which
    // a minified identifier could satisfy by accident.
    const entry = await import(
      pathToFileURL(path.join(publicDir, "remoteEntry.js")).href
    );
    assert.equal(typeof entry.get, "function", "remoteEntry must export get()");
    assert.equal(
      typeof entry.init,
      "function",
      "remoteEntry must export init()",
    );
  });

  it("actually exposes ./PluginConfigurationPanel", async () => {
    requirePanelBuild();
    // A container can export get/init and still expose nothing under the
    // name the Admin UI asks for — a renamed or dropped `exposes` key in
    // vite.config.ts builds cleanly and fails only when a user opens the
    // panel. So request the module the way the host does.
    const entry = await import(
      pathToFileURL(path.join(publicDir, "remoteEntry.js")).href
    );
    const factory = await entry.get("./PluginConfigurationPanel");
    assert.equal(
      typeof factory,
      "function",
      "get('./PluginConfigurationPanel') must return a module factory",
    );
  });

  it("ships every chunk the remote entry imports", () => {
    requirePanelBuild();
    // The panel 404s at load if a chunk is missing — which is what
    // excluding public/assets/ from .npmignore would have caused.
    const referenced = reachableChunks();
    assert.ok(referenced.length > 0, "remote entry should import chunks");
    for (const chunk of referenced) {
      assert.ok(
        existsSync(path.join(publicDir, "assets", chunk)),
        `assets/${chunk} is reachable from remoteEntry but not in the build`,
      );
    }
  });

  it("renders through the shared React singleton, not its own copy", () => {
    requirePanelBuild();
    // Classic JSX compiles to React.createElement against the shared
    // instance. The automatic runtime would import react/jsx-runtime,
    // which is not in the federation `shared` scope — the host page then
    // has two Reacts, useState reads null, and the panel silently fails
    // to mount with no build error.
    const sources = allBundleSources();
    const createElementCalls = sources.reduce(
      (n, src) => n + (src.match(/createElement/g)?.length ?? 0),
      0,
    );
    assert.ok(
      createElementCalls > 100,
      `expected classic-runtime createElement calls, found ${createElementCalls}`,
    );

    for (const src of sources) {
      assert.doesNotMatch(
        src,
        /jsx-runtime|jsxRuntime/,
        "bundle must not pull in react/jsx-runtime (second React instance)",
      );
    }
  });

  it("keeps server-only modules out of the browser bundle", () => {
    requirePanelBuild();
    // The panel imports types from the server tree. Those imports must
    // stay `import type`: a value import drags the module's runtime
    // dependencies into the browser — typebox via config/schema,
    // fs/promises via host-limits (which webpack/rollup cannot polyfill).
    for (const src of allBundleSources()) {
      assert.doesNotMatch(
        src,
        /\bfs\/promises\b/,
        "fs/promises leaked into the bundle",
      );
      assert.doesNotMatch(src, /typebox/i, "typebox leaked into the bundle");
    }
  });
});

describe("published package layout", () => {
  it("points main at a file that exists", () => {
    const pkg = JSON.parse(
      readFileSync(path.join(repoRoot, "package.json"), "utf8"),
    );
    assert.ok(
      existsSync(path.join(repoRoot, pkg.main)),
      `${pkg.main} is missing`,
    );
  });

  it("publishes the entry point Signal K requires", () => {
    const pkg = JSON.parse(
      readFileSync(path.join(repoRoot, "package.json"), "utf8"),
    );
    assert.ok(
      publishedFiles().includes(pkg.main),
      `${pkg.main} is on disk but excluded from the package`,
    );
  });

  it("publishes the panel and every chunk it imports", () => {
    requirePanelBuild();
    // The check that matters for the Admin UI: remoteEntry.js alone is
    // useless. Excluding public/assets/ leaves the panel 404ing on load
    // for every user, while the build output on disk looks complete.
    const published = publishedFiles();
    assert.ok(
      published.includes("public/remoteEntry.js"),
      "public/remoteEntry.js is not in the package",
    );

    // Transitive: the entry names only a few chunks directly, and a
    // second-level one going missing breaks the panel just as completely.
    const referenced = reachableChunks();
    assert.ok(referenced.length > 0, "remote entry should import chunks");
    for (const chunk of referenced) {
      assert.ok(
        published.includes(`public/assets/${chunk}`),
        `assets/${chunk} is reachable from remoteEntry but the package excludes it`,
      );
    }
  });

  it("keeps local build artifacts out of the package", () => {
    // .mf/ is federation's diagnostics dir; it shipped once already because
    // .gitignore does not govern npm.
    for (const p of publishedFiles()) {
      assert.doesNotMatch(p, /^\.mf\//, `${p} should not be published`);
      assert.doesNotMatch(p, /^src\//, `${p} should not be published`);
    }
  });

  it("declares itself ESM, which is what makes the panel load as a module", () => {
    const pkg = JSON.parse(
      readFileSync(path.join(repoRoot, "package.json"), "utf8"),
    );
    // The server emits <script type="module"> for the panel only when the
    // plugin's package.json says "type": "module"; without it the ESM
    // remote is loaded as a classic script and never registers.
    assert.equal(pkg.type, "module");
  });

  it("keeps the Admin UI keyword that makes the panel appear at all", () => {
    const pkg = JSON.parse(
      readFileSync(path.join(repoRoot, "package.json"), "utf8"),
    );
    assert.ok(pkg.keywords.includes("signalk-node-server-plugin"));
    assert.ok(pkg.keywords.includes("signalk-plugin-configurator"));
  });
});

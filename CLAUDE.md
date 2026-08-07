# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- `npm run build` — TypeScript compile (`tsc`) + panel typecheck + Vite bundle of the React config panel into `public/`
- `npm run build:config` — Vite only (rebuild the config panel without recompiling TS)
- `npm run typecheck:panel` — typecheck the config panel (`tsc -p src/configpanel --noEmit`); Vite only strips its types, so this is the only thing that checks them
- `npm run watch` — `tsc --watch` for the plugin source
- `npm test` — runs the Node built-in test runner against compiled output (`node --test 'dist/test/**/*.test.js'`). Tests must be built first; use `npm run build:all` to build then test.
- `npm run format` — prettier write + eslint --fix
- `npm run ci-lint` — eslint + prettier --check (used in CI)
- Run a single test file: `node --test dist/test/history-v2.test.js` (after `npm run build`)

Node ≥ 22 is required (uses the built-in test runner).

## Architecture

This is a Signal K **server plugin** that ingests vessel data into a managed **QuestDB** container and serves it back via Signal K's history APIs. It is intended as a drop-in replacement for `signalk-to-influxdb`.

### Two surfaces

1. **Plugin runtime** (`src/index.ts` and siblings) — compiled by `tsc` to `dist/`, loaded by Signal K server as `main` (`dist/index.js`). The package is **ESM** (`"type": "module"`): `src/index.ts` ends in `export default`, and Signal K's `importOrRequire()` unwraps it. Node ≥ 20.19 can `require()` an ESM module, so this works on both supported Node versions.
2. **React config panel** (`src/configpanel/`) — TypeScript + JSX, bundled by **Vite** (`vite.config.ts`) via **Module Federation** into `public/remoteEntry.js`, exposed to the Signal K Admin UI as `./PluginConfigurationPanel`. React 19 is shared as a singleton with the host UI. It has its own `tsconfig.json` (browser libs, classic JSX, `noEmit`) so DOM types never reach the Node compile.

   Because the package is `"type": "module"`, the server injects the panel's script tag with `type="module"`, and the Admin UI then loads it as an **ESM remote**: `await import(remoteEntry.js)` expecting exported `get`/`init` (see the admin UI's `views/Webapps/dynamicutilities.ts`). Vite's federation output provides exactly that.

Both build outputs are shipped in the npm package; `prepublishOnly` rebuilds them.

### Data flow

- Subscribes to the Signal K **streambundle** for all paths and writes filtered/throttled deltas to QuestDB via **ILP over raw TCP** (`src/ilp-writer.ts`) — no QuestDB client library.
- Three tables (`signalk`, `signalk_str`, `signalk_position`) defined and queried in `src/query-client.ts`. All use WAL mode + daily partitioning + dedup.
- Reads come from two registered providers:
  - **v2** (`src/history-v2.ts`) — `app.registerHistoryApiProvider()`, REST under `/signalk/v2/api/history/`. Aggregates map directly to QuestDB SQL except `sma`/`ema` (computed client-side).
  - **v1** (`src/history-v1.ts`) — `app.registerHistoryProvider()`, WebSocket playback with chunked reads.
- Retention (`src/retention.ts`) drops old daily partitions on a timer.
- Startup restore (`src/restore.ts`) optionally replays each vessel's last recorded position back into the live model after a server restart.
- Console proxy (`src/console-proxy.ts`) reverse-proxies QuestDB's own web console so the embeddable webapp (`public/index.html`) can host it in the admin UI.

### Container integration

The plugin **does not run QuestDB itself**. It declares an optional peer dep on `signalk-container` and looks up that plugin's API at runtime (`ContainerManagerApi` in `src/index.ts`). When `managedContainer` is true, it calls `ensureRunning` / `ensureNetwork` / `pullImage` / `stop` to control a QuestDB container on the shared `sk-network` Podman/Docker network. The plugin lifecycle (`start`/`stop`) drives container lifecycle.

When `managedContainer` is false, the plugin connects to an external QuestDB at the configured host/ports.

### Config schema

`src/config/schema.ts` uses **TypeBox** — the schema is the single source of truth for the JSON-schema Signal K shows in the Admin UI **and** for the `Config` TS type. Add new options there.

The package is the unscoped `typebox` (1.x), not `@sinclair/typebox` (which stopped at 0.34). TypeBox 1 was published under a new name rather than a major bump, so `npm outdated` will never point at it.

### REST endpoints

All extra plugin endpoints live under `/plugins/signalk-questdb/api/` and are wired in `src/index.ts` via the `IRouter` Signal K passes to `registerWithRouter`. The `/query` endpoint is gated by `isReadOnlySQL` in `src/query-client.ts` — DDL/DML must remain blocked.

⚠️ **`/console` is deliberately NOT gated that way.** It proxies QuestDB's own console, which is a full SQL client and can drop tables. It is safe only because signalk-server gives plugin routes an **admin-only default**, and routes are downgraded to readwrite/readonly _only_ by registering them through `router.access(...)`. Never wrap the console mount in `.access()` — doing so hands non-admins the ability to delete recorded history. The mount is also gated on the `enableConsole` config flag, so turning it off removes the route rather than leaving it to refuse.

Their wire shapes live in `src/api-contract.ts` and are shared by both surfaces: handlers assert responses with `satisfies`, and the panel casts fetch results to the same types. Add or rename a response field there, not inline — that is what stops the server and panel drifting apart silently.

### Tests

`src/test/` contains `node:test` suites for ILP encoding, time range parsing, query building, and v2 history. They run against the **compiled** JS in `dist/test/`, so always build before testing.

## Conventions

- TypeScript strict mode everywhere, panel included; do not loosen either `tsconfig.json`. In particular never add `jsx` or a DOM `lib` to the root one — server code referencing `document` would then typecheck clean.
- **Relative imports in the runtime need the `.js` extension** (`./query-client.js`, even though the source is `.ts`). That is Node's ESM rule, enforced by `moduleResolution: "nodenext"`. Omitting it fails the build, which is the point — switching to `"bundler"` resolution would silently emit unresolvable specifiers instead.
- **`.npmignore` excludes config files by exact filename** — rename one and the stale entry silently publishes it. `eslint.config.ts` is loaded through `jiti` (eslint 10 needs it for TypeScript configs); it is a devDependency and never ships.
- **The three ignore files serve opposite purposes for build output.** `public/assets/` is gitignored and prettier-ignored, but must NEVER be added to `.npmignore`: `remoteEntry.js` imports those chunks by name, so excluding them publishes a panel that 404s on load. Only genuinely local artifacts (`.mf/`) belong in all three.
- Prettier + eslint flat config (`eslint.config.ts`); run `npm run format` before committing.
- The config panel uses React 19 with Module Federation — keep the federation `shared` block in sync with `package.json`'s React version.
- **Classic JSX is load-bearing.** `jsxRuntime: "classic"` (vite.config.ts) and `"jsx": "react"` (panel tsconfig) must agree. The automatic runtime imports `react/jsx-runtime`, which is not in the federation `shared` scope, so the remote would carry its own copy of React's jsx runtime — a second React instance whose dispatcher is not the host's, so `useState` reads null and the panel fails to mount. At runtime, with no build error.
- **Imports leaving `src/configpanel/` must be `import type`.** A value import pulls the module's runtime dependencies into the browser bundle (typebox via `config/schema`, `fs/promises` via `host-limits`). After changing panel imports, confirm with `grep -rl "typebox\|fs/promises" public/` — it must print nothing.
- `signalk.appIcon` and `signalk.displayName` in `package.json` control how the plugin appears in the Admin UI.

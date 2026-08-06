# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- `npm run build` — TypeScript compile (`tsc`) + panel typecheck + webpack bundle of the React config panel into `public/`
- `npm run build:config` — Webpack only (rebuild the config panel without recompiling TS)
- `npm run typecheck:panel` — typecheck the config panel (`tsc -p src/configpanel --noEmit`); webpack only strips its types, so this is the only thing that checks them
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
2. **React config panel** (`src/configpanel/`) — TypeScript + JSX, bundled by webpack via **Module Federation** into `public/remoteEntry.js`, exposed to the Signal K Admin UI as `./PluginConfigurationPanel`. React 19 is shared as a singleton with the host UI. It has its own `tsconfig.json` (browser libs, classic JSX, `noEmit`) so DOM types never reach the Node compile.

Both build outputs are shipped in the npm package; `prepublishOnly` rebuilds them.

### Data flow

- Subscribes to the Signal K **streambundle** for all paths and writes filtered/throttled deltas to QuestDB via **ILP over raw TCP** (`src/ilp-writer.ts`) — no QuestDB client library.
- Three tables (`signalk`, `signalk_str`, `signalk_position`) defined and queried in `src/query-client.ts`. All use WAL mode + daily partitioning + dedup.
- Reads come from two registered providers:
  - **v2** (`src/history-v2.ts`) — `app.registerHistoryApiProvider()`, REST under `/signalk/v2/api/history/`. Aggregates map directly to QuestDB SQL except `sma`/`ema` (computed client-side).
  - **v1** (`src/history-v1.ts`) — `app.registerHistoryProvider()`, WebSocket playback with chunked reads.
- Retention (`src/retention.ts`) drops old daily partitions on a timer.

### Container integration

The plugin **does not run QuestDB itself**. It declares an optional peer dep on `signalk-container` and looks up that plugin's API at runtime (`ContainerManagerApi` in `src/index.ts`). When `managedContainer` is true, it calls `ensureRunning` / `ensureNetwork` / `pullImage` / `stop` to control a QuestDB container on the shared `sk-network` Podman/Docker network. The plugin lifecycle (`start`/`stop`) drives container lifecycle.

When `managedContainer` is false, the plugin connects to an external QuestDB at the configured host/ports.

### Config schema

`src/config/schema.ts` uses **TypeBox** (`@sinclair/typebox`) — the schema is the single source of truth for the JSON-schema Signal K shows in the Admin UI **and** for the `Config` TS type. Add new options there.

### REST endpoints

All extra plugin endpoints live under `/plugins/signalk-questdb/api/` and are wired in `src/index.ts` via the `IRouter` Signal K passes to `registerWithRouter`. The `/query` endpoint is gated by `isReadOnlySQL` in `src/query-client.ts` — DDL/DML must remain blocked.

Their wire shapes live in `src/api-contract.ts` and are shared by both surfaces: handlers assert responses with `satisfies`, and the panel casts fetch results to the same types. Add or rename a response field there, not inline — that is what stops the server and panel drifting apart silently.

### Tests

`src/test/` contains `node:test` suites for ILP encoding, time range parsing, query building, and v2 history. They run against the **compiled** JS in `dist/test/`, so always build before testing.

## Conventions

- TypeScript strict mode everywhere, panel included; do not loosen either `tsconfig.json`. In particular never add `jsx` or a DOM `lib` to the root one — server code referencing `document` would then typecheck clean.
- **Relative imports in the runtime need the `.js` extension** (`./query-client.js`, even though the source is `.ts`). That is Node's ESM rule, enforced by `moduleResolution: "nodenext"`. Omitting it fails the build, which is the point — switching to `"bundler"` resolution would silently emit unresolvable specifiers instead.
- **Tooling configs are `.cjs`** (`webpack.config.cjs`, `eslint.config.cjs`) because they use `require`, which `"type": "module"` forbids in `.js`. If you rename them again, update `.npmignore` — it excludes them by exact filename, and a stale entry silently publishes them.
- Prettier + eslint flat config (`eslint.config.cjs`); run `npm run format` before committing.
- The config panel uses React 19 with Module Federation — keep the federation `shared` block in sync with `package.json`'s React version.
- **Classic JSX is load-bearing.** Babel's `runtime: "classic"` (webpack.config.cjs) and `"jsx": "react"` (panel tsconfig) must agree. The automatic runtime imports `react/jsx-runtime`, which is not in the federation `shared` scope, so webpack would bundle a second React into the remote and the Admin UI's panel loader breaks — at runtime, with no build error.
- **Imports leaving `src/configpanel/` must be `import type`.** Babel strips types per-file without type information, so a value import pulls the module's runtime dependencies into the browser bundle (typebox via `config/schema`, `fs/promises` via `host-limits`). After changing panel imports, confirm with `grep -l "typebox\|fs/promises" public/*.js` — it must print nothing.
- `signalk.appIcon` and `signalk.displayName` in `package.json` control how the plugin appears in the Admin UI.

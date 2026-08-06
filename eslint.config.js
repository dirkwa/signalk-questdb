const { defineConfig, globalIgnores } = require("eslint/config");
const js = require("@eslint/js");
const tseslint = require("typescript-eslint");
const prettier = require("eslint-config-prettier/flat");
const reactHooks = require("eslint-plugin-react-hooks");
const globals = require("globals");

module.exports = defineConfig([
  globalIgnores(["dist", "public", "node_modules"]),

  {
    files: ["**/*.ts"],
    // The config panel is a browser bundle with its own block below; without
    // this it would match here and be linted against Node globals.
    ignores: ["src/configpanel/**"],
    extends: [js.configs.recommended, tseslint.configs.recommended, prettier],
    languageOptions: {
      parser: tseslint.parser,
      globals: globals.node,
    },
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unused-vars": "error",
    },
  },

  {
    files: ["src/configpanel/**/*.{ts,tsx}"],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      // configs.flat.* — the top-level presets still ship the legacy array
      // `plugins` shape, which eslint 10 flat config rejects outright.
      reactHooks.configs.flat["recommended-latest"],
      prettier,
    ],
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: { ecmaFeatures: { jsx: true } },
      globals: globals.browser,
    },
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unused-vars": "error",
      // The panel polls plugin REST endpoints from effects and stores the
      // replies in state — the "subscribe to an external system" case the
      // rule explicitly allows, but it cannot see through the async
      // fetch helpers to tell that apart from a render-cascade. Silencing
      // it would need either a data-fetching library or hoisting the polls
      // out of React entirely; neither belongs in a type conversion.
      // rules-of-hooks and exhaustive-deps stay on.
      "react-hooks/set-state-in-effect": "off",
    },
  },
]);

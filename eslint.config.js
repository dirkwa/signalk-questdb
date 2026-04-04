const { defineConfig, globalIgnores } = require("eslint/config");
const js = require("@eslint/js");
const tseslint = require("typescript-eslint");
const prettier = require("eslint-config-prettier/flat");
const globals = require("globals");

module.exports = defineConfig([
  globalIgnores(["dist", "public", "node_modules", "src/configpanel"]),

  {
    files: ["**/*.ts"],
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
]);

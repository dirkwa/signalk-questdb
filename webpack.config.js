const path = require("path");
const { ModuleFederationPlugin } = require("webpack").container;
const packageJson = require("./package.json");

/** @type {import("webpack").Configuration} */
module.exports = {
  entry: "./src/configpanel/index",
  mode: "production",
  output: {
    path: path.resolve(__dirname, "public"),
    clean: false,
  },
  module: {
    rules: [
      {
        test: /\.[jt]sx?$/,
        loader: "babel-loader",
        exclude: /node_modules/,
        // runtime "classic" is load-bearing: Babel 8 changed preset-react's
        // default to the "automatic" runtime, whose emitted code imports
        // react/jsx-runtime. That module is not in the Module Federation
        // `shared` scope below, so webpack bundles a private copy of React's
        // jsx runtime into the remote — a second React instance in the host
        // page — and the Admin UI's panel loader fails. Classic emits
        // React.createElement against the shared singleton React.
        //
        // preset-typescript only STRIPS types — it never checks them. The
        // real gate is `npm run typecheck:panel` (tsc --noEmit), which the
        // `build` script runs before webpack. Presets apply right-to-left,
        // so types are stripped first and preset-react then transforms the
        // remaining JSX.
        options: {
          presets: [
            ["@babel/preset-react", { runtime: "classic" }],
            "@babel/preset-typescript",
          ],
        },
      },
    ],
  },
  resolve: {
    extensions: [".tsx", ".ts", ".js", ".jsx"],
  },
  plugins: [
    new ModuleFederationPlugin({
      name: packageJson.name.replace(/[-@/]/g, "_"),
      library: {
        type: "var",
        name: packageJson.name.replace(/[-@/]/g, "_"),
      },
      filename: "remoteEntry.js",
      exposes: {
        "./PluginConfigurationPanel":
          "./src/configpanel/PluginConfigurationPanel",
      },
      shared: {
        react: { singleton: true, requiredVersion: "^19" },
        "react-dom": { singleton: true, requiredVersion: "^19" },
      },
    }),
  ],
};

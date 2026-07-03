const path = require("path");
const { ModuleFederationPlugin } = require("webpack").container;
const packageJson = require("./package.json");

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
        test: /\.jsx?$/,
        loader: "babel-loader",
        exclude: /node_modules/,
        // runtime "classic" is load-bearing: Babel 8 changed preset-react's
        // default to the "automatic" runtime, whose emitted code imports
        // react/jsx-runtime. That module is not in the Module Federation
        // `shared` scope below, so webpack bundles a private copy of React's
        // jsx runtime into the remote — a second React instance in the host
        // page — and the Admin UI's panel loader fails. Classic emits
        // React.createElement against the shared singleton React.
        options: {
          presets: [["@babel/preset-react", { runtime: "classic" }]],
        },
      },
    ],
  },
  resolve: {
    extensions: [".js", ".jsx"],
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

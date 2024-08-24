const path = require("path");
const CompressionPlugin = require("compression-webpack-plugin");
const { BundleAnalyzerPlugin } = require("webpack-bundle-analyzer");

module.exports = {
  entry: "./src/index.ts", // Entry point of the application
  output: {
    filename: "index.js", // Use [name] to ensure unique filenames for different chunks
    path: path.resolve(__dirname, "dist"), // Output directory
    clean: true, // Clean the output directory before building
    libraryTarget: "commonjs2", // Export as a Node.js module
  },
  mode: "production", // Set the mode to production for optimization
  module: {
    rules: [
      {
        test: /\.ts$/, // Apply this rule to .ts files
        use: "ts-loader", // Use ts-loader to transpile TypeScript to JavaScript
        exclude: /node_modules/, // Exclude the node_modules directory
      },
    ],
  },
  resolve: {
    extensions: [".ts", ".js"], // Resolve these extensions
  },
  optimization: {
    minimize: true, // Minimize the output bundle
    splitChunks: false, // Disable chunk splitting to ensure a single output file
  },
  plugins: [
    new CompressionPlugin({
      test: /\.js(\?.*)?$/i, // Apply compression to .js files
    }),
    new BundleAnalyzerPlugin({
      analyzerMode: "static", // Output a static HTML file for analysis
      reportFilename: path.resolve(__dirname, "report.html"), // Root directory
      openAnalyzer: false, // Prevent the analyzer from opening automatically
    }),
  ],
  performance: {
    hints: false, // Disable all performance hints to suppress warnings about large bundles
  },
  target: "node", // Targeting a Node.js environment
};

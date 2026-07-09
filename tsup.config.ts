import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "memory/index": "src/memory/index.ts",
    "dynamodb/index": "src/dynamodb/index.ts",
  },
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  external: ["@aws-sdk/client-dynamodb", "@opentelemetry/api"],
});

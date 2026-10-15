// Typed async-safety gate.
// Only promise-handling rules — no style rules.
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist/**", "node_modules/**", "bin/**", "**/*.mjs", "*.config.ts"] },
  {
    files: ["src/**/*.ts"],
    languageOptions: {
      parser: tseslint.parser,
      // tsconfig.test.json = tsconfig.json + the *.test.ts files, so tests are linted too.
      parserOptions: { project: ["./tsconfig.test.json"], tsconfigRootDir: import.meta.dirname },
    },
    plugins: { "@typescript-eslint": tseslint.plugin },
    linterOptions: { reportUnusedDisableDirectives: "off" },
    rules: {
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": ["error", { checksConditionals: true }],
      "@typescript-eslint/return-await": ["error", "in-try-catch"],
    },
  },
);

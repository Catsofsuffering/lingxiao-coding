---
applyTo: "scripts/**/*.mjs"
---

# Build And Script Review Guidance

- This repository must build and test reliably on Windows, macOS, and Linux. Flag shell assumptions that only work on Unix, path handling that breaks on Windows, and quoting or glob behavior that differs in PowerShell.
- Pay close attention to `build`, `test`, packaging, release, install, and health-check scripts. Small script regressions can break the entire developer workflow.
- Prefer findings about destructive filesystem behavior, incorrect cwd assumptions, stale generated output, dist/source-of-truth drift, and non-idempotent install or build steps.
- Check that script changes still agree with `package.json` scripts and the intended build flow:
  - `npm run build`
  - `npm run build:server`
  - `npm run build:web`
  - `npm test`
  - `npm run test:ci`
- Flag changes that silently skip validation or make failures harder to diagnose in CI.

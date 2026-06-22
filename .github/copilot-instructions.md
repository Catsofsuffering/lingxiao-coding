# LingXiao Copilot Review Instructions

Review pull requests for bugs, regressions, security issues, and missing verification first. Keep feedback concrete and tied to changed files.

## Repository shape

- `src/`: Node.js 24+ TypeScript backend, CLI, orchestration runtime, Fastify server, MCP integrations, build and tool plumbing.
- `web/src/`: React 19 + Vite frontend with Zustand stores and Tailwind CSS v4 styling.
- `scripts/`: build, test, packaging, health-check, and release helpers.

## What to pay attention to

- Cross-platform behavior matters. This project supports Windows, macOS, and Linux. Flag Unix-only shell assumptions, path separator mistakes, shell glob assumptions, and commands that break in PowerShell.
- Build and packaging flows are critical. Be careful around `scripts/build.mjs`, `scripts/run-tests.mjs`, `package.json` scripts, dist generation, and release/package logic.
- The backend has many long-lived runtime and orchestration paths. Prioritize correctness, state consistency, cancellation/cleanup behavior, permissions, and event ordering over style feedback.
- The frontend is state-heavy. Watch for stale Zustand state, broken SSE/runtime synchronization, regressions in task/session/workbench views, and mismatches between backend contracts and frontend types.
- Security-sensitive areas include browser automation, shell execution, Git operations, MCP/server bridges, file system access, auth or token handling, and any route exposed from Fastify services.
- Prefer comments about user-visible regressions, broken invariants, race conditions, error handling gaps, and missing tests. Avoid low-value style nits unless they hide a real maintenance risk.

## Verification expectations

- When backend logic changes, check whether tests should be added or updated under `src/**/*.test.ts`, `web/src/**/*.test.ts`, or `scripts/*.test.mjs`.
- The main verification commands are:
  - `npm run build`
  - `npm run build:server`
  - `npm run build:web`
  - `npm test`
  - `npm run test:ci`
- Many tests use the built-in `node:test` runner.
- Some web tests run directly with `tsx --test`, for example store and view-model tests in `web/src/`.

## Review preferences

- Call out when a PR changes generated or packaged behavior without updating the corresponding source-of-truth files or tests.
- Call out when a change introduces new configuration, workflow, or automation behavior without documenting the operational impact.
- If a change touches both `src/` and `web/src/`, check that API shapes, event names, and runtime state projections still line up.
- If a change is safe, say so briefly. If something is uncertain, explain what should be verified manually.

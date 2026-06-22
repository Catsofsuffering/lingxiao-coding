# LingXiao Copilot Coding Agent Instructions

When working in this repository, keep changes narrow and finish with clear verification notes.

## Repository expectations

- Backend code lives in `src/` and uses Node.js 24+, TypeScript, Fastify, and long-lived orchestration/runtime flows.
- Frontend code lives in `web/src/` and uses React 19, Vite, Zustand, and shared contract types with the backend.
- Build and release plumbing lives in `scripts/` and must remain cross-platform.

## Working rules

- Prefer modifying existing patterns instead of introducing new abstractions.
- Avoid unrelated refactors while addressing a task.
- Treat shell execution, browser automation, filesystem operations, Git integration, and MCP bridges as security-sensitive.
- Keep Windows, macOS, and Linux compatibility in mind when editing scripts or path handling.
- If a task touches both backend and frontend, verify that payload shapes and event names still align.

## Validation

- Use the smallest relevant checks first, then broader checks if the change is cross-cutting.
- Common commands:
  - `npm run build`
  - `npm run build:server`
  - `npm run build:web`
  - `npm test`
  - `npm run test:ci`
- Some frontend tests run directly with `tsx --test` under `web/src/`.

## PR expectations

- Summarize user-visible impact, key files changed, and exact validation performed.
- Call out any follow-up manual GitHub settings work if code alone does not complete the task.
- For GitHub Project automation changes, keep field names, project numbers, and GraphQL mutations aligned with the live Project configuration and mention any required secrets or Project-side setup in the PR.

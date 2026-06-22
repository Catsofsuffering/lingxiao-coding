---
applyTo: "src/**/*.ts"
---

# Backend Review Guidance

- Treat orchestration, session runtime, worker lifecycle, permissions, Git integration, browser automation, MCP bridges, and Fastify routes as high-risk areas.
- Prefer findings about correctness, cleanup, cancellation, event ordering, permission bypass, data leakage, and contract drift over style feedback.
- Watch for changes that can break long-lived processes or daemon-style behavior, especially around `dist/` assumptions, worker spawning, and runtime recovery.
- Check that new or changed APIs still line up with the frontend consumers in `web/src/` and with shared contract types.
- Flag insecure use of shell execution, filesystem access, browser automation, or externally supplied content.
- When behavior changes, look for missing tests in `src/**/*.test.ts` or missing updates to script-based checks.

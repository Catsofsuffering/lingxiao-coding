---
applyTo: "web/src/**/*.{ts,tsx,css}"
---

# Frontend Review Guidance

- Focus on state correctness first. This frontend relies heavily on Zustand stores, SSE/runtime synchronization, and shared backend contracts.
- Flag regressions where UI state can become stale, optimistic state is never reconciled, or backend event changes are not reflected in view-model logic.
- Check that TypeScript types still match server payloads, especially for sessions, workflows, Git state, browser state, and workbench panels.
- For React code, prefer findings about broken behavior, rendering errors, accessibility issues, loading/error states, and contract mismatches over naming or formatting nits.
- For CSS changes, watch for layout breakage, unreadable contrast, hidden overflow, and unintended theme regressions in `theme.css` and related style files.
- When stores, view models, or UI coordination logic change, look for missing tests under `web/src/**/*.test.ts`.

# GitHub PR Review Agent Evaluation

## Context

This repository has already started adopting GitHub-native review infrastructure:

- Copilot code review instructions
- Copilot cloud agent preparation
- Dependabot update policy
- CodeQL workflow

The remaining question is whether a third-party PR review agent should be introduced on top of the native GitHub stack.

## Current repository fit

- The repository is public.
- The codebase spans backend runtime orchestration, frontend state-heavy UI, build scripts, browser automation, Git integration, and MCP bridges.
- Review quality matters more than review volume reduction alone.
- The current pull request volume is still low, so tool overlap would be more noticeable than at high PR throughput.

## Candidate

The most credible third-party candidate at this stage is CodeRabbit.

Why it is the main candidate:

- It is purpose-built for PR review workflows.
- It supports repository-level configuration.
- It is widely used as a GitHub App rather than a custom self-hosted reviewer pipeline.

## Recommendation

Do not introduce a third-party PR review agent yet.

Adopt the native GitHub stack first, then reassess after there is enough real PR traffic to judge review gaps.

## Why not now

- GitHub-native review capability is not fully exercised yet. Copilot review, Copilot cloud agent, Dependabot, and CodeQL should be validated first in real PRs.
- Adding CodeRabbit now would likely create overlapping comments with Copilot review before we know where native coverage is insufficient.
- Extra bot noise is especially costly in a repository that mixes infrastructure, frontend, runtime, and security-sensitive code.
- A second reviewer bot is easier to justify when maintainers can point to a repeated gap, for example:
  - PR summaries are still too weak
  - regression risks are missed in review
  - reviewers spend too much time on large routine dependency or refactor PRs

## Re-evaluation triggers

Revisit third-party adoption when one or more of the following becomes true:

- PR volume increases and review latency becomes a maintainer bottleneck.
- Native Copilot review repeatedly misses the kinds of findings maintainers care about.
- Reviewers want stronger PR summaries, change risk clustering, or follow-up suggestions than GitHub-native review provides.
- The team wants a dedicated PR review bot policy separate from Copilot coding agent usage.

## If adoption is reconsidered later

Recommended order:

1. Keep GitHub-native Copilot review and CodeQL enabled.
2. Trial CodeRabbit on a small set of PRs or a time-boxed evaluation window.
3. Compare signal quality, duplicate comment rate, and reviewer effort.
4. Keep it only if the incremental signal is clearly worth the extra bot surface.

## Decision

For now: no third-party PR review agent.

Track this as an evaluation item, not an implementation item.

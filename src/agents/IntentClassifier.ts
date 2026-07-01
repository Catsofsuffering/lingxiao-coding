/**
 * Capability intent profile normalization.
 *
 * This module does not call an LLM. The Leader records a profile through the
 * `record_capability_intent` tool, then runtime validates and stores it here.
 */

import {
  isCapabilityDeny,
  isCapabilityGrant,
  isIntentPhase,
  isIntentScopeKind,
  isPrimaryIntent,
  isRequiredGate,
  type CapabilityDeny,
  type CapabilityGrant,
  type CapabilityIntentProfile,
  type IntentConstraints,
  type IntentScope,
  type RequiredGate,
} from '../contracts/types/Autonomy.js';
import { isOperationRisk } from '../contracts/types/AutonomyDecision.js';

export function normalizeInputForIntent(raw: unknown): string {
  if (typeof raw !== 'string') return '';
  return raw
    .replace(/[​-‍﻿]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function uniqueStrings(value: unknown): readonly string[] | null {
  if (!Array.isArray(value)) return null;
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== 'string') return null;
    const trimmed = normalizeInputForIntent(item);
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function knownArray<T extends string>(value: unknown, guard: (v: unknown) => v is T): readonly T[] | null {
  if (!Array.isArray(value)) return null;
  const out: T[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (!guard(item)) return null;
    if (seen.has(item)) continue;
    seen.add(item);
    out.push(item);
  }
  return out;
}

function normalizeScope(value: unknown): IntentScope | null {
  const record = asRecord(value);
  if (!record || !isIntentScopeKind(record.kind)) return null;
  const paths = record.paths === undefined ? undefined : uniqueStrings(record.paths);
  const surfaces = record.surfaces === undefined ? undefined : uniqueStrings(record.surfaces);
  const taskIds = record.taskIds === undefined ? undefined : uniqueStrings(record.taskIds);
  const subsystemIds = record.subsystemIds === undefined ? undefined : uniqueStrings(record.subsystemIds);
  const externalTargets = record.externalTargets === undefined ? undefined : uniqueStrings(record.externalTargets);
  if (paths === null || surfaces === null || taskIds === null || subsystemIds === null || externalTargets === null) return null;
  if (record.kind === 'selected_paths' && (!paths || paths.length === 0)) return null;
  return {
    kind: record.kind,
    ...(paths ? { paths } : {}),
    ...(surfaces ? { surfaces } : {}),
    ...(taskIds ? { taskIds } : {}),
    ...(subsystemIds ? { subsystemIds } : {}),
    ...(externalTargets ? { externalTargets } : {}),
  };
}

function normalizeConstraints(value: unknown): IntentConstraints | null {
  if (value === undefined || value === null) return {};
  const record = asRecord(value);
  if (!record) return null;
  const allowedTools = record.allowedTools === undefined ? undefined : uniqueStrings(record.allowedTools);
  const deniedTools = record.deniedTools === undefined ? undefined : uniqueStrings(record.deniedTools);
  const allowedPaths = record.allowedPaths === undefined ? undefined : uniqueStrings(record.allowedPaths);
  const deniedPaths = record.deniedPaths === undefined ? undefined : uniqueStrings(record.deniedPaths);
  const commandAllowlist = record.commandAllowlist === undefined ? undefined : uniqueStrings(record.commandAllowlist);
  const commandDenylist = record.commandDenylist === undefined ? undefined : uniqueStrings(record.commandDenylist);
  if (allowedTools === null || deniedTools === null || allowedPaths === null || deniedPaths === null || commandAllowlist === null || commandDenylist === null) return null;
  if (record.maxRisk !== undefined && !isOperationRisk(record.maxRisk)) return null;
  if (record.mustStayWithinBlueprint !== undefined && typeof record.mustStayWithinBlueprint !== 'boolean') return null;
  if (record.requireEvidence !== undefined && typeof record.requireEvidence !== 'boolean') return null;
  return {
    ...(record.maxRisk !== undefined ? { maxRisk: record.maxRisk } : {}),
    ...(allowedTools ? { allowedTools } : {}),
    ...(deniedTools ? { deniedTools } : {}),
    ...(allowedPaths ? { allowedPaths } : {}),
    ...(deniedPaths ? { deniedPaths } : {}),
    ...(commandAllowlist ? { commandAllowlist } : {}),
    ...(commandDenylist ? { commandDenylist } : {}),
    ...(typeof record.mustStayWithinBlueprint === 'boolean' ? { mustStayWithinBlueprint: record.mustStayWithinBlueprint } : {}),
    ...(typeof record.requireEvidence === 'boolean' ? { requireEvidence: record.requireEvidence } : {}),
  };
}

export interface NormalizeCapabilityIntentProfileOptions {
  turnId?: number | null;
  now?: number;
  source?: string;
}

export function normalizeCapabilityIntentProfile(
  payload: unknown,
  options: NormalizeCapabilityIntentProfileOptions = {},
): CapabilityIntentProfile | null {
  const record = typeof payload === 'string' ? (() => {
    try { return asRecord(JSON.parse(payload) as unknown); } catch { return null; }
  })() : asRecord(payload);
  if (!record) return null;
  if (!isPrimaryIntent(record.primaryIntent)) return null;
  if (!isIntentPhase(record.phase)) return null;
  const scope = normalizeScope(record.scope);
  if (!scope) return null;
  const grants = knownArray<CapabilityGrant>(record.grants, isCapabilityGrant);
  const denies = knownArray<CapabilityDeny>(record.denies, isCapabilityDeny);
  const requiredGates = knownArray<RequiredGate>(record.requiredGates, isRequiredGate);
  const constraints = normalizeConstraints(record.constraints);
  if (!grants || !denies || !requiredGates || !constraints) return null;
  if (typeof record.confidence !== 'number' || !Number.isFinite(record.confidence)) return null;
  const reason = typeof record.reason === 'string' ? normalizeInputForIntent(record.reason) : '';
  if (!reason) return null;
  return {
    primaryIntent: record.primaryIntent,
    scope,
    phase: record.phase,
    grants,
    denies,
    requiredGates,
    constraints,
    confidence: Math.max(0, Math.min(1, record.confidence)),
    reason,
    turnId: options.turnId ?? null,
    recordedAt: options.now ?? Date.now(),
    source: options.source ?? 'record_capability_intent',
  };
}

export function defaultCapabilityIntentProfile(
  reason = 'intent_profile_not_recorded',
  options: { turnId?: number | null; now?: number } = {},
): CapabilityIntentProfile {
  return {
    primaryIntent: 'diagnose',
    scope: { kind: 'read_only' },
    phase: 'understand',
    grants: ['read'],
    denies: ['write', 'shell', 'task', 'dispatch'],
    requiredGates: [
      'confirm_before_write',
      'confirm_before_command',
      'confirm_before_dispatch',
      'confirm_before_workflow_apply',
      'confirm_before_scope_expansion',
    ],
    constraints: {
      maxRisk: 'low',
      requireEvidence: false,
    },
    confidence: 0,
    reason,
    turnId: options.turnId ?? null,
    recordedAt: options.now ?? Date.now(),
    source: 'runtime_default',
  };
}

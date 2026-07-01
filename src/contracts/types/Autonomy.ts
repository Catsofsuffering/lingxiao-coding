/**
 * Autonomy Harness core contracts.
 *
 * The user-request model is a capability envelope, not a single intent label.
 * `primaryIntent` summarizes the goal for display, while `grants`, `denies`,
 * `requiredGates`, `scope`, and `phase` are the policy-bearing fields used by
 * leader gates and runtime projection.
 */

import type { OperationRisk } from './AutonomyDecision.js';

/* ──────────────────────────── Capability intent profile ──────────────────────────── */

export type PrimaryIntent =
  | 'diagnose'
  | 'explain'
  | 'plan'
  | 'implement'
  | 'fix'
  | 'refactor'
  | 'verify'
  | 'operate'
  | 'research';

export const PRIMARY_INTENTS = [
  'diagnose',
  'explain',
  'plan',
  'implement',
  'fix',
  'refactor',
  'verify',
  'operate',
  'research',
] as const;

export function isPrimaryIntent(value: unknown): value is PrimaryIntent {
  return typeof value === 'string' && (PRIMARY_INTENTS as readonly string[]).includes(value);
}

export type IntentScopeKind =
  | 'read_only'
  | 'workspace'
  | 'selected_paths'
  | 'project'
  | 'system'
  | 'external';

export const INTENT_SCOPE_KINDS = [
  'read_only',
  'workspace',
  'selected_paths',
  'project',
  'system',
  'external',
] as const;

export function isIntentScopeKind(value: unknown): value is IntentScopeKind {
  return typeof value === 'string' && (INTENT_SCOPE_KINDS as readonly string[]).includes(value);
}

export interface IntentScope {
  readonly kind: IntentScopeKind;
  readonly paths?: readonly string[];
  readonly surfaces?: readonly string[];
  readonly taskIds?: readonly string[];
  readonly subsystemIds?: readonly string[];
  readonly externalTargets?: readonly string[];
}

export type IntentPhase =
  | 'understand'
  | 'design'
  | 'prepare'
  | 'execute'
  | 'verify'
  | 'finalize'
  | 'recover';

export const INTENT_PHASES = [
  'understand',
  'design',
  'prepare',
  'execute',
  'verify',
  'finalize',
  'recover',
] as const;

export function isIntentPhase(value: unknown): value is IntentPhase {
  return typeof value === 'string' && (INTENT_PHASES as readonly string[]).includes(value);
}

export type Capability = 'read' | 'write' | 'shell' | 'task' | 'dispatch';
export type CapabilityGrant = Capability;
export type CapabilityDeny = Capability;

export const CAPABILITIES = [
  'read',
  'write',
  'shell',
  'task',
  'dispatch',
] as const;

export const CAPABILITY_GRANTS = CAPABILITIES;
export const CAPABILITY_DENIES = CAPABILITIES;

export function isCapability(value: unknown): value is Capability {
  return typeof value === 'string' && (CAPABILITIES as readonly string[]).includes(value);
}

export function isCapabilityGrant(value: unknown): value is CapabilityGrant {
  return isCapability(value);
}

export function isCapabilityDeny(value: unknown): value is CapabilityDeny {
  return isCapability(value);
}

export type RequiredGate =
  | 'confirm_before_write'
  | 'confirm_before_command'
  | 'confirm_before_dispatch'
  | 'confirm_before_workflow_apply'
  | 'confirm_before_scope_expansion'
  | 'confirm_before_network'
  | 'confirm_before_git'
  | 'confirm_before_permission_change'
  | 'blueprint_coverage'
  | 'read_before_write'
  | 'verify_after_change';

export const REQUIRED_GATES = [
  'confirm_before_write',
  'confirm_before_command',
  'confirm_before_dispatch',
  'confirm_before_workflow_apply',
  'confirm_before_scope_expansion',
  'confirm_before_network',
  'confirm_before_git',
  'confirm_before_permission_change',
  'blueprint_coverage',
  'read_before_write',
  'verify_after_change',
] as const;

export function isRequiredGate(value: unknown): value is RequiredGate {
  return typeof value === 'string' && (REQUIRED_GATES as readonly string[]).includes(value);
}

export interface IntentConstraints {
  readonly maxRisk?: OperationRisk;
  readonly allowedTools?: readonly string[];
  readonly deniedTools?: readonly string[];
  readonly allowedPaths?: readonly string[];
  readonly deniedPaths?: readonly string[];
  readonly commandAllowlist?: readonly string[];
  readonly commandDenylist?: readonly string[];
  readonly mustStayWithinBlueprint?: boolean;
  readonly requireEvidence?: boolean;
}

export interface CapabilityIntentProfile {
  readonly primaryIntent: PrimaryIntent;
  readonly scope: IntentScope;
  readonly phase: IntentPhase;
  readonly grants: readonly CapabilityGrant[];
  readonly denies: readonly CapabilityDeny[];
  readonly requiredGates: readonly RequiredGate[];
  readonly constraints: IntentConstraints;
  readonly confidence: number;
  readonly reason: string;
  readonly turnId: number | null;
  readonly recordedAt: number;
  readonly source: 'record_capability_intent' | 'runtime_default' | string;
}

/* ──────────────────────────── AutonomyMode ──────────────────────────── */

export type AutonomyMode = 'review_first' | 'balanced' | 'autonomous';

export const AUTONOMY_MODES = [
  'review_first',
  'balanced',
  'autonomous',
] as const;

export const AUTONOMY_MODE_ALIASES = {
  full_auto: 'autonomous',
} as const satisfies Record<string, AutonomyMode>;

export const DEFAULT_AUTONOMY_MODE: AutonomyMode = 'balanced';

export function isAutonomyMode(value: unknown): value is AutonomyMode {
  return typeof value === 'string' && (AUTONOMY_MODES as readonly string[]).includes(value);
}

export function coerceAutonomyModeAlias(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  const normalized = value.trim();
  return AUTONOMY_MODE_ALIASES[normalized as keyof typeof AUTONOMY_MODE_ALIASES] ?? normalized;
}

export function normalizeAutonomyMode(value: unknown): AutonomyMode {
  const canonical = coerceAutonomyModeAlias(value);
  return isAutonomyMode(canonical) ? canonical : DEFAULT_AUTONOMY_MODE;
}

/* ──────────────────────────── AutonomyLifecyclePhase ──────────────────────────── */

export type AutonomyLifecyclePhase = 'bootstrap' | 'active' | 'recovery' | 'stable';

export const AUTONOMY_LIFECYCLE_PHASES = [
  'bootstrap',
  'active',
  'recovery',
  'stable',
] as const;

export const DEFAULT_AUTONOMY_LIFECYCLE_PHASE: AutonomyLifecyclePhase = 'bootstrap';

export function isAutonomyLifecyclePhase(value: unknown): value is AutonomyLifecyclePhase {
  return typeof value === 'string' && (AUTONOMY_LIFECYCLE_PHASES as readonly string[]).includes(value);
}

export function normalizeAutonomyLifecyclePhase(value: unknown): AutonomyLifecyclePhase {
  return isAutonomyLifecyclePhase(value) ? value : DEFAULT_AUTONOMY_LIFECYCLE_PHASE;
}

/* ──────────────────────────── AutonomyDecisionKind ──────────────────────────── */

export type AutonomyDecisionKind =
  | 'allow'
  | 'dry_run_only'
  | 'require_confirmation'
  | 'forbid'
  | 'escalate_to_leader';

export const AUTONOMY_DECISION_KINDS = [
  'allow',
  'dry_run_only',
  'require_confirmation',
  'forbid',
  'escalate_to_leader',
] as const;

export function isAutonomyDecisionKind(value: unknown): value is AutonomyDecisionKind {
  return (
    typeof value === 'string' &&
    (AUTONOMY_DECISION_KINDS as readonly string[]).includes(value)
  );
}

export interface AutonomyDecision {
  kind: AutonomyDecisionKind;
  intentProfile: CapabilityIntentProfile;
  autonomyMode: AutonomyMode;
  reason: string;
  affectedTools: readonly string[];
}

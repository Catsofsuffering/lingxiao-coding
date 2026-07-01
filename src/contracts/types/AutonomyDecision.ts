/**
 * AutonomyDecisionEngine input/output contracts.
 *
 * The decision axis is capability profile × action × risk, with autonomy and
 * permission modes applied as overlays. `primaryIntent` is display context; it
 * is not a permission boundary.
 */

import type {
  AutonomyDecision as BaseAutonomyDecision,
  AutonomyDecisionKind,
  AutonomyMode,
  Capability,
  CapabilityGrant,
  CapabilityIntentProfile,
} from './Autonomy.js';

export {
  AUTONOMY_DECISION_KINDS,
  AUTONOMY_MODES,
  CAPABILITIES,
  CAPABILITY_DENIES,
  CAPABILITY_GRANTS,
  DEFAULT_AUTONOMY_MODE,
  INTENT_PHASES,
  INTENT_SCOPE_KINDS,
  PRIMARY_INTENTS,
  REQUIRED_GATES,
  isAutonomyDecisionKind,
  isAutonomyMode,
  isCapability,
  isCapabilityDeny,
  isCapabilityGrant,
  isIntentPhase,
  isIntentScopeKind,
  isPrimaryIntent,
  isRequiredGate,
  normalizeAutonomyMode,
} from './Autonomy.js';

export type {
  AutonomyDecisionKind,
  AutonomyMode,
  Capability,
  CapabilityDeny,
  CapabilityGrant,
  CapabilityIntentProfile,
  IntentConstraints,
  IntentPhase,
  IntentScope,
  IntentScopeKind,
  PrimaryIntent,
  RequiredGate,
} from './Autonomy.js';

/* ──────────────────────────── AutonomyAction ──────────────────────────── */

export type AutonomyAction =
  | 'read'
  | 'analyze'
  | 'propose'
  | 'apply_change'
  | 'dispatch'
  | 'workflow_apply'
  | 'run_command'
  | 'create_task'
  | 'scope_expand';

export const AUTONOMY_ACTIONS = [
  'read',
  'analyze',
  'propose',
  'apply_change',
  'dispatch',
  'workflow_apply',
  'run_command',
  'create_task',
  'scope_expand',
] as const;

export function isAutonomyAction(value: unknown): value is AutonomyAction {
  return typeof value === 'string' && (AUTONOMY_ACTIONS as readonly string[]).includes(value);
}

/* ──────────────────────────── OperationRisk ──────────────────────────── */

export type OperationRisk = 'low' | 'medium' | 'high' | 'critical';

export const OPERATION_RISKS = ['low', 'medium', 'high', 'critical'] as const;

export function isOperationRisk(value: unknown): value is OperationRisk {
  return typeof value === 'string' && (OPERATION_RISKS as readonly string[]).includes(value);
}

/* ──────────────────────────── Decision metadata ──────────────────────────── */

export type AutonomyDecisionSource =
  | 'intent_profile'
  | 'autonomy_default'
  | 'autonomy_explicit'
  | 'capability_grant'
  | 'capability_deny'
  | 'required_gate'
  | 'intent_constraint'
  | 'scope_match'
  | 'path_constraint'
  | 'command_constraint'
  | 'blueprint_coverage'
  | 'rule_action'
  | 'rule_permission';

export interface AutonomyDecisionEvidence {
  kind:
    | 'primary_intent'
    | 'intent_phase'
    | 'intent_scope'
    | 'autonomy_mode'
    | 'permission_mode'
    | 'control_mode'
    | 'leader_execution_mode'
    | 'tool_tier'
    | 'operation_risk'
    | 'read_only_flag'
    | 'rule_action'
    | 'capability_grant'
    | 'capability_deny'
    | 'required_gate'
    | 'intent_constraint'
    | 'scope_match'
    | 'path_constraint'
    | 'command_constraint'
    | 'blueprint_coverage'
    | 'rule_permission'
    | 'rule_hit';
  value: string;
  note?: string;
}

export interface BlueprintGateSnapshot {
  readyToDispatch: boolean;
  uncoveredSubsystemIds: readonly string[];
}

export interface AutonomyDecisionContext {
  intentProfile: CapabilityIntentProfile;
  autonomyMode: AutonomyMode;
  permissionMode?: 'strict' | 'dev' | 'networked' | 'yolo' | undefined;
  leaderExecutionMode?: 'direct' | 'hybrid' | 'delegate' | undefined;
  controlMode?: 'manual' | 'eternal' | undefined;
  toolTier?: 'read' | 'compute' | 'write' | 'execute' | undefined;
  action?: AutonomyAction | undefined;
  operationRisk: OperationRisk;
  isReadOnly: boolean;
  toolName?: string | undefined;
  targetPaths?: readonly string[] | undefined;
  command?: string | undefined;
  scopeExpansion?: boolean | undefined;
  blueprintCoverage?: BlueprintGateSnapshot | null | undefined;
}

export interface AutonomyDecision extends BaseAutonomyDecision {
  source: AutonomyDecisionSource;
  evidence: readonly AutonomyDecisionEvidence[];
  requiredCapability?: CapabilityGrant | null;
}

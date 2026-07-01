/**
 * AutonomyDecisionEngine —— runtime policy hint renderer for leader tool calls.
 *
 * This layer is intentionally NOT an authorization boundary. Capability profile
 * and confirmation policy are hints/evidence for the LLM/UI/audit trail. Actual
 * hard permission remains in PermissionSystem / ToolRegistry / explicit user
 * approval flows.
 */

import {
  AUTONOMY_ACTIONS,
  DEFAULT_AUTONOMY_MODE,
  type AutonomyAction,
  type AutonomyDecision,
  type AutonomyDecisionContext,
  type AutonomyDecisionEvidence,
  type AutonomyDecisionSource,
  type AutonomyMode,
  type CapabilityGrant,
  type OperationRisk,
} from '../contracts/types/AutonomyDecision.js';
import { normalizeAutonomyMode } from '../contracts/types/Autonomy.js';

export function requiredCapabilityForAction(action: AutonomyAction): CapabilityGrant | null {
  switch (action) {
    case 'read':
    case 'analyze':
    case 'propose':
      return 'read';
    case 'apply_change':
    case 'workflow_apply':
      return 'write';
    case 'run_command':
      return 'shell';
    case 'create_task':
      return 'task';
    case 'dispatch':
      return 'dispatch';
    case 'scope_expand':
      return null;
  }
}

export function decideAutonomyAction(ctx: AutonomyDecisionContext): AutonomyDecision {
  const autonomyMode = normalizeAutonomyMode(ctx.autonomyMode);
  const profile = ctx.intentProfile;
  const action = ctx.action ?? (ctx.toolName ? inferActionFromToolTier(ctx.toolTier, ctx.isReadOnly) : inferActionFromRisk(ctx));
  const requiredCapability = requiredCapabilityForAction(action);
  const evidence: AutonomyDecisionEvidence[] = [];
  const sources: AutonomyDecisionSource[] = [];

  pushEvidence(evidence, sources, 'primary_intent', profile.primaryIntent, profile.reason, 'intent_profile');
  pushEvidence(evidence, sources, 'intent_phase', profile.phase, undefined, 'intent_profile');
  pushEvidence(evidence, sources, 'intent_scope', profile.scope.kind, undefined, 'intent_profile');
  pushEvidence(evidence, sources, 'autonomy_mode', autonomyMode, `hint only; default ${DEFAULT_AUTONOMY_MODE}`, autonomyMode === DEFAULT_AUTONOMY_MODE ? 'autonomy_default' : 'autonomy_explicit');
  if (ctx.permissionMode) pushEvidence(evidence, sources, 'permission_mode', ctx.permissionMode, 'actual permission boundary is PermissionSystem');
  if (ctx.leaderExecutionMode) pushEvidence(evidence, sources, 'leader_execution_mode', ctx.leaderExecutionMode, 'route hint');
  if (ctx.controlMode) pushEvidence(evidence, sources, 'control_mode', ctx.controlMode, 'control hint');
  if (ctx.toolTier) pushEvidence(evidence, sources, 'tool_tier', ctx.toolTier);
  pushEvidence(evidence, sources, 'operation_risk', ctx.operationRisk);
  pushEvidence(evidence, sources, 'read_only_flag', String(ctx.isReadOnly));
  pushEvidence(evidence, sources, 'rule_action', action, ctx.action ? 'caller normalized tool category' : 'engine fallback category', 'rule_action');

  if (requiredCapability) {
    if (profile.denies.includes(requiredCapability)) {
      pushEvidence(evidence, sources, 'capability_deny', requiredCapability, 'hint: profile says this capability is not intended', 'capability_deny');
    } else if (profile.grants.includes(requiredCapability)) {
      pushEvidence(evidence, sources, 'capability_grant', requiredCapability, 'hint: profile grants this capability', 'capability_grant');
    } else {
      pushEvidence(evidence, sources, 'capability_grant', requiredCapability, 'hint: profile does not explicitly grant this capability', 'capability_grant');
    }
  }

  if (profile.requiredGates.length > 0) {
    pushEvidence(evidence, sources, 'required_gate', profile.requiredGates.join(','), 'hint only; does not hard-block in AutonomyDecisionEngine', 'required_gate');
  }
  if (profile.constraints.allowedPaths?.length) {
    pushEvidence(evidence, sources, 'path_constraint', profile.constraints.allowedPaths.join(','), 'hint: intended write/read paths', 'path_constraint');
  }
  if (profile.constraints.deniedTools?.length) {
    pushEvidence(evidence, sources, 'intent_constraint', profile.constraints.deniedTools.join(','), 'hint: tools user does not intend to use', 'intent_constraint');
  }

  return {
    kind: 'allow',
    intentProfile: profile,
    autonomyMode,
    reason: buildReason(action, ctx.operationRisk, profile.primaryIntent, profile.phase),
    affectedTools: ctx.toolName ? [ctx.toolName] : [],
    source: pickPrimarySource(sources),
    evidence,
    requiredCapability,
  };
}

export function deriveOperationRisk(input: {
  toolTier?: 'read' | 'compute' | 'write' | 'execute' | undefined;
  readOnly?: boolean;
  modifiesWorkspace?: boolean;
  isCritical?: boolean;
}): OperationRisk {
  if (input.isCritical) return 'critical';
  if (input.readOnly) return 'low';
  switch (input.toolTier) {
    case 'read':
    case 'compute':
      return 'low';
    case 'write':
      return 'medium';
    case 'execute':
      return 'high';
    default:
      return 'medium';
  }
}

export function inferActionFromToolName(toolName: string | undefined): AutonomyAction {
  if (!toolName) return 'analyze';
  const lower = toolName.toLowerCase();
  if (lower.includes('read') || lower.includes('search') || lower.includes('list') || lower.includes('grep') || lower.includes('glob') || lower.includes('code_search') || lower.includes('ast_query') || lower === 'find_tools') return 'read';
  if (lower === 'create_task' || lower === 'update_task' || lower === 'delete_task') return 'create_task';
  if (lower.includes('dispatch') || lower.includes('spawn')) return 'dispatch';
  if (lower.includes('workflow_apply') || lower.includes('apply_workflow')) return 'workflow_apply';
  if (lower === 'shell' || lower === 'bash' || lower.includes('run_command') || lower.includes('execute_command')) return 'run_command';
  if (lower.includes('patch') || lower.includes('file_create') || lower.includes('write_file')) return 'apply_change';
  if (lower.includes('propose') || lower.includes('plan')) return 'propose';
  return 'analyze';
}

function inferActionFromToolTier(toolTier: AutonomyDecisionContext['toolTier'], isReadOnly: boolean): AutonomyAction {
  if (isReadOnly || toolTier === 'read') return 'read';
  if (toolTier === 'compute') return 'analyze';
  if (toolTier === 'write') return 'apply_change';
  if (toolTier === 'execute') return 'run_command';
  return 'analyze';
}

function inferActionFromRisk(ctx: AutonomyDecisionContext): AutonomyAction {
  if (ctx.isReadOnly || ctx.operationRisk === 'low') return 'read';
  if (ctx.operationRisk === 'high' || ctx.operationRisk === 'critical') return 'run_command';
  return 'apply_change';
}

function pushEvidence(
  evidence: AutonomyDecisionEvidence[],
  sources: AutonomyDecisionSource[],
  kind: AutonomyDecisionEvidence['kind'],
  value: string,
  note?: string,
  source?: AutonomyDecisionSource,
): void {
  evidence.push(note ? { kind, value, note } : { kind, value });
  if (source) sources.push(source);
}

function pickPrimarySource(sources: readonly AutonomyDecisionSource[]): AutonomyDecisionSource {
  return sources[0] ?? 'intent_profile';
}

function buildReason(
  action: AutonomyAction,
  risk: OperationRisk,
  primaryIntent: string,
  phase: string,
): string {
  return `runtime policy hint: action=${action}; risk=${risk}; profile=${primaryIntent}/${phase}; actual authorization is handled outside AutonomyDecisionEngine`;
}

export { AUTONOMY_ACTIONS };

export type LeaderExecutionMode = 'direct' | 'hybrid' | 'delegate';
export type ControlMode = 'manual' | 'eternal';

export interface SessionIdentity {
  id: string;
  workspace: string;
}

// Re-export autonomy-harness types from a single Session module entry point.
export type {
  AutonomyMode,
  AutonomyDecisionKind,
  AutonomyDecision,
  AutonomyLifecyclePhase,
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

export {
  AUTONOMY_MODES,
  AUTONOMY_DECISION_KINDS,
  AUTONOMY_LIFECYCLE_PHASES,
  AUTONOMY_MODE_ALIASES,
  CAPABILITIES,
  CAPABILITY_DENIES,
  CAPABILITY_GRANTS,
  DEFAULT_AUTONOMY_MODE,
  DEFAULT_AUTONOMY_LIFECYCLE_PHASE,
  INTENT_PHASES,
  INTENT_SCOPE_KINDS,
  PRIMARY_INTENTS,
  REQUIRED_GATES,
  isAutonomyMode,
  isAutonomyLifecyclePhase,
  isAutonomyDecisionKind,
  isCapability,
  isCapabilityDeny,
  isCapabilityGrant,
  isIntentPhase,
  isIntentScopeKind,
  isPrimaryIntent,
  isRequiredGate,
  normalizeAutonomyMode,
  coerceAutonomyModeAlias,
  normalizeAutonomyLifecyclePhase,
} from './Autonomy.js';

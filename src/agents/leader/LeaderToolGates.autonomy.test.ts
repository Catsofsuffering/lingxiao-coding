import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { ModeRuntimeProjection } from '../../core/ModeRuntimeProjection.js';
import type { ToolPermissionContext } from '../../core/PermissionSystem.js';
import type { AutonomyMode, CapabilityIntentProfile } from '../../contracts/types/Autonomy.js';
import { evaluateLeaderAutonomyToolGate } from './LeaderToolGates.js';

function profile(overrides: Partial<CapabilityIntentProfile> = {}): CapabilityIntentProfile {
  return {
    primaryIntent: 'diagnose',
    scope: { kind: 'read_only' },
    phase: 'understand',
    grants: ['read'],
    denies: ['write', 'shell', 'task', 'dispatch'],
    requiredGates: [],
    constraints: { maxRisk: 'low' },
    confidence: 1,
    reason: 'test profile',
    turnId: 1,
    recordedAt: 1,
    source: 'record_capability_intent',
    ...overrides,
  };
}

function projection(input: {
  intentProfile?: CapabilityIntentProfile;
  autonomy?: AutonomyMode;
  permissionMode?: ToolPermissionContext['mode'];
}): ModeRuntimeProjection {
  return {
    controlMode: 'manual',
    route: { mode: 'direct', preference: 'direct', source: 'default' },
    collaboration: { mode: 'solo', source: 'default', activeTeamName: null, teamEnabled: false },
    workflow: { enabled: false, activeExecutionCount: 0 },
    modePlugins: { bughunt: false, office: false, workflow: false },
    blackboard: { mode: 'off', source: 'default' },
    permission: { mode: input.permissionMode ?? 'dev' },
    blueprint: null,
    autonomy: input.autonomy ?? 'balanced',
    intentProfile: input.intentProfile ?? profile(),
    lifecyclePhase: 'bootstrap',
    modeGeneration: 1,
    policyId: null,
    policyHash: null,
    lastDecisionTrace: null,
  };
}

function permission(mode: ToolPermissionContext['mode'] = 'dev'): ToolPermissionContext {
  return {
    mode,
    allowedHosts: [],
    sandboxBackend: 'app-guard',
    allowBackendFallback: true,
    allowRules: [],
    denyRules: [],
    askRules: [],
  };
}

describe('evaluateLeaderAutonomyToolGate', () => {
  it('allows read-only profile to use read tools', () => {
    const result = evaluateLeaderAutonomyToolGate({
      toolName: 'glob',
      modes: projection({ intentProfile: profile() }),
      permissionContext: permission('dev'),
    });

    assert.equal(result.ok, true);
    assert.equal(result.decision.intentProfile.primaryIntent, 'diagnose');
    assert.equal(result.decision.kind, 'allow');
  });

  it('does not hard-block writes when profile denies write; records deny as hint evidence', () => {
    const result = evaluateLeaderAutonomyToolGate({
      toolName: 'structured_patch',
      modes: projection({ intentProfile: profile() }),
      permissionContext: permission('dev'),
    });

    assert.equal(result.ok, true);
    assert.equal(result.decision.kind, 'allow');
    assert.ok(result.decision.evidence.some((item) => item.kind === 'capability_deny' && item.value === 'write'));
  });

  it('allows granted medium-risk writes regardless of confirmation policy', () => {
    const result = evaluateLeaderAutonomyToolGate({
      toolName: 'structured_patch',
      modes: projection({
        intentProfile: profile({
          primaryIntent: 'implement',
          scope: { kind: 'workspace' },
          phase: 'execute',
          grants: ['read', 'write'],
          denies: [],
          constraints: {},
        }),
        autonomy: 'review_first',
      }),
      permissionContext: permission('dev'),
    });

    assert.equal(result.ok, true);
    assert.equal(result.decision.intentProfile.primaryIntent, 'implement');
    assert.equal(result.decision.kind, 'allow');
  });

  it('does not hard-block shell when profile denies shell; records deny as hint evidence', () => {
    const result = evaluateLeaderAutonomyToolGate({
      toolName: 'shell',
      args: { command: 'npm test' },
      modes: projection({
        intentProfile: profile({
          primaryIntent: 'verify',
          scope: { kind: 'workspace' },
          phase: 'verify',
          grants: ['read'],
          denies: ['shell'],
          constraints: {},
        }),
        autonomy: 'autonomous',
      }),
      permissionContext: permission('dev'),
    });

    assert.equal(result.ok, true);
    assert.ok(result.decision.evidence.some((item) => item.kind === 'capability_deny' && item.value === 'shell'));
  });
});

import type { PrimaryIntent } from '../../../contracts/types/Autonomy.js';

export interface PrimaryIntentLabel {
  readonly key: PrimaryIntent;
  readonly label: string;
  readonly description: string;
  readonly accent: 'neutral' | 'info' | 'success' | 'warning' | 'danger';
}

export const PRIMARY_INTENT_LABELS: ReadonlyArray<PrimaryIntentLabel> = [
  { key: 'diagnose', label: '诊断', description: 'Inspect, understand, or locate issues.', accent: 'info' },
  { key: 'explain', label: '解释', description: 'Explain behavior or code without changing it.', accent: 'info' },
  { key: 'plan', label: '方案', description: 'Produce a proposal or blueprint.', accent: 'neutral' },
  { key: 'implement', label: '实现', description: 'Build or extend functionality.', accent: 'success' },
  { key: 'fix', label: '修复', description: 'Repair a bug or failing behavior.', accent: 'warning' },
  { key: 'refactor', label: '重构', description: 'Improve structure without changing intended behavior.', accent: 'neutral' },
  { key: 'verify', label: '验证', description: 'Run checks and collect evidence.', accent: 'success' },
  { key: 'operate', label: '运维', description: 'Run commands, migrations, deployments, or other operations.', accent: 'danger' },
  { key: 'research', label: '研究', description: 'Investigate information and synthesize findings.', accent: 'info' },
];

const PRIMARY_INTENT_LABEL_BY_KEY: ReadonlyMap<PrimaryIntent, PrimaryIntentLabel> = new Map(
  PRIMARY_INTENT_LABELS.map((label) => [label.key, label]),
);

export function describePrimaryIntent(intent: PrimaryIntent): PrimaryIntentLabel {
  return PRIMARY_INTENT_LABEL_BY_KEY.get(intent) ?? {
    key: intent,
    label: intent,
    description: 'Capability intent profile primary intent.',
    accent: 'neutral',
  };
}

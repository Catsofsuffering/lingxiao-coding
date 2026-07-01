export interface SkillSelectionPolicy {
  disabledSkillNames: string[];
  digestGuidance: string[];
}

export const SKILL_SELECTION_POLICY: SkillSelectionPolicy = {
  disabledSkillNames: [],
  digestGuidance: [
    '按任务目标主动选择 skill_names；用户显式写 $skill 时按指定 skill 优先。',
  ],
};

export function isSkillDisabledByPolicy(name: string): boolean {
  return SKILL_SELECTION_POLICY.disabledSkillNames.includes(name);
}

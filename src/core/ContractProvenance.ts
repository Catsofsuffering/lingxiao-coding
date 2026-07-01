import type { GraphNode } from './blackboard/types.js';

/**
 * Contract provenance authority model.
 *
 * MOA 不变量: 物化契约必须来自最权威的描述,不能由薄模板/运行时证明凭时间戳覆盖。
 * 数字越小越权威；同优先级再按 createdAt 取新。
 */
const CONTRACT_PROVENANCE_PRIORITY: Record<string, number> = {
  // worker graph_contract 通常没有 provenance tag,视为最权威的运行时真实契约。
  '': 0,
  declared: 1,
  system: 1,
  audit: 2,
  // provenance:worker 历史上曾被 compliance stub 使用,不应压过真实契约。
  worker: 5,
  // Leader create_task 的薄模板永远最低。
  template: 10,
};

export function getContractProvenance(tags: readonly string[] | undefined): string {
  return tags?.find((t) => t.startsWith('provenance:'))?.slice('provenance:'.length).trim() || '';
}

export function getContractProvenancePriority(tags: readonly string[] | undefined): number {
  return CONTRACT_PROVENANCE_PRIORITY[getContractProvenance(tags)] ?? 0;
}

export function shouldPreferContractNode(
  candidate: Pick<GraphNode, 'tags' | 'createdAt'>,
  existing: Pick<GraphNode, 'tags' | 'createdAt'>,
): boolean {
  const candidatePriority = getContractProvenancePriority(candidate.tags);
  const existingPriority = getContractProvenancePriority(existing.tags);
  if (candidatePriority !== existingPriority) return candidatePriority < existingPriority;
  return candidate.createdAt > existing.createdAt;
}

export function shouldSupersedeExistingContract(
  incoming: Pick<GraphNode, 'tags'>,
  existing: Pick<GraphNode, 'tags'>,
): boolean {
  const incomingProvenance = getContractProvenance(incoming.tags);
  const existingProvenance = getContractProvenance(existing.tags);
  // 薄模板只能替换薄模板,永远不能 supersede 已有真实/声明/审计/系统契约。
  if (incomingProvenance === 'template' && existingProvenance !== 'template') return false;
  return true;
}

/**
 * costCalculator.ts — Frontend cost calculation utility
 *
 * Lightweight version of backend CostService for real-time cost display in the UI.
 * Uses the same pricing data as the backend.
 *
 * Cache-aware: supports cache read / cache creation sub-prices. When a model's
 * pricing entry is missing cache sub-prices (or the model falls back to
 * DEFAULT_PRICING), we mark the result as `partial` so the UI can show a
 * visible "≈" / "partial" marker instead of pretending the cost is exact.
 */

import type { TokenUsageView } from '@contracts/types/TokenUsage';

export interface ModelPricing {
  inputPerMToken: number;
  outputPerMToken: number;
  cacheReadPerMToken: number;
  cacheCreationPerMToken: number;
}

export type ModelPricingResolution = ModelPricing & {
  /** 定价来自 DEFAULT_PRICING 兜底（不是精确模型表），整张 cost 都是估算 */
  estimated: boolean;
  /**
   * cache 子价（cacheReadPerMToken / cacheCreationPerMToken）是从更便宜的精确价
   * 或 input 价回退得到的。即使 estimated=false, 也可能 partial=true。
   */
  pricingPartial: boolean;
};

const MODEL_PRICING: Record<string, ModelPricing> = {
  // Anthropic Claude
  'claude-opus-4-5': { inputPerMToken: 15, outputPerMToken: 75, cacheReadPerMToken: 1.5, cacheCreationPerMToken: 18.75 },
  'claude-sonnet-4-5': { inputPerMToken: 3, outputPerMToken: 15, cacheReadPerMToken: 0.3, cacheCreationPerMToken: 3.75 },
  'claude-3-5-haiku-20241022': { inputPerMToken: 0.8, outputPerMToken: 4, cacheReadPerMToken: 0.08, cacheCreationPerMToken: 1 },
  'claude-3-opus-20240229': { inputPerMToken: 15, outputPerMToken: 75, cacheReadPerMToken: 1.5, cacheCreationPerMToken: 18.75 },
  'claude-3-sonnet-20240229': { inputPerMToken: 3, outputPerMToken: 15, cacheReadPerMToken: 0.3, cacheCreationPerMToken: 3.75 },
  'claude-3-haiku-20240307': { inputPerMToken: 0.25, outputPerMToken: 1.25, cacheReadPerMToken: 0.03, cacheCreationPerMToken: 0.3 },

  // OpenAI GPT
  'gpt-4o': { inputPerMToken: 2.5, outputPerMToken: 10, cacheReadPerMToken: 1.25, cacheCreationPerMToken: 2.5 },
  'gpt-4o-mini': { inputPerMToken: 0.15, outputPerMToken: 0.6, cacheReadPerMToken: 0.075, cacheCreationPerMToken: 0.15 },
  'gpt-4-turbo': { inputPerMToken: 10, outputPerMToken: 30, cacheReadPerMToken: 5, cacheCreationPerMToken: 10 },
  'gpt-4': { inputPerMToken: 30, outputPerMToken: 60, cacheReadPerMToken: 15, cacheCreationPerMToken: 30 },
  'gpt-3.5-turbo': { inputPerMToken: 0.5, outputPerMToken: 1.5, cacheReadPerMToken: 0.25, cacheCreationPerMToken: 0.5 },
  'o1': { inputPerMToken: 15, outputPerMToken: 60, cacheReadPerMToken: 7.5, cacheCreationPerMToken: 15 },
  'o1-mini': { inputPerMToken: 3, outputPerMToken: 12, cacheReadPerMToken: 1.5, cacheCreationPerMToken: 3 },
  'o3-mini': { inputPerMToken: 1.1, outputPerMToken: 4.4, cacheReadPerMToken: 0.55, cacheCreationPerMToken: 1.1 },

  // Google Gemini
  'gemini-2.5-pro': { inputPerMToken: 1.25, outputPerMToken: 10, cacheReadPerMToken: 0.31, cacheCreationPerMToken: 1.25 },
  'gemini-2.5-flash': { inputPerMToken: 0.3, outputPerMToken: 2.5, cacheReadPerMToken: 0.075, cacheCreationPerMToken: 0.3 },
  'gemini-2.0-flash': { inputPerMToken: 0.1, outputPerMToken: 0.4, cacheReadPerMToken: 0.025, cacheCreationPerMToken: 0.1 },
  'gemini-1.5-pro': { inputPerMToken: 1.25, outputPerMToken: 5, cacheReadPerMToken: 0.3125, cacheCreationPerMToken: 1.25 },
};

/**
 * 兜底价（不是真实模型定价）。
 * - estimated=true: 整张 cost 都是估算
 * - pricingPartial=true: cache 子价被回退为 input 价（DEFAULT_PRICING 仅作为兜底，
 *   没有真实 cache 子价）
 */
const DEFAULT_PRICING: ModelPricing = {
  inputPerMToken: 3,
  outputPerMToken: 15,
  cacheReadPerMToken: 0.3,
  cacheCreationPerMToken: 3.75,
};

/**
 * 解析模型定价。
 *
 * 返回的 resolution 携带两个独立维度：
 * - estimated: 整个价格来自 DEFAULT_PRICING 兜底（精确模型未命中）
 * - pricingPartial: cache 子价被回退为 input 价，cache 段费用是估算
 *
 * 二者可以独立为 true，例如某精确模型定价表只给 input/output，没给 cache。
 */
export function resolveModelPricing(modelName: string): ModelPricingResolution {
  const model = modelName.trim();
  if (!model) {
    return { ...DEFAULT_PRICING, estimated: true, pricingPartial: true };
  }
  const exact = MODEL_PRICING[model] ?? MODEL_PRICING[model.toLowerCase()];
  if (exact) {
    // 精确命中：input/output 一定在；只要 cache 子价缺一就算 partial。
    // 当前 MODEL_PRICING 全部四字段齐全，所以此处 partial 永远为 false。
    // 保留 partial 字段是为了将来模型表扩列（只填 input/output 不填 cache）时
    // 仍然能正确反映"部分定价"。
    const hasCache = exact.cacheReadPerMToken > 0 || exact.cacheCreationPerMToken > 0;
    return { ...exact, estimated: false, pricingPartial: !hasCache };
  }
  return { ...DEFAULT_PRICING, estimated: true, pricingPartial: true };
}

export type TokenUsageInput = Pick<TokenUsageView, 'prompt' | 'completion' | 'cache_read' | 'cache_creation'>;

/**
 * 详细费用结果。
 * - total: 总费用（美元）
 * - estimated: 定价来自 DEFAULT_PRICING 兜底
 * - partial: cache 子价被回退为 input，cache 段费用是估算
 * - cacheHitRate: cache_read / prompt 的百分比（>0 时有意义）
 */
export interface CostCalculation {
  total: number;
  estimated: boolean;
  partial: boolean;
  cacheHitRate: number;
}

export function calculateCostDetailed(
  modelName: string,
  usage: TokenUsageInput,
  pricingOverride?: ModelPricing,
): CostCalculation {
  const resolution = pricingOverride
    ? {
        ...pricingOverride,
        estimated: false,
        pricingPartial: false,
      }
    : resolveModelPricing(modelName);
  const cacheRead = usage.cache_read ?? 0;
  const cacheCreation = usage.cache_creation ?? 0;
  const netInput = Math.max(0, usage.prompt - cacheRead - cacheCreation);
  const total =
    (netInput / 1_000_000) * resolution.inputPerMToken +
    (usage.completion / 1_000_000) * resolution.outputPerMToken +
    (cacheRead / 1_000_000) * resolution.cacheReadPerMToken +
    (cacheCreation / 1_000_000) * resolution.cacheCreationPerMToken;
  const cacheHitRate = usage.prompt > 0 ? (cacheRead / usage.prompt) * 100 : 0;
  return {
    total,
    estimated: resolution.estimated,
    partial: resolution.pricingPartial || resolution.estimated,
    cacheHitRate,
  };
}

/**
 * 向后兼容：仅返回费用数字。
 *
 * 新代码请改用 {@link calculateCostDetailed} 以便在 UI 上展示 estimated / partial /
 * cache hit 标记，避免无参考价值的费用伪精确。
 */
export function calculateCost(
  modelName: string,
  usage: TokenUsageInput,
  pricingOverride?: ModelPricing,
): number {
  return calculateCostDetailed(modelName, usage, pricingOverride).total;
}

export function formatCost(cost: number): string {
  if (cost >= 1) return `$${cost.toFixed(2)}`;
  if (cost >= 0.01) return `$${cost.toFixed(3)}`;
  if (cost > 0) return `$${cost.toFixed(4)}`;
  return '$0.00';
}

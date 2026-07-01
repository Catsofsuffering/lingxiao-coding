/**
 * extractTokenUsage — 统一从 LLM 原始 usage 对象中提取 TokenUsage。
 *
 * 历史上每个 provider 各有一份字段映射：
 *   - Anthropic: input_tokens / output_tokens / cache_creation_input_tokens / cache_read_input_tokens
 *   - OpenAI:    prompt_tokens / completion_tokens / prompt_tokens_details.cached_tokens
 *   - Claude Code (CLI 流转 Anthropic schema): 同 Anthropic
 *   - Codex (Agents API): input_tokens / output_tokens / cached_input_tokens
 *   - OpenAI-compatible gateways: prompt_cache_hit_tokens / prompt_cache_write_tokens /
 *     completion_tokens_details.reasoning_tokens / completion_thinking_tokens / credit
 *
 * 这里收口成一个工具函数，所有 provider 走同一份字段探测。
 */

import type { TokenUsage } from './types.js';

export type UsageProvider = 'anthropic' | 'openai' | 'claude_code' | 'codex';

interface NormalizedUsage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  cacheReadInputTokens: number;
  cacheCreationInputTokens: number;
  reasoningTokens: number;
  credit?: number;
}

function asNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function firstNumber(...values: unknown[]): number | undefined {
  for (const value of values) {
    const n = asNumber(value);
    if (n !== undefined) return n;
  }
  return undefined;
}

/**
 * 从原始 usage 对象中提取归一化字段。
 *
 * 统一口径（关键）：promptTokens 一律为「毛输入」——包含 cache_read 与
 * cache_creation；totalTokens = promptTokens + completionTokens。这样
 * 无论哪个 provider，prompt / total 都自洽且都包含缓存，下游 /stats、
 * TUI header、CostService 拿到的都是真实消耗。
 *
 * 各 provider 的输入语义差异：
 *   - Anthropic / Claude Code：input_tokens 是「净输入」，不含 cache_read /
 *     cache_creation；真实总输入 = input_tokens + cache_read + cache_creation。
 *   - OpenAI chat：prompt_tokens 已是毛输入，prompt_tokens_details.cached_tokens
 *     只是其子集。
 *   - OpenAI responses / Codex：input_tokens 已是毛输入，cached_input_tokens /
 *     input_tokens_details.cached_tokens 是其子集。
 */
function normalize(raw: Record<string, unknown>): NormalizedUsage | undefined {
  const promptDetails = asRecord(raw['prompt_tokens_details']);
  const inputDetails = asRecord(raw['input_tokens_details']);
  const completionDetails = asRecord(raw['completion_tokens_details']);

  // cache_read / prompt-cache-hit 别名（覆盖各 provider schema）。不要累加多个别名，
  // 它们通常表示同一批缓存命中 tokens。
  const cacheRead = firstNumber(
    raw.cache_read_input_tokens,
    raw['prompt_cache_hit_tokens'],
    raw['cached_input_tokens'],
    promptDetails?.cached_tokens,
    inputDetails?.cached_tokens,
    raw['cached_tokens'],
    raw['cacheRead'],
  ) ?? 0;

  // cache_creation / prompt-cache-write 别名。prompt_cache_miss_tokens 不是 write，
  // 只在缺少 prompt/input 时参与 prompt 兜底推导，避免双算。
  const cacheCreation = firstNumber(
    raw.cache_creation_input_tokens,
    raw['prompt_cache_write_tokens'],
    raw['cacheCreation'],
  ) ?? 0;

  const reasoningTokens = firstNumber(
    completionDetails?.reasoning_tokens,
    raw['reasoning_tokens'],
    raw['completion_thinking_tokens'],
    raw['thinking_tokens'],
  ) ?? 0;

  // 判定输入计数是否为「净输入」（需补回 cache 才是毛输入）。
  // cache_read_input_tokens / cache_creation_input_tokens 是 Anthropic schema
  // 独有字段名，据此识别 Anthropic / Claude Code 的净输入语义。
  // 同时排除「已归一化结果再次进入」——归一化结果带 prompt_tokens（毛输入），
  // 若再次叠加 cache 会双计。
  const hasAnthropicCacheFields =
    'cache_read_input_tokens' in raw || 'cache_creation_input_tokens' in raw;
  const alreadyGrossPrompt = asNumber(raw.prompt_tokens) !== undefined;
  const isNetInput = hasAnthropicCacheFields && !alreadyGrossPrompt;

  const rawPrompt = firstNumber(
    raw.prompt_tokens,
    raw.input_tokens,
    raw['inputTokens'],
  );

  const cacheMiss = firstNumber(raw['prompt_cache_miss_tokens']) ?? 0;
  const fallbackPrompt = cacheRead > 0 || cacheMiss > 0
    ? cacheRead + cacheMiss
    : cacheCreation > 0
      ? cacheCreation
      : undefined;

  if (rawPrompt === undefined && fallbackPrompt === undefined) return undefined;

  const promptTokens = rawPrompt === undefined
    ? fallbackPrompt!
    : isNetInput
      ? rawPrompt + cacheRead + cacheCreation
      : rawPrompt;

  const completionTokens = firstNumber(
    raw.completion_tokens,
    raw.output_tokens,
    raw['outputTokens'],
  ) ?? 0;

  // total 一律重算为毛 prompt + completion；reasoningTokens 是 completion 的子集。
  const totalTokens = promptTokens + completionTokens;

  return {
    promptTokens,
    completionTokens,
    totalTokens,
    cacheReadInputTokens: cacheRead,
    cacheCreationInputTokens: cacheCreation,
    reasoningTokens,
    credit: asNumber(raw['credit']),
  };
}

/**
 * 把 provider 原始 usage 抽成统一 TokenUsage。
 *
 * @param raw provider SDK 直接给出的 usage 对象（可能是流式累计值）
 * @param delta 流式 message_delta 时的增量 usage（仅 Anthropic 用，其他 provider 传 undefined）
 * @returns TokenUsage 或 undefined（raw 不可识别 / 不含 prompt 字段）
 */
export function extractTokenUsage(
  raw: unknown,
  delta?: Record<string, unknown> | undefined,
): TokenUsage | undefined {
  if ((!raw || typeof raw !== 'object') && !delta) return undefined;
  const base = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>;
  const normalized = normalize(base);
  if (!normalized) return undefined;

  // Anthropic 流式：completion 来自 message_delta.usage.output_tokens
  let completion = normalized.completionTokens;
  if (delta) {
    const deltaCompletion = firstNumber(delta.output_tokens, delta.completion_tokens);
    if (deltaCompletion !== undefined) completion = deltaCompletion;
  }

  // total 统一为「毛输入 + 输出」，与 normalize 口径一致。
  // 流式 delta 仅用于刷新 completion，prompt 端不变。
  const result: TokenUsage = {
    prompt_tokens: normalized.promptTokens,
    completion_tokens: completion,
    total_tokens: normalized.promptTokens + completion,
  };
  if (normalized.cacheCreationInputTokens > 0) {
    result.cache_creation_input_tokens = normalized.cacheCreationInputTokens;
  }
  if (normalized.cacheReadInputTokens > 0) {
    result.cache_read_input_tokens = normalized.cacheReadInputTokens;
  }
  if (normalized.reasoningTokens > 0) {
    result.reasoning_tokens = normalized.reasoningTokens;
  }
  if (normalized.credit !== undefined) {
    result.credit = normalized.credit;
  }
  return result;
}

/**
 * Driver 使用的简化 usage 三元组（含 cache）。供 ClaudeCodeDriver / CodexDriver 用。
 */
export interface DriverUsage {
  prompt: number;
  completion: number;
  total: number;
  cacheRead: number;
  cacheCreation: number;
  reasoning?: number;
}

export function extractDriverUsage(raw: unknown): DriverUsage | undefined {
  if (!raw || typeof raw !== 'object') return undefined;
  const normalized = normalize(raw as Record<string, unknown>);
  if (!normalized) return undefined;
  return {
    prompt: normalized.promptTokens,
    completion: normalized.completionTokens,
    total: normalized.totalTokens,
    cacheRead: normalized.cacheReadInputTokens,
    cacheCreation: normalized.cacheCreationInputTokens,
    reasoning: normalized.reasoningTokens,
  };
}

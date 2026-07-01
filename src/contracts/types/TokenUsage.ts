/** Canonical token usage aligned with provider API response fields. */
export interface TokenUsage {
  /** Gross input tokens: normal input plus cache read/hit and cache creation/write. */
  prompt_tokens: number;
  /** Output tokens. Reasoning/thinking tokens are a subset and must not be added again. */
  completion_tokens: number;
  /** Canonical total, normally prompt_tokens + completion_tokens. */
  total_tokens: number;
  /** Cached input tokens written/created in provider cache. */
  cache_creation_input_tokens?: number;
  /** Cached input tokens read from provider cache / prompt cache hit. */
  cache_read_input_tokens?: number;
  /** Reasoning/thinking output tokens, if reported separately by the provider. */
  reasoning_tokens?: number;
  /** Provider-specific credit/charge diagnostic, not included in token totals. */
  credit?: number;
}

/** Persistence row shape for token usage records. */
export interface TokenUsageRecord extends TokenUsage {
  id?: number;
  session_id: string;
  agent_id: string;
  agent_name: string;
  timestamp: number;
}

/** UI projection derived from TokenUsage. This shape should not be persisted. */
export interface TokenUsageView {
  prompt: number;
  completion: number;
  total: number;
  cache_read?: number;
  cache_creation?: number;
  reasoning?: number;
  credit?: number;
}

/** Eternal runtime UI projection kept separate from persisted token usage. */
export interface EternalTokenSnapshot {
  promptTokens: number;
  completionTokens: number;
  timestamp: number;
}

export const EMPTY_TOKEN_USAGE: TokenUsage = Object.freeze({
  prompt_tokens: 0,
  completion_tokens: 0,
  total_tokens: 0,
});

export function toView(usage: TokenUsage): TokenUsageView {
  return {
    prompt: usage.prompt_tokens,
    completion: usage.completion_tokens,
    total: usage.total_tokens,
    ...(usage.cache_read_input_tokens !== undefined ? { cache_read: usage.cache_read_input_tokens } : {}),
    ...(usage.cache_creation_input_tokens !== undefined ? { cache_creation: usage.cache_creation_input_tokens } : {}),
    ...(usage.reasoning_tokens !== undefined ? { reasoning: usage.reasoning_tokens } : {}),
    ...(usage.credit !== undefined ? { credit: usage.credit } : {}),
  };
}

export function fromView(view: TokenUsageView): TokenUsage {
  return {
    prompt_tokens: view.prompt,
    completion_tokens: view.completion,
    total_tokens: view.total,
    ...(view.cache_read !== undefined ? { cache_read_input_tokens: view.cache_read } : {}),
    ...(view.cache_creation !== undefined ? { cache_creation_input_tokens: view.cache_creation } : {}),
    ...(view.reasoning !== undefined ? { reasoning_tokens: view.reasoning } : {}),
    ...(view.credit !== undefined ? { credit: view.credit } : {}),
  };
}

export function merge(base: TokenUsage, patch: Partial<TokenUsage>): TokenUsage {
  const prompt = patch.prompt_tokens ?? base.prompt_tokens;
  const completion = patch.completion_tokens ?? base.completion_tokens;
  return {
    prompt_tokens: prompt,
    completion_tokens: completion,
    total_tokens: patch.total_tokens ?? prompt + completion,
    cache_creation_input_tokens: patch.cache_creation_input_tokens ?? base.cache_creation_input_tokens,
    cache_read_input_tokens: patch.cache_read_input_tokens ?? base.cache_read_input_tokens,
    reasoning_tokens: patch.reasoning_tokens ?? base.reasoning_tokens,
    credit: patch.credit ?? base.credit,
  };
}

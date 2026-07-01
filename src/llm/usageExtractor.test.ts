import test from 'node:test';
import assert from 'node:assert/strict';
import { extractTokenUsage } from './usageExtractor.js';

test('extracts basic OpenAI usage', () => {
  assert.deepEqual(extractTokenUsage({
    prompt_tokens: 100,
    completion_tokens: 20,
    total_tokens: 120,
  }), {
    prompt_tokens: 100,
    completion_tokens: 20,
    total_tokens: 120,
  });
});

test('extracts OpenAI cached and reasoning token details without double counting', () => {
  assert.deepEqual(extractTokenUsage({
    prompt_tokens: 100,
    completion_tokens: 50,
    total_tokens: 150,
    prompt_tokens_details: { cached_tokens: 60 },
    completion_tokens_details: { reasoning_tokens: 30 },
  }), {
    prompt_tokens: 100,
    completion_tokens: 50,
    total_tokens: 150,
    cache_read_input_tokens: 60,
    reasoning_tokens: 30,
  });
});

test('extracts OpenAI-compatible gateway cache aliases', () => {
  assert.deepEqual(extractTokenUsage({
    prompt_tokens: 150,
    completion_tokens: 30,
    prompt_cache_hit_tokens: 80,
    prompt_cache_miss_tokens: 70,
    prompt_cache_write_tokens: 20,
    completion_thinking_tokens: 12,
    credit: 0.02,
  }), {
    prompt_tokens: 150,
    completion_tokens: 30,
    total_tokens: 180,
    cache_read_input_tokens: 80,
    cache_creation_input_tokens: 20,
    reasoning_tokens: 12,
    credit: 0.02,
  });
});

test('uses prompt cache hit and miss as fallback prompt only when prompt is absent', () => {
  assert.deepEqual(extractTokenUsage({
    prompt_cache_hit_tokens: 80,
    prompt_cache_miss_tokens: 70,
    prompt_cache_write_tokens: 20,
    completion_tokens: 30,
  }), {
    prompt_tokens: 150,
    completion_tokens: 30,
    total_tokens: 180,
    cache_read_input_tokens: 80,
    cache_creation_input_tokens: 20,
  });
});

test('normalizes Anthropic net input plus cache to gross prompt', () => {
  assert.deepEqual(extractTokenUsage({
    input_tokens: 100,
    output_tokens: 20,
    cache_read_input_tokens: 400,
    cache_creation_input_tokens: 50,
  }), {
    prompt_tokens: 550,
    completion_tokens: 20,
    total_tokens: 570,
    cache_read_input_tokens: 400,
    cache_creation_input_tokens: 50,
  });
});

test('does not add cache again for already-normalized usage', () => {
  assert.deepEqual(extractTokenUsage({
    prompt_tokens: 550,
    completion_tokens: 20,
    total_tokens: 570,
    cache_read_input_tokens: 400,
    cache_creation_input_tokens: 50,
  }), {
    prompt_tokens: 550,
    completion_tokens: 20,
    total_tokens: 570,
    cache_read_input_tokens: 400,
    cache_creation_input_tokens: 50,
  });
});

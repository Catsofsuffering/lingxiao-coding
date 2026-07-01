import test from 'node:test';
import assert from 'node:assert/strict';
import { fromView, merge, toView } from './TokenUsage.js';

test('TokenUsage toView maps canonical fields and normalized extensions', () => {
  assert.deepEqual(toView({
    prompt_tokens: 100,
    completion_tokens: 20,
    total_tokens: 120,
    cache_read_input_tokens: 60,
    cache_creation_input_tokens: 10,
    reasoning_tokens: 5,
    credit: 0.01,
  }), {
    prompt: 100,
    completion: 20,
    total: 120,
    cache_read: 60,
    cache_creation: 10,
    reasoning: 5,
    credit: 0.01,
  });
});

test('TokenUsage fromView maps UI fields back to canonical names', () => {
  assert.deepEqual(fromView({
    prompt: 100,
    completion: 20,
    total: 120,
    cache_read: 60,
    cache_creation: 10,
    reasoning: 5,
    credit: 0.01,
  }), {
    prompt_tokens: 100,
    completion_tokens: 20,
    total_tokens: 120,
    cache_read_input_tokens: 60,
    cache_creation_input_tokens: 10,
    reasoning_tokens: 5,
    credit: 0.01,
  });
});

test('TokenUsage merge recomputes total and preserves normalized extensions', () => {
  assert.deepEqual(merge({
    prompt_tokens: 10,
    completion_tokens: 5,
    total_tokens: 15,
    cache_read_input_tokens: 3,
    reasoning_tokens: 2,
  }, {
    prompt_tokens: 20,
    completion_tokens: 7,
  }), {
    prompt_tokens: 20,
    completion_tokens: 7,
    total_tokens: 27,
    cache_read_input_tokens: 3,
    cache_creation_input_tokens: undefined,
    reasoning_tokens: 2,
    credit: undefined,
  });
});

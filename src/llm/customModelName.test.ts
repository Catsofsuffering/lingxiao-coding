import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { ClaudeCodeDriver } from '../agents/external/drivers/ClaudeCodeDriver.js';
import type { ExternalAgentInput } from '../agents/external/types.js';
import { toCustomModelName } from './customModelName.js';

test('toCustomModelName prefixes non-custom models exactly once', () => {
  assert.equal(toCustomModelName('gpt-5.4-mini'), 'custom-gpt-5.4-mini');
  assert.equal(toCustomModelName('claude-haiku-4.5'), 'custom-claude-haiku-4.5');
  assert.equal(toCustomModelName('custom-gpt-5.4-mini'), 'custom-gpt-5.4-mini');
  assert.equal(toCustomModelName('  gpt-5.4-mini  '), 'custom-gpt-5.4-mini');
});

test('Claude/CodeBuddy driver receives custom-prefixed model names', () => {
  const driver = new ClaudeCodeDriver();
  const input: ExternalAgentInput = {
    agentId: 'agent-1',
    agentName: 'worker',
    sessionId: 'session-1',
    taskId: 'task-1',
    prompt: 'do work',
    systemPrompt: 'system',
    workingDirectory: process.cwd(),
    workspace: process.cwd(),
    writeScope: [process.cwd()],
    model: {
      id: 'leader',
      apiModel: 'claude-haiku-4.5',
      provider: 'anthropic',
      baseUrl: 'http://127.0.0.1:62000/llm/anthropic',
      envKey: 'ANTHROPIC_API_KEY',
      apiKey: 'sk-test',
    },
    timeoutMs: 1000,
    idleTimeoutMs: 1000,
    extraArgs: [],
    extraEnv: {},
    logDir: process.cwd(),
  };

  const plan = driver.buildExecute(input);
  const modelIndex = plan.args.indexOf('--model');
  assert.notEqual(modelIndex, -1);
  assert.equal(plan.args[modelIndex + 1], 'custom-claude-haiku-4.5');
  assert.equal(plan.env.ANTHROPIC_MODEL, 'custom-claude-haiku-4.5');
});

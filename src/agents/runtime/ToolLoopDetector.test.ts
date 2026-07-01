import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ToolLoopDetector } from './ToolLoopDetector.js';
import type { ToolCall } from '../../llm/types.js';

function toolCall(name: string, args: unknown): ToolCall {
  return {
    id: `${name}-1`,
    type: 'function',
    function: {
      name,
      arguments: JSON.stringify(args),
    },
  };
}

describe('ToolLoopDetector', () => {
  it('is disabled by default and never reports looping', () => {
    const detector = new ToolLoopDetector({ threshold: 2 });
    const call = toolCall('file_read', { path: 'a.ts' });

    detector.observe([call]);
    detector.observe([call]);
    detector.observe([call]);

    assert.equal(detector.consecutiveCount, 0);
    assert.equal(detector.isLooping, false);
  });

  it('reports repeated identical tool calls when explicitly enabled', () => {
    const detector = new ToolLoopDetector({ enabled: true, threshold: 2 });
    const call = toolCall('file_read', { path: 'a.ts' });

    detector.observe([call]);
    assert.equal(detector.isLooping, false);
    detector.observe([call]);
    assert.equal(detector.isLooping, true);
  });
});

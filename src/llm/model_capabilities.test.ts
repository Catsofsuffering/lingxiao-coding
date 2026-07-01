import test from 'node:test';
import assert from 'node:assert/strict';
import { toAnthropicEffort, pickEffortValue } from './model_capabilities.js';

/**
 * toAnthropicEffort：把凌霄通用 effort 档位映射到 Anthropic output_config.effort
 * 合法值（'low' | 'medium' | 'high' | 'max'）。
 *
 * 背景：凌霄代理网关等 Anthropic 兼容远端依据顶层 output_config.effort 判定思考强度，
 * 旧实现只发 thinking.budget_tokens（预算量纲）不发 effort → 远端显示「没有思考强度指定」。
 */
test('toAnthropicEffort maps native Anthropic levels verbatim', () => {
  assert.equal(toAnthropicEffort('low'), 'low');
  assert.equal(toAnthropicEffort('medium'), 'medium');
  assert.equal(toAnthropicEffort('high'), 'high');
  assert.equal(toAnthropicEffort('max'), 'max');
});

test('toAnthropicEffort converges extended levels to nearest legal Anthropic level', () => {
  // minimal 向下收敛到 low；xhigh 向上收敛到 max（保留「比 high 更强」语义）
  assert.equal(toAnthropicEffort('minimal'), 'low');
  assert.equal(toAnthropicEffort('xhigh'), 'max');
});

test('toAnthropicEffort returns undefined for adaptive/none/unknown (no effort injected)', () => {
  // adaptive：交给模型/网关自适应，不应显式发 effort
  assert.equal(toAnthropicEffort('adaptive'), undefined);
  // none：用户明确不要思考，不发 effort
  assert.equal(toAnthropicEffort('none'), undefined);
  // 未知档位：保守不发，避免触发 400
  assert.equal(toAnthropicEffort('turbo'), undefined);
  assert.equal(toAnthropicEffort(''), undefined);
});

test('pickEffortValue keeps exact match when available', () => {
  assert.equal(pickEffortValue('high', ['low', 'medium', 'high', 'max']), 'high');
});

test('pickEffortValue rounds up to nearest available level', () => {
  // glm-5.2 仅支持 [high, max]，medium 应向上取 high
  assert.equal(pickEffortValue('medium', ['high', 'max']), 'high');
  // low 在 [high, max] 中无更低档，向上取 high
  assert.equal(pickEffortValue('low', ['high', 'max']), 'high');
});

test('pickEffortValue returns user effort verbatim when no available values', () => {
  // 空集：发用户配置，由 API 决定是否接受（透明而非静默降级）
  assert.equal(pickEffortValue('xhigh', []), 'xhigh');
});

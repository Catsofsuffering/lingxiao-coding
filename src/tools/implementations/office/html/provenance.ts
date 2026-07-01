/**
 * provenance —— 生成期溯源标记注入（both_layered 的 spec 主干）。
 *
 * HtmlOfficeEngine 装配 HTML 时，给每个顶层可交互单元（slide / doc block）
 * 注入 data-node-id / data-anchor / data-spec-path，并产出对应的
 * SourceProvenance[] 作为 sourcemap.json 的 nodes。
 *
 * 这是「成品 ↔ 源码」映射的生成端：用户在 Canvas 上选中带这些 data-* 的
 * DOM 单元，即可反查到 spec 节点路径，交给 LLM 精准改 spec → 重新装配。
 */

import type { SourceProvenance } from '../../../../contracts/types/Canvas.js';

/** 把一个已渲染单元的最外层标签注入溯源 data-* 属性（幂等：只注入第一个开标签）。 */
export function injectProvenanceAttrs(
  renderedHtml: string,
  anchor: { nodeId: string; specPath: string },
): string {
  // 在第一个标签的 class="..." 之后插入 data-* 属性。
  // 顶层单元统一形如 <section class="..." ...> 或 <h1 class="..."> 等。
  const attrs = ` data-node-id="${escAttr(anchor.nodeId)}" data-anchor="spec" data-spec-path="${escAttr(anchor.specPath)}"`;
  // 匹配第一个开标签的结束 '>'（排除自闭合的特殊情况，办公组件外层不会自闭合）。
  const m = renderedHtml.match(/^(\s*<[a-zA-Z][^>]*?)(\/?>)/);
  if (!m) return renderedHtml;
  // 已注入则跳过（幂等）。
  if (m[1].includes('data-node-id=')) return renderedHtml;
  const injected = m[1] + attrs + m[2];
  return renderedHtml.replace(m[0], injected);
}

/** 为 slides 模式构建一组 spec 锚点（nodeId=slides.i，specPath=slides[i]）。 */
export function buildSlideProvenance(
  slides: readonly { kind: string }[],
): SourceProvenance[] {
  return slides.map((s, i) => ({
    kind: 'spec' as const,
    nodeId: `slides.${i}`,
    specPath: `slides[${i}]`,
    role: s.kind,
  }));
}

/** 为 document 模式构建一组 spec 锚点（nodeId=blocks.i，specPath=blocks[i]）。 */
export function buildBlockProvenance(
  blocks: readonly { kind: string }[],
): SourceProvenance[] {
  return blocks.map((b, i) => ({
    kind: 'spec' as const,
    nodeId: `blocks.${i}`,
    specPath: `blocks[${i}]`,
    role: b.kind,
  }));
}

function escAttr(value: string): string {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

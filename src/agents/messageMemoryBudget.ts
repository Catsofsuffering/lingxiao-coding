import type { ChatMessage } from '../llm/types.js';

/**
 * Estimate retained in-memory bytes for a chat message.
 *
 * JSON.stringify is intentionally used instead of content-only length because tool calls,
 * tool result ids, timestamps and structured multimodal content also stay resident in the
 * message array and are serialized during LLM calls.
 */
export function estimateChatMessageBytes(message: ChatMessage): number {
  try {
    return Buffer.byteLength(JSON.stringify(message), 'utf8');
  } catch {
    return Buffer.byteLength(String(message.content ?? ''), 'utf8') + 512;
  }
}

export function estimateChatMessagesBytes(messages: readonly ChatMessage[]): number {
  return messages.reduce((sum, message) => sum + estimateChatMessageBytes(message), 0);
}

function stripImagePartsFromMessage(message: ChatMessage): ChatMessage {
  if (!Array.isArray(message.content)) {
    return message;
  }

  const content = message.content.filter((part) => {
    if (!part || typeof part !== 'object') return true;
    const type = (part as { type?: unknown }).type;
    return type !== 'image_url' && type !== 'image' && type !== 'input_image' && type !== 'image_blob_ref';
  });

  return content.length === message.content.length ? message : { ...message, content };
}

function hasImagePart(message: ChatMessage): boolean {
  return Array.isArray(message.content) && message.content.some((part) => {
    if (!part || typeof part !== 'object') return false;
    const type = (part as { type?: unknown }).type;
    return type === 'image_url' || type === 'image' || type === 'input_image' || type === 'image_blob_ref';
  });
}

/**
 * Remove old multimodal image payloads while preserving the surrounding text turns.
 * Large 1M-context models can carry long text histories, but base64/image refs are still
 * expensive and often stale after visual grounding has moved on. Keep only the latest N
 * messages that actually contain image parts; older image parts are stripped in-place.
 */
export function stripOldImageParts(
  messages: ChatMessage[],
  options: { retainImageMessages: number; protectedCount?: number },
): ChatMessage[] {
  const retainImageMessages = Math.max(0, Math.floor(options.retainImageMessages));
  if (retainImageMessages <= 0 || messages.length === 0) {
    return messages.map(stripImagePartsFromMessage);
  }

  const protectedCount = Math.max(0, Math.min(options.protectedCount ?? 0, messages.length));
  let remaining = retainImageMessages;
  const keepImage = new Array<boolean>(messages.length).fill(false);
  for (let i = messages.length - 1; i >= protectedCount; i -= 1) {
    if (!hasImagePart(messages[i])) continue;
    if (remaining > 0) {
      keepImage[i] = true;
      remaining -= 1;
    }
  }
  for (let i = 0; i < protectedCount; i += 1) {
    if (hasImagePart(messages[i])) keepImage[i] = true;
  }

  return messages.map((message, index) => (keepImage[index] ? message : stripImagePartsFromMessage(message)));
}

/**
 * 压缩旧 tool_result 消息，保留最近 N 轮的完整内容，旧轮次用占位符替换。
 *
 * 设计原理：
 * - Tool results（文件内容、命令输出等）是上下文膨胀主因
 * - 模型主要依赖 assistant 总结，很少反复查阅旧 tool results
 * - 占位符保留元信息（工具名、时间戳、大小），模型知道信息存在过
 *
 * 轮次定义：
 * - 一轮 = 一次 assistant 响应周期（从后往前，每个 assistant 消息算一轮）
 * - 保留最近 N 轮的 assistant 消息及其之后的所有 tool results
 *
 * @param messages - 消息历史
 * @param options.retainRecentTurns - 保留最近 N 轮 assistant 响应（默认 50）
 * @param options.protectedCount - 保护前 N 条消息不压缩（默认 0，system prompt 等）
 */
export function compactOldToolResults(
  messages: ChatMessage[],
  _options: { retainRecentTurns?: number; protectedCount?: number },
): ChatMessage[] {
  // 压缩注入已禁用：此前该函数会把所有 tool_result 无差别替换为
  // `[Tool result compacted - ID: ..., Size: ...]` 占位符，导致模型完全无法
  // 读取任何工具输出（连最新一轮也被命中）。此处原样返回，不再做任何替换。
  // 若未来需要恢复按轮次压缩，请重新实现下方的 per-message 替换逻辑，并
  // 确保只在“超过 retainRecentTurns 的旧轮次”上生效，而非全部消息。
  return messages;
}



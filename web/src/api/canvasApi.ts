/**
 * canvasApi — 剑阁可交互 Canvas 的 REST 客户端封装。
 *
 * 对接后端 CanvasRoutes（src/web-server/CanvasRoutes.ts，8 端点）。
 * both_layered 双向映射：用户在成品上选区 + 自然语言 → LLM 改源码 → 重新生成 →
 * 版本栈入栈热更新。本模块只负责「成品交互 → 后端」这段网络调用。
 *
 * 全部端点需 header `x-lingxiao-token`（与其余前端 API 一致，见 headers.ts）。
 */
import { getServerToken } from './headers';
import type {
  CanvasArtifactState,
  CanvasSourceMap,
  CanvasVersion,
  CanvasComment,
  CanvasCommentStatus,
  SelectionIntent,
  SourceProvenance,
} from '@contracts/types/Canvas';

// 重新导出契约类型，前端组件统一从这里取，避免散落的相对路径 import。
export type {
  CanvasArtifactState,
  CanvasSourceMap,
  CanvasVersion,
  CanvasComment,
  CanvasCommentStatus,
  SelectionIntent,
  SourceProvenance,
  SpecAnchor,
  ScriptAnchor,
  SourceAnchorKind,
} from '@contracts/types/Canvas';

/**
 * 把产物相对 workspace 路径规范化成后端使用的 artifactId。
 * 必须与后端 src/core/canvas/CanvasStore.ts 的 toArtifactId 保持一致：
 *   剥前导 ./  →  非法字符转 _  →  路径分隔 / 转 __
 */
export function toArtifactId(artifactPath: string): string {
  return artifactPath
    .replace(/^[./]+/, '')
    .replace(/[^a-zA-Z0-9._\-/]/g, '_')
    .replace(/\//g, '__');
}

/**
 * 把可能是绝对路径的产物路径转成相对 workspace 路径，再 toArtifactId。
 * preview.path 通常已是相对 workspace；workspace 传入时做一次兜底裁剪。
 */
export function artifactIdFromPath(artifactPath: string, workspace?: string | null): string {
  let rel = artifactPath;
  if (workspace && rel.startsWith(workspace)) {
    rel = rel.slice(workspace.length);
  }
  return toArtifactId(rel);
}

class CanvasApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'CanvasApiError';
  }
}

function authHeaders(json = false): Record<string, string> {
  const h: Record<string, string> = { 'x-lingxiao-token': getServerToken() };
  if (json) h['Content-Type'] = 'application/json';
  return h;
}

async function parseJsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new CanvasApiError(res.status, body.error || `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}

function buildQuery(params: Record<string, string | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== '') q.set(k, v);
  }
  return q.toString();
}

/** GET /api/v1/canvas/state → 完整 Canvas 状态（404 表示该产物尚未纳入 Canvas）。 */
export async function fetchCanvasState(
  artifactId: string,
  sessionId?: string | null,
): Promise<CanvasArtifactState | null> {
  const qs = buildQuery({ artifactId, sessionId: sessionId ?? undefined });
  const res = await fetch(`/api/v1/canvas/state?${qs}`, { headers: authHeaders() });
  if (res.status === 404) return null;
  return parseJsonOrThrow<CanvasArtifactState>(res);
}

/** GET /api/v1/canvas/sourcemap → sourcemap（404 → null）。 */
export async function fetchSourceMap(
  artifactId: string,
  sessionId?: string | null,
): Promise<CanvasSourceMap | null> {
  const qs = buildQuery({ artifactId, sessionId: sessionId ?? undefined });
  const res = await fetch(`/api/v1/canvas/sourcemap?${qs}`, { headers: authHeaders() });
  if (res.status === 404) return null;
  return parseJsonOrThrow<CanvasSourceMap>(res);
}

/** GET /api/v1/canvas/versions → 版本栈。 */
export async function fetchVersions(
  artifactId: string,
  sessionId?: string | null,
): Promise<CanvasVersion[]> {
  const qs = buildQuery({ artifactId, sessionId: sessionId ?? undefined });
  const res = await fetch(`/api/v1/canvas/versions?${qs}`, { headers: authHeaders() });
  if (res.status === 404) return [];
  const data = await parseJsonOrThrow<{ versions: CanvasVersion[] }>(res);
  return data.versions ?? [];
}

/** POST /api/v1/canvas/version/activate → 切换/回退到指定版本。 */
export async function activateVersion(
  artifactId: string,
  version: number,
  sessionId?: string | null,
): Promise<void> {
  const res = await fetch('/api/v1/canvas/version/activate', {
    method: 'POST',
    headers: authHeaders(true),
    body: JSON.stringify({ artifactId, version, sessionId: sessionId ?? undefined }),
  });
  await parseJsonOrThrow<{ ok: boolean }>(res);
}

/** GET /api/v1/canvas/comments → 结构化批注列表。 */
export async function fetchComments(
  artifactId: string,
  sessionId?: string | null,
): Promise<CanvasComment[]> {
  const qs = buildQuery({ artifactId, sessionId: sessionId ?? undefined });
  const res = await fetch(`/api/v1/canvas/comments?${qs}`, { headers: authHeaders() });
  if (res.status === 404) return [];
  const data = await parseJsonOrThrow<{ comments: CanvasComment[] }>(res);
  return data.comments ?? [];
}

export interface AddCommentInput {
  artifactId: string;
  nodeId?: string;
  body: string;
  selectionBox?: { x: number; y: number; w: number; h: number };
}

/** POST /api/v1/canvas/comment → 新增批注，返回服务端创建的 CanvasComment。 */
export async function addComment(
  input: AddCommentInput,
  sessionId?: string | null,
): Promise<CanvasComment> {
  const res = await fetch('/api/v1/canvas/comment', {
    method: 'POST',
    headers: authHeaders(true),
    body: JSON.stringify({ ...input, sessionId: sessionId ?? undefined }),
  });
  return parseJsonOrThrow<CanvasComment>(res);
}

/** POST /api/v1/canvas/comment/status → 更新批注状态。 */
export async function updateCommentStatus(
  artifactId: string,
  commentId: string,
  status: CanvasCommentStatus,
  sessionId?: string | null,
): Promise<void> {
  const res = await fetch('/api/v1/canvas/comment/status', {
    method: 'POST',
    headers: authHeaders(true),
    body: JSON.stringify({ artifactId, commentId, status, sessionId: sessionId ?? undefined }),
  });
  await parseJsonOrThrow<{ ok: boolean }>(res);
}

export interface SubmitIntentInput {
  artifactId: string;
  nodeId: string;
  anchor: SourceProvenance;
  currentContent?: string;
  userIntent: string;
  selectionBox?: { x: number; y: number; w: number; h: number };
}

/**
 * POST /api/v1/canvas/intent → 提交选区意图（回写闭环入口）。
 * 后端将其注入 Leader prompt，由 Leader 改 spec/script → 重新装配 → 版本入栈。
 */
export async function submitIntent(
  input: SubmitIntentInput,
  sessionId?: string | null,
): Promise<{ ok: boolean; intent: SelectionIntent }> {
  const res = await fetch('/api/v1/canvas/intent', {
    method: 'POST',
    headers: authHeaders(true),
    body: JSON.stringify({ ...input, sessionId: sessionId ?? undefined }),
  });
  return parseJsonOrThrow<{ ok: boolean; intent: SelectionIntent }>(res);
}

export { CanvasApiError };

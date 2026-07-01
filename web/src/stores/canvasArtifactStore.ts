/**
 * canvasArtifactStore — 剑阁可交互 Canvas 的前端状态管理。
 *
 * 管理「一份产物」的 Canvas 交互态：sourcemap + 版本栈 + 结构化批注 + 选区意图。
 * 与既有的 canvasStore.ts（Workflow ReactFlow 画布）完全独立，互不影响。
 *
 * both_layered 双向映射闭环（前端这一侧）：
 *   1. 用户在成品 iframe 上点选带 data-node-id 的元素 → 拾取 SourceProvenance 锚点
 *   2. 在改写框写自然语言 → submitIntent → 提示"已提交，凌霄正在修改"
 *   3. 版本栈 UI 列出 v1→v2→...，点击 activate 切换预览
 *   4. SSE canvas:version_pushed → 自动刷新版本栈 + 预览切到新版本
 *   5. 结构化批注：持久化到后端，可列出 / 改状态 / 定位回元素
 */

import { create } from 'zustand';
import { acpClient } from '../api/AcpClient';
import { useSessionStore } from './sessionStore';
import { extractCanonicalEventEnvelope, SESSION_UPDATE_METHOD } from '@contracts/adapters/EventAdapter';
import {
  fetchCanvasState,
  fetchVersions,
  fetchComments,
  activateVersion as apiActivateVersion,
  addComment as apiAddComment,
  updateCommentStatus as apiUpdateCommentStatus,
  submitIntent as apiSubmitIntent,
  type CanvasVersion,
  type CanvasComment,
  type CanvasCommentStatus,
  type CanvasSourceMap,
  type SourceProvenance,
} from '../api/canvasApi';

const CANVAS_VERSION_PUSHED = 'canvas:version_pushed';

/** SSE canvas:version_pushed 事件 payload（与后端 EventMap 对齐）。 */
interface CanvasVersionPushedPayload {
  sessionId?: string;
  artifactId?: string;
  version?: number;
  activeVersion?: number;
  intent?: string;
  timestamp?: number;
}

/** 当前选中单元的拾取结果（来自 iframe DOM 的 data-* 锚点）。 */
export interface CanvasSelection {
  nodeId: string;
  anchor: SourceProvenance;
  /** 选中单元当前文本快照，给 LLM 做定位佐证 */
  currentContent?: string;
  /** 归一化选区框（0..1），用于在预览上回显高亮 */
  selectionBox?: { x: number; y: number; w: number; h: number };
}

/** 意图提交状态机，用于 UI 反馈"已提交，凌霄正在修改"。 */
export type IntentSubmitStatus = 'idle' | 'submitting' | 'submitted' | 'error';

/** 最近一次 SSE 热更新提示（"凌霄已更新，已生成 vN"）。 */
export interface CanvasUpdateNotice {
  artifactId: string;
  version: number;
  intent?: string;
  at: number;
}

interface CanvasArtifactStoreState {
  /** 当前聚焦的产物 ID（规范化后的 artifactId）。null 表示当前产物未纳入 Canvas。 */
  activeArtifactId: string | null;
  /** 该产物的 sourcemap（nodeId ↔ 锚点）。 */
  sourceMap: CanvasSourceMap | null;
  /** 版本栈，version 升序。 */
  versions: CanvasVersion[];
  /** 当前激活版本号（0 表示尚无版本）。 */
  activeVersion: number;
  /** 结构化批注列表。 */
  comments: CanvasComment[];
  /** 当前选中单元。 */
  selection: CanvasSelection | null;
  /** 意图提交状态。 */
  intentStatus: IntentSubmitStatus;
  /** 意图提交错误信息。 */
  intentError: string | null;
  /** 加载中。 */
  loading: boolean;
  /** 加载/操作错误。 */
  error: string | null;
  /** 最近一次 SSE 热更新提示。消费后置 null。 */
  updateNotice: CanvasUpdateNotice | null;

  /** 载入一份产物的完整 Canvas 状态。artifactId 为空时清空。 */
  loadArtifact: (artifactId: string | null) => Promise<void>;
  /** 重新拉取版本栈 + 批注（SSE 热更新时用）。 */
  refresh: () => Promise<void>;
  /** 设置 / 清空当前选区。 */
  setSelection: (selection: CanvasSelection | null) => void;
  /** 切换激活版本（调 activate 端点）。 */
  activateVersion: (version: number) => Promise<void>;
  /** 提交选区意图（回写闭环入口）。 */
  submitSelectionIntent: (userIntent: string) => Promise<boolean>;
  /** 重置意图提交状态机到 idle。 */
  resetIntentStatus: () => void;
  /** 新增结构化批注。 */
  addComment: (input: { nodeId?: string; body: string; selectionBox?: { x: number; y: number; w: number; h: number } }) => Promise<CanvasComment | null>;
  /** 更新批注状态。 */
  setCommentStatus: (commentId: string, status: CanvasCommentStatus) => Promise<void>;
  /** 消费一次性热更新提示。 */
  consumeUpdateNotice: () => void;
  /** 清空错误。 */
  clearError: () => void;
}

function currentSessionId(): string | null {
  const s = useSessionStore.getState();
  return s.sessionId || s.activeSessionId || acpClient.getSessionId() || null;
}

export const useCanvasArtifactStore = create<CanvasArtifactStoreState>((set, get) => ({
  activeArtifactId: null,
  sourceMap: null,
  versions: [],
  activeVersion: 0,
  comments: [],
  selection: null,
  intentStatus: 'idle',
  intentError: null,
  loading: false,
  error: null,
  updateNotice: null,

  loadArtifact: async (artifactId) => {
    if (!artifactId) {
      set({
        activeArtifactId: null,
        sourceMap: null,
        versions: [],
        activeVersion: 0,
        comments: [],
        selection: null,
        intentStatus: 'idle',
        intentError: null,
        error: null,
      });
      return;
    }
    set({ activeArtifactId: artifactId, loading: true, error: null, selection: null });
    try {
      const sid = currentSessionId();
      const state = await fetchCanvasState(artifactId, sid);
      // 防止竞态：载入期间用户切到别的产物则丢弃结果
      if (get().activeArtifactId !== artifactId) return;
      if (!state) {
        // 该产物尚未纳入 Canvas（无 sourcemap）。这是常规情况，不算错误。
        set({ sourceMap: null, versions: [], activeVersion: 0, comments: [], loading: false });
        return;
      }
      set({
        sourceMap: state.sourceMap,
        versions: state.versions ?? [],
        activeVersion: state.activeVersion ?? 0,
        comments: state.comments ?? [],
        loading: false,
      });
    } catch (err) {
      if (get().activeArtifactId !== artifactId) return;
      set({ loading: false, error: err instanceof Error ? err.message : String(err) });
    }
  },

  refresh: async () => {
    const artifactId = get().activeArtifactId;
    if (!artifactId) return;
    try {
      const sid = currentSessionId();
      const [versions, comments] = await Promise.all([
        fetchVersions(artifactId, sid),
        fetchComments(artifactId, sid),
      ]);
      if (get().activeArtifactId !== artifactId) return;
      const active = versions.find((v) => v.status === 'active');
      set({
        versions,
        comments,
        activeVersion: active ? active.version : get().activeVersion,
      });
    } catch (err) {
      set({ error: err instanceof Error ? err.message : String(err) });
    }
  },

  setSelection: (selection) => set({ selection }),

  activateVersion: async (version) => {
    const artifactId = get().activeArtifactId;
    if (!artifactId) return;
    const prev = get().activeVersion;
    // 乐观更新
    set({ activeVersion: version, error: null });
    try {
      await apiActivateVersion(artifactId, version, currentSessionId());
      // 重新拉取版本栈以同步 status
      await get().refresh();
    } catch (err) {
      set({ activeVersion: prev, error: err instanceof Error ? err.message : String(err) });
    }
  },

  submitSelectionIntent: async (userIntent) => {
    const { activeArtifactId, selection } = get();
    if (!activeArtifactId || !selection || !userIntent.trim()) return false;
    set({ intentStatus: 'submitting', intentError: null });
    try {
      await apiSubmitIntent(
        {
          artifactId: activeArtifactId,
          nodeId: selection.nodeId,
          anchor: selection.anchor,
          currentContent: selection.currentContent,
          userIntent: userIntent.trim(),
          selectionBox: selection.selectionBox,
        },
        currentSessionId(),
      );
      set({ intentStatus: 'submitted' });
      return true;
    } catch (err) {
      set({ intentStatus: 'error', intentError: err instanceof Error ? err.message : String(err) });
      return false;
    }
  },

  resetIntentStatus: () => set({ intentStatus: 'idle', intentError: null }),

  addComment: async (input) => {
    const artifactId = get().activeArtifactId;
    if (!artifactId || !input.body.trim()) return null;
    try {
      const comment = await apiAddComment(
        { artifactId, nodeId: input.nodeId, body: input.body.trim(), selectionBox: input.selectionBox },
        currentSessionId(),
      );
      set((s) => ({ comments: [...s.comments, comment] }));
      return comment;
    } catch (err) {
      set({ error: err instanceof Error ? err.message : String(err) });
      return null;
    }
  },

  setCommentStatus: async (commentId, status) => {
    const artifactId = get().activeArtifactId;
    if (!artifactId) return;
    const prev = get().comments;
    // 乐观更新
    set({ comments: prev.map((c) => (c.id === commentId ? { ...c, status, updatedAt: Date.now() } : c)) });
    try {
      await apiUpdateCommentStatus(artifactId, commentId, status, currentSessionId());
    } catch (err) {
      set({ comments: prev, error: err instanceof Error ? err.message : String(err) });
    }
  },

  consumeUpdateNotice: () => set({ updateNotice: null }),

  clearError: () => set({ error: null }),
}));

// ─── SSE 订阅：canvas:version_pushed 热更新 ────────────────────────────

let canvasSseUnsubscribe: (() => void) | null = null;

/** 注册一次 session/update 订阅，过滤 canvas:version_pushed 事件。 */
export function ensureCanvasSseSubscription(): void {
  if (canvasSseUnsubscribe) return;
  canvasSseUnsubscribe = acpClient.on(SESSION_UPDATE_METHOD, (data: unknown) => {
    const envelope = extractCanonicalEventEnvelope(data);
    if (!envelope || envelope.type !== CANVAS_VERSION_PUSHED) return;

    const payload = (envelope.payload ?? {}) as CanvasVersionPushedPayload;
    const store = useCanvasArtifactStore.getState();
    const activeArtifactId = store.activeArtifactId;
    if (!activeArtifactId) return;

    // 只处理当前聚焦产物的版本推送
    if (payload.artifactId && payload.artifactId !== activeArtifactId) return;

    // 会话隔离：事件 sessionId 与当前 session 不一致时忽略
    const eventSessionId = payload.sessionId || envelope.sessionId;
    const sid = currentSessionId();
    if (eventSessionId && sid && eventSessionId !== sid) return;

    const version = typeof payload.version === 'number' ? payload.version : 0;

    // 刷新版本栈 + 批注，并切到新版本
    void store.refresh().then(() => {
      const s = useCanvasArtifactStore.getState();
      if (s.activeArtifactId !== activeArtifactId) return;
      if (version > 0) {
        useCanvasArtifactStore.setState({
          activeVersion: version,
          updateNotice: { artifactId: activeArtifactId, version, intent: payload.intent, at: Date.now() },
          // 收到新版本即清掉"提交中"状态
          intentStatus: 'idle',
          intentError: null,
        });
      }
    });
  });
}

export function disposeCanvasSseSubscription(): void {
  canvasSseUnsubscribe?.();
  canvasSseUnsubscribe = null;
}

// 模块加载即订阅（与 wikiStore / langfuseStore 范式一致），支持 HMR dispose。
ensureCanvasSseSubscription();

if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    disposeCanvasSseSubscription();
  });
}

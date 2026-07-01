/**
 * JiangeCanvas — 剑阁 Canvas 工作台主体。
 *
 * 取代旧 WorkbenchSidePanel 的 10-tab 体系（launcher/browser/review/terminal/
 * worktrees/side-chat/files/office 等老功能全废除）。整个剑阁就是一个 Canvas：
 *
 *   ┌──────────┬─────────────────────────────┐
 *   │ 文件树    │  Canvas 舞台（ArtifactView，默认进 canvasMode）          │
 *   │ (左栏)    │  打开任意文件即预览；HTML 产物天然可选区改写            │
 *   │          │  右侧自带版本栈 + 批注（ArtifactView canvasMode 内置）  │
 *   └──────────┴─────────────────────────────┘
 *
 * 点击文件 → openArtifact(useArtifactStore) → ArtifactView 监听 activeArtifact
 * 变化自动 loadPreview；HTML 产物 + defaultCanvasMode 即进入选区改写闭环。
 */

import { lazy, Suspense, useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Folder, FolderOpen, File as FileIcon, ChevronRight, ChevronDown,
  FileCode, FileText, Image as ImageIcon, Loader2, RefreshCw, Home,
  PanelRightClose,
} from 'lucide-react';
import { useSessionStore } from '../../stores/sessionStore';
import { useArtifactStore } from '../../stores/artifactStore';
import { getServerToken } from '../../api/headers';
import { createLogger } from '../../utils/logger';

const log = createLogger('JiangeCanvas');
const ArtifactView = lazy(() => import('../artifacts/ArtifactView'));

// ─── API ───
async function apiFetch<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(`/api/v1${path}`, {
    ...opts,
    headers: {
      'Content-Type': 'application/json',
      'x-lingxiao-token': getServerToken(),
      ...(opts?.headers || {}),
    },
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ─── Types ───
interface FsEntry {
  name: string;
  path: string;
  type: 'file' | 'directory';
  size?: number;
  children?: FsEntry[];
  loaded?: boolean;
}

// ─── 树合并（懒加载子目录后并入对应节点）───
function mergeChildren(nodes: FsEntry[], targetPath: string, children: FsEntry[]): FsEntry[] {
  return nodes.map((node) => {
    if (node.path === targetPath) {
      return { ...node, children, loaded: true };
    }
    if (node.type === 'directory' && node.children && targetPath.startsWith(node.path)) {
      return { ...node, children: mergeChildren(node.children, targetPath, children) };
    }
    return node;
  });
}

function fileIcon(name: string) {
  const ext = name.split('.').pop()?.toLowerCase() || '';
  if (['ts', 'tsx', 'js', 'jsx', 'mjs', 'cjs', 'json', 'py', 'sh', 'css', 'scss'].includes(ext)) {
    return <FileCode size={12} className="text-text-tertiary flex-shrink-0" />;
  }
  if (['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'ico'].includes(ext)) {
    return <ImageIcon size={12} className="text-text-tertiary flex-shrink-0" />;
  }
  if (['html', 'htm', 'md', 'txt', 'pdf', 'docx', 'pptx', 'xlsx'].includes(ext)) {
    return <FileText size={12} className="text-text-tertiary flex-shrink-0" />;
  }
  return <FileIcon size={12} className="text-text-tertiary flex-shrink-0" />;
}

// ─── 文件树递归节点 ───
function TreeNodes({
  nodes, expanded, activePath, onToggleDir, onFileClick, depth,
}: {
  nodes: FsEntry[];
  expanded: Set<string>;
  activePath: string | null;
  onToggleDir: (node: FsEntry) => void;
  onFileClick: (node: FsEntry) => void;
  depth: number;
}) {
  return (
    <>
      {nodes.map((node) => {
        const isDir = node.type === 'directory';
        const isExpanded = expanded.has(node.path);
        const isActive = node.path === activePath;
        return (
          <div key={node.path}>
            <button
              type="button"
              onClick={() => (isDir ? onToggleDir(node) : onFileClick(node))}
              className={`flex w-full items-center gap-1 py-0.5 pr-2 text-left text-[12px] hover:bg-bg-hover ${
                isActive ? 'bg-accent-brand/10 text-accent-brand' : 'text-text-secondary'
              }`}
              style={{ paddingLeft: `${depth * 12 + 8}px` }}
              title={node.path}
            >
              {isDir ? (
                <>
                  {isExpanded ? <ChevronDown size={11} className="flex-shrink-0" /> : <ChevronRight size={11} className="flex-shrink-0" />}
                  {isExpanded ? <FolderOpen size={12} className="text-accent-brand/70 flex-shrink-0" /> : <Folder size={12} className="text-text-tertiary flex-shrink-0" />}
                </>
              ) : (
                <>
                  <span className="w-[11px] flex-shrink-0" />
                  {fileIcon(node.name)}
                </>
              )}
              <span className="truncate">{node.name}</span>
            </button>
            {isDir && isExpanded && node.children && (
              <TreeNodes
                nodes={node.children}
                expanded={expanded}
                activePath={activePath}
                onToggleDir={onToggleDir}
                onFileClick={onFileClick}
                depth={depth + 1}
              />
            )}
          </div>
        );
      })}
    </>
  );
}

export interface JiangeCanvasProps {
  /** 折叠剑阁回调。 */
  onCollapse: () => void;
  /** 刷新工作台上下文。 */
  onRefresh?: () => void;
  /** 当前工作区名称（标题展示）。 */
  workspaceName?: string;
}

export default function JiangeCanvas({ onCollapse, onRefresh, workspaceName }: JiangeCanvasProps) {
  const { t } = useTranslation();
  const sessionId = useSessionStore((s) => s.sessionId || s.activeSessionId);
  const serverCwd = useSessionStore((s) => s.serverCwd);
  const sessions = useSessionStore((s) => s.sessions);
  const workspace = sessions.find((s) => s.id === sessionId)?.workspace || serverCwd || '.';

  const openArtifact = useArtifactStore((s) => s.openArtifact);
  const activeArtifact = useArtifactStore((s) => s.activeArtifact);

  const [tree, setTree] = useState<FsEntry[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchTree = useCallback(async (dirPath?: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await apiFetch<{ entries: FsEntry[] }>('/fs/list', {
        method: 'POST',
        body: JSON.stringify({ path: dirPath || workspace, sessionId }),
      });
      if (dirPath) {
        setTree((prev) => mergeChildren(prev, dirPath, data.entries || []));
      } else {
        setTree(data.entries || []);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to list files');
      log.warn('[JiangeCanvas] fetchTree failed:', err);
    } finally {
      setIsLoading(false);
    }
  }, [workspace, sessionId]);

  useEffect(() => { void fetchTree(); }, [fetchTree]);

  const toggleDir = useCallback(async (node: FsEntry) => {
    const next = new Set(expanded);
    if (next.has(node.path)) {
      next.delete(node.path);
      setExpanded(next);
    } else {
      next.add(node.path);
      setExpanded(next);
      if (!node.loaded) await fetchTree(node.path);
    }
  }, [expanded, fetchTree]);

  const handleFileClick = useCallback((node: FsEntry) => {
    // 点击文件 → 设为 activeArtifact，驱动右侧 ArtifactView 预览。
    openArtifact({ name: node.name, path: node.path });
  }, [openArtifact]);

  return (
    <div className="jiange-canvas flex h-full min-h-0 flex-col bg-bg-primary">
      {/* 顶部条 */}
      <div className="flex items-center gap-2 px-3 h-9 border-b border-border-muted bg-bg-secondary/50 flex-shrink-0">
        <div className="flex h-5 w-5 items-center justify-center rounded bg-accent-brand/15 text-[11px] font-semibold text-accent-brand" aria-hidden="true">凌</div>
        <span className="text-sm font-medium text-text-primary">{t('workbench.pavilion', '剑阁')}</span>
        <span className="text-[11px] text-text-tertiary truncate flex-1" title={workspace}>
          {workspaceName || (workspace === '.' ? 'workspace' : workspace.split('/').pop())}
        </span>
        <button
          type="button"
          onClick={() => { void fetchTree(); onRefresh?.(); }}
          className="rounded p-1 text-text-tertiary hover:bg-bg-hover hover:text-text-primary"
          title={t('app.refresh', '刷新')}
        >
          <RefreshCw size={13} />
        </button>
        <button
          type="button"
          onClick={onCollapse}
          className="rounded p-1 text-text-tertiary hover:bg-bg-hover hover:text-text-primary"
          title={t('workbench.collapsePavilion', '折叠剑阁')}
        >
          <PanelRightClose size={14} />
        </button>
      </div>

      {/* 主体：左文件树 + 右 Canvas 舞台 */}
      <div className="flex min-h-0 flex-1 overflow-hidden">
        {/* 左栏文件树 */}
        <div className="flex w-52 flex-col border-r border-border-muted bg-bg-secondary/30 flex-shrink-0">
          <div className="flex items-center gap-1.5 px-2 h-7 border-b border-border-muted bg-bg-tertiary/40 flex-shrink-0">
            <Home size={11} className="text-text-tertiary" />
            <span className="text-[10px] font-mono text-text-tertiary truncate flex-1" title={workspace}>
              {workspace === '.' ? 'root' : workspace.split('/').pop() || 'root'}
            </span>
          </div>
          <div className="flex-1 overflow-auto py-0.5">
            {isLoading && tree.length === 0 ? (
              <div className="flex items-center justify-center py-6 text-text-tertiary"><Loader2 size={14} className="animate-spin" /></div>
            ) : tree.length === 0 ? (
              <div className="px-2 py-4 text-[11px] text-text-tertiary text-center">{error || t('workbench.noFiles', '无文件')}</div>
            ) : (
              <TreeNodes
                nodes={tree}
                expanded={expanded}
                activePath={activeArtifact?.path || null}
                onToggleDir={toggleDir}
                onFileClick={handleFileClick}
                depth={0}
              />
            )}
          </div>
        </div>

        {/* 右栏 Canvas 舞台：ArtifactView 默认进 canvasMode */}
        <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
          {activeArtifact ? (
            <Suspense fallback={
              <div className="flex h-full items-center justify-center text-xs text-text-tertiary">
                <Loader2 size={14} className="mr-2 animate-spin" />{t('app.loading', '加载中')}
              </div>
            }>
              <ArtifactView defaultCanvasMode embedded />
            </Suspense>
          ) : (
            <div className="flex h-full flex-col items-center justify-center gap-3 text-text-tertiary">
              <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-accent-brand/10 text-lg font-semibold text-accent-brand">凌</div>
              <p className="text-sm">{t('canvas.emptyHint', '从左侧选择文件，在画布中预览与改写')}</p>
              <p className="text-[11px] text-text-tertiary/70">{t('canvas.emptyHintSub', 'HTML 成品可直接选区，写下诉求让凌霄改源码重新生成')}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

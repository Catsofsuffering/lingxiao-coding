/**
 * CanvasVersionStack — 剑阁版本栈 UI。
 *
 * 列出一份产物的版本栈（v1→v2→...），当前 active 版本高亮，
 * 点击任意版本 → activate 切换/回退。同时展示 SSE 热更新提示。
 * 复用既有 design token，不引入新 UI 库。
 */
import { useTranslation } from 'react-i18next';
import { History, GitBranch, RotateCcw, CheckCircle2, X } from 'lucide-react';
import type { CanvasUpdateNotice } from '../../stores/canvasArtifactStore';
import type { CanvasVersion } from '../../api/canvasApi';

interface CanvasVersionStackProps {
  versions: CanvasVersion[];
  activeVersion: number;
  /** 切换版本回调。 */
  onActivate: (version: number) => void;
  /** 最近一次 SSE 热更新提示。 */
  updateNotice: CanvasUpdateNotice | null;
  /** 消费热更新提示。 */
  onDismissNotice: () => void;
}

function relTime(ts: number): string {
  const diff = Date.now() - ts;
  const m = Math.floor(diff / 60000);
  if (m < 1) return '刚刚';
  if (m < 60) return `${m} 分钟前`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} 小时前`;
  return `${Math.floor(h / 24)} 天前`;
}

export default function CanvasVersionStack({
  versions,
  activeVersion,
  onActivate,
  updateNotice,
  onDismissNotice,
}: CanvasVersionStackProps) {
  const { t } = useTranslation();
  // 版本号降序显示（最新在上）
  const ordered = [...versions].sort((a, b) => b.version - a.version);

  return (
    <div className="flex flex-col gap-2">
      {/* 标题 */}
      <div className="flex items-center gap-1.5 px-1 text-[11px] font-medium text-text-secondary">
        <History size={12} className="text-accent-brand" />
        {t('canvas.versions.title', '版本栈')}
        {versions.length > 0 && (
          <span className="text-text-tertiary">({versions.length})</span>
        )}
      </div>

      {/* SSE 热更新提示 */}
      {updateNotice && (
        <div className="flex items-start gap-2 rounded-md border border-accent-green/30 bg-accent-green/10 px-2.5 py-2 text-[11px] text-accent-green">
          <CheckCircle2 size={13} className="mt-0.5 shrink-0" />
          <div className="min-w-0 flex-1">
            <div className="font-medium">
              {t('canvas.versions.updated', '凌霄已更新，已生成 v{{n}}', { n: updateNotice.version })}
            </div>
            {updateNotice.intent && (
              <div className="mt-0.5 truncate text-accent-green/80" title={updateNotice.intent}>
                {updateNotice.intent}
              </div>
            )}
          </div>
          <button
            onClick={onDismissNotice}
            className="shrink-0 rounded p-0.5 text-accent-green/70 hover:text-accent-green"
            title={t('app.close', '关闭')}
          >
            <X size={12} />
          </button>
        </div>
      )}

      {/* 版本列表 */}
      {ordered.length === 0 ? (
        <div className="px-1 py-2 text-[10px] text-text-tertiary">
          {t('canvas.versions.empty', '尚无版本。在预览上选区并提交修改后，凌霄会生成新版本。')}
        </div>
      ) : (
        <div className="flex flex-col gap-1">
          {ordered.map((v) => {
            const isActive = v.version === activeVersion;
            return (
              <button
                key={v.version}
                onClick={() => { if (!isActive) onActivate(v.version); }}
                className={`group flex items-start gap-2 rounded-md border px-2.5 py-1.5 text-left transition-colors ${
                  isActive
                    ? 'border-accent-brand bg-accent-brand/10'
                    : 'border-border-muted hover:border-accent-brand/40 hover:bg-bg-hover'
                }`}
                title={isActive ? t('canvas.versions.current', '当前版本') : t('canvas.versions.switchTo', '切换到 v{{n}}', { n: v.version })}
              >
                <span className={`mt-0.5 shrink-0 ${isActive ? 'text-accent-brand' : 'text-text-tertiary group-hover:text-accent-brand/70'}`}>
                  {isActive ? <GitBranch size={13} /> : <RotateCcw size={13} />}
                </span>
                <span className="min-w-0 flex-1">
                  <span className="flex items-center gap-1.5">
                    <span className={`font-mono text-xs font-semibold ${isActive ? 'text-accent-brand' : 'text-text-primary'}`}>
                      v{v.version}
                    </span>
                    {isActive && (
                      <span className="rounded bg-accent-brand/20 px-1 py-px text-[9px] text-accent-brand">
                        {t('canvas.versions.active', '当前')}
                      </span>
                    )}
                    {v.status === 'reverted' && (
                      <span className="text-[9px] text-text-tertiary">{t('canvas.versions.reverted', '已回退')}</span>
                    )}
                  </span>
                  {v.intent && (
                    <span className="mt-0.5 block truncate text-[10px] text-text-secondary" title={v.intent}>
                      {v.intent}
                    </span>
                  )}
                  <span className="mt-0.5 block text-[9px] text-text-tertiary">{relTime(v.createdAt)}</span>
                </span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

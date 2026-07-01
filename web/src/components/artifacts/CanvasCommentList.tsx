/**
 * CanvasCommentList — 剑阁结构化批注列表。
 *
 * 替代「一次性纯文本 prompt」：列出一份产物的全部持久化批注，
 * 显示状态（pending / applied / dismissed），点击可定位回对应元素，
 * 并支持改状态。复用既有 design token。
 */
import { useTranslation } from 'react-i18next';
import { MessageSquare, Check, X, CircleDot, MapPin } from 'lucide-react';
import type { CanvasComment, CanvasCommentStatus } from '../../api/canvasApi';

interface CanvasCommentListProps {
  comments: CanvasComment[];
  /** 点击批注定位回元素（按 nodeId）。 */
  onLocate: (nodeId: string | undefined) => void;
  /** 更新批注状态。 */
  onSetStatus: (commentId: string, status: CanvasCommentStatus) => void;
}

const STATUS_META: Record<CanvasCommentStatus, { label: string; cls: string }> = {
  pending: { label: '待处理', cls: 'text-accent-yellow' },
  applied: { label: '已应用', cls: 'text-accent-green' },
  dismissed: { label: '已忽略', cls: 'text-text-tertiary' },
};

function relTime(ts: number): string {
  const diff = Date.now() - ts;
  const m = Math.floor(diff / 60000);
  if (m < 1) return '刚刚';
  if (m < 60) return `${m} 分钟前`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} 小时前`;
  return `${Math.floor(h / 24)} 天前`;
}

export default function CanvasCommentList({ comments, onLocate, onSetStatus }: CanvasCommentListProps) {
  const { t } = useTranslation();
  const ordered = [...comments].sort((a, b) => b.createdAt - a.createdAt);

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-1.5 px-1 text-[11px] font-medium text-text-secondary">
        <MessageSquare size={12} className="text-accent-brand" />
        {t('canvas.comments.title', '批注')}
        {comments.length > 0 && <span className="text-text-tertiary">({comments.length})</span>}
      </div>

      {ordered.length === 0 ? (
        <div className="px-1 py-2 text-[10px] text-text-tertiary">
          {t('canvas.comments.empty', '尚无批注。在预览上选区即可添加结构化批注。')}
        </div>
      ) : (
        <div className="flex flex-col gap-1.5">
          {ordered.map((c) => {
            const meta = STATUS_META[c.status] ?? STATUS_META.pending;
            return (
              <div
                key={c.id}
                className="rounded-md border border-border-muted bg-bg-secondary/40 px-2.5 py-2"
              >
                {/* 头部：状态 + 版本 + 时间 */}
                <div className="flex items-center gap-1.5 text-[10px]">
                  <CircleDot size={10} className={meta.cls} />
                  <span className={meta.cls}>{t(`canvas.comments.status.${c.status}`, meta.label)}</span>
                  <span className="text-text-tertiary">· v{c.version}</span>
                  <span className="ml-auto text-text-tertiary">{relTime(c.createdAt)}</span>
                </div>

                {/* 正文 */}
                <div className="mt-1 text-[11px] leading-snug text-text-primary">{c.body}</div>

                {/* nodeId 定位 */}
                {c.nodeId && (
                  <button
                    onClick={() => onLocate(c.nodeId)}
                    className="mt-1 inline-flex items-center gap-1 text-[10px] text-text-secondary hover:text-accent-brand"
                    title={t('canvas.comments.locate', '定位到元素')}
                  >
                    <MapPin size={10} />
                    <span className="truncate font-mono">{c.nodeId}</span>
                  </button>
                )}

                {/* 状态操作 */}
                <div className="mt-1.5 flex items-center gap-1.5">
                  {c.status !== 'applied' && (
                    <button
                      onClick={() => onSetStatus(c.id, 'applied')}
                      className="inline-flex items-center gap-1 rounded border border-accent-green/30 px-1.5 py-0.5 text-[9px] text-accent-green hover:bg-accent-green/10"
                    >
                      <Check size={10} />
                      {t('canvas.comments.markApplied', '标记已应用')}
                    </button>
                  )}
                  {c.status !== 'dismissed' && (
                    <button
                      onClick={() => onSetStatus(c.id, 'dismissed')}
                      className="inline-flex items-center gap-1 rounded border border-border-muted px-1.5 py-0.5 text-[9px] text-text-tertiary hover:bg-bg-hover"
                    >
                      <X size={10} />
                      {t('canvas.comments.dismiss', '忽略')}
                    </button>
                  )}
                  {c.status !== 'pending' && (
                    <button
                      onClick={() => onSetStatus(c.id, 'pending')}
                      className="inline-flex items-center gap-1 rounded border border-border-muted px-1.5 py-0.5 text-[9px] text-text-secondary hover:bg-bg-hover"
                    >
                      <CircleDot size={10} />
                      {t('canvas.comments.reopen', '重开')}
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

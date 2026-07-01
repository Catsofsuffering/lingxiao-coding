/**
 * CanvasIntentPopup — 剑阁选区改写框。
 *
 * 用户在成品预览上点选一个带 data-node-id 的元素后弹出此浮框，
 * 写下自然语言诉求 → 提交 SelectionIntent（回写闭环入口）→
 * 提示"已提交，凌霄正在修改"。
 *
 * 复用 CommentPopup 的视觉语言（同一套 design token / 浮层结构），
 * 但语义是「选区 → 源码改写意图」而非一次性纯文本评论。
 */
import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Send, X, Loader2, Wand2, CheckCircle2, Target } from 'lucide-react';
import type { CanvasSelection, IntentSubmitStatus } from '../../stores/canvasArtifactStore';

interface CanvasIntentPopupProps {
  /** 当前选区，null 时不渲染。 */
  selection: CanvasSelection | null;
  /** 提交状态机。 */
  status: IntentSubmitStatus;
  /** 提交错误信息。 */
  error: string | null;
  /** 提交意图回调。 */
  onSubmit: (userIntent: string) => Promise<boolean>;
  /** 关闭回调。 */
  onClose: () => void;
}

export default function CanvasIntentPopup({ selection, status, error, onSubmit, onClose }: CanvasIntentPopupProps) {
  const { t } = useTranslation();
  const [intent, setIntent] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (selection && textareaRef.current) {
      textareaRef.current.focus();
    }
  }, [selection]);

  // 选区切换时清空输入
  useEffect(() => {
    setIntent('');
  }, [selection?.nodeId]);

  if (!selection) return null;

  const submitting = status === 'submitting';
  const submitted = status === 'submitted';

  const handleSubmit = async () => {
    if (!intent.trim() || submitting) return;
    const ok = await onSubmit(intent);
    if (ok) setIntent('');
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      void handleSubmit();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      onClose();
    }
  };

  const anchorLabel = selection.anchor.kind === 'spec'
    ? `spec · ${(selection.anchor as { specPath?: string }).specPath ?? selection.nodeId}`
    : `script · ${(selection.anchor as { srcFile?: string }).srcFile ?? selection.nodeId}`;

  return (
    <div className="absolute right-3 top-3 z-30 w-80 max-w-[calc(100%-1.5rem)] animate-[fadeIn_140ms_ease-out]">
      <div className="overflow-hidden rounded-[var(--radius-card)] border border-border-default bg-bg-card shadow-[var(--shadow-floating)]">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-border-muted px-3 py-2.5">
          <div className="flex items-center gap-1.5 text-xs font-medium text-text-primary">
            <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-[var(--radius-tile)] bg-accent-brand/12 text-accent-brand">
              <Wand2 size={12} />
            </span>
            {t('canvas.intent.title', '改这里')}
          </div>
          <button
            onClick={onClose}
            className="rounded-[var(--radius-tile)] p-1 text-text-tertiary transition-colors hover:bg-bg-hover hover:text-text-primary"
            title={t('app.close', '关闭')}
          >
            <X size={14} />
          </button>
        </div>

        {/* 选中单元信息 */}
        <div className="border-b border-border-muted px-3 py-2.5">
          <div className="flex items-center gap-1.5 text-[11px] text-text-secondary">
            <Target size={11} className="shrink-0 text-accent-brand/80" />
            <span className="truncate font-mono" title={selection.nodeId}>{selection.nodeId}</span>
          </div>
          <div className="mt-1 truncate text-[10px] text-text-tertiary" title={anchorLabel}>{anchorLabel}</div>
          {selection.currentContent && (
            <div className="mt-2 max-h-16 overflow-y-auto rounded-[var(--radius-tile)] border border-border-muted bg-bg-secondary/40 px-2 py-1.5 text-[10px] leading-relaxed text-text-tertiary">
              {selection.currentContent.slice(0, 200)}
            </div>
          )}
        </div>

        {/* 提交成功提示 */}
        {submitted ? (
          <div className="flex flex-col items-center gap-2 px-4 py-6 text-center">
            <span className="flex h-12 w-12 items-center justify-center rounded-full bg-accent-green/12 text-accent-green">
              <CheckCircle2 size={26} />
            </span>
            <div className="text-sm font-medium text-text-primary">{t('canvas.intent.submitted', '已提交，凌霄正在修改…')}</div>
            <div className="text-[11px] leading-relaxed text-text-tertiary">{t('canvas.intent.submittedHint', '改完会自动生成新版本并刷新预览')}</div>
            <button
              onClick={onClose}
              className="mt-1 rounded-[var(--radius-control)] border border-border-muted px-3 py-1.5 text-xs text-text-secondary transition-colors hover:bg-bg-hover hover:text-text-primary"
            >
              {t('app.close', '关闭')}
            </button>
          </div>
        ) : (
          <>
            {/* 输入区 */}
            <div className="p-3">
              <textarea
                ref={textareaRef}
                value={intent}
                onChange={(e) => setIntent(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder={t('canvas.intent.placeholder', '描述要怎么改，例如：标题再大一点，加一道金色描边…')}
                className="h-24 w-full resize-none rounded-[var(--radius-control)] border border-border-input bg-bg-input px-3 py-2.5 text-sm text-text-primary placeholder:text-text-tertiary outline-none transition-colors focus:border-accent-brand/60 focus:shadow-[var(--ring-focus)]"
                disabled={submitting}
              />
              {error && status === 'error' && (
                <div className="mt-2 text-xs text-accent-red">{error}</div>
              )}
            </div>

            {/* Footer */}
            <div className="flex items-center justify-between border-t border-border-muted px-3 py-2.5">
              <span className="text-[10px] text-text-tertiary">
                {t('canvas.intent.hint', '⌘/Ctrl+Enter 提交 · Esc 取消')}
              </span>
              <div className="flex items-center gap-2">
                <button
                  onClick={onClose}
                  disabled={submitting}
                  className="rounded-[var(--radius-control)] border border-border-muted px-3 py-1.5 text-xs text-text-secondary transition-colors hover:bg-bg-hover hover:text-text-primary disabled:opacity-40"
                >
                  {t('app.cancel', '取消')}
                </button>
                <button
                  onClick={() => void handleSubmit()}
                  disabled={!intent.trim() || submitting}
                  className="inline-flex items-center gap-1.5 rounded-[var(--radius-control)] bg-accent-brand px-3.5 py-1.5 text-xs font-medium text-[color:var(--primary-button-fg)] transition-opacity hover:opacity-90 disabled:opacity-40"
                >
                  {submitting ? <Loader2 size={13} className="animate-spin" /> : <Send size={13} />}
                  {t('canvas.intent.submit', '提交修改')}
                </button>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

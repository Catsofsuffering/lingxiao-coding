/**
 * CommentPopup — 预览评论浮窗
 *
 * 用户在预览面板选中内容后点击"评论"按钮，弹出此浮窗。
 * 用户填写评论后，点击发送，将 文件路径 + 源码级上下文 + 评论
 * 通过 session/prompt 发送给凌霄 Leader，由 Leader 做源码级修改。
 */
import { useState, useRef, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { Send, X, Loader2, MessageSquare, FileText, MapPin, Table, Presentation, Link2, Quote, ChevronUp, ChevronDown } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

export interface CommentContext {
  /** 文件路径 */
  filePath: string;
  /** 文件格式 */
  format: string;
  /** 选中内容类型 */
  selectionType: 'text' | 'line' | 'cell' | 'slide' | 'element' | 'full';
  /** 选中文本 */
  selectedText?: string;
  /** 行号范围 (Monaco) */
  startLine?: number;
  endLine?: number;
  /** XLSX sheet 名 */
  sheet?: string;
  /** PPTX slide index */
  slideIndex?: number;
  /** 表格行列 */
  row?: number;
  col?: number;
}

interface CommentPopupProps {
  /** 评论上下文 */
  context: CommentContext | null;
  /** 发送回调 */
  onSend: (comment: string, ctx: CommentContext) => Promise<void>;
  /** 关闭回调 */
  onClose: () => void;
}

export default function CommentPopup({ context, onSend, onClose }: CommentPopupProps) {
  const { t } = useTranslation();
  const [comment, setComment] = useState('');
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectionExpanded, setSelectionExpanded] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (context && textareaRef.current) {
      textareaRef.current.focus();
    }
  }, [context]);

  if (!context) return null;

  const handleSend = async () => {
    if (!comment.trim() || sending) return;
    setSending(true);
    setError(null);
    try {
      await onSend(comment.trim(), context);
      setComment('');
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : t('artifact.comment.sendFailed', 'Failed to send comment'));
    } finally {
      setSending(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      handleSend();
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      onClose();
    }
  };

  // 构建上下文摘要：结构化小标签（chip），用 lucide 图标替代 emoji 拼接
  type ContextChip = { icon: LucideIcon; label: string; mono?: boolean };
  const contextChips: ContextChip[] = (() => {
    const chips: ContextChip[] = [];
    chips.push({ icon: FileText, label: context.filePath, mono: true });
    if (context.selectionType === 'line' && context.startLine) {
      const range = context.endLine && context.endLine !== context.startLine
        ? `L${context.startLine}-${context.endLine}`
        : `L${context.startLine}`;
      chips.push({ icon: MapPin, label: range, mono: true });
    }
    if (context.sheet) chips.push({ icon: Table, label: `Sheet ${context.sheet}`, mono: true });
    if (context.slideIndex !== undefined) chips.push({ icon: Presentation, label: `Slide ${context.slideIndex}`, mono: true });
    if (context.row !== undefined && context.col !== undefined) chips.push({ icon: Link2, label: `R${context.row}C${context.col}`, mono: true });
    return chips;
  })();

  const selectedPreview = context.selectedText
    ? (context.selectedText.length > 160 ? context.selectedText.slice(0, 160) + '…' : context.selectedText)
    : null;

  return (
    <div
      className="absolute inset-0 z-50 flex items-center justify-center bg-bg-primary/70 backdrop-blur-md animate-[fadeIn_120ms_ease-out]"
      onClick={onClose}
    >
      <div
        className="w-[540px] max-w-[92vw] overflow-hidden rounded-[var(--radius-card)] border border-border-default bg-bg-card shadow-[var(--shadow-floating)] animate-[fadeIn_160ms_var(--motion-soft)]"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center gap-2.5 border-b border-border-muted px-5 py-3.5">
          <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-[var(--radius-control)] bg-accent-brand/12 text-accent-brand">
            <MessageSquare size={15} />
          </span>
          <span className="text-sm font-medium text-text-primary">
            {t('artifact.comment.title', '源码评论 → 发送给 Leader')}
          </span>
          <button
            onClick={onClose}
            className="ml-auto rounded-[var(--radius-control)] p-1.5 text-text-tertiary transition-colors hover:bg-bg-hover hover:text-text-primary"
            title={t('app.close', '关闭')}
          >
            <X size={16} />
          </button>
        </div>

        {/* Context preview */}
        <div className="border-b border-border-muted px-5 py-3.5">
          <div className="mb-2 text-[10px] font-medium uppercase tracking-wider text-text-tertiary">
            {t('artifact.comment.contextLabel', '评论上下文')}
          </div>
          <div className="flex flex-wrap gap-1.5">
            {contextChips.map((chip, i) => {
              const Icon = chip.icon;
              return (
                <span
                  key={i}
                  className={`inline-flex max-w-full items-center gap-1.5 rounded-[var(--radius-control)] border border-border-muted bg-bg-secondary/50 px-2 py-1 text-[11px] text-text-secondary ${chip.mono ? 'font-mono' : ''}`}
                  title={chip.label}
                >
                  <Icon size={12} className="shrink-0 text-accent-brand/80" />
                  <span className="truncate">{chip.label}</span>
                </span>
              );
            })}
          </div>

          {/* 选中文本预览：优雅卡片，可折叠/展开 */}
          {selectedPreview && (
            <div className="mt-2.5 rounded-[var(--radius-control)] border border-border-muted bg-bg-secondary/40">
              <button
                type="button"
                onClick={() => setSelectionExpanded((v) => !v)}
                className="flex w-full items-center gap-1.5 px-2.5 py-2 text-left text-[11px] text-text-secondary transition-colors hover:text-text-primary"
              >
                <Quote size={12} className="shrink-0 text-accent-brand/70" />
                <span className="min-w-0 flex-1 truncate font-mono text-text-tertiary">
                  {selectionExpanded ? t('artifact.comment.selectionLabel', '选中文本') : selectedPreview}
                </span>
                {selectionExpanded
                  ? <ChevronUp size={13} className="shrink-0 text-text-tertiary" />
                  : <ChevronDown size={13} className="shrink-0 text-text-tertiary" />}
              </button>
              {selectionExpanded && (
                <pre className="max-h-40 overflow-auto border-t border-border-muted px-2.5 py-2 text-[10px] font-mono leading-relaxed text-text-secondary whitespace-pre-wrap break-all">
                  {context.selectedText}
                </pre>
              )}
            </div>
          )}
        </div>

        {/* Comment input */}
        <div className="px-5 py-4">
          <textarea
            ref={textareaRef}
            value={comment}
            onChange={(e) => setComment(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={t('artifact.comment.placeholder', '输入你的修改意见，Leader 会根据上下文做源码级修改...')}
            className="h-24 w-full resize-none rounded-[var(--radius-control)] border border-border-input bg-bg-input px-3 py-2.5 text-sm text-text-primary placeholder:text-text-tertiary outline-none transition-colors focus:border-accent-brand/60 focus:shadow-[var(--ring-focus)]"
            disabled={sending}
          />
          {error && (
            <div className="mt-2 text-xs text-accent-red">{error}</div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between border-t border-border-muted px-5 py-3.5">
          <span className="text-[10px] text-text-tertiary">
            {t('artifact.comment.hint', '⌘/Ctrl+Enter 发送 · Esc 取消')}
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={onClose}
              disabled={sending}
              className="rounded-[var(--radius-control)] border border-border-muted px-3 py-1.5 text-xs text-text-secondary transition-colors hover:bg-bg-hover hover:text-text-primary disabled:opacity-40"
            >
              {t('app.cancel', '取消')}
            </button>
            <button
              onClick={handleSend}
              disabled={!comment.trim() || sending}
              className="inline-flex items-center gap-1.5 rounded-[var(--radius-control)] bg-accent-brand px-3.5 py-1.5 text-xs font-medium text-[color:var(--primary-button-fg)] transition-opacity hover:opacity-90 disabled:opacity-40"
            >
              {sending ? <Loader2 size={13} className="animate-spin" /> : <Send size={13} />}
              {t('artifact.comment.send', '发送给 Leader')}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

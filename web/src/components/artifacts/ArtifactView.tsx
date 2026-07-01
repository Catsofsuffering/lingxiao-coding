import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import DOMPurify from 'dompurify';
import SafeMarkdown from '../ui/SafeMarkdown';
import {
  AlertTriangle, Archive, Columns2, Copy, Download, Edit3, Eye, FileText,
  Image as ImageIcon, Loader2, RefreshCw, Save, Table, XCircle,
  ChevronDown, ChevronRight, Folder, FolderOpen, FileCode,
} from 'lucide-react';
import { apiHeaders, getServerToken } from '../../api/headers';
import { useSessionStore } from '../../stores/sessionStore';
import { useArtifactStore, type ArtifactTarget } from '../../stores/artifactStore';
import { useTranslation } from 'react-i18next';
import CommentPopup, { type CommentContext } from './CommentPopup';
import { acpClient } from '../../api/AcpClient';
import { MessageSquare, Wand2 } from 'lucide-react';
import CanvasHtmlPreview from './CanvasHtmlPreview';
import CanvasIntentPopup from './CanvasIntentPopup';
import CanvasVersionStack from './CanvasVersionStack';
import CanvasCommentList from './CanvasCommentList';
import { useCanvasArtifactStore } from '../../stores/canvasArtifactStore';
import { artifactIdFromPath } from '../../api/canvasApi';

// Monaco 按需加载，避免影响初始 bundle 体积
const MonacoEditor = lazy(() => import('@monaco-editor/react').then(m => ({ default: m.default })));

// ======================== 类型定义 ========================

type JsonRecord = Record<string, unknown>;
type ArtifactPreviewStatus = 'ok' | 'partial' | 'parse_error';
type ArtifactTrustLevel = 'raw' | 'rich_text' | 'structure' | 'table' | 'text' | 'untrusted';
type ArtifactRenderer = 'text' | 'html' | 'pptx-structure' | 'xlsx-table' | 'raw';
type ArtifactEditableKind = 'text' | 'markdown' | 'html' | 'office-native' | 'read-only';

interface ArtifactPreview {
  path: string;
  name: string;
  size: number;
  mimeType: string;
  format: string;
  status?: ArtifactPreviewStatus;
  trustLevel?: ArtifactTrustLevel;
  warnings?: string[];
  content: string;
  metadata?: {
    pages?: number;
    hasTextLayer?: boolean;
    imageOnly?: boolean;
    sheets?: string[];
    entries?: string[];
    slides?: Array<{
      index: number;
      title?: string;
      text: string;
      bullets: string[];
      notes?: string;
    }>;
    renderer?: ArtifactRenderer;
    editableKind?: ArtifactEditableKind;
    warnings?: string[];
    plainText?: string;
    wordCount?: number;
    totalChars?: number;
  };
  truncated: boolean;
  editable: boolean;
  rawPreviewable: boolean;
}

type MarkdownViewMode = 'preview' | 'edit' | 'split';

// ======================== 工具函数 ========================

const MIME_FORMATS: Record<string, string> = {
  'application/pdf': 'pdf',
  'application/msword': 'doc',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
  'application/vnd.ms-powerpoint': 'ppt',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
  'application/vnd.ms-excel': 'xls',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
  'text/csv': 'csv',
  'text/markdown': 'markdown',
  'text/html': 'html',
  'image/svg+xml': 'svg',
};

const IMAGE_FORMATS = new Set(['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'bmp']);
const RASTER_IMAGE_FORMATS = new Set(['png', 'jpg', 'jpeg', 'gif', 'webp', 'bmp']);
const TABLE_FORMATS = new Set(['csv', 'xlsx', 'xls']);
const ARCHIVE_FORMATS = new Set(['zip', 'tar', 'gzip']);
const VIDEO_FORMATS = new Set(['video', 'mp4', 'webm', 'mov']);
const AUDIO_FORMATS = new Set(['audio', 'mp3', 'wav']);
const RAW_PREVIEWABLE_FORMATS = new Set(['pdf', 'png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'bmp', 'audio', 'video', 'mp4', 'webm', 'mov']);
const PREVIEW_TOGGLE_HIDDEN_FORMATS = new Set(['pdf', 'png', 'jpg', 'jpeg', 'gif', 'webp', 'bmp', 'svg', 'video', 'audio']);
const SPREADSHEET_FORMATS = new Set(['xlsx', 'xls']);
const PREVIEW_STATUS_VALUES = new Set<ArtifactPreviewStatus>(['ok', 'partial', 'parse_error']);
const TRUST_LEVEL_VALUES = new Set<ArtifactTrustLevel>(['raw', 'rich_text', 'structure', 'table', 'text', 'untrusted']);
const RENDERER_VALUES = new Set<ArtifactRenderer>(['text', 'html', 'pptx-structure', 'xlsx-table', 'raw']);
const EDITABLE_KIND_VALUES = new Set<ArtifactEditableKind>(['text', 'markdown', 'html', 'office-native', 'read-only']);

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback;
}

function readOptionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function readNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function readOptionalNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function readBoolean(value: unknown, fallback = false): boolean {
  return typeof value === 'boolean' ? value : fallback;
}

function readOptionalBoolean(value: unknown): boolean | undefined {
  return typeof value === 'boolean' ? value : undefined;
}

function readStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : [];
}

function parseOneOf<T extends string>(value: unknown, allowed: ReadonlySet<T>): T | undefined {
  return typeof value === 'string' && allowed.has(value as T) ? value as T : undefined;
}

function formatFileSize(size?: number): string {
  if (typeof size !== 'number' || !Number.isFinite(size)) return '';
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  if (size < 1024 * 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`;
  return `${(size / 1024 / 1024 / 1024).toFixed(1)} GB`;
}

function normalizeFormat(value?: string): string {
  const raw = (value || '').toLowerCase();
  const compact = raw.replace(/^\./, '').split(';')[0].trim();
  if (MIME_FORMATS[compact]) return MIME_FORMATS[compact];
  const slashIndex = compact.indexOf('/');
  if (slashIndex >= 0) {
    const subtype = compact.slice(slashIndex + 1) || compact;
    return subtype.replace(/^x-/, '');
  }
  return compact;
}

function iconFor(format?: string) {
  const normalized = normalizeFormat(format);
  if (IMAGE_FORMATS.has(normalized)) return <ImageIcon size={18} />;
  if (TABLE_FORMATS.has(normalized)) return <Table size={18} />;
  if (ARCHIVE_FORMATS.has(normalized)) return <Archive size={18} />;
  return <FileText size={18} />;
}

function isImage(format: string): boolean {
  return RASTER_IMAGE_FORMATS.has(normalizeFormat(format));
}
function isVideo(format: string): boolean {
  return VIDEO_FORMATS.has(normalizeFormat(format));
}
function isAudio(format: string): boolean {
  return AUDIO_FORMATS.has(normalizeFormat(format));
}
function isRawPreviewableFormat(format: string): boolean {
  return RAW_PREVIEWABLE_FORMATS.has(normalizeFormat(format));
}
function hidesPreviewModeToggle(format?: string): boolean {
  return PREVIEW_TOGGLE_HIDDEN_FORMATS.has(normalizeFormat(format));
}
function isMarkdown(name: string, format?: string): boolean {
  const normalized = normalizeFormat(format);
  return normalized === 'markdown' || /\.(md|markdown)$/i.test(name);
}

/** 根据文件扩展名推断 Monaco language id */
function monacoLanguage(filename: string): string {
  const ext = filename.split('.').pop()?.toLowerCase() ?? '';
  const map: Record<string, string> = {
    ts: 'typescript', tsx: 'typescript', js: 'javascript', jsx: 'javascript',
    py: 'python', go: 'go', rs: 'rust', java: 'java',
    c: 'c', h: 'c', cpp: 'cpp', hpp: 'cpp',
    cs: 'csharp', rb: 'ruby', php: 'php', swift: 'swift', kt: 'kotlin',
    json: 'json', jsonl: 'json', yaml: 'yaml', yml: 'yaml', toml: 'ini',
    xml: 'xml', html: 'html', htm: 'html', css: 'css', scss: 'scss', less: 'less',
    sh: 'shell', bash: 'shell', zsh: 'shell',
    sql: 'sql', md: 'markdown', markdown: 'markdown',
    dockerfile: 'dockerfile', makefile: 'makefile',
    env: 'ini', gitignore: 'ini', txt: 'plaintext',
  };
  return map[ext] ?? 'plaintext';
}

/**
 * 正确解析 CSV 行（处理带逗号的引号单元格）
 */
function parseCsvRow(line: string): string[] {
  const cells: string[] = [];
  let i = 0;
  while (i < line.length) {
    if (line[i] === '"') {
      let cell = '';
      i++; // skip opening quote
      while (i < line.length) {
        if (line[i] === '"' && line[i + 1] === '"') { cell += '"'; i += 2; }
        else if (line[i] === '"') { i++; break; }
        else { cell += line[i++]; }
      }
      cells.push(cell);
      if (line[i] === ',') i++;
    } else {
      const end = line.indexOf(',', i);
      if (end === -1) { cells.push(line.slice(i)); break; }
      cells.push(line.slice(i, end));
      i = end + 1;
    }
  }
  return cells;
}

function parseCsvTable(csv: string, maxRows = 500): { headers: string[]; rows: string[][]; total: number; truncated: boolean } {
  const lines = csv.split(/\r?\n/).filter(l => l.trim());
  const total = lines.length;
  const slice = lines.slice(0, maxRows + 1);
  const parsed = slice.map(parseCsvRow);
  const headers = parsed[0] ?? [];
  const rows = parsed.slice(1);
  return { headers, rows, total, truncated: total > maxRows };
}

function warningText(warning: string, t: (key: string, options?: Record<string, unknown>) => string): string {
  const known: Record<string, string> = {
    'preview-truncated': t('artifact.preview.warning.truncated'),
    'pptx-structure-preview-not-layout-faithful': t('artifact.preview.warning.pptxStructure'),
    'table-preview-omits-formulas-styles': t('artifact.preview.warning.tableStructure'),
    'office-binary-readonly': t('artifact.preview.warning.officeBinary'),
  };
  if (known[warning]) return known[warning];
  if (warning.startsWith('xlsx-sheet-not-found:')) {
    return t('artifact.preview.warning.sheetMissing', { name: warning.slice('xlsx-sheet-not-found:'.length) });
  }
  return warning;
}

function parsePreviewSlide(value: unknown): NonNullable<NonNullable<ArtifactPreview['metadata']>['slides']>[number] | null {
  if (!isRecord(value)) return null;
  return {
    index: readNumber(value.index),
    title: readOptionalString(value.title),
    text: readString(value.text),
    bullets: readStringArray(value.bullets),
    notes: readOptionalString(value.notes),
  };
}

function parsePreviewMetadata(value: unknown): ArtifactPreview['metadata'] | undefined {
  if (!isRecord(value)) return undefined;
  const slides = Array.isArray(value.slides)
    ? value.slides.flatMap((item) => {
      const parsed = parsePreviewSlide(item);
      return parsed ? [parsed] : [];
    })
    : undefined;
  return {
    pages: readOptionalNumber(value.pages),
    hasTextLayer: readOptionalBoolean(value.hasTextLayer),
    imageOnly: readOptionalBoolean(value.imageOnly),
    sheets: readStringArray(value.sheets),
    entries: readStringArray(value.entries),
    slides,
    renderer: parseOneOf(value.renderer, RENDERER_VALUES),
    editableKind: parseOneOf(value.editableKind, EDITABLE_KIND_VALUES),
    warnings: readStringArray(value.warnings),
    plainText: readOptionalString(value.plainText),
    wordCount: readOptionalNumber(value.wordCount),
    totalChars: readOptionalNumber(value.totalChars),
  };
}

function parseArtifactPreview(value: unknown): ArtifactPreview {
  const record = isRecord(value) ? value : {};
  const mimeType = readString(record.mimeType);
  const format = normalizeFormat(readString(record.format, mimeType));
  const path = readString(record.path);
  return {
    path,
    name: readString(record.name, path),
    size: readNumber(record.size),
    mimeType,
    format,
    status: parseOneOf(record.status, PREVIEW_STATUS_VALUES),
    trustLevel: parseOneOf(record.trustLevel, TRUST_LEVEL_VALUES),
    warnings: readStringArray(record.warnings),
    content: readString(record.content),
    metadata: parsePreviewMetadata(record.metadata),
    truncated: readBoolean(record.truncated),
    editable: readBoolean(record.editable),
    rawPreviewable: readBoolean(record.rawPreviewable, isRawPreviewableFormat(format)),
  };
}

function artifactErrorMessage(value: unknown): string | null {
  if (!isRecord(value)) return null;
  const error = readOptionalString(value.error);
  if (error) return error;
  return readStringArray(value.warnings)[0] ?? null;
}

function PreviewWarningStrip({ preview, extra }: { preview: ArtifactPreview | null; extra?: string | null }) {
  const { t } = useTranslation();
  const warnings = [
    ...(preview?.status === 'parse_error' ? [t('artifact.preview.parseError')] : []),
    ...(preview?.status === 'partial' ? [t('artifact.preview.partial')] : []),
    ...(preview?.warnings ?? []).map((warning) => warningText(warning, t)),
    ...(extra ? [extra] : []),
  ].filter(Boolean);
  const uniqueWarnings = Array.from(new Set(warnings));
  if (uniqueWarnings.length === 0) return null;
  return (
    <div className="shrink-0 border-b border-accent-yellow/25 bg-accent-yellow/10 px-3 py-2 text-[11px] text-accent-yellow">
      <div className="flex min-w-0 items-start gap-2">
        <AlertTriangle size={14} className="mt-0.5 shrink-0" />
        <div className="min-w-0 space-y-1">
          {uniqueWarnings.slice(0, 4).map((warning, index) => (
            <div key={`${warning}-${index}`} className="break-words">{warning}</div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ======================== XLSX / CSV 表格渲染组件 ========================

interface XlsxPreviewProps {
  preview: ArtifactPreview;
  content: string;
  onSheetChange: (sheetName: string) => void;
  currentSheet: string | null;
  loadingSheet: boolean;
  onCellComment?: (row: number, col: number, value: string, sheet: string) => void;
}

function XlsxPreview({ preview, content, onSheetChange, currentSheet, loadingSheet, onCellComment }: XlsxPreviewProps) {
  const { t } = useTranslation();
  const sheets = preview.metadata?.sheets ?? [];
  const { headers, rows, total, truncated } = useMemo(() => parseCsvTable(content), [content]);

  const activeSheet = currentSheet ?? sheets[0] ?? '';

  return (
    <div className="flex h-full flex-col min-h-0">
      {/* Sheet Tab 栏 */}
      {sheets.length > 0 && (
        <div className="shrink-0 flex items-center gap-1 overflow-x-auto border-b border-border-muted bg-bg-secondary/50 px-3 py-1.5">
          {sheets.map((name) => (
            <button
              key={name}
              onClick={() => onSheetChange(name)}
              disabled={loadingSheet}
              className={`shrink-0 rounded px-2.5 py-1 text-[11px] font-mono transition-colors ${
                name === activeSheet
                  ? 'bg-accent-brand/15 text-accent-brand'
                  : 'text-text-tertiary hover:text-text-secondary hover:bg-bg-hover'
              }`}
            >
              {name}
            </button>
          ))}
          {loadingSheet && <Loader2 size={12} className="animate-spin text-text-tertiary ml-2 shrink-0" />}
        </div>
      )}

      {/* 表格区 */}
      <div className="min-h-0 flex-1 overflow-auto">
        {headers.length === 0 ? (
          <div className="flex h-full items-center justify-center text-xs text-text-tertiary">{t('artifact.table.empty')}</div>
        ) : (
          <table className="min-w-full border-collapse text-xs">
            <thead className="sticky top-0 z-10">
              <tr className="bg-bg-secondary">
                {/* 行号列 */}
                <th className="w-10 border border-border-muted px-2 py-1.5 text-right text-[10px] font-mono text-text-tertiary select-none">#</th>
                {headers.map((h, ci) => (
                  <th
                    key={ci}
                    className="min-w-[80px] max-w-[260px] border border-border-muted px-2 py-1.5 text-left font-semibold text-text-primary whitespace-nowrap"
                  >
                    {h || String.fromCharCode(65 + ci)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, ri) => (
                <tr key={ri} className="hover:bg-bg-hover/50">
                  <td className="border border-border-muted px-2 py-1 text-right text-[10px] font-mono text-text-tertiary select-none">{ri + 1}</td>
                  {headers.map((_, ci) => (
                    <td
                      key={ci}
                      className="max-w-[260px] truncate border border-border-muted px-2 py-1 text-text-secondary cursor-cell"
                      title={row[ci] ?? ''}
                      onClick={() => onCellComment?.(ri + 1, ci + 1, row[ci] ?? '', activeSheet)}
                    >
                      {row[ci] ?? ''}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* 底部信息 */}
      <div className="shrink-0 border-t border-border-muted bg-bg-secondary/50 px-3 py-1 text-[10px] text-text-tertiary flex items-center gap-3">
        <span>{t('artifact.xlsx.rows', { count: rows.length })}</span>
        {truncated && <span className="text-accent-yellow">{t('artifact.xlsx.tooManyRows', { limit: 500 })}</span>}
        {sheets.length > 0 && <span className="ml-auto">{t('artifact.xlsx.sheet', { name: activeSheet })}</span>}
      </div>
    </div>
  );
}

// ======================== SVG 内联渲染组件 ========================

function SvgPreview({ content, name }: { content: string; name: string }) {
  const isSvg = useMemo(() => {
    const trimmed = content.trim();
    return trimmed.startsWith('<svg') || trimmed.startsWith('<?xml');
  }, [content]);

  if (!isSvg) {
    return <div className="flex h-full items-center justify-center p-4"><img src={content} alt={name} className="max-h-full max-w-full rounded" /></div>;
  }

  return (
    <div className="h-full overflow-auto p-4 flex items-center justify-center">
      <div
        className="max-w-full max-h-full [&_svg]:max-w-full [&_svg]:h-auto"
        // eslint-disable-next-line react/no-danger
        dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(content, { USE_PROFILES: { svg: true, svgFilters: true } }) }}
      />
    </div>
  );
}

// ======================== Markdown 编辑/预览组件 ========================

function MarkdownEditor({
  content,
  onChange,
  viewMode,
  onMount,
}: {
  content: string;
  onChange: (v: string) => void;
  viewMode: MarkdownViewMode;
  onMount?: (editor: Parameters<NonNullable<Parameters<typeof MonacoEditor>[0]['onMount']>>[0]) => void;
}) {
  const { t } = useTranslation();
  const editorNode = (
    <Suspense fallback={<div className="flex h-full items-center justify-center text-xs text-text-tertiary"><Loader2 size={14} className="animate-spin mr-1" />{t('artifact.loading.editor')}</div>}>
      <MonacoEditor
        height="100%"
        language="markdown"
        value={content}
        onChange={(v) => onChange(v ?? '')}
        onMount={onMount}
        theme="vs-dark"
        options={{
          minimap: { enabled: false },
          fontSize: 13,
          lineHeight: 22,
          wordWrap: 'on',
          scrollBeyondLastLine: false,
          renderLineHighlight: 'gutter',
          padding: { top: 12, bottom: 12 },
        }}
      />
    </Suspense>
  );

  const previewNode = (
    <div className="artifact-prose h-full overflow-auto px-6 py-4 text-sm text-text-secondary">
      <SafeMarkdown>{content}</SafeMarkdown>
    </div>
  );

  if (viewMode === 'edit') return editorNode;
  if (viewMode === 'preview') return previewNode;

  // split
  return (
    <div className="flex h-full min-w-0">
      <div className="w-1/2 min-w-0 border-r border-border-muted">{editorNode}</div>
      <div className="w-1/2 min-w-0">{previewNode}</div>
    </div>
  );
}

// ======================== 代码文件 Monaco 编辑器 ========================

function CodeEditor({ content, onChange, language, onMount }: { content: string; onChange: (v: string) => void; language: string; onMount?: (editor: Parameters<NonNullable<Parameters<typeof MonacoEditor>[0]['onMount']>>[0]) => void; }) {
  const { t } = useTranslation();
  return (
    <Suspense fallback={<div className="flex h-full items-center justify-center text-xs text-text-tertiary"><Loader2 size={14} className="animate-spin mr-1" />{t('artifact.loading.editor')}</div>}>
      <MonacoEditor
        height="100%"
        language={language}
        value={content}
        onChange={(v) => onChange(v ?? '')}
        onMount={onMount}
        theme="vs-dark"
        options={{
          minimap: { enabled: false },
          fontSize: 13,
          lineHeight: 20,
          scrollBeyondLastLine: false,
          renderLineHighlight: 'gutter',
          padding: { top: 12, bottom: 12 },
        }}
      />
    </Suspense>
  );
}

// ======================== 图片渲染（含元数据） ========================

function ImagePreview({ rawUrl, preview }: { rawUrl: string; preview: ArtifactPreview }) {
  return (
    <div className="h-full overflow-auto p-4 flex flex-col items-center gap-3">
      <img src={rawUrl} alt={preview.name} className="max-w-full rounded border border-border-muted bg-bg-secondary object-contain" />
      <div className="flex items-center gap-3 text-[10px] text-text-tertiary font-mono">
        <span>{preview.format.toUpperCase()}</span>
        <span>{formatFileSize(preview.size)}</span>
      </div>
    </div>
  );
}

function DocxPreview({ html, onTextSelect }: { html: string; onTextSelect?: (text: string) => void }) {
  const { t } = useTranslation();
  const sanitized = useMemo(() => DOMPurify.sanitize(html, {
    USE_PROFILES: { html: true },
    FORBID_ATTR: ['style'],
  }), [html]);
  return (
    <div className="artifact-office-scroll">
      <div className="artifact-office-page">
        <div className="mb-4 inline-flex rounded border border-border-muted bg-bg-secondary px-2 py-1 text-[10px] text-text-tertiary">
          {t('artifact.docx.htmlRenderNotice')}
        </div>
        <div
          className="artifact-docx-prose"
          onMouseUp={() => {
            if (!onTextSelect) return;
            const sel = window.getSelection();
            const text = sel?.toString().trim();
            if (text) onTextSelect(text);
          }}
          // eslint-disable-next-line react/no-danger
          dangerouslySetInnerHTML={{ __html: sanitized }}
        />
      </div>
    </div>
  );
}

function OfficePdfPreview({
  renderUrl,
  preview,
  fallback,
}: {
  renderUrl: string;
  preview: ArtifactPreview;
  fallback: React.ReactNode;
}) {
  const { t } = useTranslation();
  const [state, setState] = useState<'loading' | 'ready' | 'failed'>('loading');
  const [blobUrl, setBlobUrl] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    let objectUrl: string | null = null;
    setState('loading');
    setBlobUrl(null);

    (async () => {
      try {
        const res = await fetch(renderUrl, { headers: apiHeaders() });
        if (!res.ok) {
          // 503 LibreOffice 不可用 / 422 转换失败 → 回退结构预览
          if (!cancelled) setState('failed');
          return;
        }
        const blob = await res.blob();
        if (cancelled) return;
        objectUrl = URL.createObjectURL(blob);
        setBlobUrl(objectUrl);
        setState('ready');
      } catch {
        if (!cancelled) setState('failed');
      }
    })();

    return () => {
      cancelled = true;
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [renderUrl]);

  if (state === 'loading') {
    return (
      <div className="flex h-full items-center justify-center text-xs text-text-tertiary">
        {t('artifact.office.rendering')}
      </div>
    );
  }

  if (state === 'failed' || !blobUrl) {
    return <>{fallback}</>;
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="shrink-0 border-b border-border-muted bg-bg-secondary/70 px-3 py-1.5 text-[11px] text-text-tertiary">
        {t('artifact.office.realLayout')}
      </div>
      <iframe src={blobUrl} className="min-h-0 flex-1 bg-bg-primary" title={preview.name} />
    </div>
  );
}

function PptxStructurePreview({ preview, content, onSlideComment }: { preview: ArtifactPreview; content: string; onSlideComment?: (slideIndex: number, slideTitle: string, slideText: string) => void }) {
  const { t } = useTranslation();
  const slides = preview.metadata?.slides ?? [];
  if (slides.length === 0) {
    return (
      <pre className="h-full overflow-auto whitespace-pre-wrap break-words px-4 py-3 font-mono text-xs leading-6 text-text-secondary">
        {content || t('artifact.pptx.structureFallback')}
      </pre>
    );
  }

  return (
    <div className="pptx-preview-container h-full overflow-auto bg-bg-secondary/45">
      <div className="w-full max-w-5xl text-[11px] text-text-tertiary">
        {t('artifact.pptx.renderNotice')} · {t('artifact.pptx.slides', { count: preview.metadata?.pages ?? slides.length })}
      </div>
      {slides.map((slide) => (
        <section
          key={slide.index}
          className={`artifact-slide ${onSlideComment ? 'cursor-pointer' : ''}`}
          onClick={() => onSlideComment?.(slide.index, slide.title || `Slide ${slide.index}`, slide.text)}
        >
          <div className="artifact-slide-rune">{String(slide.index).padStart(2, '0')}</div>
          <div className="artifact-slide-content">
            <h2>{slide.title || t('artifact.pptx.untitledSlide', { index: slide.index })}</h2>
            {slide.bullets.length > 0 ? (
              <ul>
                {slide.bullets.map((bullet, index) => <li key={index}>{bullet}</li>)}
              </ul>
            ) : (
              <p>{slide.text}</p>
            )}
          </div>
        </section>
      ))}
    </div>
  );
}

// ======================== 主组件 ========================
// ======================== Workspace 文件树 ========================

interface WsFsEntry {
  name: string;
  path: string;
  type: 'file' | 'directory';
  size?: number;
  children?: WsFsEntry[];
  loaded?: boolean;
}

async function wsFetch<T>(path: string, opts?: RequestInit): Promise<T> {
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

function WsFileTree({ sessionId, onOpenFile }: { sessionId: string | null; onOpenFile: (path: string, name: string) => void }) {
  const serverCwd = useSessionStore((s) => s.serverCwd);
  const sessions = useSessionStore((s) => s.sessions);
  const workspace = sessions.find(s => s.id === sessionId)?.workspace || serverCwd || '.';
  const [tree, setTree] = useState<WsFsEntry[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [activePath, setActivePath] = useState<string | null>(null);

  const fetchTree = useCallback(async (dirPath?: string) => {
    setLoading(true);
    try {
      const data = await wsFetch<{ entries: WsFsEntry[] }>('/fs/list', {
        method: 'POST',
        body: JSON.stringify({ path: dirPath || workspace, sessionId }),
      });
      if (dirPath) {
        setTree(prev => mergeWsChildren(prev, dirPath, data.entries || []));
      } else {
        setTree(data.entries || []);
      }
    } catch { /* silent */ } finally { setLoading(false); }
  }, [workspace, sessionId]);

  useEffect(() => { fetchTree(); }, [fetchTree]);

  const toggleDir = useCallback(async (node: WsFsEntry) => {
    const next = new Set(expanded);
    if (next.has(node.path)) { next.delete(node.path); }
    else { next.add(node.path); if (!node.loaded) await fetchTree(node.path); }
    setExpanded(next);
  }, [expanded, fetchTree]);

  const handleFileClick = (node: WsFsEntry) => {
    setActivePath(node.path);
    onOpenFile(node.path, node.name);
  };

  if (loading && tree.length === 0) return <div className="flex justify-center py-4"><Loader2 size={13} className="animate-spin text-text-tertiary" /></div>;
  if (tree.length === 0) return <div className="px-2 py-3 text-center text-[10px] text-text-tertiary">无文件</div>;

  return <WsTreeNodes nodes={tree} expanded={expanded} activePath={activePath} onToggleDir={toggleDir} onFileClick={handleFileClick} depth={0} />;
}

function mergeWsChildren(tree: WsFsEntry[], parentPath: string, children: WsFsEntry[]): WsFsEntry[] {
  return tree.map(e => e.path === parentPath && e.type === 'directory'
    ? { ...e, children, loaded: true }
    : e.children ? { ...e, children: mergeWsChildren(e.children, parentPath, children) } : e
  );
}

function WsTreeNodes({ nodes, expanded, activePath, onToggleDir, onFileClick, depth }: {
  nodes: WsFsEntry[]; expanded: Set<string>; activePath: string | null;
  onToggleDir: (n: WsFsEntry) => void; onFileClick: (n: WsFsEntry) => void; depth: number;
}) {
  return <>{nodes.map(node => {
    const isExp = expanded.has(node.path);
    const isDir = node.type === 'directory';
    const ext = node.name.split('.').pop()?.toLowerCase() || '';
    return <div key={node.path}>
      <button
        onClick={() => isDir ? onToggleDir(node) : onFileClick(node)}
        className={`w-full flex items-center gap-1 px-1.5 py-0.5 text-[11px] hover:bg-bg-hover text-left ${activePath === node.path ? 'bg-accent-brand/10 text-accent-brand' : 'text-text-secondary'}`}
        style={{ paddingLeft: `${depth * 10 + 4}px` }}
      >
        {isDir ? (
          <>
            {isExp ? <ChevronDown size={9} className="shrink-0" /> : <ChevronRight size={9} className="shrink-0" />}
            {isExp ? <FolderOpen size={11} className="text-accent-brand/60 shrink-0" /> : <Folder size={11} className="text-accent-brand/60 shrink-0" />}
          </>
        ) : (
          <>
            <span className="w-[9px] shrink-0" />
            {['ts','tsx','js','jsx','json','py'].includes(ext) ? <FileCode size={11} className="text-accent-blue/60 shrink-0" /> :
             ['html','htm','svg','xml'].includes(ext) ? <FileCode size={11} className="text-accent-orange/60 shrink-0" /> :
             ['png','jpg','jpeg','gif','webp','ico'].includes(ext) ? <ImageIcon size={11} className="text-accent-green/60 shrink-0" /> :
             ['md','txt','log'].includes(ext) ? <FileText size={11} className="text-text-tertiary shrink-0" /> :
             <Archive size={11} className="text-text-tertiary shrink-0" />}
          </>
        )}
        <span className="truncate">{node.name}</span>
      </button>
      {isDir && isExp && node.children && <WsTreeNodes nodes={node.children} expanded={expanded} activePath={activePath} onToggleDir={onToggleDir} onFileClick={onFileClick} depth={depth + 1} />}
    </div>;
  })}</>;
}



export default function ArtifactView({ defaultCanvasMode = false, embedded = false }: { defaultCanvasMode?: boolean; embedded?: boolean } = {}) {
  const { t } = useTranslation();
  const sessionId = useSessionStore((s) => s.sessionId || s.activeSessionId);
  const activeArtifact = useArtifactStore((s) => s.activeArtifact);
  const recentArtifacts = useArtifactStore((s) => s.recentArtifacts);
  const openArtifact = useArtifactStore((s) => s.openArtifact);
  const clearArtifact = useArtifactStore((s) => s.clearArtifact);

  const [preview, setPreview] = useState<ArtifactPreview | null>(null);
  const [content, setContent] = useState('');
  const [mode, setMode] = useState<'preview' | 'full'>('preview');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);
  const [sheetError, setSheetError] = useState<string | null>(null);
  const previewRequestSeq = useRef(0);
  const sheetRequestSeq = useRef(0);

  // XLSX 多 Sheet 状态
  const [selectedSheet, setSelectedSheet] = useState<string | null>(null);
  const [loadingSheet, setLoadingSheet] = useState(false);

  // Markdown 视图模式
  const [mdViewMode, setMdViewMode] = useState<MarkdownViewMode>('preview');

  // ── 评论模式 ──
  const [commentMode, setCommentMode] = useState(false);
  const [commentContext, setCommentContext] = useState<CommentContext | null>(null);
  const monacoEditorRef = useRef<Parameters<NonNullable<Parameters<typeof MonacoEditor>[0]['onMount']>>[0] | null>(null);

  // ── 剑阁可交互 Canvas 模式 ──
  const [canvasMode, setCanvasMode] = useState(defaultCanvasMode);
  const canvas = useCanvasArtifactStore();
  // 当前产物的规范化 artifactId（用于 Canvas 后端寻址）
  const canvasArtifactId = useMemo(() => {
    return preview?.path ? artifactIdFromPath(preview.path) : null;
  }, [preview?.path]);
  // 是否为 HTML 产物（剑阁选区拾取目前覆盖 HTML 成品）
  const isHtmlArtifact = normalizeFormat(preview?.format) === 'html';

  const rawUrl = useMemo(() => {
    if (!preview?.path) return activeArtifact?.url || '';
    const params = new URLSearchParams({ path: preview.path, token: getServerToken() });
    if (sessionId) params.set('sessionId', sessionId);
    return `/api/v1/artifacts/raw?${params.toString()}`;
  }, [activeArtifact?.url, preview?.path, sessionId]);

  // Office 真实版式渲染地址：PPTX/DOCX/XLSX → LibreOffice → PDF，复用 PDF iframe 渲染。
  const renderUrl = useMemo(() => {
    if (!preview?.path) return '';
    const params = new URLSearchParams({ path: preview.path, token: getServerToken() });
    if (sessionId) params.set('sessionId', sessionId);
    return `/api/v1/artifacts/render?${params.toString()}`;
  }, [preview?.path, sessionId]);

  // 剑阁模式开启 + HTML 产物时，载入该产物的 Canvas 状态（sourcemap/版本栈/批注）。
  const loadArtifactCanvas = canvas.loadArtifact;
  useEffect(() => {
    if (canvasMode && isHtmlArtifact && canvasArtifactId) {
      void loadArtifactCanvas(canvasArtifactId);
    } else {
      void loadArtifactCanvas(null);
    }
  }, [canvasMode, isHtmlArtifact, canvasArtifactId, loadArtifactCanvas]);

  // 非 HTML 产物自动退出剑阁模式（选区拾取目前只覆盖 HTML 成品）。
  useEffect(() => {
    if (canvasMode && preview && !isHtmlArtifact) {
      setCanvasMode(false);
    }
  }, [canvasMode, preview, isHtmlArtifact]);

  const downloadPreview = useCallback(async () => {
    if (!preview || !rawUrl) return;
    const res = await fetch(rawUrl, { headers: apiHeaders() });
    if (!res.ok) throw new Error(t('artifact.error.downloadFailed', { status: res.status }));
    const blob = await res.blob();
    const objectUrl = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = objectUrl;
    a.download = preview.name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(objectUrl);
  }, [preview, rawUrl, t]);

  const loadPreview = useCallback(async (artifact: ArtifactTarget | null, nextMode = mode, sheet?: string) => {
    const requestId = ++previewRequestSeq.current;
    if (!artifact?.path) {
      if (artifact?.url) {
        const format = normalizeFormat(artifact.mimeType || artifact.name.split('.').pop());
        setPreview({
          path: '',
          name: artifact.name,
          size: artifact.size || 0,
          mimeType: artifact.mimeType || '',
          format,
          status: 'ok',
          trustLevel: 'raw',
          content: '',
          truncated: false,
          editable: false,
          rawPreviewable: isRawPreviewableFormat(format),
        });
        setContent('');
        setError(null);
      } else {
        setPreview(null);
        setContent('');
        setError(artifact ? t('artifact.error.noPath') : null);
      }
      return;
    }

    setLoading(true);
    setError(null);
    setSheetError(null);
    setSaveMessage(null);
    try {
      const params = new URLSearchParams({ path: artifact.path, mode: nextMode });
      if (sessionId) params.set('sessionId', sessionId);
      if (sheet) { params.set('mode', 'sheet'); params.set('sheet', sheet); }
      const res = await fetch(`/api/v1/artifacts/preview?${params.toString()}`, { headers: apiHeaders() });
      const json: unknown = await res.json().catch(() => ({}));
      if (requestId !== previewRequestSeq.current) return;
      if (!res.ok) throw new Error(artifactErrorMessage(json) || t('artifact.error.previewFailed'));
      const nextPreview = parseArtifactPreview(json);
      setPreview(nextPreview);
      setContent(nextPreview.content);
      // 初始化选中 Sheet
      if (!sheet && nextPreview.metadata?.sheets?.length) {
        setSelectedSheet(nextPreview.metadata.sheets[0]);
      }
    } catch (err) {
      if (requestId !== previewRequestSeq.current) return;
      setPreview(null);
      setContent('');
      setError(err instanceof Error ? err.message : t('artifact.error.previewFailed'));
    } finally {
      if (requestId === previewRequestSeq.current) setLoading(false);
    }
  }, [mode, sessionId, t]);

  useEffect(() => {
    setSelectedSheet(null);
    setMdViewMode('preview');
    void loadPreview(activeArtifact, mode);
  }, [activeArtifact?.path, activeArtifact?.url, activeArtifact?.name, activeArtifact?.mimeType, sessionId]);

  const handleSheetChange = async (sheetName: string) => {
    if (!activeArtifact?.path || sheetName === selectedSheet) return;
    const requestId = ++sheetRequestSeq.current;
    setLoadingSheet(true);
    setSheetError(null);
    try {
      const params = new URLSearchParams({
        path: activeArtifact.path,
        mode: 'sheet',
        sheet: sheetName,
      });
      if (sessionId) params.set('sessionId', sessionId);
      const res = await fetch(`/api/v1/artifacts/preview?${params.toString()}`, { headers: apiHeaders() });
      const json: unknown = await res.json().catch(() => ({}));
      if (requestId !== sheetRequestSeq.current) return;
      const nextPreview = parseArtifactPreview(json);
      if (!res.ok || nextPreview.status === 'parse_error') throw new Error(artifactErrorMessage(json) || t('artifact.error.previewFailed'));
      setSelectedSheet(sheetName);
      setContent(nextPreview.content);
      setPreview(nextPreview);
    } catch (err) {
      if (requestId === sheetRequestSeq.current) {
        setSheetError(err instanceof Error ? err.message : t('artifact.error.previewFailed'));
      }
    } finally {
      if (requestId === sheetRequestSeq.current) setLoadingSheet(false);
    }
  };

  const save = async () => {
    if (!preview?.editable) return;
    setSaving(true);
    setError(null);
    setSaveMessage(null);
    try {
      const res = await fetch('/api/v1/artifacts/save', {
        method: 'PUT',
        headers: apiHeaders({ 'content-type': 'application/json' }),
        body: JSON.stringify({ path: preview.path, sessionId, content }),
      });
      const json: unknown = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(artifactErrorMessage(json) || t('artifact.error.saveFailed'));
      const size = isRecord(json) ? readOptionalNumber(json.size) : undefined;
      setSaveMessage(t('artifact.status.saved', { size: formatFileSize(size) }));
      await loadPreview(activeArtifact, mode);
    } catch (err) {
      setError(err instanceof Error ? err.message : t('artifact.error.saveFailed'));
    } finally {
      setSaving(false);
    }
  };

  const switchMode = (nextMode: 'preview' | 'full') => {
    setMode(nextMode);
    void loadPreview(activeArtifact, nextMode);
  };

  // ── 评论相关逻辑 ──

  /** 从 Monaco editor 获取选区上下文 */
  const getMonacoSelection = (): CommentContext | null => {
    const editor = monacoEditorRef.current;
    if (!editor || !preview?.path) return null;
    const selection = editor.getSelection();
    if (!selection) return null;
    const startLine = selection.startLineNumber;
    const endLine = selection.endLineNumber;
    const selectedText = editor.getModel()?.getValueInRange(selection) ?? '';
    // 如果没有选中文本，取当前行内容
    const finalText = selectedText || (editor.getModel()?.getLineContent(startLine) ?? '');
    return {
      filePath: preview.path,
      format: preview.format,
      selectionType: 'line',
      selectedText: finalText || undefined,
      startLine,
      endLine: endLine > startLine ? endLine : undefined,
    };
  };

  /** 发送评论给 Leader */
  const sendCommentToLeader = async (comment: string, ctx: CommentContext) => {
    // 构建结构化消息
    const parts: string[] = [];
    parts.push(`[源码评论] 文件: ${ctx.filePath}`);
    if (ctx.format) parts.push(`格式: ${ctx.format}`);
    if (ctx.selectionType === 'line' && ctx.startLine) {
      const range = ctx.endLine ? `L${ctx.startLine}-${ctx.endLine}` : `L${ctx.startLine}`;
      parts.push(`位置: ${range}`);
    }
    if (ctx.sheet) parts.push(`Sheet: ${ctx.sheet}`);
    if (ctx.slideIndex !== undefined) parts.push(`Slide: ${ctx.slideIndex}`);
    if (ctx.row !== undefined && ctx.col !== undefined) parts.push(`单元格: R${ctx.row}C${ctx.col}`);
    if (ctx.selectedText) {
      parts.push(`选中内容:`);
      parts.push('```');
      parts.push(ctx.selectedText);
      parts.push('```');
    }
    parts.push('');
    parts.push(`评论: ${comment}`);
    parts.push('');
    parts.push('请根据以上上下文和评论，对该文件进行源码级修改。');

    const message = parts.join('\n');
    await acpClient.sendJsonRpc('session/prompt', { prompt: message });
  };

  /** 处理评论按钮点击 */
  const handleCommentAction = () => {
    if (!preview?.path) return;
    // 切换评论模式
    const nextMode = !commentMode;
    setCommentMode(nextMode);
    if (nextMode) {
      // 对于 Monaco 编辑器，尝试立即获取选区
      const monacoSel = getMonacoSelection();
      if (monacoSel && monacoSel.selectedText) {
        setCommentContext(monacoSel);
      }
      // 其他渲染器等用户选中/点击后再弹出
    } else {
      setCommentContext(null);
    }
  };

  /** 处理 Monaco editor mount */
  const handleEditorMount = (editor: Parameters<NonNullable<Parameters<typeof MonacoEditor>[0]['onMount']>>[0]) => {
    monacoEditorRef.current = editor;
  };

  // SVG: 从后端拿到内容，如果 editable=false 且是 svg 格式，从 content 渲染
  const isSvgFormat = normalizeFormat(preview?.format) === 'svg';

  // Markdown 判断（editable 文本文件 + .md 扩展名）
  const isMarkdownFile = preview ? isMarkdown(preview.name, preview.format) : false;

  const renderContent = () => {
    if (loading) {
      return (
        <div className="h-full flex items-center justify-center text-text-tertiary">
          <Loader2 className="animate-spin mr-2" size={16} />{t('artifact.status.loading')}
        </div>
      );
    }
    if (error) {
      return <div className="m-4 rounded-lg border border-accent-red/30 bg-accent-red/5 p-4 text-sm text-accent-red">{error}</div>;
    }
    if (!preview) {
      return (
        <div className="h-full flex flex-col items-center justify-center text-center text-text-tertiary">
          <Eye size={28} className="mb-3 opacity-70" />
          <div className="text-sm text-text-secondary">{t('artifact.empty.title')}</div>
          <div className="mt-1 text-xs">{t('artifact.empty.subtitle')}</div>
        </div>
      );
    }

    // ── PDF ──
    const format = normalizeFormat(preview.format);

    if (preview.rawPreviewable && format === 'pdf') {
      return (
        <div className="flex h-full min-h-0 flex-col">
          <div className="shrink-0 border-b border-border-muted bg-bg-secondary/70 px-3 py-1.5 text-[11px] text-text-tertiary">
            {preview.metadata?.imageOnly
              ? t('artifact.pdf.imageOnly')
              : preview.metadata?.hasTextLayer
                ? t('artifact.pdf.textLayer')
                : t('artifact.pdf.unknownLayer')}
          </div>
          <iframe src={rawUrl} className="min-h-0 flex-1 bg-bg-primary" title={preview.name} />
        </div>
      );
    }

    // ── 图片（非 SVG）──
    if (preview.rawPreviewable && isImage(format)) {
      return <ImagePreview rawUrl={rawUrl} preview={preview} />;
    }

    // ── SVG 内联 ──
    if (isSvgFormat) {
      // SVG 原始内容由 parseText 返回（svg 命中 extmap → text，rawPreviewable=true）
      // 先尝试用 content，content 为空则 fallback 到 img
      return content
        ? <SvgPreview content={content} name={preview.name} />
        : <div className="h-full overflow-auto p-4 flex items-center justify-center"><img src={rawUrl} alt={preview.name} className="max-w-full max-h-full rounded border border-border-muted" /></div>;
    }

    // ── 视频 ──
    if (preview.rawPreviewable && isVideo(format)) {
      return (
        <div className="h-full flex items-center justify-center p-4">
          <video src={rawUrl} controls className="max-h-full max-w-full rounded border border-border-muted" />
        </div>
      );
    }

    // ── 音频 ──
    if (preview.rawPreviewable && isAudio(format)) {
      return (
        <div className="h-full flex items-center justify-center p-4">
          <audio src={rawUrl} controls className="w-full max-w-xl" />
        </div>
      );
    }

    // ── HTML：sandbox iframe 渲染，避免把完整页面当普通文本/消毒片段展示 ──
    if (format === 'html') {
      // 剑阁模式：same-origin iframe + 选区拾取；常规模式：sandbox iframe 纯展示。
      if (canvasMode) {
        return (
          <CanvasHtmlPreview
            src={rawUrl}
            title={preview.name}
            activeNodeId={canvas.selection?.nodeId}
            onPick={(sel) => canvas.setSelection(sel)}
          />
        );
      }
      return (
        <iframe
          src={rawUrl}
          className="h-full w-full bg-bg-primary"
          title={preview.name}
          sandbox="allow-forms allow-downloads"
          referrerPolicy="no-referrer"
        />
      );
    }

    // ── XLSX / XLS：LibreOffice → PDF 还原样式/公式/合并单元格/图表，失败回退交互表格 ──
    if (SPREADSHEET_FORMATS.has(format)) {
      const tableFallback = (
        <XlsxPreview
          preview={preview}
          content={content}
          onSheetChange={handleSheetChange}
          currentSheet={selectedSheet}
          loadingSheet={loadingSheet}
          onCellComment={commentMode && preview.path ? (row, col, value, sheet) => {
            setCommentContext({ filePath: preview.path, format: preview.format, selectionType: 'cell', selectedText: value, row, col, sheet });
          } : undefined}
        />
      );
      return renderUrl
        ? <OfficePdfPreview renderUrl={renderUrl} preview={preview} fallback={tableFallback} />
        : tableFallback;
    }

    // ── DOCX：LibreOffice → PDF 真实版式渲染，失败回退 mammoth 富文本 ──
    if (format === 'docx' && preview.metadata?.renderer === 'html') {
      const docxFallback = (
        <DocxPreview
          html={content}
          onTextSelect={commentMode && preview.path ? (text) => {
            setCommentContext({ filePath: preview.path, format: preview.format, selectionType: 'text', selectedText: text });
          } : undefined}
        />
      );
      return renderUrl
        ? <OfficePdfPreview renderUrl={renderUrl} preview={preview} fallback={docxFallback} />
        : docxFallback;
    }

    // ── PPTX：LibreOffice → PDF 真实版式渲染，失败回退 OOXML slide 结构 ──
    if (format === 'pptx') {
      const structureFallback = (
        <PptxStructurePreview
          preview={preview}
          content={content}
          onSlideComment={commentMode && preview.path ? (slideIndex, slideTitle, slideText) => {
            setCommentContext({ filePath: preview.path, format: preview.format, selectionType: 'slide', selectedText: slideText, slideIndex });
          } : undefined}
        />
      );
      return renderUrl
        ? <OfficePdfPreview renderUrl={renderUrl} preview={preview} fallback={structureFallback} />
        : structureFallback;
    }

    // ── CSV（纯 CSV 格式）──
    if (format === 'csv') {
      return (
        <XlsxPreview
          preview={preview}
          content={content}
          onSheetChange={handleSheetChange}
          currentSheet={selectedSheet}
          loadingSheet={loadingSheet}
          onCellComment={commentMode && preview.path ? (row, col, value, sheet) => {
            setCommentContext({ filePath: preview.path, format: preview.format, selectionType: 'cell', selectedText: value, row, col, sheet });
          } : undefined}
        />
      );
    }

    // ── Markdown 编辑/预览 ──
    if (isMarkdownFile && preview.editable) {
      return (
        <MarkdownEditor
          content={content}
          onChange={setContent}
          viewMode={mdViewMode}
          onMount={handleEditorMount}
        />
      );
    }

    // ── Markdown 只读 ──
    if (isMarkdownFile) {
      return (
        <div
          className="artifact-prose h-full overflow-auto px-6 py-4 text-sm text-text-secondary"
          onMouseUp={() => {
            if (!commentMode) return;
            const sel = window.getSelection();
            const text = sel?.toString().trim();
            if (text && preview?.path) {
              setCommentContext({ filePath: preview.path, format: preview.format, selectionType: 'text', selectedText: text });
            }
          }}
        >
          <SafeMarkdown>{content}</SafeMarkdown>
        </div>
      );
    }

    // ── 可编辑文本/代码文件 → Monaco ──
    if (preview.editable) {
      const lang = monacoLanguage(preview.name);
      return <CodeEditor content={content} onChange={setContent} language={lang} onMount={handleEditorMount} />;
    }

    // ── 纯文本回退 ──
    return (
      <pre
        className="h-full overflow-auto whitespace-pre-wrap break-words px-4 py-3 font-mono text-xs leading-6 text-text-secondary"
        onMouseUp={() => {
          if (!commentMode) return;
          const sel = window.getSelection();
          const text = sel?.toString().trim();
          if (text && preview?.path) {
            setCommentContext({ filePath: preview.path, format: preview.format, selectionType: 'text', selectedText: text });
          }
        }}
      >
        {content}
      </pre>
    );
  };

  // Markdown 视图模式切换按钮
  const renderMdModeButtons = () => {
    if (!isMarkdownFile || !preview?.editable) return null;
    return (
      <div className="flex items-center rounded border border-border-muted overflow-hidden">
        {([['edit', <Edit3 size={12} />, t('artifact.md.modeEdit')], ['preview', <Eye size={12} />, t('artifact.md.modePreview')], ['split', <Columns2 size={12} />, t('artifact.md.modeSplit')]] as const).map(([m, icon, label]) => (
          <button
            key={m}
            onClick={() => setMdViewMode(m as MarkdownViewMode)}
            className={`flex items-center gap-1 px-2 py-1 text-xs transition-colors ${mdViewMode === m ? 'bg-accent-brand/15 text-accent-brand' : 'text-text-secondary hover:text-text-primary'}`}
          >
            {icon}{label}
          </button>
        ))}
      </div>
    );
  };

  const hasUnsavedChanges = preview?.editable && content !== preview.content;

  return (
    <div className="flex h-full min-w-0 bg-bg-primary">
      {/* 左侧：workspace 文件树 + 最近文件。embedded（在 JiangeCanvas 内）时隐藏——
          JiangeCanvas 已持有唯一文件树，避免双树重复。 */}
      {!embedded && (
      <aside className="artifact-recent-pane w-56 shrink-0 border-r border-border-muted bg-bg-secondary/50 flex flex-col min-h-0">
        <div className="px-3 py-2 border-b border-border-muted">
          <div className="text-xs font-mono font-semibold text-text-primary">{t('artifact.title')}</div>
          <div className="mt-0.5 text-[10px] text-text-tertiary">{t('artifact.subtitle')}</div>
        </div>
        {/* Workspace 文件树 */}
        <div className="flex-1 min-h-0 overflow-y-auto p-1">
          <WsFileTree
            sessionId={sessionId}
            onOpenFile={(path, name) => { openArtifact({ name, path }); }}
          />
        </div>
        {/* 最近文件 */}
        {recentArtifacts.length > 0 && (
          <div className="shrink-0 max-h-40 border-t border-border-muted overflow-y-auto p-1">
            <div className="px-1 py-1 text-[9px] font-medium text-text-tertiary uppercase tracking-wide">最近</div>
            {recentArtifacts.map((artifact) => (
              <button
                key={artifact.path || artifact.url || artifact.name}
                onClick={() => openArtifact(artifact)}
                className={`w-full rounded px-1.5 py-1 text-left text-[10px] transition-colors ${
                  artifact.path === activeArtifact?.path
                    ? 'bg-accent-brand/10 text-accent-brand'
                    : 'text-text-secondary hover:bg-bg-hover'
                }`}>
                <div className="flex items-center gap-1.5 min-w-0">
                  <span className="shrink-0">{iconFor(artifact.mimeType)}</span>
                  <span className="truncate font-mono">{artifact.name}</span>
                </div>
              </button>
            ))}
          </div>
        )}
      </aside>
      )}

      {/* 右侧：主内容区 */}
      <main className="flex-1 min-w-0 flex flex-col">
        {/* 顶部工具栏 */}
        <div className="shrink-0 border-b border-border-muted bg-bg-secondary/70 px-3 py-2">
          <div className="flex items-center gap-2 min-w-0 flex-wrap">
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-border-muted bg-bg-primary text-accent-brand">
              {iconFor(preview?.format)}
            </div>
            <div className="min-w-0 flex-1">
              <div className="truncate text-sm font-medium text-text-primary">
                {preview?.name || activeArtifact?.name || t('artifact.title')}
              </div>
              <div className="mt-0.5 flex items-center gap-2 truncate text-[10px] font-mono text-text-tertiary">
                {preview && <span>{preview.format}</span>}
                {preview && <span>{formatFileSize(preview.size)}</span>}
                {activeArtifact?.line && <span>line {activeArtifact.line}</span>}
                {preview?.metadata?.pages && <span>{preview.metadata.pages} {t('artifact.meta.pages')}</span>}
                {preview?.metadata?.wordCount && <span>{preview.metadata.wordCount} {t('artifact.meta.words')}</span>}
                {preview?.truncated && <span className="text-accent-yellow">{t('artifact.meta.truncated')}</span>}
                {preview?.editable && !hasUnsavedChanges && <span className="text-accent-green">{t('artifact.meta.editable')}</span>}
                {preview?.metadata?.editableKind === 'office-native' && <span className="text-accent-blue">{t('artifact.meta.officeNative')}</span>}
                {hasUnsavedChanges && <span className="text-accent-yellow">{t('artifact.meta.unsaved')}</span>}
                {saveMessage && <span className="text-accent-green">{saveMessage}</span>}
              </div>
            </div>

            {/* Markdown 模式切换 */}
            {renderMdModeButtons()}

            {/* 预览/全文切换（非二进制格式） */}
            {!hidesPreviewModeToggle(preview?.format) && (
              <button
                onClick={() => switchMode(mode === 'preview' ? 'full' : 'preview')}
                disabled={!activeArtifact?.path || loading}
                className="rounded border border-border-muted px-2 py-1 text-xs text-text-secondary hover:text-text-primary disabled:opacity-40"
              >
                {mode === 'preview' ? t('artifact.action.full') : t('artifact.action.preview')}
              </button>
            )}

            <button
              onClick={() => void loadPreview(activeArtifact, mode)}
              disabled={!activeArtifact?.path || loading}
              className="rounded border border-border-muted p-1.5 text-text-secondary hover:text-text-primary disabled:opacity-40"
              title={t('artifact.action.refresh')}
            >
              <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            </button>

            {preview?.path && (
              <button
                onClick={() => navigator.clipboard?.writeText(preview.path)}
                className="rounded border border-border-muted p-1.5 text-text-secondary hover:text-text-primary"
                title={t('artifact.action.copyPath')}
              >
                <Copy size={14} />
              </button>
            )}

            {preview && (
              <button
                type="button"
                onClick={() => { void downloadPreview(); }}
                className="rounded border border-border-muted p-1.5 text-text-secondary hover:text-text-primary"
                title={t('artifact.action.download')}
              >
                <Download size={14} />
              </button>
            )}

            {preview?.editable && (
              <button
                onClick={save}
                disabled={saving || !hasUnsavedChanges}
                className="inline-flex items-center gap-1 rounded border border-accent-brand/40 bg-accent-brand/10 px-2 py-1 text-xs text-accent-brand disabled:opacity-40"
              >
                {saving ? <Loader2 size={13} className="animate-spin" /> : <Save size={13} />}
                {t('artifact.action.save')}
              </button>
            )}

            {/* 剑阁可交互 Canvas 模式按钮（仅 HTML 成品） */}
            {preview?.path && isHtmlArtifact && (
              <button
                onClick={() => setCanvasMode((v) => !v)}
                className={`inline-flex items-center gap-1 rounded border px-2 py-1 text-xs transition-colors ${
                  canvasMode
                    ? 'border-accent-brand bg-accent-brand/10 text-accent-brand'
                    : 'border-border-muted text-text-secondary hover:text-text-primary hover:border-accent-brand/40'
                }`}
                title={t('canvas.mode.button', '剑阁：选区改写 + 版本栈')}
              >
                <Wand2 size={13} />
                {t('canvas.mode.buttonText', '剑阁')}
              </button>
            )}

            {/* 评论按钮 */}
            {preview?.path && (
              <button
                onClick={handleCommentAction}
                className={`inline-flex items-center gap-1 rounded border px-2 py-1 text-xs transition-colors ${
                  commentMode
                    ? 'border-accent-brand bg-accent-brand/10 text-accent-brand'
                    : 'border-border-muted text-text-secondary hover:text-text-primary hover:border-accent-brand/40'
                }`}
                title={t('artifact.comment.button', '评论并请求源码修改')}
              >
                <MessageSquare size={13} />
                {t('artifact.comment.buttonText', '评论')}
              </button>
            )}

            <button
              onClick={clearArtifact}
              disabled={!activeArtifact}
              className="rounded border border-border-muted p-1.5 text-text-secondary hover:text-text-primary disabled:opacity-40"
              title={t('artifact.action.close')}
            >
              <XCircle size={14} />
            </button>
          </div>
        </div>

        {/* 内容区 */}
        <div className="relative min-h-0 flex-1 flex flex-col">
          {!loading && !error && <PreviewWarningStrip preview={preview} extra={sheetError} />}
          {commentMode && !commentContext && (
            <div className="shrink-0 border-b border-accent-brand/30 bg-accent-brand/10 px-3 py-1.5 text-[11px] text-accent-brand flex items-center gap-2">
              <MessageSquare size={12} />
              {t('artifact.comment.modeHint', '评论模式已开启 — 选中内容后点击评论')}
            </div>
          )}
          {/* 剑阁模式提示条 */}
          {canvasMode && !canvas.selection && (
            <div className="shrink-0 border-b border-accent-brand/30 bg-accent-brand/10 px-3 py-1.5 text-[11px] text-accent-brand flex items-center gap-2">
              <Wand2 size={12} />
              {canvas.sourceMap
                ? t('canvas.modeHint', '剑阁模式已开启 — 点击成品上的元素，写下诉求让凌霄改源码')
                : t('canvas.modeHintNoMap', '剑阁模式已开启 — 该产物尚未建立源码映射，无法拾取选区')}
            </div>
          )}
          {/* SSE 热更新提示 */}
          {canvasMode && canvas.updateNotice && (
            <div className="shrink-0 flex items-center justify-between gap-2 border-b border-accent-green/30 bg-accent-green/10 px-3 py-1.5 text-[11px] text-accent-green">
              <span className="flex items-center gap-1.5 min-w-0">
                <RefreshCw size={12} className="shrink-0" />
                <span className="truncate">
                  {t('canvas.updateNotice', '凌霄已更新，已生成 v{{n}}', { n: canvas.updateNotice.version })}
                </span>
              </span>
              <button
                onClick={() => canvas.consumeUpdateNotice()}
                className="shrink-0 rounded p-0.5 hover:bg-accent-green/20"
                title={t('app.close', '关闭')}
              >
                <XCircle size={12} />
              </button>
            </div>
          )}
          {/* 剑阁模式：预览 + 右侧版本栈/批注栏；常规模式：纯预览 */}
          {canvasMode ? (
            <div className="min-h-0 flex-1 flex">
              <div className="relative min-h-0 flex-1 ring-1 ring-inset ring-accent-brand/20">
                {renderContent()}
                {/* 改写框 */}
                <CanvasIntentPopup
                  selection={canvas.selection}
                  status={canvas.intentStatus}
                  error={canvas.intentError}
                  onSubmit={(text) => canvas.submitSelectionIntent(text)}
                  onClose={() => { canvas.setSelection(null); canvas.resetIntentStatus(); }}
                />
              </div>
              {/* 右侧栏：版本栈 + 批注 */}
              <aside className="w-60 shrink-0 border-l border-border-muted bg-bg-secondary/40 overflow-y-auto p-3 flex flex-col gap-4">
                <CanvasVersionStack
                  versions={canvas.versions}
                  activeVersion={canvas.activeVersion}
                  onActivate={(v) => canvas.activateVersion(v)}
                  updateNotice={canvas.updateNotice}
                  onDismissNotice={() => canvas.consumeUpdateNotice()}
                />
                <CanvasCommentList
                  comments={canvas.comments}
                  onLocate={(nodeId) => {
                    if (!nodeId) return;
                    const node = canvas.sourceMap?.nodes.find((n) => n.nodeId === nodeId);
                    if (node) {
                      canvas.setSelection({ nodeId, anchor: node });
                    }
                  }}
                  onSetStatus={(id, status) => canvas.setCommentStatus(id, status)}
                />
                {canvas.error && (
                  <div className="rounded border border-accent-red/30 bg-accent-red/5 px-2 py-1.5 text-[10px] text-accent-red">
                    {canvas.error}
                  </div>
                )}
              </aside>
            </div>
          ) : (
            <div className={`min-h-0 flex-1 ${commentMode ? 'ring-1 ring-inset ring-accent-brand/20' : ''}`}>{renderContent()}</div>
          )}
          {/* 评论浮窗 */}
          <CommentPopup
            context={commentContext}
            onSend={sendCommentToLeader}
            onClose={() => { setCommentContext(null); setCommentMode(false); }}
          />
        </div>
      </main>
    </div>
  );
}

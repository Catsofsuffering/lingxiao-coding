import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertCircle,
  ArrowLeft,
  ArrowRight,
  Check,
  CheckCircle2,
  CornerLeftUp,
  Eye,
  EyeOff,
  Folder,
  FolderOpen,
  Loader2,
  Moon,
  Sparkles,
  Sun,
  X,
  Zap,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { settingsApiFetch, createModelProvider } from '../settings/settingsApi';
import { DEFAULT_MODEL_BASE_URL, type ModelProtocol } from '../settings/types';
import { useSessionStore } from '../../stores/sessionStore';
import { useThemeStore } from '../../stores/themeStore';

// ── Types ───────────────────────────────────────────────────────────

type Step = 0 | 1 | 2 | 3;

interface LlmConfig {
  provider: ModelProtocol;
  apiKey: string;
  baseUrl: string;
  model: string;
  contextWindowSize: string;
}

type TestStatus = 'idle' | 'testing' | 'success' | 'error';
type PathStatus = 'idle' | 'checking' | 'valid' | 'error';

interface DirEntry {
  name: string;
  path: string;
  type: 'directory' | 'file';
}

// ── Self-contained ink palette ──────────────────────────────────────
// 引导页独立配色：完全脱离全局 --color-* 半透明灰白变量，
// 使用不透明的宣纸/墨色实色 + 墨玉绿 / 暖金 / 印章红强调。

interface Palette {
  // 全屏遮罩背景
  overlayBase: string;
  overlayWash: string;
  // 卡片
  cardBg: string;
  cardBorder: string;
  cardShadow: string;
  headerBg: string;
  // 文字
  ink: string;
  inkSoft: string;
  inkMuted: string;
  // 强调
  jade: string;        // 墨玉绿（主强调）
  jadeSoft: string;
  gold: string;        // 暖金
  seal: string;        // 印章红
  green: string;       // 成功
  red: string;         // 错误
  // 控件
  inputBg: string;
  inputBorder: string;
  inputBorderFocus: string;
  fieldBg: string;
  // 主按钮（墨玉渐变）
  btnBg: string;
  btnHover: string;
  btnFg: string;
  // pill / chip
  pillActiveBg: string;
  pillActiveFg: string;
  pillIdleFg: string;
  hoverBg: string;
}

const LIGHT: Palette = {
  overlayBase: 'radial-gradient(120% 120% at 50% 0%, #f4efe4 0%, #ece4d4 42%, #dcd2bd 100%)',
  overlayWash:
    'radial-gradient(80% 60% at 78% 100%, rgba(58, 82, 74, 0.16), transparent 60%), radial-gradient(70% 55% at 12% 90%, rgba(138, 101, 31, 0.12), transparent 58%)',
  cardBg: 'linear-gradient(168deg, #fdfbf5 0%, #f8f3e7 100%)',
  cardBorder: 'rgba(138, 101, 31, 0.22)',
  cardShadow: '0 28px 70px -24px rgba(60, 48, 20, 0.42), 0 4px 14px rgba(60, 48, 20, 0.10)',
  headerBg: 'rgba(255, 252, 244, 0.6)',
  ink: '#2a2117',
  inkSoft: '#5a4f3e',
  inkMuted: '#988a72',
  jade: '#3a6f5f',
  jadeSoft: 'rgba(58, 111, 95, 0.12)',
  gold: '#a9791e',
  seal: '#b23b2e',
  green: '#3a8f5a',
  red: '#c0392b',
  inputBg: '#fffdf8',
  inputBorder: 'rgba(138, 101, 31, 0.26)',
  inputBorderFocus: '#3a6f5f',
  fieldBg: 'rgba(247, 241, 228, 0.7)',
  btnBg: 'linear-gradient(135deg, #2f5d4f, #3a6f5f 56%, #4a7a5f)',
  btnHover: 'linear-gradient(135deg, #356757, #437a68 56%, #54886a)',
  btnFg: '#f8f5ec',
  pillActiveBg: '#fdfbf5',
  pillActiveFg: '#2a2117',
  pillIdleFg: '#988a72',
  hoverBg: 'rgba(58, 111, 95, 0.08)',
};

const DARK: Palette = {
  overlayBase: 'radial-gradient(120% 120% at 50% 0%, #1c211f 0%, #141917 46%, #0d100f 100%)',
  overlayWash:
    'radial-gradient(80% 60% at 78% 100%, rgba(74, 122, 95, 0.16), transparent 60%), radial-gradient(70% 55% at 12% 90%, rgba(180, 140, 60, 0.10), transparent 58%)',
  cardBg: 'linear-gradient(168deg, #1f2523 0%, #181d1b 100%)',
  cardBorder: 'rgba(180, 140, 60, 0.2)',
  cardShadow: '0 30px 76px -24px rgba(0, 0, 0, 0.7), 0 4px 16px rgba(0, 0, 0, 0.4)',
  headerBg: 'rgba(26, 32, 30, 0.6)',
  ink: '#ece4d4',
  inkSoft: '#b3ab9a',
  inkMuted: '#7d7666',
  jade: '#6fae93',
  jadeSoft: 'rgba(111, 174, 147, 0.14)',
  gold: '#d4a84a',
  seal: '#d4574a',
  green: '#5fb87e',
  red: '#e06a5c',
  inputBg: '#161b19',
  inputBorder: 'rgba(180, 140, 60, 0.22)',
  inputBorderFocus: '#6fae93',
  fieldBg: 'rgba(22, 27, 25, 0.6)',
  btnBg: 'linear-gradient(135deg, #356757, #437a68 56%, #54886a)',
  btnHover: 'linear-gradient(135deg, #3d7362, #4d8975 56%, #5f9878)',
  btnFg: '#f4f1e8',
  pillActiveBg: '#262d2a',
  pillActiveFg: '#ece4d4',
  pillIdleFg: '#7d7666',
  hoverBg: 'rgba(111, 174, 147, 0.1)',
};

// ── Constants ───────────────────────────────────────────────────────

const TOTAL_STEPS = 4;
const PROVIDERS: { id: ModelProtocol; label: string; defaultModel: string }[] = [
  { id: 'openai', label: 'OpenAI', defaultModel: 'gpt-4o' },
  { id: 'anthropic', label: 'Anthropic', defaultModel: 'claude-sonnet-4-20250514' },
];

const LOGO_SRC = `/logo.svg?v=${typeof __APP_VERSION__ === 'string' ? __APP_VERSION__ : 'dev'}`;

// 跨平台路径父目录：兼容 Windows (D:\a\b) 与 POSIX (/a/b)。
function parentPath(path: string): string | null {
  const trimmed = path.replace(/[\\/]+$/, '');
  if (!trimmed) return null;
  const sep = trimmed.includes('\\') ? '\\' : '/';
  const idx = Math.max(trimmed.lastIndexOf('\\'), trimmed.lastIndexOf('/'));
  if (idx < 0) return null;
  if (idx === 0) return sep === '/' ? '/' : trimmed.slice(0, 1);
  const parent = trimmed.slice(0, idx);
  // 盘符根归一：D: -> D:\
  if (/^[A-Za-z]:$/.test(parent)) return `${parent}\\`;
  return parent;
}

// ── Sub-components ──────────────────────────────────────────────────

function SealLogo({ size = 56, pal }: { size?: number; pal: Palette }) {
  return (
    <div
      className="flex items-center justify-center rounded-2xl"
      style={{
        width: size,
        height: size,
        background: `linear-gradient(145deg, ${pal.seal}, ${pal.gold})`,
        boxShadow: `0 10px 26px -8px ${pal.seal}66, inset 0 1px 0 rgba(255,255,255,0.25)`,
      }}
    >
      <img src={LOGO_SRC} alt="" aria-hidden="true" style={{ width: size * 0.56, height: size * 0.56 }} />
    </div>
  );
}

function FieldLabel({ children, hint, pal }: { children: React.ReactNode; hint?: React.ReactNode; pal: Palette }) {
  return (
    <label className="mb-1.5 flex items-center gap-1 text-xs font-medium" style={{ color: pal.inkMuted }}>
      {children}
      {hint}
    </label>
  );
}

function TextInput({
  value,
  onChange,
  type = 'text',
  placeholder,
  className = '',
  onEnter,
  rightSlot,
  pal,
  status,
}: {
  value: string;
  onChange: (v: string) => void;
  type?: string;
  placeholder?: string;
  className?: string;
  onEnter?: () => void;
  rightSlot?: React.ReactNode;
  pal: Palette;
  status?: 'valid' | 'error';
}) {
  const [focused, setFocused] = useState(false);
  const borderColor =
    status === 'error'
      ? pal.red
      : status === 'valid'
        ? pal.green
        : focused
          ? pal.inputBorderFocus
          : pal.inputBorder;
  return (
    <div className="relative">
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && onEnter) onEnter();
        }}
        placeholder={placeholder}
        className={`w-full rounded-lg px-3 py-2 text-sm transition-all duration-200 focus:outline-none ${rightSlot ? 'pr-10' : ''} ${className}`}
        style={{
          background: pal.inputBg,
          border: `1px solid ${borderColor}`,
          color: pal.ink,
          boxShadow: focused ? `0 0 0 3px ${pal.jadeSoft}` : 'none',
        }}
      />
      {rightSlot && <div className="absolute right-2 top-1/2 -translate-y-1/2">{rightSlot}</div>}
    </div>
  );
}

function StepNav({ current, pal, t }: { current: number; pal: Palette; t: (k: string) => string }) {
  const steps = [
    t('onboarding.steps.welcome'),
    t('onboarding.steps.llm'),
    t('onboarding.steps.workspace'),
    t('onboarding.steps.complete'),
  ];
  return (
    <div
      className="inline-flex items-center gap-0.5 rounded-full p-1"
      style={{ background: pal.fieldBg, border: `1px solid ${pal.cardBorder}` }}
    >
      {steps.map((label, i) => {
        const active = i === current;
        const done = i < current;
        return (
          <div
            key={label}
            className="inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium transition-all duration-300"
            style={{
              background: active ? pal.pillActiveBg : 'transparent',
              color: active ? pal.jade : done ? pal.inkSoft : pal.pillIdleFg,
              boxShadow: active ? `0 1px 4px ${pal.cardBorder}` : 'none',
            }}
          >
            {done ? (
              <Check size={12} style={{ color: pal.green }} />
            ) : (
              <span
                className="inline-flex h-4 w-4 items-center justify-center rounded-full text-[10px]"
                style={{
                  background: active ? pal.jade : 'transparent',
                  color: active ? pal.btnFg : pal.pillIdleFg,
                  border: active ? 'none' : `1px solid ${pal.cardBorder}`,
                }}
              >
                {i + 1}
              </span>
            )}
            {label}
          </div>
        );
      })}
    </div>
  );
}

// ── Step 0: Welcome ─────────────────────────────────────────────────

function WelcomeStep({ onNext, pal }: { onNext: () => void; pal: Palette }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center px-8 py-10 text-center">
      <SealLogo size={64} pal={pal} />
      <div
        className="mt-5 inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium"
        style={{ background: pal.jadeSoft, color: pal.jade }}
      >
        <Sparkles size={12} />
        {t('onboarding.appName')}
      </div>
      <h2
        className="mt-4 text-2xl font-semibold tracking-wide"
        style={{ color: pal.ink, fontFamily: 'var(--font-display)' }}
      >
        {t('onboarding.welcome.title')}
      </h2>
      <p className="mt-3 max-w-sm text-sm leading-relaxed" style={{ color: pal.inkSoft }}>
        {t('onboarding.welcome.desc')}
      </p>
      <button
        type="button"
        onClick={onNext}
        className="mt-8 inline-flex items-center gap-2 rounded-xl px-8 py-3 text-sm font-semibold transition-all duration-200 hover:-translate-y-0.5"
        style={{ background: pal.btnBg, color: pal.btnFg, boxShadow: `0 12px 28px -10px ${pal.jade}99` }}
        onMouseEnter={(e) => (e.currentTarget.style.background = pal.btnHover)}
        onMouseLeave={(e) => (e.currentTarget.style.background = pal.btnBg)}
      >
        {t('onboarding.welcome.start')}
        <ArrowRight size={16} />
      </button>
    </div>
  );
}

// ── Step 1: LLM Config ──────────────────────────────────────────────

function LlmConfigStep({
  config,
  onChange,
  testStatus,
  testMessage,
  onTest,
  pal,
}: {
  config: LlmConfig;
  onChange: (patch: Partial<LlmConfig>) => void;
  testStatus: TestStatus;
  testMessage: string;
  onTest: () => void;
  pal: Palette;
}) {
  const { t } = useTranslation();
  const [showApiKey, setShowApiKey] = useState(false);

  const handleProviderChange = (provider: ModelProtocol) => {
    const shouldReplaceBaseUrl =
      !config.baseUrl.trim() || Object.values(DEFAULT_MODEL_BASE_URL).includes(config.baseUrl);
    onChange({
      provider,
      baseUrl: shouldReplaceBaseUrl ? DEFAULT_MODEL_BASE_URL[provider] : config.baseUrl,
    });
  };

  return (
    <div className="space-y-4 px-8 py-6">
      <div>
        <h3 className="text-lg font-semibold" style={{ color: pal.ink, fontFamily: 'var(--font-display)' }}>
          {t('onboarding.llm.title')}
        </h3>
        <p className="mt-1 text-sm" style={{ color: pal.inkSoft }}>{t('onboarding.llm.desc')}</p>
      </div>

      {/* Provider segmented control */}
      <div>
        <FieldLabel pal={pal}>{t('onboarding.llm.provider')}</FieldLabel>
        <div
          className="flex gap-1 rounded-lg p-1"
          style={{ background: pal.fieldBg, border: `1px solid ${pal.cardBorder}` }}
        >
          {PROVIDERS.map((p) => {
            const active = config.provider === p.id;
            return (
              <button
                key={p.id}
                type="button"
                onClick={() => handleProviderChange(p.id)}
                className="inline-flex flex-1 items-center justify-center rounded-md px-3 py-1.5 text-sm font-medium transition-all duration-200"
                style={{
                  background: active ? pal.btnBg : 'transparent',
                  color: active ? pal.btnFg : pal.inkSoft,
                  boxShadow: active ? `0 4px 12px -4px ${pal.jade}88` : 'none',
                }}
              >
                {p.label}
              </button>
            );
          })}
        </div>
      </div>

      {/* API Key */}
      <div>
        <FieldLabel pal={pal}>{t('onboarding.llm.apiKey')}</FieldLabel>
        <TextInput
          pal={pal}
          value={config.apiKey}
          onChange={(v) => onChange({ apiKey: v })}
          type={showApiKey ? 'text' : 'password'}
          placeholder={t('onboarding.llm.apiKeyPlaceholder')}
          className="font-mono"
          rightSlot={
            <button
              type="button"
              onClick={() => setShowApiKey((s) => !s)}
              className="rounded p-1 transition-colors"
              style={{ color: pal.inkMuted }}
              tabIndex={-1}
            >
              {showApiKey ? <EyeOff size={15} /> : <Eye size={15} />}
            </button>
          }
        />
      </div>

      {/* Base URL */}
      <div>
        <FieldLabel pal={pal}>{t('onboarding.llm.baseUrl')}</FieldLabel>
        <TextInput
          pal={pal}
          value={config.baseUrl}
          onChange={(v) => onChange({ baseUrl: v })}
          placeholder={DEFAULT_MODEL_BASE_URL[config.provider]}
          className="font-mono"
        />
      </div>

      {/* Model + Context window in a row */}
      <div className="grid grid-cols-2 gap-3">
        <div>
          <FieldLabel pal={pal}>{t('onboarding.llm.model')}</FieldLabel>
          <TextInput
            pal={pal}
            value={config.model}
            onChange={(v) => onChange({ model: v })}
            placeholder={PROVIDERS.find((p) => p.id === config.provider)?.defaultModel || 'gpt-4o'}
            className="font-mono"
          />
        </div>
        <div>
          <FieldLabel
            pal={pal}
            hint={<span style={{ color: pal.inkMuted }}>({t('onboarding.common.optional')})</span>}
          >
            {t('onboarding.llm.contextWindowSize')}
          </FieldLabel>
          <TextInput
            pal={pal}
            value={config.contextWindowSize}
            onChange={(v) => onChange({ contextWindowSize: v })}
            type="number"
            placeholder="128000"
          />
        </div>
      </div>

      {/* Test connection */}
      <div className="flex items-center gap-3 pt-1">
        <button
          type="button"
          onClick={onTest}
          disabled={testStatus === 'testing' || !config.apiKey.trim() || !config.model.trim()}
          className="inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-all disabled:cursor-not-allowed disabled:opacity-50"
          style={{ border: `1px solid ${pal.jade}`, background: pal.jadeSoft, color: pal.jade }}
        >
          {testStatus === 'testing' ? <Loader2 size={15} className="animate-spin" /> : <Zap size={15} />}
          {testStatus === 'testing' ? t('onboarding.llm.testing') : t('onboarding.llm.testConnection')}
        </button>
        {testStatus === 'success' && (
          <span className="inline-flex items-center gap-1 text-sm" style={{ color: pal.green }}>
            <CheckCircle2 size={15} />
            {t('onboarding.llm.testSuccess')}
          </span>
        )}
        {testStatus === 'error' && (
          <span className="inline-flex items-center gap-1 text-sm" style={{ color: pal.red }} title={testMessage}>
            <AlertCircle size={15} />
            {t('onboarding.llm.testFailed')}: {testMessage.slice(0, 60)}
          </span>
        )}
      </div>
    </div>
  );
}

// ── Step 2: Workspace Selection ─────────────────────────────────────

function WorkspaceStep({
  workspacePath,
  onChange,
  pathStatus,
  pathError,
  pal,
}: {
  workspacePath: string;
  onChange: (v: string) => void;
  pathStatus: PathStatus;
  pathError: string;
  pal: Palette;
}) {
  const { t } = useTranslation();
  const [browsing, setBrowsing] = useState(false);
  const [browsePath, setBrowsePath] = useState('');
  const [entries, setEntries] = useState<DirEntry[]>([]);
  const [showBrowser, setShowBrowser] = useState(false);

  const browse = useCallback(async (path: string) => {
    setBrowsing(true);
    try {
      const res = await settingsApiFetch<{ entries?: DirEntry[] }>('/fs/list', {
        method: 'POST',
        body: JSON.stringify({ path: path || '/' }),
      });
      setEntries((res.entries || []).filter((e) => e.type === 'directory'));
      setBrowsePath(path);
    } catch {
      setEntries([]);
    } finally {
      setBrowsing(false);
    }
  }, []);

  useEffect(() => {
    if (showBrowser && entries.length === 0) browse(workspacePath || '/');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showBrowser]);

  const parent = useMemo(() => parentPath(browsePath || workspacePath), [browsePath, workspacePath]);

  return (
    <div className="space-y-4 px-8 py-6">
      <div>
        <h3 className="text-lg font-semibold" style={{ color: pal.ink, fontFamily: 'var(--font-display)' }}>
          {t('onboarding.workspace.title')}
        </h3>
        <p className="mt-1 text-sm" style={{ color: pal.inkSoft }}>{t('onboarding.workspace.desc')}</p>
      </div>

      <div>
        <FieldLabel pal={pal}>{t('onboarding.workspace.path')}</FieldLabel>
        <div className="flex gap-2">
          <div className="flex-1">
            <TextInput
              pal={pal}
              value={workspacePath}
              onChange={onChange}
              placeholder={t('onboarding.workspace.pathPlaceholder')}
              className="font-mono"
              status={pathStatus === 'error' ? 'error' : pathStatus === 'valid' ? 'valid' : undefined}
              rightSlot={
                pathStatus === 'checking' ? (
                  <Loader2 size={15} className="animate-spin" style={{ color: pal.inkMuted }} />
                ) : pathStatus === 'valid' ? (
                  <CheckCircle2 size={15} style={{ color: pal.green }} />
                ) : pathStatus === 'error' ? (
                  <AlertCircle size={15} style={{ color: pal.red }} />
                ) : null
              }
            />
          </div>
          <button
            type="button"
            onClick={() => setShowBrowser((s) => !s)}
            className="inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-sm font-medium transition-all"
            style={{
              border: `1px solid ${showBrowser ? pal.jade : pal.inputBorder}`,
              background: showBrowser ? pal.jadeSoft : pal.inputBg,
              color: showBrowser ? pal.jade : pal.inkSoft,
            }}
          >
            <Folder size={15} />
            {t('onboarding.workspace.browse')}
          </button>
        </div>
        {/* 校验三态文字反馈 */}
        <div className="mt-1.5 min-h-[18px] text-xs">
          {pathStatus === 'checking' && (
            <span style={{ color: pal.inkMuted }}>{t('onboarding.workspace.checking')}</span>
          )}
          {pathStatus === 'valid' && (
            <span className="inline-flex items-center gap-1" style={{ color: pal.green }}>
              <Check size={12} />
              {t('onboarding.workspace.valid')}
            </span>
          )}
          {pathStatus === 'error' && <span style={{ color: pal.red }}>{pathError}</span>}
        </div>
      </div>

      {/* Directory browser */}
      {showBrowser && (
        <div className="rounded-lg" style={{ border: `1px solid ${pal.cardBorder}`, background: pal.fieldBg }}>
          <div
            className="flex items-center gap-2 px-2 py-1.5"
            style={{ borderBottom: `1px solid ${pal.cardBorder}` }}
          >
            <button
              type="button"
              onClick={() => parent && browse(parent)}
              disabled={!parent || browsing}
              className="inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs transition-colors disabled:opacity-40"
              style={{ color: pal.inkSoft }}
              title={t('onboarding.workspace.up')}
            >
              <CornerLeftUp size={13} />
            </button>
            <input
              value={browsePath}
              onChange={(e) => setBrowsePath(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') browse(browsePath);
              }}
              className="flex-1 rounded-md px-2 py-1 font-mono text-xs focus:outline-none"
              style={{ background: pal.inputBg, border: `1px solid ${pal.inputBorder}`, color: pal.ink }}
            />
            <button
              type="button"
              onClick={() => browse(browsePath)}
              disabled={browsing}
              className="rounded-md px-2.5 py-1 text-xs font-medium disabled:opacity-50"
              style={{ background: pal.jadeSoft, color: pal.jade, border: `1px solid ${pal.jade}` }}
            >
              {browsing ? <Loader2 size={12} className="animate-spin" /> : t('onboarding.workspace.go')}
            </button>
          </div>
          <div className="max-h-44 overflow-y-auto p-1">
            {entries.length === 0 && !browsing ? (
              <p className="py-5 text-center text-xs" style={{ color: pal.inkMuted }}>
                {t('onboarding.workspace.noDirs')}
              </p>
            ) : (
              entries.map((entry) => (
                <button
                  key={entry.path}
                  type="button"
                  onClick={() => onChange(entry.path)}
                  onDoubleClick={() => browse(entry.path)}
                  className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-xs transition-colors"
                  style={{ color: pal.inkSoft }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = pal.hoverBg)}
                  onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
                >
                  <FolderOpen size={13} style={{ color: pal.gold }} />
                  <span className="truncate font-mono">{entry.name}</span>
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Step 3: Complete ────────────────────────────────────────────────

function CompleteStep({
  config,
  workspacePath,
  saving,
  error,
  pal,
}: {
  config: LlmConfig;
  workspacePath: string;
  saving: boolean;
  error: string;
  pal: Palette;
}) {
  const { t } = useTranslation();
  const summary = [
    { label: t('onboarding.complete.labelProvider'), value: PROVIDERS.find((p) => p.id === config.provider)?.label || config.provider },
    { label: t('onboarding.complete.labelModel'), value: config.model || '-' },
    { label: t('onboarding.complete.labelWorkspace'), value: workspacePath || '-' },
  ];
  return (
    <div className="flex flex-col items-center px-8 py-10 text-center">
      <div
        className="flex h-16 w-16 items-center justify-center rounded-full"
        style={{ background: pal.jadeSoft, border: `1px solid ${pal.jade}` }}
      >
        <CheckCircle2 size={30} style={{ color: pal.green }} />
      </div>
      <h2 className="mt-5 text-xl font-semibold" style={{ color: pal.ink, fontFamily: 'var(--font-display)' }}>
        {t('onboarding.complete.title')}
      </h2>
      <p className="mt-2 max-w-sm text-sm" style={{ color: pal.inkSoft }}>{t('onboarding.complete.desc')}</p>
      <div
        className="mt-6 w-full max-w-sm space-y-2.5 rounded-xl p-4 text-left"
        style={{ background: pal.fieldBg, border: `1px solid ${pal.cardBorder}` }}
      >
        {summary.map((item) => (
          <div key={item.label} className="flex items-start justify-between gap-3 text-sm">
            <span className="shrink-0" style={{ color: pal.inkMuted }}>{item.label}</span>
            <span className="truncate font-mono" style={{ color: pal.ink }}>{item.value}</span>
          </div>
        ))}
      </div>
      {error && <p className="mt-4 max-w-sm text-sm" style={{ color: pal.red }}>{error}</p>}
      {saving && (
        <p className="mt-4 inline-flex items-center gap-2 text-sm" style={{ color: pal.inkSoft }}>
          <Loader2 size={14} className="animate-spin" />
          {t('onboarding.complete.saving')}
        </p>
      )}
    </div>
  );
}

// ── Main Wizard Component ───────────────────────────────────────────

export interface OnboardingWizardProps {
  onComplete: () => void;
  onSkip?: () => void;
}

export default function OnboardingWizard({ onComplete, onSkip }: OnboardingWizardProps) {
  const { t } = useTranslation();
  const { resolved, toggle } = useThemeStore();
  const pal = resolved === 'dark' ? DARK : LIGHT;

  const [step, setStep] = useState<Step>(0);
  const [llmConfig, setLlmConfig] = useState<LlmConfig>({
    provider: 'openai',
    apiKey: '',
    baseUrl: DEFAULT_MODEL_BASE_URL.openai,
    model: '',
    contextWindowSize: '',
  });
  const [workspacePath, setWorkspacePath] = useState('');
  const [testStatus, setTestStatus] = useState<TestStatus>('idle');
  const [testMessage, setTestMessage] = useState('');
  const [pathStatus, setPathStatus] = useState<PathStatus>('idle');
  const [pathError, setPathError] = useState('');
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');
  const connectingRef = useRef(false);
  const pathDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Initialize workspace path from current cwd
  useEffect(() => {
    settingsApiFetch<{ data: { cwd?: string } }>('/info')
      .then((res) => {
        if (res?.data?.cwd) setWorkspacePath(res.data.cwd);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    return () => {
      if (pathDebounceRef.current) clearTimeout(pathDebounceRef.current);
    };
  }, []);

  const updateLlmConfig = useCallback((patch: Partial<LlmConfig>) => {
    setLlmConfig((prev) => ({ ...prev, ...patch }));
    setTestStatus('idle');
  }, []);

  const handleTestConnection = useCallback(async () => {
    if (!llmConfig.apiKey.trim() || !llmConfig.model.trim()) return;
    setTestStatus('testing');
    setTestMessage('');
    try {
      await settingsApiFetch('/settings/test-llm', {
        method: 'POST',
        body: JSON.stringify({
          provider: llmConfig.provider,
          apiKey: llmConfig.apiKey.trim(),
          baseUrl: llmConfig.baseUrl.trim() || undefined,
          model: llmConfig.model.trim(),
        }),
      });
      setTestStatus('success');
    } catch (e) {
      setTestStatus('error');
      setTestMessage(e instanceof Error ? e.message : 'Unknown error');
    }
  }, [llmConfig.apiKey, llmConfig.model, llmConfig.provider, llmConfig.baseUrl]);

  // 验证 workspace 路径是否存在且是目录（三态）
  const validateWorkspacePath = useCallback(async (path: string) => {
    const trimmed = path.trim();
    if (!trimmed) {
      setPathStatus('idle');
      setPathError('');
      return;
    }
    setPathStatus('checking');
    try {
      const res = await settingsApiFetch<{ entries?: DirEntry[]; isDirectory?: boolean }>('/fs/list', {
        method: 'POST',
        body: JSON.stringify({ path: trimmed }),
      });
      if (Array.isArray(res.entries)) {
        setPathStatus('valid');
        setPathError('');
      } else {
        setPathStatus('error');
        setPathError(t('onboarding.workspace.pathNotFound'));
      }
    } catch (e) {
      setPathStatus('error');
      const msg = e instanceof Error ? e.message : '';
      // 区分"不是目录"和"不存在"
      setPathError(/not a directory|ENOTDIR/i.test(msg)
        ? t('onboarding.workspace.notDirectory')
        : t('onboarding.workspace.pathNotFound'));
    }
  }, [t]);

  const handleWorkspacePathChange = useCallback((v: string) => {
    setWorkspacePath(v);
    const trimmed = v.trim();
    if (pathDebounceRef.current) {
      clearTimeout(pathDebounceRef.current);
      pathDebounceRef.current = null;
    }
    if (!trimmed) {
      setPathStatus('idle');
      setPathError('');
      return;
    }
    setPathStatus('checking');
    pathDebounceRef.current = setTimeout(() => {
      validateWorkspacePath(trimmed);
      pathDebounceRef.current = null;
    }, 450);
  }, [validateWorkspacePath]);

  const canProceed = useMemo(() => {
    if (step === 1) {
      return llmConfig.apiKey.trim() !== '' && llmConfig.model.trim() !== '' && llmConfig.baseUrl.trim() !== '';
    }
    if (step === 2) {
      return workspacePath.trim() !== '' && pathStatus !== 'error' && pathStatus !== 'checking';
    }
    return true;
  }, [step, llmConfig, workspacePath, pathStatus]);

  const handleComplete = useCallback(async () => {
    if (connectingRef.current) return;
    connectingRef.current = true;
    setSaving(true);
    setSaveError('');
    try {
      const providerPayload = {
        provider: llmConfig.provider,
        name: llmConfig.model.trim(),
        model: llmConfig.model.trim(),
        apiKey: llmConfig.apiKey.trim(),
        baseUrl: llmConfig.baseUrl.trim() || DEFAULT_MODEL_BASE_URL[llmConfig.provider],
        ...(llmConfig.contextWindowSize !== '' && Number(llmConfig.contextWindowSize) > 0
          ? { contextWindowSize: Number(llmConfig.contextWindowSize) }
          : {}),
      };
      const createRes = await createModelProvider(providerPayload);
      const modelId = createRes?.data?.id || llmConfig.model.trim();

      await settingsApiFetch('/settings/general', {
        method: 'PUT',
        body: JSON.stringify({ key: 'model', value: modelId }),
      });
      await settingsApiFetch('/settings/general', {
        method: 'PUT',
        body: JSON.stringify({ key: 'agentModel', value: modelId }),
      });
      await settingsApiFetch('/settings/general', {
        method: 'PUT',
        body: JSON.stringify({ key: 'initialized', value: true }),
      });

      await useSessionStore.getState().createAndConnect(
        workspacePath.trim() ? { workspace: workspacePath.trim() } : {},
      );

      onComplete();
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'Failed to save configuration');
    } finally {
      setSaving(false);
      connectingRef.current = false;
    }
  }, [llmConfig, workspacePath, onComplete]);

  const handleNext = useCallback(() => {
    if (step === 3) {
      void handleComplete();
      return;
    }
    setStep((s) => Math.min(s + 1, 3) as Step);
  }, [step, handleComplete]);

  const handlePrev = useCallback(() => {
    setStep((s) => Math.max(s - 1, 0) as Step);
  }, []);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: pal.overlayBase }}
    >
      {/* 水墨意境遮罩层 */}
      <div className="pointer-events-none absolute inset-0" style={{ background: pal.overlayWash }} />

      {/* 顶部右侧：主题切换 + 跳过 */}
      <div className="absolute right-5 top-5 z-10 flex items-center gap-2">
        <button
          type="button"
          onClick={toggle}
          className="inline-flex items-center gap-1.5 rounded-full px-3 py-1.5 text-xs font-medium transition-all"
          style={{ background: pal.headerBg, border: `1px solid ${pal.cardBorder}`, color: pal.inkSoft }}
          title={resolved === 'dark' ? t('onboarding.theme.light') : t('onboarding.theme.dark')}
        >
          {resolved === 'dark' ? <Sun size={13} /> : <Moon size={13} />}
          {resolved === 'dark' ? t('onboarding.theme.light') : t('onboarding.theme.dark')}
        </button>
        {onSkip && (
          <button
            type="button"
            onClick={onSkip}
            className="inline-flex h-8 w-8 items-center justify-center rounded-full transition-all"
            style={{ background: pal.headerBg, border: `1px solid ${pal.cardBorder}`, color: pal.inkMuted }}
            title={t('onboarding.common.skip')}
          >
            <X size={15} />
          </button>
        )}
      </div>

      {/* Card */}
      <div
        className="relative w-full max-w-[520px] overflow-hidden rounded-3xl"
        style={{ background: pal.cardBg, border: `1px solid ${pal.cardBorder}`, boxShadow: pal.cardShadow }}
      >
        {/* Header */}
        <div
          className="flex items-center justify-between px-6 py-4"
          style={{ borderBottom: `1px solid ${pal.cardBorder}`, background: pal.headerBg }}
        >
          <div className="flex items-center gap-2.5">
            <SealLogo size={28} pal={pal} />
            <span className="text-sm font-semibold" style={{ color: pal.ink }}>
              {t('onboarding.appName')}
            </span>
          </div>
          <span className="text-xs font-medium" style={{ color: pal.inkMuted }}>
            {t('onboarding.common.step', { current: step + 1, total: TOTAL_STEPS })}
          </span>
        </div>

        {/* Body */}
        <div>
          {step === 0 && <WelcomeStep onNext={handleNext} pal={pal} />}
          {step === 1 && (
            <LlmConfigStep
              config={llmConfig}
              onChange={updateLlmConfig}
              testStatus={testStatus}
              testMessage={testMessage}
              onTest={handleTestConnection}
              pal={pal}
            />
          )}
          {step === 2 && (
            <WorkspaceStep
              workspacePath={workspacePath}
              onChange={handleWorkspacePathChange}
              pathStatus={pathStatus}
              pathError={pathError}
              pal={pal}
            />
          )}
          {step === 3 && (
            <CompleteStep
              config={llmConfig}
              workspacePath={workspacePath}
              saving={saving}
              error={saveError}
              pal={pal}
            />
          )}
        </div>

        {/* Footer */}
        {step > 0 && (
          <div
            className="flex items-center justify-between px-6 py-4"
            style={{ borderTop: `1px solid ${pal.cardBorder}`, background: pal.headerBg }}
          >
            <button
              type="button"
              onClick={handlePrev}
              disabled={saving}
              className="inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-40"
              style={{ color: pal.inkSoft }}
            >
              <ArrowLeft size={15} />
              {t('onboarding.common.prev')}
            </button>
            <button
              type="button"
              onClick={handleNext}
              disabled={!canProceed || saving}
              className="inline-flex items-center gap-1.5 rounded-xl px-6 py-2 text-sm font-semibold transition-all duration-200 hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0"
              style={{ background: pal.btnBg, color: pal.btnFg, boxShadow: `0 10px 24px -10px ${pal.jade}99` }}
            >
              {saving ? (
                <>
                  <Loader2 size={15} className="animate-spin" />
                  {t('onboarding.complete.saving')}
                </>
              ) : step === 3 ? (
                <>
                  <Sparkles size={15} />
                  {t('onboarding.complete.start')}
                </>
              ) : (
                <>
                  {t('onboarding.common.next')}
                  <ArrowRight size={15} />
                </>
              )}
            </button>
          </div>
        )}

        {/* Step nav at bottom for welcome step (no footer there) */}
        {step === 0 && (
          <div className="flex justify-center pb-6">
            <StepNav current={step} pal={pal} t={t} />
          </div>
        )}
      </div>
    </div>
  );
}

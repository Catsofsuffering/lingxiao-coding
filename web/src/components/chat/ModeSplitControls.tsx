import { useEffect, useState } from 'react';
import { AlertTriangle, BrainCircuit, CheckCircle2, ChevronDown, ChevronUp, Loader2, Route, User, Users, Zap } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { acpClient } from '../../api/AcpClient';
import { applyRuntimeSnapshotFromRpcResult, useSessionStore } from '../../stores/sessionStore';

type CollaborationMode = 'solo' | 'team';
type AutonomyMode = 'review_first' | 'balanced' | 'autonomous';
type AutonomyLifecyclePhase = 'bootstrap' | 'active' | 'recovery' | 'stable';
// UI 仅暴露 auto/direct/delegate；hybrid 不再作为用户选项（auto 运行时默认即解析为 hybrid，二者语义重叠）。
// 后端 preference 类型仍保留 hybrid 做向后兼容，老快照里的 hybrid 偏好由 normalizeRoutePreference 容错回退为 auto。
type RoutePreference = 'auto' | 'direct' | 'delegate';
type ActualRouteMode = RoutePreference | 'hybrid' | 'unknown';

const COLLABORATION_MODES: CollaborationMode[] = ['solo', 'team'];
const AUTONOMY_MODES: AutonomyMode[] = ['review_first', 'balanced', 'autonomous'];
const ROUTE_PREFERENCES: RoutePreference[] = ['auto', 'direct', 'delegate'];
type BusyKey = `collab:${CollaborationMode}` | `route:${RoutePreference}` | `autonomy:${AutonomyMode}`;
type NoticeTone = 'pending' | 'success' | 'error';
type ModeNotice = { id: number; tone: NoticeTone; message: string };

const MODE_SPLIT_COLLAPSED_KEY = 'lingxiao:mode-split-collapsed';

function readInitialCollapsed(): boolean {
  if (typeof window === 'undefined') return true;
  try {
    return window.localStorage.getItem(MODE_SPLIT_COLLAPSED_KEY) !== 'false';
  } catch {
    return true;
  }
}

function normalizeCollaborationMode(value: unknown): CollaborationMode {
  return value === 'team' ? 'team' : 'solo';
}

function normalizeRoutePreference(value: unknown): RoutePreference {
  return ROUTE_PREFERENCES.includes(value as RoutePreference) ? value as RoutePreference : 'auto';
}

function normalizeAutonomyMode(value: unknown): AutonomyMode {
  return AUTONOMY_MODES.includes(value as AutonomyMode) ? value as AutonomyMode : 'balanced';
}

function normalizeAutonomyLifecyclePhase(value: unknown): AutonomyLifecyclePhase {
  return value === 'active' || value === 'recovery' || value === 'stable' ? value : 'bootstrap';
}

function normalizeActualRouteMode(value: unknown): ActualRouteMode {
  return value === 'direct' || value === 'hybrid' || value === 'delegate' || value === 'auto' || value === 'unknown'
    ? (value as ActualRouteMode)
    : 'direct';
}

function segmentClass(active: boolean, loading: boolean): string {
  return active
    ? `border-accent-brand/70 bg-accent-brand/15 text-accent-brand shadow-[0_0_16px_rgba(69,190,255,0.22)] ${loading ? 'animate-pulse' : 'scale-[1.015]'}`
    : 'border-transparent text-text-tertiary hover:border-border-default hover:bg-bg-tertiary hover:text-text-primary';
}

function noticeClass(tone: NoticeTone): string {
  if (tone === 'error') return 'border-accent-red/30 bg-accent-red/10 text-accent-red';
  if (tone === 'success') return 'border-accent-green/30 bg-accent-green/10 text-accent-green';
  return 'border-accent-brand/30 bg-accent-brand/10 text-accent-brand';
}

export function ModeSplitControls() {
  const { t } = useTranslation();
  const sessionId = useSessionStore((s) => s.sessionId);
  const isConnected = useSessionStore((s) => s.isConnected);
  const modes = useSessionStore((s) => s.runtimeSnapshot?.modes);
  const [busy, setBusy] = useState<BusyKey | null>(null);
  const [notice, setNotice] = useState<ModeNotice | null>(null);
  const [collapsed, setCollapsed] = useState(readInitialCollapsed);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(MODE_SPLIT_COLLAPSED_KEY, collapsed ? 'true' : 'false');
    } catch {
      // Ignore storage failures in privacy mode or restricted WebViews; the control still works for the current render.
    }
  }, [collapsed]);

  useEffect(() => {
    if (!notice || notice.tone === 'pending') return undefined;
    const timer = window.setTimeout(() => setNotice((current) => current?.id === notice.id ? null : current), 1800);
    return () => window.clearTimeout(timer);
  }, [notice]);

  if (!sessionId || !isConnected) return null;

  const collaborationMode = normalizeCollaborationMode(modes?.collaboration.mode);
  const autonomyMode = normalizeAutonomyMode(modes?.autonomy);
  const intentProfile = modes?.intentProfile ?? null;
  const lastDecisionTrace = modes?.lastDecisionTrace ?? null;
  const lifecyclePhase = normalizeAutonomyLifecyclePhase(modes?.lifecyclePhase);
  const modeGeneration = modes?.modeGeneration ?? 1;
  const routePreference = normalizeRoutePreference(modes?.route.preference);
  const actualRouteMode = normalizeActualRouteMode(modes?.route.mode);
  const collaborationLabels: Record<CollaborationMode, string> = {
    solo: t('chat.modeSplit.solo'),
    team: t('chat.modeSplit.team'),
  };
  const routeLabels: Record<RoutePreference, string> = {
    auto: t('chat.modeSplit.auto'),
    direct: t('chat.modeSplit.direct'),
    delegate: t('chat.modeSplit.delegate'),
  };
  const autonomyLabels: Record<AutonomyMode, string> = {
    review_first: t('chat.modeSplit.autonomyReviewFirst', { defaultValue: 'Review First' }),
    balanced: t('chat.modeSplit.autonomyBalanced', { defaultValue: 'Balanced' }),
    autonomous: t('chat.modeSplit.autonomyAutonomous', { defaultValue: 'Auto-Advance' }),
  };
  const formatIntentPart = (kind: 'primary' | 'phase' | 'scope', value: string | undefined) => {
    if (!value) return t('chat.modeSplit.intentUnknown', { defaultValue: 'Unknown' });
    return t(`chat.modeSplit.intent.${kind}.${value}`, { defaultValue: value.replace(/_/g, ' ') });
  };
  const decisionLabels: Record<string, string> = {
    allow: t('chat.modeSplit.decisionAllow', { defaultValue: 'Allow' }),
    blocked: t('chat.modeSplit.decisionBlocked', { defaultValue: 'Blocked' }),
    confirmation_required: t('chat.modeSplit.decisionConfirmationRequired', { defaultValue: 'Confirm' }),
  };
  const actualRouteLabels: Record<ActualRouteMode, string> = {
    auto: t('chat.modeSplit.auto'),
    direct: t('chat.modeSplit.direct'),
    delegate: t('chat.modeSplit.delegate'),
    hybrid: t('chat.modeSplit.hybrid'),
    unknown: '—',
  };
  const routeHints: Record<RoutePreference, string> = {
    auto: t('chat.modeSplit.autoHint'),
    direct: t('chat.modeSplit.directHint'),
    delegate: t('chat.modeSplit.delegateHint'),
  };
  const collaborationHints: Record<CollaborationMode, string> = {
    solo: t('chat.modeSplit.soloHint'),
    team: t('chat.modeSplit.teamHint'),
  };
  const autonomyHints: Record<AutonomyMode, string> = {
    review_first: t('chat.modeSplit.autonomyReviewFirstHint', { defaultValue: 'Ask before side effects even when the capability profile grants them.' }),
    balanced: t('chat.modeSplit.autonomyBalancedHint', { defaultValue: 'Auto-run low-risk granted actions; ask before medium/high-risk or gated actions.' }),
    autonomous: t('chat.modeSplit.autonomyAutonomousHint', { defaultValue: 'Advance proactively inside the granted capability profile; hard gates and permissions still apply.' }),
  };
  const lifecycleLabel = t(`chat.modeSplit.lifecycle.${lifecyclePhase}`, { defaultValue: lifecyclePhase });
  const effectivePolicySummary = t('chat.modeSplit.effectivePolicySummary', {
    defaultValue: `${autonomyLabels[autonomyMode]} · phase=${lifecyclePhase} · gen=${modeGeneration}`,
    mode: autonomyLabels[autonomyMode],
    phase: lifecycleLabel,
    generation: modeGeneration,
  });
  const setModeNotice = (tone: NoticeTone, message: string) => {
    setNotice({ id: Date.now(), tone, message });
  };

  // 钉死 direct/delegate 时，实际运行 mode 偏离才提示；auto 偏好下 mode 经常变（hybrid/delegate），不提示，避免"当前混合"式困惑。
  const routeDeviation =
    routePreference !== 'auto' && actualRouteMode !== 'unknown' && actualRouteMode !== routePreference;

  const applyCollaboration = async (mode: CollaborationMode) => {
    if (busy || mode === collaborationMode) return;
    setBusy(`collab:${mode}`);
    setModeNotice('pending', t('chat.modeSplit.pendingCollaboration', { mode: collaborationLabels[mode] }));
    try {
      const result = await acpClient.sendJsonRpc('session/set_collaboration_mode', { mode });
      if (!applyRuntimeSnapshotFromRpcResult(result, sessionId)) {
        setModeNotice('error', t('chat.modeSplit.noSnapshot'));
        return;
      }
      setModeNotice('success', t('chat.modeSplit.collaborationSuccess', { mode: collaborationLabels[mode] }));
    } catch {
      setModeNotice('error', t('chat.modeSplit.errorCollaboration', { mode: collaborationLabels[mode] }));
    } finally {
      setBusy(null);
    }
  };

  const applyRoute = async (mode: RoutePreference) => {
    if (busy || mode === routePreference) return;
    setBusy(`route:${mode}`);
    setModeNotice('pending', t('chat.modeSplit.pendingRoute', { mode: routeLabels[mode] }));
    try {
      const result = await acpClient.sendJsonRpc('session/set_execution_route', { mode });
      if (!applyRuntimeSnapshotFromRpcResult(result, sessionId)) {
        setModeNotice('error', t('chat.modeSplit.noSnapshot'));
        return;
      }
      setModeNotice('success', t('chat.modeSplit.routeSuccess', { mode: routeLabels[mode] }));
    } catch {
      setModeNotice('error', t('chat.modeSplit.errorRoute', { mode: routeLabels[mode] }));
    } finally {
      setBusy(null);
    }
  };

  const applyAutonomy = async (mode: AutonomyMode) => {
    if (busy || mode === autonomyMode) return;
    setBusy(`autonomy:${mode}`);
    setModeNotice('pending', t('chat.modeSplit.pendingAutonomy', { defaultValue: `Switching autonomy to ${autonomyLabels[mode]}...`, mode: autonomyLabels[mode] }));
    try {
      const result = await acpClient.setAutonomyMode(mode, { lifecyclePhase, updatedBy: 'web', reason: 'web_mode_split_controls' });
      if (!applyRuntimeSnapshotFromRpcResult(result, sessionId)) {
        setModeNotice('error', t('chat.modeSplit.noSnapshot'));
        return;
      }
      setModeNotice('success', t('chat.modeSplit.autonomySuccess', { defaultValue: `Autonomy set to ${autonomyLabels[mode]}.`, mode: autonomyLabels[mode] }));
    } catch {
      setModeNotice('error', t('chat.modeSplit.errorAutonomy', { defaultValue: `Failed to switch autonomy to ${autonomyLabels[mode]}.`, mode: autonomyLabels[mode] }));
    } finally {
      setBusy(null);
    }
  };

  const executionPolicySummary = `${routeLabels[routePreference]} · ${actualRouteLabels[actualRouteMode]} · ${autonomyLabels[autonomyMode]}`;
  const currentSummary = `${collaborationLabels[collaborationMode]} · ${executionPolicySummary}`;
  const intentSummary = intentProfile
    ? `${formatIntentPart('primary', intentProfile.primaryIntent)} · ${formatIntentPart('phase', intentProfile.phase)} · ${formatIntentPart('scope', intentProfile.scope.kind)}`
    : t('chat.modeSplit.intentUnknown', { defaultValue: 'Unknown' });
  const intentConfidence = intentProfile ? `${Math.round(intentProfile.confidence * 100)}%` : '—';
  const intentTooltip = intentProfile
    ? [
        intentSummary,
        `${t('chat.modeSplit.grants', { defaultValue: 'Grants' })}: ${intentProfile.grants.join(', ') || '—'}`,
        `${t('chat.modeSplit.denies', { defaultValue: 'Denies' })}: ${intentProfile.denies.join(', ') || '—'}`,
        `${t('chat.modeSplit.requiredGates', { defaultValue: 'Required gates' })}: ${intentProfile.requiredGates.join(', ') || '—'}`,
        intentProfile.reason,
      ].join('\n')
    : t('chat.modeSplit.intentUnknownHint', { defaultValue: 'No capability intent profile recorded for the current turn yet.' });
  const gateSummary = lastDecisionTrace
    ? `${decisionLabels[lastDecisionTrace.gateResult] ?? lastDecisionTrace.gateResult} · ${lastDecisionTrace.toolName}`
    : t('chat.modeSplit.noDecision', { defaultValue: 'No gate decision yet' });
  const decisionTooltip = lastDecisionTrace
    ? `${lastDecisionTrace.toolName}\n${lastDecisionTrace.decision?.kind ?? lastDecisionTrace.gateResult}\n${lastDecisionTrace.decision?.reason ?? ''}`
    : gateSummary;
  const toggleLabel = collapsed ? t('chat.modeSplit.expand') : t('chat.modeSplit.collapse');

  if (collapsed) {
    return (
      <div className="mb-2 flex w-full justify-start">
        <button
          type="button"
          onClick={() => setCollapsed(false)}
          aria-expanded={false}
          aria-label={toggleLabel}
          title={`${toggleLabel} · ${currentSummary}`}
          className="group inline-flex max-w-full items-center gap-2 rounded-lg border border-border-muted bg-bg-primary/72 px-2.5 py-1.5 text-[11px] font-medium text-text-secondary shadow-[0_8px_22px_rgba(0,0,0,0.10)] backdrop-blur-xl transition-all duration-200 hover:border-border-default hover:bg-bg-secondary/80 hover:text-text-primary"
        >
          <Route size={13} className="shrink-0 text-accent-brand" />
          <span className="shrink-0 font-semibold">{t('chat.modeSplit.title')}</span>
          <span className="min-w-0 truncate text-text-tertiary">{currentSummary} · {t('chat.modeSplit.intentLabel', { defaultValue: 'Intent' })}: {intentSummary}</span>
          {notice && (
            <span
              role={notice.tone === 'error' ? 'alert' : 'status'}
              className={`hidden max-w-[220px] items-center gap-1 rounded-md border px-1.5 py-0.5 text-[10px] sm:inline-flex ${noticeClass(notice.tone)}`}
            >
              {notice.tone === 'pending' ? <Loader2 size={10} className="animate-spin" /> : notice.tone === 'success' ? <CheckCircle2 size={10} /> : <AlertTriangle size={10} />}
              <span className="truncate">{notice.message}</span>
            </span>
          )}
          {busy && <Loader2 size={12} className="shrink-0 animate-spin text-accent-brand" />}
          <ChevronDown size={13} className="shrink-0 text-text-tertiary transition-transform duration-200 group-hover:translate-y-0.5" />
        </button>
      </div>
    );
  }

  return (
    <div className="mb-2 w-full rounded-xl border border-border-muted bg-bg-primary/80 p-2.5 shadow-[0_10px_28px_rgba(0,0,0,0.12)] backdrop-blur-xl transition-all duration-200 hover:border-border-default">
      <div className="flex flex-col gap-2 border-b border-border-muted/70 pb-2 md:flex-row md:items-center md:justify-between">
        <div className="flex min-w-0 items-start gap-2">
          <div className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-accent-brand/25 bg-accent-brand/10 text-accent-brand">
            <Route size={14} />
          </div>
          <div className="min-w-0">
            <div
              className="flex flex-wrap items-center gap-1.5 text-[11px]"
              title={t('chat.modeSplit.permissionHint')}
            >
              <span className="mr-0.5 font-semibold uppercase tracking-normal text-text-primary">{t('chat.modeSplit.title')}</span>
              <span className="inline-flex h-6 items-center rounded-full border border-accent-brand/35 bg-accent-brand/10 px-2 font-medium text-accent-brand">
                {collaborationLabels[collaborationMode]}
              </span>
              <span className="inline-flex h-6 items-center rounded-full border border-border-muted bg-bg-secondary/70 px-2 font-medium text-text-secondary">
                {routeLabels[routePreference]}
              </span>
              <span className="inline-flex h-6 items-center rounded-full border border-border-muted bg-bg-secondary/70 px-2 font-medium text-text-secondary">
                {autonomyLabels[autonomyMode]}
              </span>
              {routeDeviation && (
                <span
                  className="inline-flex h-6 items-center gap-1 rounded-full border border-border-default bg-bg-tertiary px-2 font-medium text-text-secondary"
                  title={t('chat.modeSplit.routeDeviation', { preference: routeLabels[routePreference], actual: actualRouteLabels[actualRouteMode] })}
                >
                  <Zap size={10} className="text-accent-brand" />
                  <span>{actualRouteLabels[actualRouteMode]}</span>
                </span>
              )}
            </div>
            <div className="mt-1 hidden max-w-[760px] truncate text-[11px] text-text-tertiary md:block">
              {routeHints[routePreference]} · {autonomyHints[autonomyMode]}
            </div>
          </div>
        </div>

        <div className="flex shrink-0 items-center gap-2 self-start md:self-center">
          {notice && (
            <div
              role={notice.tone === 'error' ? 'alert' : 'status'}
              className={`inline-flex max-w-[280px] items-center gap-1.5 rounded-md border px-2 py-1 text-[11px] font-medium transition-all duration-200 ${noticeClass(notice.tone)}`}
            >
              {notice.tone === 'pending' ? <Loader2 size={12} className="animate-spin" /> : notice.tone === 'success' ? <CheckCircle2 size={12} /> : <AlertTriangle size={12} />}
              <span className="truncate">{notice.message}</span>
            </div>
          )}
          <button
            type="button"
            onClick={() => setCollapsed(true)}
            aria-expanded={true}
            aria-label={toggleLabel}
            title={toggleLabel}
            className="inline-flex h-7 shrink-0 items-center gap-1 rounded-md border border-border-muted bg-bg-secondary/70 px-2 text-[11px] font-medium text-text-tertiary transition-all duration-200 hover:border-border-default hover:bg-bg-tertiary hover:text-text-primary"
          >
            <ChevronUp size={12} />
            <span className="hidden sm:inline">{t('chat.modeSplit.collapse')}</span>
          </button>
        </div>
      </div>

      <div className="mt-2 grid gap-2 lg:grid-cols-[minmax(170px,0.7fr)_minmax(360px,1.35fr)_minmax(240px,0.95fr)]">
        <section
          className="rounded-lg border border-border-muted bg-bg-secondary/45 p-2 transition-colors duration-200 hover:border-border-default"
          title={t('chat.modeSplit.collaborationTitle', { mode: collaborationLabels[collaborationMode] })}
        >
          <div className="flex items-center gap-1.5 px-1 text-[10px] font-medium uppercase tracking-normal text-text-tertiary">
            <Users size={11} />
            <span>{t('chat.modeSplit.collaborationLabel')}</span>
          </div>
          <div className="mt-2 grid grid-cols-2 gap-1">
            {COLLABORATION_MODES.map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => void applyCollaboration(mode)}
                disabled={Boolean(busy)}
                aria-label={t('chat.modeSplit.switchTo', { mode: collaborationLabels[mode] })}
                title={collaborationHints[mode]}
                aria-pressed={mode === collaborationMode}
                className={`inline-flex h-8 min-w-0 items-center justify-center gap-1 rounded-[6px] border px-2 text-[11px] font-medium transition-all duration-200 ease-out ${segmentClass(mode === collaborationMode, busy === `collab:${mode}`)} ${busy ? 'opacity-70' : ''}`}
              >
                {busy === `collab:${mode}` ? <Loader2 size={12} className="animate-spin" /> : mode === 'solo' ? <User size={12} /> : <Users size={12} />}
                <span className="truncate">{collaborationLabels[mode]}</span>
              </button>
            ))}
          </div>
        </section>

        <section
          className="rounded-lg border border-border-muted bg-bg-secondary/45 p-2 transition-colors duration-200 hover:border-border-default"
          title={`${executionPolicySummary}\n${routeHints[routePreference]}\n${autonomyHints[autonomyMode]}\n${effectivePolicySummary}`}
        >
          <div className="grid gap-2 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
            <div className="min-w-0">
              <div className="flex items-center justify-between gap-2 px-1 text-[10px] font-medium uppercase tracking-normal text-text-tertiary">
                <span className="inline-flex items-center gap-1.5">
                  <Route size={11} />
                  <span>{t('chat.modeSplit.executionPolicyLabel', { defaultValue: '执行策略' })}</span>
                </span>
                {routeDeviation && (
                  <span
                    className="inline-flex shrink-0 items-center gap-0.5 rounded-full border border-border-default bg-bg-tertiary px-1.5 py-0.5 text-[10px] font-medium normal-case text-text-secondary"
                    title={t('chat.modeSplit.routeDeviation', { preference: routeLabels[routePreference], actual: actualRouteLabels[actualRouteMode] })}
                  >
                    <Zap size={10} className="text-accent-brand" />
                    <span>{actualRouteLabels[actualRouteMode]}</span>
                  </span>
                )}
              </div>
              <div className="mt-2 grid grid-cols-3 gap-1">
                {ROUTE_PREFERENCES.map((mode) => (
                  <button
                    key={`route:${mode}`}
                    type="button"
                    onClick={() => void applyRoute(mode)}
                    disabled={Boolean(busy)}
                    aria-label={t('chat.modeSplit.switchTo', { mode: routeLabels[mode] })}
                    title={routeHints[mode]}
                    aria-pressed={mode === routePreference}
                    className={`inline-flex h-8 min-w-0 items-center justify-center rounded-[6px] border px-2 text-[11px] font-medium transition-all duration-200 ease-out ${segmentClass(mode === routePreference, busy === `route:${mode}`)} ${busy ? 'opacity-70' : ''}`}
                  >
                    {busy === `route:${mode}` ? <Loader2 size={12} className="animate-spin" /> : <span className="truncate">{routeLabels[mode]}</span>}
                  </button>
                ))}
              </div>
            </div>

            <div className="min-w-0 border-t border-border-muted/60 pt-2 xl:border-l xl:border-t-0 xl:pl-2 xl:pt-0">
              <div className="flex items-center gap-1.5 px-1 text-[10px] font-medium uppercase tracking-normal text-text-tertiary">
                <Zap size={11} />
                <span>{t('chat.modeSplit.autonomyLabel', { defaultValue: '自主度' })}</span>
              </div>
              <div className="mt-2 grid grid-cols-3 gap-1">
                {AUTONOMY_MODES.map((mode) => (
                  <button
                    key={`autonomy:${mode}`}
                    type="button"
                    onClick={() => void applyAutonomy(mode)}
                    disabled={Boolean(busy)}
                    aria-label={t('chat.modeSplit.switchTo', { mode: autonomyLabels[mode] })}
                    title={autonomyHints[mode]}
                    aria-pressed={mode === autonomyMode}
                    className={`inline-flex h-8 min-w-0 items-center justify-center rounded-[6px] border px-2 text-[11px] font-medium transition-all duration-200 ease-out ${segmentClass(mode === autonomyMode, busy === `autonomy:${mode}`)} ${busy ? 'opacity-70' : ''}`}
                  >
                    {busy === `autonomy:${mode}` ? <Loader2 size={12} className="animate-spin" /> : <span className="truncate">{autonomyLabels[mode]}</span>}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section
          className="rounded-lg border border-border-muted bg-bg-secondary/45 p-2 transition-colors duration-200 hover:border-border-default"
          title={`${intentTooltip}\n\n${decisionTooltip}`}
        >
          <div className="flex items-center gap-1.5 px-1 text-[10px] font-medium uppercase tracking-normal text-text-tertiary">
            <BrainCircuit size={11} />
            <span>{t('chat.modeSplit.intentLabel', { defaultValue: 'Intent' })}</span>
          </div>
          <div className="mt-2 flex min-w-0 flex-col gap-1">
            <span className="inline-flex h-8 min-w-0 items-center justify-between gap-2 rounded-[6px] border border-accent-brand/40 bg-accent-brand/10 px-2 text-[11px] font-medium text-accent-brand">
              <span className="min-w-0 truncate">{intentSummary}</span>
              <span className="shrink-0 text-[10px] text-text-tertiary">{intentConfidence}</span>
            </span>
            <span className={`inline-flex h-8 min-w-0 items-center gap-1 rounded-[6px] border px-2 text-[11px] font-medium ${lastDecisionTrace?.gateResult === 'blocked' ? 'border-accent-red/40 bg-accent-red/10 text-accent-red' : lastDecisionTrace?.gateResult === 'confirmation_required' ? 'border-accent-orange/40 bg-accent-orange/10 text-accent-orange' : 'border-border-default bg-bg-tertiary text-text-secondary'}`}>
              <span className="shrink-0 text-text-tertiary">{t('chat.modeSplit.gateLabel', { defaultValue: 'Gate' })}</span>
              <span className="min-w-0 truncate">{gateSummary}</span>
            </span>
          </div>
        </section>
      </div>
    </div>
  );
}

import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Activity, CheckCircle2, Clock, Code2, FileEdit, GitCommitHorizontal,
  PlayCircle, Shell, Trash2, User, Wrench, XCircle,
} from 'lucide-react';
import { useAgentActivityStore, type AgentActivityEvent } from '../../stores/agentActivityStore';
import { useSessionStore } from '../../stores/sessionStore';

function formatTime(ts: number): string {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatRelative(ts: number): string {
  const diff = Math.max(0, Date.now() - ts);
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  return `${Math.floor(diff / 86_400_000)}d ago`;
}

function iconForTool(event: AgentActivityEvent) {
  const name = event.toolName.toLowerCase();
  if (name === 'shell' || name.includes('terminal')) return <Shell size={14} />;
  if (name === 'file_create' || name === 'structured_patch' || name.includes('file')) return <FileEdit size={14} />;
  if (name === 'git' || name.includes('git')) return <GitCommitHorizontal size={14} />;
  if (event.toolTier === 'execute') return <PlayCircle size={14} />;
  if (event.toolTier === 'write') return <Code2 size={14} />;
  return <Wrench size={14} />;
}

function StatCard({ label, value, accent }: { label: string; value: string | number; accent?: string }) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-border-muted bg-bg-card/60 px-4 py-3">
      <span className="text-[10px] uppercase tracking-wider text-text-tertiary">{label}</span>
      <span className={`text-lg font-semibold ${accent ?? 'text-text-primary'}`}>{value}</span>
    </div>
  );
}

function ActivityRow({ event }: { event: AgentActivityEvent }) {
  const firstFile = event.files?.[0];
  const moreFiles = event.files && event.files.length > 1 ? event.files.length - 1 : 0;
  const target = event.target || firstFile;
  return (
    <div className="group flex gap-3 rounded-lg border border-border-muted bg-bg-card/40 p-3 hover:border-accent-brand/40 hover:bg-bg-card/70 transition-colors">
      <div className={`mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full border ${event.success ? 'border-green-500/30 bg-green-500/10 text-green-400' : 'border-red-500/30 bg-red-500/10 text-red-400'}`}>
        {event.success ? <CheckCircle2 size={15} /> : <XCircle size={15} />}
      </div>
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-2">
          <span className="inline-flex items-center gap-1 rounded-md bg-bg-muted px-2 py-0.5 text-xs font-medium text-text-primary">
            {iconForTool(event)}
            {event.toolName}
          </span>
          {event.action && <span className="text-[11px] text-text-tertiary">{event.action}</span>}
          <span className="text-[11px] text-text-tertiary">{formatTime(event.timestamp)} · {formatRelative(event.timestamp)}</span>
        </div>
        <div className="mt-1 truncate text-sm text-text-primary" title={event.summary || event.command || target || event.toolName}>
          {event.summary || event.command || target || event.toolName}
        </div>
        {target && (
          <div className="mt-1 truncate font-mono text-[11px] text-text-tertiary" title={target}>
            {target}{moreFiles > 0 ? ` +${moreFiles} files` : ''}
          </div>
        )}
        {event.error && <div className="mt-1 truncate text-xs text-red-300" title={event.error}>{event.error}</div>}
      </div>
      <div className="flex shrink-0 flex-col items-end justify-center gap-1 text-[11px] text-text-tertiary">
        <span className="inline-flex items-center gap-1"><User size={11} />{event.agentName}</span>
        {event.taskId && <span className="font-mono">{event.taskId}</span>}
      </div>
    </div>
  );
}

function AgentStats({ events }: { events: AgentActivityEvent[] }) {
  const byAgent = useMemo(() => {
    const map = new Map<string, { name: string; total: number; failed: number; write: number; execute: number }>();
    for (const event of events) {
      const key = event.agentId || event.agentName;
      const entry = map.get(key) ?? { name: event.agentName, total: 0, failed: 0, write: 0, execute: 0 };
      entry.total += 1;
      if (!event.success) entry.failed += 1;
      if (event.toolTier === 'write') entry.write += 1;
      if (event.toolTier === 'execute') entry.execute += 1;
      map.set(key, entry);
    }
    return [...map.entries()].sort((a, b) => b[1].total - a[1].total);
  }, [events]);

  if (byAgent.length === 0) {
    return <div className="text-center text-xs text-text-tertiary py-6">No agent activity yet</div>;
  }

  return (
    <div className="space-y-2 overflow-auto">
      <div className="flex items-center gap-2 px-1 pb-1">
        <User size={12} className="text-text-tertiary" />
        <h3 className="text-xs font-semibold uppercase tracking-wider text-text-secondary">Agents</h3>
      </div>
      {byAgent.map(([id, stat]) => (
        <div key={id} className="rounded-lg border border-border-muted bg-bg-card/40 p-3">
          <div className="truncate text-sm font-medium text-text-primary" title={stat.name}>{stat.name}</div>
          <div className="mt-2 grid grid-cols-4 gap-1 text-center text-[11px]">
            <span className="rounded bg-bg-muted py-1 text-text-secondary">{stat.total}<br />total</span>
            <span className="rounded bg-bg-muted py-1 text-text-secondary">{stat.write}<br />write</span>
            <span className="rounded bg-bg-muted py-1 text-text-secondary">{stat.execute}<br />exec</span>
            <span className={`rounded bg-bg-muted py-1 ${stat.failed ? 'text-red-300' : 'text-green-300'}`}>{stat.failed}<br />fail</span>
          </div>
        </div>
      ))}
    </div>
  );
}

export default function AgentActivityView() {
  const { t } = useTranslation();
  const sessionId = useSessionStore((state) => state.sessionId);
  const allEvents = useAgentActivityStore((state) => state.events);
  const clear = useAgentActivityStore((state) => state.clear);

  const events = useMemo(() => {
    return allEvents
      .filter((event) => !sessionId || event.sessionId === sessionId)
      .slice()
      .sort((a, b) => b.timestamp - a.timestamp);
  }, [allEvents, sessionId]);

  const stats = useMemo(() => {
    const total = events.length;
    const failed = events.filter((event) => !event.success).length;
    const writes = events.filter((event) => event.toolTier === 'write').length;
    const executes = events.filter((event) => event.toolTier === 'execute').length;
    const uniqueAgents = new Set(events.map((event) => event.agentId || event.agentName)).size;
    const successRate = total > 0 ? Math.round(((total - failed) / total) * 100) : 100;
    return { total, failed, writes, executes, uniqueAgents, successRate };
  }, [events]);

  return (
    <div className="codex-chat-surface flex h-full flex-col overflow-hidden bg-bg-primary">
      <div className="lingxiao-cloud-line codex-topbar flex shrink-0 items-center justify-between gap-3 border-b border-border-muted px-5 py-3 backdrop-blur-2xl">
        <div className="flex items-center gap-2">
          <Activity size={16} className="text-accent-brand" />
          <span className="text-sm font-semibold text-text-primary">{t('sidebar.agentActivity', 'Agent Activity')}</span>
          <span className="rounded-full bg-bg-muted px-2 py-0.5 text-[11px] text-text-tertiary">{events.length}</span>
        </div>
        {events.length > 0 && (
          <button
            onClick={clear}
            className="inline-flex items-center gap-1 rounded-md border border-border-muted px-2 py-1 text-xs text-text-secondary hover:border-red-400/50 hover:text-red-300"
            title="Clear activity log"
          >
            <Trash2 size={12} />
            Clear
          </button>
        )}
      </div>

      <div className="flex shrink-0 items-center gap-2 overflow-x-auto border-b border-border-muted px-5 py-3">
        <StatCard label="Total" value={stats.total} />
        <StatCard label="Writes" value={stats.writes} accent="text-accent-brand" />
        <StatCard label="Executes" value={stats.executes} />
        <StatCard label="Success" value={`${stats.successRate}%`} accent={stats.failed ? 'text-accent-yellow' : 'text-green-400'} />
        <StatCard label="Agents" value={stats.uniqueAgents} />
      </div>

      <div className="flex min-h-0 flex-1 overflow-hidden">
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden p-3">
          <div className="flex items-center gap-2 px-1 pb-2">
            <Clock size={12} className="text-text-tertiary" />
            <h3 className="text-xs font-semibold uppercase tracking-wider text-text-secondary">Timeline</h3>
          </div>
          {events.length === 0 ? (
            <div className="flex flex-1 items-center justify-center rounded-lg border border-dashed border-border-muted text-sm text-text-tertiary">
              No agent activity yet
            </div>
          ) : (
            <div className="space-y-2 overflow-auto pr-1">
              {events.map((event) => <ActivityRow key={event.id} event={event} />)}
            </div>
          )}
        </div>
        <div className="flex w-72 shrink-0 flex-col gap-3 overflow-hidden border-l border-border-muted p-3">
          <AgentStats events={events} />
        </div>
      </div>
    </div>
  );
}

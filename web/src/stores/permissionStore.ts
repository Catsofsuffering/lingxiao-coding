import { create } from 'zustand';
import { acpClient } from '../api/AcpClient';
import { createLogger } from '../utils/logger';

const log = createLogger('permissionStore');

export interface PermissionRequest {
  requestId: string;
  sessionId: string;
  source: 'leader' | 'worker';
  toolName: string;
  reason: string;
  requestedMode?: string;
  requestedHosts?: string[];
  workerName?: string;
  autoApproved?: boolean;
  bypass?: boolean;
  timestamp: number;
}

interface PermissionState {
  pendingRequests: PermissionRequest[];
  resolvingRequestIds: Record<string, 'approve' | 'deny' | 'allowAll'>;
  errors: Record<string, string>;
  // 历史记录保存后端协议原值，避免 Web 本地再造 denied/rejected 两套拒绝状态。
  history: Array<PermissionRequest & { decision: 'approved' | 'rejected' | 'allowAll' }>;
  addRequest: (req: unknown) => void;
  clearError: (requestId: string) => void;
  approve: (requestId: string) => Promise<void>;
  deny: (requestId: string) => Promise<void>;
  allowAll: (requestId: string) => Promise<void>;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function stringValue(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  return trimmed || undefined;
}

function stringArrayValue(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const values = value.map(String).map((item) => item.trim()).filter(Boolean);
  return values.length > 0 ? values : undefined;
}

function normalizePermissionRequest(value: unknown): PermissionRequest | null {
  const record = asRecord(value);
  if (!record) return null;
  const requestId = stringValue(record.requestId)
    ?? stringValue(record.request_id)
    ?? stringValue(record.toolCallId)
    ?? stringValue(record.id);
  const toolName = stringValue(record.toolName)
    ?? stringValue(record.tool_name)
    ?? stringValue(record.tool)
    ?? 'unknown';
  if (!requestId) return null;
  const timestamp = typeof record.timestamp === 'number' && Number.isFinite(record.timestamp)
    ? record.timestamp
    : Date.now();
  return {
    requestId,
    sessionId: stringValue(record.sessionId) ?? stringValue(record.session_id) ?? '',
    source: record.source === 'worker' ? 'worker' : 'leader',
    toolName,
    reason: stringValue(record.reason) ?? '',
    requestedMode: stringValue(record.requestedMode) ?? stringValue(record.requested_mode) ?? stringValue(record.mode),
    requestedHosts: stringArrayValue(record.requestedHosts) ?? stringArrayValue(record.requested_hosts),
    workerName: stringValue(record.workerName) ?? stringValue(record.worker_name),
    autoApproved: typeof record.autoApproved === 'boolean' ? record.autoApproved : undefined,
    bypass: typeof record.bypass === 'boolean' ? record.bypass : undefined,
    timestamp,
  };
}

function withoutKey<T>(record: Record<string, T>, key: string): Record<string, T> {
  const next = { ...record };
  delete next[key];
  return next;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message) return error.message;
  if (typeof error === 'string' && error.trim()) return error;
  return 'Permission request failed';
}

// Keep permission history bounded: a long-running session in an auto-approval
// mode resolves one prompt per turn, and this array previously grew without cap
// across the whole app lifetime (never reset on session switch).
const MAX_PERMISSION_HISTORY = 200;
type PermissionHistoryEntry = PermissionRequest & {
  decision: 'approved' | 'rejected' | 'allowAll';
};
function appendPermissionHistory(
  history: PermissionHistoryEntry[],
  entry: PermissionHistoryEntry,
): PermissionHistoryEntry[] {
  const next = [...history, entry];
  return next.length > MAX_PERMISSION_HISTORY ? next.slice(-MAX_PERMISSION_HISTORY) : next;
}

export const usePermissionStore = create<PermissionState>((set, get) => ({
  pendingRequests: [],
  resolvingRequestIds: {},
  errors: {},
  history: [],

  addRequest: (req) => {
    const normalized = normalizePermissionRequest(req);
    if (!normalized) return;
    set((s) => ({
      pendingRequests: [...s.pendingRequests.filter(r => r.requestId !== normalized.requestId), normalized],
      errors: withoutKey(s.errors, normalized.requestId),
    }));
  },

  clearError: (requestId) => set((s) => ({
    errors: withoutKey(s.errors, requestId),
  })),

  approve: async (requestId) => {
    try {
      set((s) => ({
        resolvingRequestIds: { ...s.resolvingRequestIds, [requestId]: 'approve' },
        errors: withoutKey(s.errors, requestId),
      }));
      await acpClient.sendJsonRpc('_lingxiao.ai/resolvePermission', {
        requestId,
        decision: 'approved',
      });
      const req = get().pendingRequests.find(r => r.requestId === requestId);
      if (req) {
        set((s) => ({
          pendingRequests: s.pendingRequests.filter(r => r.requestId !== requestId),
          history: appendPermissionHistory(s.history, { ...req, decision: 'approved' as const }),
        }));
      }
    } catch (e) {
      log.error('approve RPC failed:', e);
      set((s) => ({ errors: { ...s.errors, [requestId]: getErrorMessage(e) } }));
    } finally {
      set((s) => ({ resolvingRequestIds: withoutKey(s.resolvingRequestIds, requestId) }));
    }
  },

  deny: async (requestId) => {
    try {
      set((s) => ({
        resolvingRequestIds: { ...s.resolvingRequestIds, [requestId]: 'deny' },
        errors: withoutKey(s.errors, requestId),
      }));
      await acpClient.sendJsonRpc('_lingxiao.ai/resolvePermission', {
        requestId,
        decision: 'rejected',
      });
      const req = get().pendingRequests.find(r => r.requestId === requestId);
      if (req) {
        set((s) => ({
          pendingRequests: s.pendingRequests.filter(r => r.requestId !== requestId),
          history: appendPermissionHistory(s.history, { ...req, decision: 'rejected' as const }),
        }));
      }
    } catch (e) {
      log.error('deny RPC failed:', e);
      set((s) => ({ errors: { ...s.errors, [requestId]: getErrorMessage(e) } }));
    } finally {
      set((s) => ({ resolvingRequestIds: withoutKey(s.resolvingRequestIds, requestId) }));
    }
  },

  allowAll: async (requestId) => {
    try {
      set((s) => ({
        resolvingRequestIds: { ...s.resolvingRequestIds, [requestId]: 'allowAll' },
        errors: withoutKey(s.errors, requestId),
      }));
      await acpClient.sendJsonRpc('_lingxiao.ai/resolvePermission', {
        requestId,
        decision: 'allowAll',
      });
      const req = get().pendingRequests.find(r => r.requestId === requestId);
      if (req) {
        set((s) => ({
          pendingRequests: s.pendingRequests.filter(r => r.requestId !== requestId),
          history: appendPermissionHistory(s.history, { ...req, decision: 'allowAll' as const }),
        }));
      }
    } catch (e) {
      log.error('allowAll RPC failed:', e);
      set((s) => ({ errors: { ...s.errors, [requestId]: getErrorMessage(e) } }));
    } finally {
      set((s) => ({ resolvingRequestIds: withoutKey(s.resolvingRequestIds, requestId) }));
    }
  },
}));

/**
 * Crash Reporter
 *
 * 在崩溃路径（uncaughtException / unhandledRejection / worker 异常）中调用，
 * best-effort 将结构化崩溃报告落盘到 ~/.lingxiao/logs/crash-<ISO>.json。
 *
 * 设计约束：
 * - 绝不抛异常（崩溃路径里调用，自身失败只 console.error 兜底）
 * - 写入前对常见敏感字段做掩码
 * - 采集最近内存日志作为上下文（来自 Log 的环形缓冲）
 */

import { existsSync, mkdirSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { getLogDir, getRecentLogEntries, type LogEntry } from './Log.js';
import { VERSION } from '../version.js';

export type CrashSource =
  | 'main'
  | 'worker'
  | 'unhandledRejection'
  | 'uncaughtException';

export interface CrashContext {
  /** 抛出的错误对象（或任意被 reject 的值）。 */
  error: unknown;
  /** 崩溃来源。 */
  source: CrashSource;
  /** 可选：当前 session ID。 */
  sessionId?: string;
  /** 可选：触发崩溃的 agent 名称。 */
  agentName?: string;
  /** 可选：额外结构化上下文（会被脱敏）。 */
  extra?: Record<string, unknown>;
}

interface SerializedError {
  name: string;
  message: string;
  stack?: string;
  /** 非 Error 值时保留原始字符串表示。 */
  raw?: string;
}

interface CrashReportPayload {
  ts: string;
  source: CrashSource;
  version: string;
  platform: string;
  arch: string;
  nodeVersion: string;
  sessionId?: string;
  agentName?: string;
  error: SerializedError;
  extra?: Record<string, unknown>;
  recentLogs: LogEntry[];
}

export interface CrashReportInfo {
  path: string;
  mtime: number;
  size: number;
}

// ─── Redaction ───

const SENSITIVE_KEY_RE = /(api[_-]?key|token|password|passwd|secret|authorization|auth[_-]?token|access[_-]?key|private[_-]?key|client[_-]?secret|bearer|cookie|session[_-]?token|refresh[_-]?token)/i;
const MASK = '***REDACTED***';
const MAX_REDACT_DEPTH = 8;

/**
 * 递归脱敏：对 key 命中敏感模式的字段做掩码。
 * 处理循环引用，限制深度，best-effort 不抛异常。导出供 Diagnostics 复用。
 */
export function redactSensitive<T>(value: T): T {
  try {
    return redactInner(value, 0, new WeakSet<object>()) as T;
  } catch {
    return value;
  }
}

function redactInner(value: unknown, depth: number, seen: WeakSet<object>): unknown {
  if (depth > MAX_REDACT_DEPTH) return '[Truncated: max depth]';
  if (value === null || typeof value !== 'object') return value;

  if (seen.has(value as object)) return '[Circular]';
  seen.add(value as object);

  if (Array.isArray(value)) {
    return value.map((item) => redactInner(item, depth + 1, seen));
  }

  const out: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
    if (SENSITIVE_KEY_RE.test(key)) {
      out[key] = MASK;
    } else if (typeof val === 'string') {
      out[key] = redactSensitiveString(val);
    } else {
      out[key] = redactInner(val, depth + 1, seen);
    }
  }
  return out;
}

/**
 * 对字符串值中形如 "token=xxx" / "Authorization: Bearer xxx" 的内联敏感片段做掩码。
 * 导出供 Diagnostics 在处理日志文本时复用。
 */
export function redactSensitiveString(text: string): string {
  if (!text) return text;
  let result = text;
  // key=value / key: value / "key":"value"(JSON) 形式。
  // sep 允许 key 后可选结束引号 + 冒号/等号 + value 前可选起始引号，
  // 以覆盖 JSON 日志行 "apiKey":"xxx"（key 被引号包裹后紧跟冒号）。
  result = result.replace(
    /\b(api[_-]?key|token|password|passwd|secret|authorization|access[_-]?key|client[_-]?secret|refresh[_-]?token|session[_-]?token)\b(["']?\s*[:=]\s*["']?)([^\s"'&,}]+)/gi,
    (_m, key: string, sep: string) => `${key}${sep}${MASK}`,
  );
  // Bearer <token>
  result = result.replace(/\bBearer\s+[A-Za-z0-9._\-]+/gi, `Bearer ${MASK}`);
  return result;
}

// ─── Error serialization ───

function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  if (typeof error === 'object' && error !== null) {
    const maybe = error as { name?: unknown; message?: unknown; stack?: unknown };
    return {
      name: typeof maybe.name === 'string' ? maybe.name : 'NonError',
      message: typeof maybe.message === 'string' ? maybe.message : safeStringify(error),
      stack: typeof maybe.stack === 'string' ? maybe.stack : undefined,
      raw: safeStringify(error),
    };
  }
  return {
    name: 'NonError',
    message: String(error),
    raw: String(error),
  };
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

// ─── Public API ───

/**
 * 写崩溃报告到 ~/.lingxiao/logs/crash-<ISO>.json，返回写入文件绝对路径。
 * best-effort：任何失败只 console.error 兜底，绝不抛异常。
 * 失败时返回空字符串。
 */
export function writeCrashReport(ctx: CrashContext): string {
  try {
    const logDir = getLogDir();
    if (!existsSync(logDir)) {
      mkdirSync(logDir, { recursive: true });
    }

    const now = new Date();
    const stamp = now.toISOString().replace(/[:.]/g, '-');
    const filePath = join(logDir, `crash-${stamp}.json`);

    const payload: CrashReportPayload = {
      ts: now.toISOString(),
      source: ctx.source,
      version: VERSION,
      platform: process.platform,
      arch: process.arch,
      nodeVersion: process.version,
      sessionId: ctx.sessionId,
      agentName: ctx.agentName,
      error: serializeError(ctx.error),
      extra: ctx.extra,
      recentLogs: getRecentLogEntries(),
    };

    const redacted = redactSensitive(payload);
    writeFileSync(filePath, JSON.stringify(redacted, null, 2), 'utf-8');
    return filePath;
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[CrashReporter] 写崩溃报告失败:', err);
    } catch {
      // 连 console 都失败则彻底放弃
    }
    return '';
  }
}

/**
 * 列出 ~/.lingxiao/logs 下所有 crash-*.json，按 mtime 倒序（最新在前）。
 * best-effort：失败返回空数组。
 */
export function listCrashReports(): CrashReportInfo[] {
  try {
    const logDir = getLogDir();
    if (!existsSync(logDir)) return [];
    const result: CrashReportInfo[] = [];
    for (const name of readdirSync(logDir)) {
      if (!name.startsWith('crash-') || !name.endsWith('.json')) continue;
      const full = join(logDir, name);
      try {
        const st = statSync(full);
        if (!st.isFile()) continue;
        result.push({ path: full, mtime: st.mtimeMs, size: st.size });
      } catch {
        // 跳过无法 stat 的条目
      }
    }
    result.sort((a, b) => b.mtime - a.mtime);
    return result;
  } catch {
    return [];
  }
}

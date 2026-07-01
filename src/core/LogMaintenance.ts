/**
 * Log Maintenance
 *
 * 定期清理 ~/.lingxiao/logs 下的历史文件以控制磁盘占用：
 * - crash-*.json / diagnostics-*.zip / diagnostics-*.md
 * - 轮转日志（lingxiao.log.1 / lingxiao.log.2 等）
 * - session 日志（sessions/*.log）
 *
 * 清理策略：
 * 1. 删除超过 maxAgeDays 的上述文件
 * 2. 若日志目录总占用仍超过 maxTotalBytes，按 mtime 从旧到新删到阈值内
 *
 * 安全约束：绝不删当前活跃主日志 lingxiao.log。
 * best-effort：所有 fs 操作 try-catch，单点失败不影响整体。
 */

import {
  existsSync,
  readdirSync,
  statSync,
  unlinkSync,
} from 'node:fs';
import { join } from 'node:path';
import { getLogDir, getMainLogFilePath, coreLogger } from './Log.js';
import { registerCleanup, cleanupRegistry } from './CleanupRegistry.js';

// ─── Config ───

export interface LogMaintenanceConfig {
  /** 文件最大保留天数，超过则删除。默认 14。 */
  maxAgeDays?: number;
  /** 日志目录总占用上限（字节），超过则按最旧优先删除。默认 200MB。 */
  maxTotalBytes?: number;
  /** 定时清理间隔（毫秒）。默认 6 小时。 */
  intervalMs?: number;
}

export interface LogMaintenanceResult {
  deletedFiles: string[];
  freedBytes: number;
}

const DEFAULT_MAX_AGE_DAYS = 14;
const DEFAULT_MAX_TOTAL_BYTES = 200 * 1024 * 1024; // 200MB
const DEFAULT_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6h

interface FileMeta {
  path: string;
  name: string;
  mtimeMs: number;
  size: number;
}

// ─── Helpers ───

/** 当前活跃主日志文件名（绝不删除）。 */
function getActiveLogName(): string {
  const full = getMainLogFilePath();
  const slash = Math.max(full.lastIndexOf('/'), full.lastIndexOf('\\'));
  return slash >= 0 ? full.slice(slash + 1) : full;
}

/**
 * 判断文件是否属于「可清理」类别。
 * 注意：活跃 lingxiao.log 本身不在可清理类别内。
 */
function isCleanableLogFile(name: string, activeLogName: string): boolean {
  if (name === activeLogName) return false;
  if (name.startsWith('crash-') && name.endsWith('.json')) return true;
  if (name.startsWith('diagnostics-') && (name.endsWith('.zip') || name.endsWith('.md'))) return true;
  // 轮转日志：lingxiao.log.1 / lingxiao.log.2 等，或按天命名 lingxiao-YYYY-MM-DD.log[.N]
  if (name.startsWith('lingxiao')) return true;
  return false;
}

/** 收集日志目录顶层可清理文件元信息（best-effort）。 */
function collectCleanableFiles(logDir: string, activeLogName: string): FileMeta[] {
  const out: FileMeta[] = [];
  let names: string[];
  try {
    names = readdirSync(logDir);
  } catch {
    return out;
  }
  for (const name of names) {
    if (!isCleanableLogFile(name, activeLogName)) continue;
    const full = join(logDir, name);
    try {
      const st = statSync(full);
      if (!st.isFile()) continue;
      out.push({ path: full, name, mtimeMs: st.mtimeMs, size: st.size });
    } catch {
      // 跳过无法 stat 的条目
    }
  }
  return out;
}

/** 收集 sessions 子目录下的 *.log（best-effort）。 */
function collectSessionLogFiles(logDir: string): FileMeta[] {
  const out: FileMeta[] = [];
  const sessionsDir = join(logDir, 'sessions');
  let names: string[];
  try {
    if (!existsSync(sessionsDir)) return out;
    names = readdirSync(sessionsDir);
  } catch {
    return out;
  }
  for (const name of names) {
    if (!name.endsWith('.log')) continue;
    const full = join(sessionsDir, name);
    try {
      const st = statSync(full);
      if (!st.isFile()) continue;
      out.push({ path: full, name, mtimeMs: st.mtimeMs, size: st.size });
    } catch {
      // 跳过
    }
  }
  return out;
}

function safeUnlink(path: string): boolean {
  try {
    unlinkSync(path);
    return true;
  } catch {
    return false;
  }
}

// ─── Public API ───

/**
 * 执行一次日志清理。绝不抛异常，返回本次删除的文件与释放字节数。
 */
export function runLogMaintenanceOnce(cfg: LogMaintenanceConfig = {}): LogMaintenanceResult {
  const maxAgeDays = cfg.maxAgeDays ?? DEFAULT_MAX_AGE_DAYS;
  const maxTotalBytes = cfg.maxTotalBytes ?? DEFAULT_MAX_TOTAL_BYTES;

  const deletedFiles: string[] = [];
  let freedBytes = 0;

  try {
    const logDir = getLogDir();
    if (!existsSync(logDir)) {
      return { deletedFiles, freedBytes };
    }
    const activeLogName = getActiveLogName();

    let files = [
      ...collectCleanableFiles(logDir, activeLogName),
      ...collectSessionLogFiles(logDir),
    ];

    // ── 1) 按年龄删除 ──
    const now = Date.now();
    const maxAgeMs = maxAgeDays * 24 * 60 * 60 * 1000;
    const survivors: FileMeta[] = [];
    for (const f of files) {
      if (now - f.mtimeMs > maxAgeMs) {
        if (safeUnlink(f.path)) {
          deletedFiles.push(f.path);
          freedBytes += f.size;
        }
      } else {
        survivors.push(f);
      }
    }
    files = survivors;

    // ── 2) 按总占用阈值删除（最旧优先） ──
    let totalBytes = files.reduce((sum, f) => sum + f.size, 0);
    if (totalBytes > maxTotalBytes) {
      const oldestFirst = files.slice().sort((a, b) => a.mtimeMs - b.mtimeMs);
      for (const f of oldestFirst) {
        if (totalBytes <= maxTotalBytes) break;
        if (safeUnlink(f.path)) {
          deletedFiles.push(f.path);
          freedBytes += f.size;
          totalBytes -= f.size;
        }
      }
    }
  } catch (err) {
    try {
      coreLogger.warn('[LogMaintenance] 清理过程出错', {
        error: err instanceof Error ? err.message : String(err),
      });
    } catch {
      // best-effort
    }
  }

  return { deletedFiles, freedBytes };
}

/**
 * 启动定时清理。启动时先执行一次 runLogMaintenanceOnce，随后按 intervalMs 周期执行。
 * 定时器 unref 不阻塞进程退出，并注册到 cleanupRegistry 以便退出时停止。
 * 返回 stop 函数（幂等），调用后停止定时器并从 cleanupRegistry 注销。
 */
export function startLogMaintenance(cfg: LogMaintenanceConfig = {}): () => void {
  const intervalMs = cfg.intervalMs ?? DEFAULT_INTERVAL_MS;

  // 启动即跑一次
  try {
    const first = runLogMaintenanceOnce(cfg);
    if (first.deletedFiles.length > 0) {
      coreLogger.info('[LogMaintenance] 启动清理完成', {
        deleted: first.deletedFiles.length,
        freedBytes: first.freedBytes,
      });
    }
  } catch {
    // best-effort
  }

  const timer = setInterval(() => {
    try {
      const result = runLogMaintenanceOnce(cfg);
      if (result.deletedFiles.length > 0) {
        coreLogger.info('[LogMaintenance] 定时清理完成', {
          deleted: result.deletedFiles.length,
          freedBytes: result.freedBytes,
        });
      }
    } catch {
      // best-effort
    }
  }, intervalMs);

  // 不阻塞进程退出
  if (typeof timer.unref === 'function') timer.unref();

  let stopped = false;
  let cleanupId: string | undefined;

  const stop = (): void => {
    if (stopped) return;
    stopped = true;
    try {
      clearInterval(timer);
    } catch {
      // ignore
    }
    if (cleanupId) {
      try {
        cleanupRegistry.unregister(cleanupId);
      } catch {
        // ignore
      }
    }
  };

  // 注册到 cleanupRegistry，确保进程退出时停止定时器
  try {
    cleanupId = registerCleanup(stop, 50);
  } catch {
    // 注册失败不影响 stop 可用性
  }

  return stop;
}

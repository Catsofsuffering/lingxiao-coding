/**
 * Logging Runtime — CLI / Web 日志配置统一口径 + 定期清理接线
 *
 * 统一目标：让 CLI（TUI）与 Web server 的 configureLogging 调用使用同一套
 * 级别 / file / 清理策略默认，仅在 console sink 上区分：
 * - CLI(TUI)：console:false —— ConsoleSink 直写 stderr 会污染 Ink 渲染区。
 * - Web    ：console:true  —— 保留 stderr 输出便于 server 日志可见。
 *
 * 同时把 startLogMaintenance() 的定时清理接线收敛到这里，保证：
 * - 默认 maxAgeDays / maxTotalBytes 与配置口径一致。
 * - 单进程内幂等（多次调用只注册一次定时器），避免重复 timer。
 */

import { configureLogging } from '../core/Log.js';
import {
  startLogMaintenance,
  type LogMaintenanceConfig,
} from '../core/LogMaintenance.js';

// ─── 共享默认（CLI / Web 统一口径）─────────────────────────────────────────────

/** 日志默认主路径：优先 LINGXIAO_LOG_PATH，否则使用 Log.ts 默认 lingxiao.log。 */
function defaultLogFile(): string | boolean {
  return process.env.LINGXIAO_LOG_PATH || true;
}

/** 内存环形缓冲容量（最近 N 条），CLI / Web 一致。 */
const SHARED_RECENT_CAPACITY = 200;

/** 定期清理默认策略，CLI / Web 一致。 */
const SHARED_MAINTENANCE: Required<Pick<LogMaintenanceConfig, 'maxAgeDays' | 'maxTotalBytes'>> = {
  maxAgeDays: 14,
  maxTotalBytes: 200 * 1024 * 1024, // 200MB
};

// ─── 日志配置 ─────────────────────────────────────────────────────────────────

export interface RuntimeLoggingOptions {
  /** 可选 session ID：额外写一份 session 分文件日志。 */
  sessionId?: string;
}

/**
 * CLI / TUI 日志配置。console:false 防止 ConsoleSink 直写 stderr 污染 Ink 渲染。
 * level 留空 → configureLogging 内部回落到 LINGXIAO_LOG_LEVEL 或 WARN（与 Web 一致）。
 */
export function configureCliLogging(opts: RuntimeLoggingOptions = {}): void {
  configureLogging({
    console: false,
    file: defaultLogFile(),
    recentCapacity: SHARED_RECENT_CAPACITY,
    sessionId: opts.sessionId,
  });
}

/**
 * Web server 日志配置。保留 console sink（stderr），其余口径与 CLI 一致。
 */
export function configureWebLogging(opts: RuntimeLoggingOptions = {}): void {
  configureLogging({
    console: true,
    file: defaultLogFile(),
    recentCapacity: SHARED_RECENT_CAPACITY,
    sessionId: opts.sessionId,
  });
}

// ─── 定期清理接线（进程内幂等）───────────────────────────────────────────────

let _maintenanceStarted = false;
let _stopMaintenance: (() => void) | undefined;

/**
 * 在主进程启动路径调用，注册定时日志清理（已由 startLogMaintenance 内部注册
 * cleanupRegistry priority=50 + unref，不阻塞进程退出）。
 * 进程内幂等：重复调用只启动一次定时器。
 */
export function ensureLogMaintenance(cfg: LogMaintenanceConfig = {}): void {
  if (_maintenanceStarted) return;
  _maintenanceStarted = true;
  try {
    _stopMaintenance = startLogMaintenance({
      maxAgeDays: SHARED_MAINTENANCE.maxAgeDays,
      maxTotalBytes: SHARED_MAINTENANCE.maxTotalBytes,
      ...cfg,
    });
  } catch {
    // best-effort：清理接线失败不应阻断启动
    _maintenanceStarted = false;
  }
}

/** 停止定时清理（幂等）。主要用于测试或显式关闭。 */
export function stopLogMaintenance(): void {
  if (_stopMaintenance) {
    try {
      _stopMaintenance();
    } catch {
      // best-effort
    }
    _stopMaintenance = undefined;
  }
  _maintenanceStarted = false;
}

/**
 * Structured Logging System
 *
 * Features:
 * - JSON lines file output (~/.lingxiao/logs/lingxiao.log)
 * - Optional per-session log file (~/.lingxiao/logs/sessions/<sessionId>.log)
 * - Human-readable console output (stderr)
 * - Simple file rotation (10MB max, 2 backups) + optional daily-named files
 * - In-memory ring buffer of recent entries (for crash report context)
 * - Structured context fields
 * - Environment variable override: LINGXIAO_LOG_LEVEL
 */

import { appendFileSync, existsSync, mkdirSync, renameSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

export enum LogLevel {
  DEBUG = 'debug',
  INFO = 'info',
  WARN = 'warn',
  ERROR = 'error',
}

const LEVEL_ORDER: LogLevel[] = [LogLevel.DEBUG, LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR];

export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
  setLevel(level: LogLevel): void;
}

// ─── Types ───

/**
 * 单条结构化日志记录。JSON lines 写入文件，同时进入内存环形缓冲。
 * 导出供 CrashReporter / Diagnostics 等模块复用。
 */
export interface LogEntry {
  ts: string;
  level: LogLevel;
  logger: string;
  msg: string;
  [key: string]: unknown;
}

interface LogSink {
  write(entry: LogEntry): void;
}

// ─── Paths ───

/**
 * 返回凌霄日志根目录绝对路径：~/.lingxiao/logs。
 * 其他模块（CrashReporter / Diagnostics / LogMaintenance）统一以此为基准。
 */
export function getLogDir(): string {
  return join(homedir(), '.lingxiao', 'logs');
}

/** 主日志文件默认绝对路径：~/.lingxiao/logs/lingxiao.log */
export function getMainLogFilePath(): string {
  return join(getLogDir(), 'lingxiao.log');
}

/** session 日志文件绝对路径：~/.lingxiao/logs/sessions/<sessionId>.log */
export function getSessionLogFilePath(sessionId: string): string {
  return join(getLogDir(), 'sessions', sanitizeSessionId(sessionId));
}

/** 防止 sessionId 含路径分隔符导致越界写入。 */
function sanitizeSessionId(sessionId: string): string {
  const safe = sessionId.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 200) || 'unknown';
  return `${safe}.log`;
}

// ─── Sinks ───

class ConsoleSink implements LogSink {
  write(entry: LogEntry): void {
    const { ts, level, logger, msg, ...rest } = entry;
    const ctxStr = Object.keys(rest).length > 0 ? ` ${JSON.stringify(rest)}` : '';
    const line = `[${ts}] [${logger}] ${level.toUpperCase()}: ${msg}${ctxStr}\n`;
    process.stderr.write(line);
  }
}

const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
const MAX_ROTATED_FILES = 2; // .1, .2

interface FileSinkOptions {
  /**
   * 按天命名：写入 <dir>/<base>-YYYY-MM-DD<ext>，跨天自动切换文件。
   * 默认 false（仍写固定文件名 + 大小轮转）。
   */
  daily?: boolean;
}

class FileSink implements LogSink {
  private basePath: string;
  private daily: boolean;
  private writeCount = 0;
  private currentDay = '';
  private resolvedPath: string;

  constructor(filePath: string, options: FileSinkOptions = {}) {
    this.basePath = filePath;
    this.daily = options.daily === true;
    this.resolvedPath = this.computePath();
    const dir = dirname(this.resolvedPath);
    if (dir && !existsSync(dir)) {
      try {
        mkdirSync(dir, { recursive: true });
      } catch {
        // Best-effort directory creation
      }
    }
  }

  private computePath(): string {
    if (!this.daily) return this.basePath;
    const day = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    this.currentDay = day;
    const dot = this.basePath.lastIndexOf('.');
    const slash = Math.max(this.basePath.lastIndexOf('/'), this.basePath.lastIndexOf('\\'));
    if (dot > slash) {
      return `${this.basePath.slice(0, dot)}-${day}${this.basePath.slice(dot)}`;
    }
    return `${this.basePath}-${day}`;
  }

  write(entry: LogEntry): void {
    try {
      if (this.daily) {
        const day = new Date().toISOString().slice(0, 10);
        if (day !== this.currentDay) {
          this.resolvedPath = this.computePath();
          const dir = dirname(this.resolvedPath);
          if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });
        }
      }
      appendFileSync(this.resolvedPath, JSON.stringify(entry) + '\n', 'utf-8');
      this.writeCount++;
      if (this.writeCount % 100 === 0) this.maybeRotate();
    } catch {
      // Best-effort file logging
    }
  }

  private maybeRotate(): void {
    // daily 模式靠日期切文件，不做大小轮转
    if (this.daily) return;
    try {
      const stat = statSync(this.resolvedPath);
      if (stat.size < MAX_FILE_SIZE) return;
      for (let i = MAX_ROTATED_FILES; i >= 1; i--) {
        const from = i === 1 ? this.resolvedPath : `${this.resolvedPath}.${i - 1}`;
        const to = `${this.resolvedPath}.${i}`;
        if (existsSync(from)) renameSync(from, to);
      }
    } catch {
      // Rotation failure is non-fatal
    }
  }
}

// ─── Ring buffer (recent entries) ───

const DEFAULT_RECENT_CAPACITY = 200;
let _recentCapacity = DEFAULT_RECENT_CAPACITY;
let _recentEntries: LogEntry[] = [];

function pushRecent(entry: LogEntry): void {
  _recentEntries.push(entry);
  const overflow = _recentEntries.length - _recentCapacity;
  if (overflow > 0) {
    _recentEntries.splice(0, overflow);
  }
}

/**
 * 返回内存中最近的日志条目（最旧→最新）。供崩溃报告采集上下文。
 * @param n 返回最近 n 条；省略时返回缓冲区全部（最多 capacity 条）。
 */
export function getRecentLogEntries(n?: number): LogEntry[] {
  if (n === undefined || n >= _recentEntries.length) {
    return _recentEntries.slice();
  }
  if (n <= 0) return [];
  return _recentEntries.slice(_recentEntries.length - n);
}

/** 配置环形缓冲容量（默认 200）。<=0 时禁用缓冲。 */
export function setRecentLogCapacity(capacity: number): void {
  _recentCapacity = Number.isFinite(capacity) && capacity > 0 ? Math.floor(capacity) : 0;
  if (_recentEntries.length > _recentCapacity) {
    _recentEntries.splice(0, _recentEntries.length - _recentCapacity);
  }
}

// ─── Logger Implementation ───

class StructuredLogger implements Logger {
  private name: string;
  private level: LogLevel = LogLevel.WARN;

  constructor(name: string) {
    this.name = name;
  }

  setLevel(level: LogLevel): void {
    this.level = level;
  }

  private shouldLog(level: LogLevel): boolean {
    return LEVEL_ORDER.indexOf(level) >= LEVEL_ORDER.indexOf(this.level);
  }

  private emit(level: LogLevel, msg: string, args: unknown[]): void {
    if (!this.shouldLog(level)) return;
    let ctxObj: Record<string, unknown> | undefined;
    if (args.length === 1 && args[0] && typeof args[0] === 'object' && !Array.isArray(args[0])) {
      ctxObj = args[0] as Record<string, unknown>;
    } else if (args.length > 0) {
      ctxObj = { args };
    }
    const entry: LogEntry = {
      ts: new Date().toISOString(),
      level,
      logger: this.name,
      msg,
      ...ctxObj,
    };
    // 进入内存环形缓冲（不受 sink 配置影响，崩溃报告始终可采集）
    if (_recentCapacity > 0) pushRecent(entry);
    for (const sink of _sinks) {
      sink.write(entry);
    }
  }

  debug(msg: string, ...args: unknown[]): void { this.emit(LogLevel.DEBUG, msg, args); }
  info(msg: string, ...args: unknown[]): void { this.emit(LogLevel.INFO, msg, args); }
  warn(msg: string, ...args: unknown[]): void { this.emit(LogLevel.WARN, msg, args); }
  error(msg: string, ...args: unknown[]): void { this.emit(LogLevel.ERROR, msg, args); }
}

// ─── Global State ───

let _sinks: LogSink[] = [new ConsoleSink()];
const _allLoggers: StructuredLogger[] = [];

// ─── Public API ───

export const leaderLogger = _createAndTrack('lingxiao.leader');
export const agentLogger = _createAndTrack('lingxiao.agent');
export const sessionLogger = _createAndTrack('lingxiao.session');
export const coreLogger = _createAndTrack('lingxiao.core');
export const serverLogger = _createAndTrack('lingxiao.server');
export const configLogger = _createAndTrack('lingxiao.config');
const wikiLogger = _createAndTrack('lingxiao.wiki');
export const llmLogger = _createAndTrack('lingxiao.llm');

function _createAndTrack(name: string): StructuredLogger {
  const logger = new StructuredLogger(name);
  _allLoggers.push(logger);
  return logger;
}

export function createLogger(name: string, level: LogLevel = LogLevel.WARN): Logger {
  const logger = new StructuredLogger(name);
  logger.setLevel(level);
  _allLoggers.push(logger);
  return logger;
}

function setGlobalLogLevel(level: LogLevel): void {
  for (const logger of _allLoggers) {
    logger.setLevel(level);
  }
}

export interface LogConfig {
  level?: LogLevel;
  file?: string | boolean;
  /**
   * 是否启用 ConsoleSink（直接写 process.stderr）。默认 true。
   * TUI 模式必须传 false：ConsoleSink 绕过 console.* 直写 stderr，会污染 Ink
   * 的渲染区、打乱 log-update 的光标行数计算，导致状态行无法原地更新而反复刷屏。
   */
  console?: boolean;
  /**
   * 可选 session ID。提供时额外写一份 session 日志到
   * ~/.lingxiao/logs/sessions/<sessionId>.log（主日志仍写 lingxiao.log）。
   */
  sessionId?: string;
  /**
   * 主日志是否按天命名（lingxiao-YYYY-MM-DD.log）。默认 false。
   * 仅当 file 为 true（使用默认路径）时生效。
   */
  daily?: boolean;
  /** 内存环形缓冲容量（最近 N 条），默认 200。 */
  recentCapacity?: number;
}

export function configureLogging(config: LogConfig): void {
  const level = config.level
    ?? (process.env.LINGXIAO_LOG_LEVEL as LogLevel | undefined)
    ?? LogLevel.WARN;

  setGlobalLogLevel(level);

  if (config.recentCapacity !== undefined) {
    setRecentLogCapacity(config.recentCapacity);
  }

  _sinks = config.console === false ? [] : [new ConsoleSink()];

  if (config.file) {
    const usingDefaultPath = typeof config.file !== 'string';
    const filePath = typeof config.file === 'string'
      ? config.file
      : getMainLogFilePath();
    // daily 仅在使用默认主日志路径时生效，避免破坏调用方显式指定的文件名
    _sinks.push(new FileSink(filePath, { daily: usingDefaultPath && config.daily === true }));
  }

  if (config.sessionId) {
    _sinks.push(new FileSink(getSessionLogFilePath(config.sessionId)));
  }
}

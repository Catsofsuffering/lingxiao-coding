/**
 * 前端统一日志模块（web/src 专用）
 *
 * 前端无法直接 import 后端 Node logger（依赖 node:fs 等），因此独立实现一个
 * 轻量、零依赖的浏览器端 logger，统一收敛散落的 console.* 调用：
 *  - 级别开关：开发环境（import.meta.env.DEV）默认全开（debug+），生产默认仅 warn+error
 *  - 统一前缀/结构：所有输出带 [scope] 前缀，便于过滤定位
 *  - 保留 console 输出：底层仍走 console.debug/info/warn/error，保留浏览器堆栈/对象展开
 *  - 运行时可调：localStorage('lingxiao:logLevel') 或 window.__LINGXIAO_LOG_LEVEL 覆盖，
 *    便于在生产环境临时打开 debug 日志排查问题
 *  - 预留上报：error 级别集中经过 logger，未来可在此挂接错误上报（Sentry / 自建接口）
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

/** 数值越小越详细；silent 用于完全关闭。 */
const LEVEL_WEIGHT: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  silent: 100,
};

const VALID_LEVELS = new Set<string>(Object.keys(LEVEL_WEIGHT));

/** import.meta.env 在某些测试/构建环境下可能不存在，安全读取。 */
function isDevEnv(): boolean {
  try {
    // vite 注入 import.meta.env.DEV；vitest 同样提供
    return Boolean((import.meta as { env?: { DEV?: boolean } }).env?.DEV);
  } catch {
    return false;
  }
}

/** 从 localStorage / window 读取运行时级别覆盖，失败时返回 null。 */
function readOverrideLevel(): LogLevel | null {
  try {
    if (typeof window !== 'undefined') {
      const fromWindow = (window as unknown as { __LINGXIAO_LOG_LEVEL?: unknown }).__LINGXIAO_LOG_LEVEL;
      if (typeof fromWindow === 'string' && VALID_LEVELS.has(fromWindow)) {
        return fromWindow as LogLevel;
      }
    }
    if (typeof localStorage !== 'undefined') {
      const fromStorage = localStorage.getItem('lingxiao:logLevel');
      if (fromStorage && VALID_LEVELS.has(fromStorage)) {
        return fromStorage as LogLevel;
      }
    }
  } catch {
    // localStorage 在隐私模式 / SSR 下可能抛错，忽略
  }
  return null;
}

function resolveDefaultLevel(): LogLevel {
  const override = readOverrideLevel();
  if (override) return override;
  return isDevEnv() ? 'debug' : 'warn';
}

let currentLevel: LogLevel = resolveDefaultLevel();

/** 设置全局日志级别（运行时可调）。 */
export function setLogLevel(level: LogLevel): void {
  currentLevel = level;
}

/** 读取当前全局日志级别。 */
export function getLogLevel(): LogLevel {
  return currentLevel;
}

/** 重新根据环境与运行时覆盖解析默认级别（用于覆盖变更后刷新）。 */
export function refreshLogLevel(): void {
  currentLevel = resolveDefaultLevel();
}

function shouldLog(level: Exclude<LogLevel, 'silent'>): boolean {
  return LEVEL_WEIGHT[level] >= LEVEL_WEIGHT[currentLevel];
}

function formatPrefix(scope?: string): string {
  return scope ? `[${scope}]` : '[app]';
}

export interface Logger {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
  /** 派生一个带嵌套 scope 的子 logger，前缀为 `[parent:child]`。 */
  child(childScope: string): Logger;
}

function emit(level: Exclude<LogLevel, 'silent'>, scope: string | undefined, args: unknown[]): void {
  if (!shouldLog(level)) return;
  const prefix = formatPrefix(scope);
  // 保留 console 原生方法以维持堆栈定位与对象展开能力
  switch (level) {
    case 'debug':
      console.debug(prefix, ...args);
      break;
    case 'info':
      console.info(prefix, ...args);
      break;
    case 'warn':
      console.warn(prefix, ...args);
      break;
    case 'error':
      console.error(prefix, ...args);
      break;
  }
}

/** 创建一个带 scope 前缀的 logger。scope 通常为模块名，如 'AcpClient'。 */
export function createLogger(scope?: string): Logger {
  return {
    debug: (...args: unknown[]) => emit('debug', scope, args),
    info: (...args: unknown[]) => emit('info', scope, args),
    warn: (...args: unknown[]) => emit('warn', scope, args),
    error: (...args: unknown[]) => emit('error', scope, args),
    child: (childScope: string) => createLogger(scope ? `${scope}:${childScope}` : childScope),
  };
}

/** 无 scope 的默认 logger。 */
export const logger = createLogger();

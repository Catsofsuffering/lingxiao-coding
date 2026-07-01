/**
 * toastBridge — 全局 Toast 调用桥
 *
 * 问题：ToastProvider 的 addToast 只能通过 useToast() 在 React 组件内拿到，
 * 但大量异步 catch / useCallback / 非组件上下文需要弹通知，逐个改造 hook 成本高。
 *
 * 方案：ToastProvider 挂载时把 addToast 注册到这个模块级单例，
 * 任何地方 import { toast } 即可调用。未挂载时降级到 console。
 */

export type ToastType = 'success' | 'error' | 'warning' | 'info';

export interface ToastBridgeOptions {
  type?: ToastType;
  duration?: number;
  action?: { label: string; onClick: () => void };
}

type AddToastFn = (toast: {
  message: string;
  type: ToastType;
  duration?: number;
  action?: { label: string; onClick: () => void };
}) => void;

let _addToast: AddToastFn | null = null;

/** ToastProvider 挂载时注册真实实现 */
export function registerToastSink(fn: AddToastFn | null): void {
  _addToast = fn;
}

function emit(message: string, opts: ToastBridgeOptions = {}): void {
  const type = opts.type ?? 'info';
  if (_addToast) {
    _addToast({ message, type, duration: opts.duration, action: opts.action });
  } else {
    // 降级：Provider 未挂载（极少见，如纯单测）
    const tag = type === 'error' ? 'error' : type === 'warning' ? 'warn' : 'log';
    // eslint-disable-next-line no-console
    (console as unknown as Record<string, (...a: unknown[]) => void>)[tag]?.(`[toast:${type}] ${message}`);
  }
}

/** 把任意 error 归一化成可读字符串 */
export function errorMessage(e: unknown, fallback = '操作失败'): string {
  if (e instanceof Error) return e.message || fallback;
  if (typeof e === 'string') return e || fallback;
  return fallback;
}

export const toast = {
  success: (message: string, opts?: ToastBridgeOptions) => emit(message, { ...opts, type: 'success' }),
  error: (message: string, opts?: ToastBridgeOptions) => emit(message, { ...opts, type: 'error' }),
  warning: (message: string, opts?: ToastBridgeOptions) => emit(message, { ...opts, type: 'warning' }),
  info: (message: string, opts?: ToastBridgeOptions) => emit(message, { ...opts, type: 'info' }),
  /** 便捷：从 catch 的 error 直接弹 error toast */
  fromError: (e: unknown, fallback?: string, opts?: ToastBridgeOptions) =>
    emit(errorMessage(e, fallback), { ...opts, type: 'error' }),
};

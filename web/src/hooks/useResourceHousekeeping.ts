import { useEffect } from 'react';
import { useAgentActivityStore } from '../stores/agentActivityStore';
import { useGitActivityStore } from '../stores/gitActivityStore';

/**
 * 性能优化 (T-3 P2)：全局 idle 周期资源回收。
 *
 * agentActivityStore / gitActivityStore 都带 prune(maxAgeMs=24h) 方法，按时间清理
 * 超过 24h 的旧 activity events，但此前没有任何地方周期性调用它们 —— 长时间运行的
 * 会话里旧事件只受 500 条数量上界保护，无法按时间释放。本 hook 补上这个缺口：
 *
 * - 每 5 分钟在浏览器空闲时（requestIdleCallback，无则 setTimeout 兜底）调用一次
 *   两个 store 的 prune()，释放 24h 前的旧事件。
 * - prune 内部已是 Array.filter，数据量受 500 条上界保护，单次回收廉价且增量，
 *   绝不会一次性遍历重建大结构，不会阻塞主渲染路径。
 * - 定时器在应用卸载时彻底清理（clearInterval + cancel pending idle callback）。
 *
 * 在 App 根组件挂载一次即可。
 */

const HOUSEKEEPING_INTERVAL_MS = 5 * 60 * 1000; // 5 分钟

type IdleHandle = number;

interface IdleWindow {
  requestIdleCallback?: (cb: () => void, opts?: { timeout?: number }) => IdleHandle;
  cancelIdleCallback?: (handle: IdleHandle) => void;
}

/** 在浏览器空闲时执行 task；无 requestIdleCallback 时用 setTimeout 兜底。返回取消函数。 */
function runWhenIdle(task: () => void): () => void {
  const w = window as unknown as IdleWindow;
  if (typeof w.requestIdleCallback === 'function') {
    const handle = w.requestIdleCallback(task, { timeout: 2000 });
    return () => w.cancelIdleCallback?.(handle);
  }
  const timer = window.setTimeout(task, 0);
  return () => window.clearTimeout(timer);
}

export function useResourceHousekeeping(): void {
  useEffect(() => {
    let cancelIdle: (() => void) | null = null;

    const sweep = () => {
      // 真正的删除放到空闲帧执行，绝不卡主渲染路径。
      cancelIdle = runWhenIdle(() => {
        useAgentActivityStore.getState().prune();
        useGitActivityStore.getState().prune();
      });
    };

    const interval = window.setInterval(sweep, HOUSEKEEPING_INTERVAL_MS);

    return () => {
      window.clearInterval(interval);
      cancelIdle?.();
    };
  }, []);
}

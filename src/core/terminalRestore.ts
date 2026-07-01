/**
 * 终端状态还原 — 单一事实源。
 *
 * 背景（bug）：TUI 启用了 mouse tracking（DECSET ?1000/?1002/?1006）与 bracketed
 * paste（?2004），还隐藏了光标（?25l）。这些 DEC 私有模式是「进程级终端状态」，
 * 进程退出时若不显式还原，宿主终端会残留在 mouse-tracking/raw 状态——表现为用户
 * 鼠标移动/点击在 shell 里打出 `^[[<65;70;25M` 这类 SGR 鼠标上报序列（截图症状）。
 *
 * 原实现只在 React useEffect cleanup（正常 unmount）里还原，异常退出路径
 * （uncaughtException / unhandledRejection / SIGINT 强退 / gracefulShutdown 强退）
 * 完全不还原，于是 crash 一次就把用户终端搞坏。
 *
 * 本模块把还原序列收敛为单一函数，挂到所有退出路径，best-effort、幂等、绝不抛错。
 */

const ESC = '\x1b[';

/** DEC 私有模式还原序列（与 useRawTerminalInput 启用序列严格对称）：
 *  - ?2004l 关闭 bracketed paste
 *  - ?1002l 关闭按钮+拖拽 mouse tracking
 *  - ?1000l 关闭基础 mouse tracking
 *  - ?1006l 关闭 SGR 扩展鼠标坐标编码
 *  - ?25h   恢复光标显示（TUI 启动时 ?25l 隐藏过）
 */
const TERMINAL_RESTORE_SEQUENCE =
  `${ESC}?2004l${ESC}?1002l${ESC}?1000l${ESC}?1006l${ESC}?25h`;

let restoredOnce = false;

/**
 * 还原宿主终端状态（关闭 mouse tracking / bracketed paste，显示光标）。
 *
 * - 幂等：多次调用只在首次真正写入（exit handler / finally / 信号 / crash 可能重复触发）；
 *   传 force=true 可绕过幂等闸（用于显式重新进入 TUI 前的清场）。
 * - best-effort：非 TTY 或写入异常一律静默吞掉，绝不影响退出流程。
 */
export function restoreTerminalState(options?: { force?: boolean }): void {
  if (restoredOnce && !options?.force) return;
  restoredOnce = true;
  try {
    const stdout = process.stdout;
    // 仅对真实 TTY 写还原序列；管道/重定向场景写控制序列会污染输出。
    if (stdout && stdout.isTTY) {
      stdout.write(TERMINAL_RESTORE_SEQUENCE);
    }
  } catch {
    // 还原是 best-effort：退出路径里任何异常都不能阻塞进程退出。
  }
}

/** 重置幂等闸——重新进入 TUI 渲染前调用，使下次退出能再次还原。 */
export function resetTerminalRestoreLatch(): void {
  restoredOnce = false;
}

export { TERMINAL_RESTORE_SEQUENCE };

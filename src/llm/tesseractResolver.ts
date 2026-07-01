/**
 * Tesseract 本地资源解析 — 单一事实源。
 *
 * 背景（bug）：OCRTool 和 local_vision_fallback 各自用一段相同的脆弱逻辑解析
 * tessdata 目录（只试 `../../tessdata` 和 `../../../tessdata` 两个相对 dist 的候选）。
 * 一旦项目布局嵌套更深（如 tessdata 在更上层目录），两个候选都落空，langPath
 * 指向空目录，tesseract.js 退回从 CDN 下载语言数据/wasm core，在隔离或弱网环境
 * 必然 15s 初始化超时（用户现象：「OCR worker 初始化超时(15000ms)」）。
 *
 * 本模块把解析逻辑收敛为单一实现：
 *  - tessdata 目录：多级向上查找 + 环境变量覆盖，覆盖打包/源码/嵌套多种布局
 *  - corePath / workerPath：用 require.resolve 从 node_modules 精确定位本地 wasm/worker，
 *    传给 createWorker 后彻底切断 CDN 下载路径（完全本地、可离线）
 *  - isTessdataAvailable：语言数据缺失时让调用方快速失败/跳过，而非空跑到超时
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const requireFromHere = createRequire(import.meta.url);

/** 必备语言数据文件名（gzip 形态，与 createWorker gzip:true 对应）。 */
const REQUIRED_TRAINEDDATA = ['eng.traineddata.gz'];

/**
 * 解析本地 tessdata 目录：
 *  1. 环境变量 LINGXIAO_TESSDATA_DIR 显式覆盖（最高优先级）
 *  2. 从当前文件向上逐级查找 `tessdata/`，直到文件系统根
 *  3. 回退到 process.cwd()/tessdata
 * 命中判据：目录存在且含 eng.traineddata.gz（避免命中空目录）。
 */
function resolveTessdataDir(): string {
  const envDir = process.env.LINGXIAO_TESSDATA_DIR;
  if (envDir && hasTrainedData(envDir)) return envDir;

  // 从 __dirname 向上逐级找 tessdata/（覆盖 dist 同级、项目根、嵌套上层等布局）
  let cursor = __dirname;
  for (let i = 0; i < 8; i++) {
    const candidate = path.join(cursor, 'tessdata');
    if (hasTrainedData(candidate)) return candidate;
    const parent = path.dirname(cursor);
    if (parent === cursor) break; // 抵达文件系统根
    cursor = parent;
  }

  // 回退：cwd/tessdata（即便不存在也返回，让 isTessdataAvailable 统一判定）
  return path.join(process.cwd(), 'tessdata');
}

function hasTrainedData(dir: string): boolean {
  if (!dir || !fs.existsSync(dir)) return false;
  return REQUIRED_TRAINEDDATA.every((f) => fs.existsSync(path.join(dir, f)));
}

/** 本地 tessdata 目录（模块加载时解析一次）。 */
export const LOCAL_TESSDATA_DIR = resolveTessdataDir();

/** 语言数据是否本地可用——调用方据此快速跳过，避免 createWorker 空跑到超时。 */
export function isTessdataAvailable(): boolean {
  return hasTrainedData(LOCAL_TESSDATA_DIR);
}

/**
 * 解析 tesseract.js-core 的本地 corePath（含 wasm）。
 * 用 require.resolve 定位包目录，比相对路径稳健（不受 dist 层级影响）。
 * 解析失败返回 undefined——createWorker 不传 corePath 时退回默认（可能联网），
 * 调用方应结合 isTessdataAvailable 决定是否启用 OCR。
 */
export function resolveCorePath(): string | undefined {
  try {
    const corePkg = requireFromHere.resolve('tesseract.js-core/package.json');
    return path.dirname(corePkg);
  } catch {
    return undefined;
  }
}

/**
 * 解析 tesseract.js 的本地 worker 脚本（dist/worker.min.js）。
 * 同样用 require.resolve 定位，避免 CDN 下载 worker。
 */
export function resolveWorkerPath(): string | undefined {
  try {
    const tjsPkg = requireFromHere.resolve('tesseract.js/package.json');
    const workerPath = path.join(path.dirname(tjsPkg), 'dist', 'worker.min.js');
    return fs.existsSync(workerPath) ? workerPath : undefined;
  } catch {
    return undefined;
  }
}

/**
 * 组装传给 createWorker 的本地资源选项：langPath + corePath + workerPath。
 * 三者齐备时 tesseract 完全本地运行，不触发任何 CDN 下载。
 */
export function localWorkerOptions(): {
  langPath: string;
  corePath?: string;
  workerPath?: string;
} {
  const corePath = resolveCorePath();
  const workerPath = resolveWorkerPath();
  return {
    langPath: LOCAL_TESSDATA_DIR,
    ...(corePath ? { corePath } : {}),
    ...(workerPath ? { workerPath } : {}),
  };
}

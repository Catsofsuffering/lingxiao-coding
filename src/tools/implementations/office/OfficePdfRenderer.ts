/**
 * OfficePdfRenderer — 把 PPTX/DOCX/XLSX 等 Office 文档用 LibreOffice headless
 * 转成 PDF，供预览器复用现有 PDF iframe 做真实版式渲染。
 *
 * 设计要点：
 * - 异步 spawn（不阻塞 Fastify event loop）
 * - 基于源文件 路径+mtime+size 的磁盘缓存，命中直接复用，避免每次预览都跑 LibreOffice
 * - 同一缓存键的并发请求去重（in-flight Promise 复用）
 * - LibreOffice 不可用时返回结构化 unavailable，让上层回退到结构预览
 */

import { spawn } from 'child_process';
import { createHash } from 'crypto';
import { existsSync, statSync, mkdirSync, readdirSync, rmSync, renameSync, copyFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { resolveCommandPath, hiddenSpawnOpts } from '../../../utils/platform.js';

export type OfficeRenderableFormat = 'pptx' | 'ppt' | 'docx' | 'doc' | 'xlsx' | 'xls' | 'odp' | 'odt' | 'ods';

const RENDERABLE_EXTS = new Set<string>([
  '.pptx', '.ppt', '.docx', '.doc', '.xlsx', '.xls', '.odp', '.odt', '.ods',
]);

const SOFFICE_CANDIDATES = ['soffice', 'libreoffice', 'libreoffice7.6', 'libreoffice7.5', 'libreoffice7.4'];
const CONVERT_TIMEOUT_MS = 90_000;
const CACHE_DIR = path.join(tmpdir(), 'lingxiao-office-pdf-cache');
const CACHE_MAX_ENTRIES = 200;

export interface OfficePdfRenderOk {
  ok: true;
  pdfPath: string;
  fromCache: boolean;
  elapsedMs: number;
}

export interface OfficePdfRenderFail {
  ok: false;
  reason: string;
  code: 'unavailable' | 'unsupported' | 'convert_failed' | 'timeout' | 'source_missing';
}

export type OfficePdfRenderResult = OfficePdfRenderOk | OfficePdfRenderFail;

const inFlight = new Map<string, Promise<OfficePdfRenderResult>>();

export function isOfficeRenderable(filePath: string): boolean {
  return RENDERABLE_EXTS.has(path.extname(filePath).toLowerCase());
}

let cachedSofficePath: string | null | undefined;

export function resolveSoffice(): string | null {
  if (cachedSofficePath !== undefined) return cachedSofficePath;
  for (const name of SOFFICE_CANDIDATES) {
    const found = resolveCommandPath(name);
    if (found) {
      cachedSofficePath = found;
      return found;
    }
  }
  cachedSofficePath = null;
  return null;
}

function cacheKeyFor(filePath: string, mtimeMs: number, size: number): string {
  const h = createHash('sha1');
  h.update(filePath);
  h.update('\0');
  h.update(String(Math.floor(mtimeMs)));
  h.update('\0');
  h.update(String(size));
  return h.digest('hex');
}

function ensureCacheDir(): void {
  if (!existsSync(CACHE_DIR)) {
    mkdirSync(CACHE_DIR, { recursive: true });
  }
}

/**
 * LRU-ish 清理：缓存条目超过上限时，按 mtime 删最旧的。
 */
function pruneCache(): void {
  try {
    const entries = readdirSync(CACHE_DIR)
      .filter((name) => name.endsWith('.pdf'))
      .map((name) => {
        const full = path.join(CACHE_DIR, name);
        try {
          return { full, mtimeMs: statSync(full).mtimeMs };
        } catch {
          return { full, mtimeMs: 0 };
        }
      })
      .sort((a, b) => b.mtimeMs - a.mtimeMs);
    if (entries.length <= CACHE_MAX_ENTRIES) return;
    for (const stale of entries.slice(CACHE_MAX_ENTRIES)) {
      try {
        rmSync(stale.full, { force: true });
      } catch {
        /* best-effort */
      }
    }
  } catch {
    /* best-effort */
  }
}

function runSoffice(executable: string, srcPath: string, outDir: string): Promise<{ status: number | null; stderr: string; timedOut: boolean }> {
  return new Promise((resolve) => {
    const args = ['--headless', '--norestore', '--convert-to', 'pdf', '--outdir', outDir, srcPath];
    const child = spawn(executable, args, {
      ...hiddenSpawnOpts(),
      // 隔离用户 profile，避免并发实例争用同一 ~/.config/libreoffice 锁
      env: {
        ...process.env,
        HOME: process.env.HOME ?? tmpdir(),
      },
    });
    let stderr = '';
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
    }, CONVERT_TIMEOUT_MS);
    child.stderr?.on('data', (chunk) => {
      if (stderr.length < 4000) stderr += String(chunk);
    });
    child.on('error', () => {
      clearTimeout(timer);
      resolve({ status: -1, stderr: stderr || 'spawn error', timedOut });
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ status: code, stderr, timedOut });
    });
  });
}

async function doRender(filePath: string, cacheKey: string): Promise<OfficePdfRenderResult> {
  const executable = resolveSoffice();
  if (!executable) {
    return { ok: false, code: 'unavailable', reason: 'LibreOffice/soffice not found on PATH' };
  }

  ensureCacheDir();
  const cachedPdf = path.join(CACHE_DIR, `${cacheKey}.pdf`);
  if (existsSync(cachedPdf) && statSync(cachedPdf).size > 0) {
    return { ok: true, pdfPath: cachedPdf, fromCache: true, elapsedMs: 0 };
  }

  // 每次转换用独立临时输出目录，避免并发争用 + LibreOffice 按源文件名生成 pdf
  const started = performance.now();
  const workDir = path.join(CACHE_DIR, `work-${cacheKey}-${process.pid}-${Date.now()}`);
  mkdirSync(workDir, { recursive: true });
  try {
    const run = await runSoffice(executable, filePath, workDir);
    if (run.timedOut) {
      return { ok: false, code: 'timeout', reason: `LibreOffice conversion timed out after ${CONVERT_TIMEOUT_MS}ms` };
    }
    if (run.status !== 0) {
      return { ok: false, code: 'convert_failed', reason: `LibreOffice exited with ${run.status}: ${run.stderr.slice(0, 500)}` };
    }
    const produced = readdirSync(workDir).find((name) => name.toLowerCase().endsWith('.pdf'));
    if (!produced) {
      return { ok: false, code: 'convert_failed', reason: 'LibreOffice produced no PDF output' };
    }
    const producedPath = path.join(workDir, produced);
    if (statSync(producedPath).size === 0) {
      return { ok: false, code: 'convert_failed', reason: 'LibreOffice produced an empty PDF' };
    }
    // 原子落入缓存：先 rename（同分区），失败再 copy
    try {
      renameSync(producedPath, cachedPdf);
    } catch {
      copyFileSync(producedPath, cachedPdf);
    }
    pruneCache();
    return { ok: true, pdfPath: cachedPdf, fromCache: false, elapsedMs: Math.round(performance.now() - started) };
  } finally {
    try {
      rmSync(workDir, { recursive: true, force: true });
    } catch {
      /* best-effort */
    }
  }
}

/**
 * 把 Office 文档渲染为 PDF（带缓存与并发去重）。
 * 成功返回 { ok:true, pdfPath }，失败/不可用返回 { ok:false, code, reason }。
 */
export async function renderOfficeToPdf(filePath: string): Promise<OfficePdfRenderResult> {
  if (!isOfficeRenderable(filePath)) {
    return { ok: false, code: 'unsupported', reason: `format not renderable: ${path.extname(filePath)}` };
  }
  if (!existsSync(filePath) || !statSync(filePath).isFile()) {
    return { ok: false, code: 'source_missing', reason: 'source file not found' };
  }

  const stat = statSync(filePath);
  const cacheKey = cacheKeyFor(filePath, stat.mtimeMs, stat.size);

  const existing = inFlight.get(cacheKey);
  if (existing) return existing;

  const task = doRender(filePath, cacheKey).finally(() => {
    inFlight.delete(cacheKey);
  });
  inFlight.set(cacheKey, task);
  return task;
}

/** 测试/运维用：清空 LibreOffice 探测缓存。 */
export function _resetSofficeProbe(): void {
  cachedSofficePath = undefined;
}

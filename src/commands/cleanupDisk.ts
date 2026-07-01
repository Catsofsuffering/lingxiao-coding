/**
 * 磁盘清理 — 回收 ~/.lingxiao 下的多余占用。
 *
 * 背景（bug）：checkpoint 的 shadow git 在 ~/.lingxiao/checkpoints/<hash>/.git 下做快照，
 * gc/repack 写新 pack 时先落地 tmp_pack_*，进程被 kill / 超时 / OOM 时这些临时文件残留，
 * 每个可与仓库等大（大工作区 10GB+），一次次累积把磁盘吃光（用户实测十几个 10.75GB）。
 *
 * 本模块提供一键清理：
 *  - stale tmp_pack：各 checkpoint 仓库 objects/pack 下的 tmp_pack_*（死文件，删除绝对安全）
 *  - checkpoints：可选整体删除全部 shadow git 仓库（放弃 /rewind 历史，回收最多空间）
 * 默认 dry-run 只报告不删除，需显式 apply 才落地。
 */

import fs from 'node:fs';
import path from 'node:path';
import { CONFIG_DIR } from '../config.js';

export interface CleanupEntry {
  path: string;
  bytes: number;
  kind: 'tmp_pack' | 'checkpoint_repo';
}

export interface CleanupReport {
  entries: CleanupEntry[];
  totalBytes: number;
  removed: boolean;
}

function safeStatSize(p: string): number {
  try {
    return fs.statSync(p).size;
  } catch {
    return 0;
  }
}

function dirSizeBytes(dir: string): number {
  let total = 0;
  let stack: string[] = [dir];
  while (stack.length > 0) {
    const cur = stack.pop()!;
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(cur, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const full = path.join(cur, e.name);
      if (e.isDirectory()) {
        stack.push(full);
      } else if (e.isFile() || e.isSymbolicLink()) {
        total += safeStatSize(full);
      }
    }
  }
  return total;
}

function checkpointsRoot(): string {
  return path.join(CONFIG_DIR, 'checkpoints');
}

/**
 * 扫描所有 checkpoint 仓库下遗留的 tmp_pack_* 临时文件。
 */
function scanStaleTmpPacks(): CleanupEntry[] {
  const root = checkpointsRoot();
  const out: CleanupEntry[] = [];
  if (!fs.existsSync(root)) return out;
  let repos: fs.Dirent[];
  try {
    repos = fs.readdirSync(root, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const repo of repos) {
    if (!repo.isDirectory()) continue;
    const packDir = path.join(root, repo.name, '.git', 'objects', 'pack');
    if (!fs.existsSync(packDir)) continue;
    let names: string[];
    try {
      names = fs.readdirSync(packDir);
    } catch {
      continue;
    }
    for (const name of names) {
      if (!name.startsWith('tmp_pack')) continue;
      const full = path.join(packDir, name);
      out.push({ path: full, bytes: safeStatSize(full), kind: 'tmp_pack' });
    }
  }
  return out;
}

/**
 * 扫描全部 checkpoint shadow git 仓库（整体目录）。
 * 删除它们会放弃 /rewind 的历史快照，但回收空间最多。
 */
function scanCheckpointRepos(): CleanupEntry[] {
  const root = checkpointsRoot();
  const out: CleanupEntry[] = [];
  if (!fs.existsSync(root)) return out;
  let repos: fs.Dirent[];
  try {
    repos = fs.readdirSync(root, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const repo of repos) {
    if (!repo.isDirectory()) continue;
    const full = path.join(root, repo.name);
    out.push({ path: full, bytes: dirSizeBytes(full), kind: 'checkpoint_repo' });
  }
  return out;
}

export interface CleanupOptions {
  /** 是否真正删除；false 时只扫描报告（dry-run）。 */
  apply?: boolean;
  /** all=连同整个 checkpoint 仓库一起删（回收最多）；tmp=只删 tmp_pack 死文件（安全）。默认 tmp。 */
  scope?: 'tmp' | 'all';
}

/**
 * 执行磁盘清理（或 dry-run 扫描）。
 */
export function cleanupLingxiaoDisk(options: CleanupOptions = {}): CleanupReport {
  const apply = options.apply === true;
  const scope = options.scope ?? 'tmp';

  const entries = scope === 'all' ? scanCheckpointRepos() : scanStaleTmpPacks();
  const totalBytes = entries.reduce((sum, e) => sum + e.bytes, 0);

  if (apply) {
    for (const entry of entries) {
      try {
        fs.rmSync(entry.path, { recursive: true, force: true });
      } catch {
        // best-effort：单个删除失败不阻塞其余
      }
    }
  }

  return { entries, totalBytes, removed: apply };
}

export function formatBytes(bytes: number): string {
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

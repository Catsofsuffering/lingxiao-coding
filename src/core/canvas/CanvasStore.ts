/**
 * CanvasStore —— 剑阁可交互 Canvas 的持久化层。
 *
 * 每份产物在 .lingxiao/canvas/<artifactId>/ 下持久化三类状态：
 *   - sourcemap.json   CanvasSourceMap（nodeId ↔ spec/script 锚点）
 *   - versions.json    CanvasVersion[]（版本栈）+ activeVersion 指针
 *   - comments.json    CanvasComment[]（结构化批注）
 *   - versions/<n>/    每个版本的产物快照（HTML/PNG/PPTX 等）
 *
 * artifactId 规范化：产物相对 workspace 路径 → 去掉非法字符的稳定 ID。
 * 设计为纯磁盘 store，无数据库依赖；并发写用「读-改-写」+ 原子 rename。
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  renameSync,
  readdirSync,
  cpSync,
} from 'node:fs';
import { join, dirname, resolve, isAbsolute, relative } from 'node:path';
import type {
  CanvasSourceMap,
  CanvasVersion,
  CanvasComment,
  CanvasArtifactState,
  CanvasVersionStatus,
} from '../../contracts/types/Canvas.js';

/** 把产物相对路径规范化成文件系统安全的 artifactId。 */
export function toArtifactId(artifactPath: string): string {
  return artifactPath
    .replace(/^[./]+/, '')
    .replace(/[^a-zA-Z0-9._\-/]/g, '_')
    .replace(/\//g, '__');
}

interface VersionsFile {
  activeVersion: number;
  versions: CanvasVersion[];
}

export interface CanvasStoreOptions {
  /** 会话根目录（.lingxiao/sessions/<id>），canvas 状态挂在它下面。 */
  sessionDir: string;
  /** workspace 根，用于把绝对产物路径转相对。 */
  workspace?: string;
  /** 版本入栈后的回调（用于 emit canvas:version_pushed SSE 事件）。 */
  onVersionPushed?: (artifactId: string, version: number) => void;
}

export class CanvasStore {
  private readonly canvasRoot: string;
  private readonly workspace?: string;
  private readonly onVersionPushed?: (artifactId: string, version: number) => void;

  constructor(opts: CanvasStoreOptions) {
    this.canvasRoot = join(opts.sessionDir, 'canvas');
    this.workspace = opts.workspace;
    this.onVersionPushed = opts.onVersionPushed;
  }

  /** 规范化产物路径：绝对路径转相对 workspace。 */
  private normalizePath(artifactPath: string): string {
    if (this.workspace && isAbsolute(artifactPath)) {
      return relative(this.workspace, artifactPath);
    }
    return artifactPath;
  }

  private dirFor(artifactId: string): string {
    return join(this.canvasRoot, artifactId);
  }

  private ensureDir(p: string): void {
    if (!existsSync(p)) mkdirSync(p, { recursive: true });
  }

  /** 原子写 JSON：先写临时文件再 rename。 */
  private writeJson(filePath: string, data: unknown): void {
    this.ensureDir(dirname(filePath));
    const tmp = `${filePath}.tmp-${process.pid}-${Date.now()}`;
    writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf-8');
    renameSync(tmp, filePath);
  }

  private readJson<T>(filePath: string): T | null {
    if (!existsSync(filePath)) return null;
    try {
      return JSON.parse(readFileSync(filePath, 'utf-8')) as T;
    } catch {
      return null;
    }
  }

  // ─── SourceMap ────────────────────────────────────────────────

  saveSourceMap(map: CanvasSourceMap): string {
    const artifactId = map.artifactId || toArtifactId(this.normalizePath(map.artifactPath));
    const normalized: CanvasSourceMap = {
      ...map,
      artifactId,
      artifactPath: this.normalizePath(map.artifactPath),
    };
    this.writeJson(join(this.dirFor(artifactId), 'sourcemap.json'), normalized);
    return artifactId;
  }

  getSourceMap(artifactId: string): CanvasSourceMap | null {
    return this.readJson<CanvasSourceMap>(join(this.dirFor(artifactId), 'sourcemap.json'));
  }

  // ─── Versions（版本栈）────────────────────────────────────────

  private versionsPath(artifactId: string): string {
    return join(this.dirFor(artifactId), 'versions.json');
  }

  private loadVersions(artifactId: string): VersionsFile {
    return this.readJson<VersionsFile>(this.versionsPath(artifactId)) ?? { activeVersion: 0, versions: [] };
  }

  /**
   * 新版本入栈。把当前产物快照复制到 versions/<n>/，旧 active 版本标记 superseded，
   * 返回新版本号。
   */
  pushVersion(input: {
    artifactId: string;
    artifactSnapshotPath?: string;
    intent?: string;
    changedFiles?: string[];
  }): CanvasVersion {
    const store = this.loadVersions(input.artifactId);
    const nextNumber = store.versions.length === 0
      ? 1
      : Math.max(...store.versions.map((v) => v.version)) + 1;

    // 旧 active 版本标记为 superseded
    for (const v of store.versions) {
      if (v.status === 'active') v.status = 'superseded';
    }

    let snapshotRel: string | undefined;
    if (input.artifactSnapshotPath && existsSync(input.artifactSnapshotPath)) {
      const snapDir = join(this.dirFor(input.artifactId), 'versions', String(nextNumber));
      this.ensureDir(snapDir);
      const target = join(snapDir, 'artifact' + extOf(input.artifactSnapshotPath));
      cpSync(input.artifactSnapshotPath, target, { recursive: false });
      snapshotRel = relative(this.canvasRoot, target);
    }

    const version: CanvasVersion = {
      version: nextNumber,
      snapshotPath: snapshotRel,
      intent: input.intent,
      changedFiles: input.changedFiles,
      status: 'active',
      createdAt: Date.now(),
    };
    store.versions.push(version);
    store.activeVersion = nextNumber;
    this.writeJson(this.versionsPath(input.artifactId), store);
    try { this.onVersionPushed?.(input.artifactId, nextNumber); } catch {/* emit 失败不影响持久化 */}
    return version;
  }

  listVersions(artifactId: string): CanvasVersion[] {
    return this.loadVersions(artifactId).versions;
  }

  getActiveVersion(artifactId: string): number {
    return this.loadVersions(artifactId).activeVersion;
  }

  /** 切换/回退到指定版本（不删除其它版本，只改 active 指针与状态）。 */
  switchVersion(artifactId: string, target: number): boolean {
    const store = this.loadVersions(artifactId);
    const found = store.versions.find((v) => v.version === target);
    if (!found) return false;
    for (const v of store.versions) {
      if (v.version === target) v.status = 'active';
      else if (v.status === 'active') v.status = 'reverted';
    }
    store.activeVersion = target;
    this.writeJson(this.versionsPath(artifactId), store);
    return true;
  }

  // ─── Comments（结构化批注）────────────────────────────────────

  private commentsPath(artifactId: string): string {
    return join(this.dirFor(artifactId), 'comments.json');
  }

  listComments(artifactId: string): CanvasComment[] {
    return this.readJson<CanvasComment[]>(this.commentsPath(artifactId)) ?? [];
  }

  addComment(comment: CanvasComment): void {
    const list = this.listComments(comment.artifactId);
    list.push(comment);
    this.writeJson(this.commentsPath(comment.artifactId), list);
  }

  updateCommentStatus(artifactId: string, commentId: string, status: CanvasCommentStatusLike): boolean {
    const list = this.listComments(artifactId);
    const c = list.find((x) => x.id === commentId);
    if (!c) return false;
    c.status = status;
    c.updatedAt = Date.now();
    this.writeJson(this.commentsPath(artifactId), list);
    return true;
  }

  // ─── 聚合 ─────────────────────────────────────────────────────

  getArtifactState(artifactId: string): CanvasArtifactState | null {
    const sourceMap = this.getSourceMap(artifactId);
    if (!sourceMap) return null;
    return {
      artifactId,
      sourceMap,
      versions: this.listVersions(artifactId),
      activeVersion: this.getActiveVersion(artifactId),
      comments: this.listComments(artifactId),
    };
  }

  /** 列出所有已登记的产物 ID。 */
  listArtifacts(): string[] {
    if (!existsSync(this.canvasRoot)) return [];
    return readdirSync(this.canvasRoot, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => d.name)
      .filter((id) => existsSync(join(this.canvasRoot, id, 'sourcemap.json')));
  }
}

type CanvasCommentStatusLike = CanvasComment['status'];

function extOf(p: string): string {
  const m = /\.[a-zA-Z0-9]+$/.exec(p);
  return m ? m[0] : '';
}

// 满足 lint：显式引用类型，避免未使用告警
export type { CanvasVersionStatus };

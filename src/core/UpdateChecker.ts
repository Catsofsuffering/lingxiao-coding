/**
 * UpdateChecker — 启动后自动检查新版本
 *
 * 功能：
 * 1. 服务器启动后延迟 10s 异步检查 GitHub releases
 * 2. 发现新版本时 emit notification:new 推送到 TUI / WebUI
 * 3. 每 24h 定期检查
 * 4. 同版本不重复通知（进程生命周期内）
 *
 * 使用 native fetch（Node 18+），不阻塞启动。
 */

import { EventEmitter } from './EventEmitter.js';
import { VERSION } from '../version.js';

const GITHUB_API = 'https://api.github.com/repos/hexian2001/lingxiao-coding/releases/latest';
const CHECK_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 小时
const INITIAL_DELAY_MS = 10_000; // 启动后 10s

interface ReleaseInfo {
  tag: string;
  version: string;
  htmlUrl: string;
  publishedAt: string;
}

// ── semver 比较（与 cli_upgrade.ts 保持一致） ──────────────────────────────────

function parseSemver(v: string): [number, number, number] {
  const cleaned = v.replace(/^v/, '');
  const parts = cleaned.split('.').map((s) => parseInt(s, 10) || 0);
  return [parts[0] || 0, parts[1] || 0, parts[2] || 0];
}

function compareVersions(a: string, b: string): number {
  const [aMaj, aMin, aPat] = parseSemver(a);
  const [bMaj, bMin, bPat] = parseSemver(b);
  if (aMaj !== bMaj) return aMaj - bMaj;
  if (aMin !== bMin) return aMin - bMin;
  return aPat - bPat;
}

// ── UpdateChecker ─────────────────────────────────────────────────────────────

export class UpdateChecker {
  private emitter: EventEmitter;
  private getActiveSessionIds: () => string[];
  private intervalTimer: ReturnType<typeof setInterval> | null = null;
  private initialTimer: ReturnType<typeof setTimeout> | null = null;
  /** 已通知过的版本，避免重复推送 */
  private notifiedVersions: Set<string> = new Set();

  constructor(emitter: EventEmitter, getActiveSessionIds: () => string[]) {
    this.emitter = emitter;
    this.getActiveSessionIds = getActiveSessionIds;
  }

  /** 启动检查 */
  start(): void {
    // 延迟首次检查，等待 session 初始化
    this.initialTimer = setTimeout(() => {
      this.check().catch(() => { /* 静默失败 */ });
    }, INITIAL_DELAY_MS);
    this.initialTimer.unref?.();

    // 定期检查
    this.intervalTimer = setInterval(() => {
      this.check().catch(() => { /* 静默失败 */ });
    }, CHECK_INTERVAL_MS);
    this.intervalTimer.unref?.();
  }

  /** 停止检查 */
  stop(): void {
    if (this.initialTimer) {
      clearTimeout(this.initialTimer);
      this.initialTimer = null;
    }
    if (this.intervalTimer) {
      clearInterval(this.intervalTimer);
      this.intervalTimer = null;
    }
  }

  /** 执行一次版本检查 */
  private async check(): Promise<void> {
    let release: ReleaseInfo;
    try {
      release = await this.fetchLatestRelease();
    } catch {
      return; // 网络不可用或 API 异常，静默跳过
    }

    if (compareVersions(release.version, VERSION) <= 0) return; // 已是最新

    if (this.notifiedVersions.has(release.version)) return; // 已通知过
    this.notifiedVersions.add(release.version);

    this.emitUpdateNotification(release);
  }

  /** 异步查询 GitHub releases/latest */
  private async fetchLatestRelease(): Promise<ReleaseInfo> {
    const response = await fetch(GITHUB_API, {
      headers: { 'User-Agent': 'lingxiao-cli' },
      signal: AbortSignal.timeout(15000),
    });

    if (!response.ok) {
      throw new Error(`GitHub API returned ${response.status}`);
    }

    const data = await response.json() as { tag_name?: string; html_url?: string; published_at?: string };
    const tag = data.tag_name || '';
    if (!tag) throw new Error('GitHub API format error');

    return {
      tag,
      version: tag.replace(/^v/, ''),
      htmlUrl: data.html_url || '',
      publishedAt: data.published_at || '',
    };
  }

  /** 向所有活跃 session 推送更新通知 */
  private emitUpdateNotification(release: ReleaseInfo): void {
    const notification = {
      id: `update_available_${release.version}_${Date.now()}`,
      type: 'update_available',
      priority: 'important' as const,
      title: `★ 发现新版本 ${release.tag}`,
      message: `当前版本 v${VERSION}，最新版本 ${release.tag}。运行 \`lingxiao upgrade\` 升级。`,
      timestamp: Date.now(),
      read: false,
    };

    const sessionIds = this.getActiveSessionIds();
    if (sessionIds.length > 0) {
      for (const sid of sessionIds) {
        this.emitter.emit('notification:new', { ...notification, sessionId: sid });
      }
    } else {
      // 无活跃 session — TUI 通过本地 emitter 仍可收到
      this.emitter.emit('notification:new', notification);
    }
  }
}

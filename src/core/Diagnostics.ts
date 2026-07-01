/**
 * Diagnostics Bundle Builder
 *
 * 聚合运行时诊断信息，生成可直接贴到 GitHub issue 的 markdown，
 * 并可选打包为 zip 写到 ~/.lingxiao/logs/diagnostics-<时间戳>.zip。
 *
 * 聚合内容：
 * - 环境信息：版本 / 平台 / 架构 / Node 版本
 * - lingxiao.log 尾部（最近 ~500 行或 200KB）
 * - 最新 crash 报告（若有）
 * - agent_logs 摘要（从 SQLite 只读查询最近若干条；不可访问则跳过并注明）
 * - 内存最近日志（环形缓冲）
 *
 * 脱敏：复用 CrashReporter 的 redactSensitive / redactSensitiveString。
 * best-effort：所有外部 IO 单独 try-catch，单点失败不影响整体产出。
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { open } from 'node:fs/promises';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import {
  getLogDir,
  getMainLogFilePath,
  getRecentLogEntries,
  type LogEntry,
} from './Log.js';
import { listCrashReports, redactSensitive, redactSensitiveString } from './CrashReporter.js';
import { VERSION } from '../version.js';

// ─── Constants ───

const DEFAULT_LOG_TAIL_BYTES = 200 * 1024; // 200KB
const DEFAULT_LOG_TAIL_LINES = 500;
const DEFAULT_AGENT_LOG_LIMIT = 50;
const DEFAULT_RECENT_LOG_LIMIT = 200;

// ─── Types ───

export interface DiagnosticsOptions {
  /** 是否额外打包为 zip。默认 false（仅返回 markdown 字符串）。 */
  zip?: boolean;
  /** 可选 session ID：用于在报告中标注并优先关联该 session 的 agent_logs。 */
  sessionId?: string;
  /** 自定义 SQLite 数据库路径。默认 ~/.lingxiao/data.db。 */
  dbPath?: string;
  /** lingxiao.log 尾部最多读取字节数。默认 200KB。 */
  logTailBytes?: number;
  /** lingxiao.log 尾部最多保留行数。默认 500。 */
  logTailLines?: number;
  /** agent_logs 查询最近条数。默认 50。 */
  agentLogLimit?: number;
  /** 内存最近日志最多包含条数。默认 200。 */
  recentLogLimit?: number;
}

export interface DiagnosticsBundle {
  /** 可直接贴到 GitHub issue 的 markdown。 */
  markdown: string;
  /** 本次产出的文件绝对路径列表（markdown 文件 + 可选 zip）。 */
  files: string[];
  /** 若成功打包 zip，为 zip 绝对路径。 */
  bundlePath?: string;
}

interface AgentLogRow {
  session_id: string | null;
  agent_id: string | null;
  agent_name: string | null;
  agent_role: string | null;
  task_id: string | null;
  event_type: string | null;
  action: string | null;
  timestamp: number | null;
}

// ─── Path helpers ───

function getDefaultDbPath(): string {
  return join(homedir(), '.lingxiao', 'data.db');
}

// ─── Section builders (each best-effort) ───

function buildEnvSection(sessionId?: string): string {
  const lines: string[] = [];
  lines.push('## 环境信息');
  lines.push('');
  lines.push('| 项 | 值 |');
  lines.push('| --- | --- |');
  lines.push(`| 版本 | ${VERSION} |`);
  lines.push(`| 平台 | ${process.platform} |`);
  lines.push(`| 架构 | ${process.arch} |`);
  lines.push(`| Node 版本 | ${process.version} |`);
  lines.push(`| 生成时间 | ${new Date().toISOString()} |`);
  if (sessionId) lines.push(`| Session | ${sessionId} |`);
  lines.push('');
  return lines.join('\n');
}

function buildCrashSection(): { markdown: string; latestCrashPath?: string } {
  try {
    const reports = listCrashReports();
    if (reports.length === 0) {
      return { markdown: '## 错误摘要\n\n_无 crash 报告。_\n' };
    }
    const latest = reports[0];
    const lines: string[] = [];
    lines.push('## 错误摘要');
    lines.push('');
    lines.push(`最新崩溃报告：\`${latest.path}\`（共 ${reports.length} 个）`);
    lines.push('');
    try {
      const raw = readFileSync(latest.path, 'utf-8');
      // 文件内容写入时已脱敏，这里再做一次字符串级兜底
      const safe = redactSensitiveString(raw);
      const parsed = JSON.parse(safe) as {
        ts?: string;
        source?: string;
        error?: { name?: string; message?: string; stack?: string };
      };
      lines.push('```json');
      lines.push(
        JSON.stringify(
          {
            ts: parsed.ts,
            source: parsed.source,
            error: {
              name: parsed.error?.name,
              message: parsed.error?.message,
              stack: parsed.error?.stack,
            },
          },
          null,
          2,
        ),
      );
      lines.push('```');
    } catch {
      lines.push('_无法解析最新 crash 报告内容。_');
    }
    lines.push('');
    return { markdown: lines.join('\n'), latestCrashPath: latest.path };
  } catch {
    return { markdown: '## 错误摘要\n\n_读取 crash 报告失败。_\n' };
  }
}

/**
 * 读取文件末尾最多 maxBytes 字节，再按行截取最后 maxLines 行。
 * best-effort，失败返回空字符串。
 */
async function readFileTail(filePath: string, maxBytes: number, maxLines: number): Promise<string> {
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  try {
    if (!existsSync(filePath)) return '';
    const st = statSync(filePath);
    if (!st.isFile() || st.size === 0) return '';
    const start = Math.max(0, st.size - maxBytes);
    const length = st.size - start;
    handle = await open(filePath, 'r');
    const buf = Buffer.alloc(length);
    await handle.read(buf, 0, length, start);
    let text = buf.toString('utf-8');
    // 若从中间截断，丢弃第一段不完整行
    if (start > 0) {
      const nl = text.indexOf('\n');
      if (nl >= 0) text = text.slice(nl + 1);
    }
    const allLines = text.split('\n');
    const tail = allLines.slice(Math.max(0, allLines.length - maxLines));
    return tail.join('\n');
  } catch {
    return '';
  } finally {
    if (handle) {
      try {
        await handle.close();
      } catch {
        // ignore
      }
    }
  }
}

async function buildLogTailSection(opts: Required<Pick<DiagnosticsOptions, 'logTailBytes' | 'logTailLines'>>): Promise<string> {
  const lines: string[] = ['## 最近日志（lingxiao.log 尾部）', ''];
  const logPath = getMainLogFilePath();
  const tail = await readFileTail(logPath, opts.logTailBytes, opts.logTailLines);
  if (!tail.trim()) {
    lines.push('_无主日志或日志为空。_');
    lines.push('');
    return lines.join('\n');
  }
  const safe = redactSensitiveString(tail);
  lines.push('```');
  lines.push(safe);
  lines.push('```');
  lines.push('');
  return lines.join('\n');
}

function buildAgentLogSection(dbPath: string, sessionId: string | undefined, limit: number): string {
  const lines: string[] = ['## Agent 日志摘要', ''];
  if (!existsSync(dbPath)) {
    lines.push('_未找到会话数据库，已跳过 agent_logs 摘要。_');
    lines.push('');
    return lines.join('\n');
  }
  let db: DatabaseSync | undefined;
  try {
    db = new DatabaseSync(dbPath, { readOnly: true });
    let rows: AgentLogRow[];
    const cols = 'session_id, agent_id, agent_name, agent_role, task_id, event_type, action, timestamp';
    if (sessionId) {
      const stmt = db.prepare(
        `SELECT ${cols} FROM agent_logs WHERE session_id = ? ORDER BY timestamp DESC LIMIT ?`,
      );
      rows = stmt.all(sessionId, limit) as unknown as AgentLogRow[];
    } else {
      const stmt = db.prepare(`SELECT ${cols} FROM agent_logs ORDER BY timestamp DESC LIMIT ?`);
      rows = stmt.all(limit) as unknown as AgentLogRow[];
    }
    if (rows.length === 0) {
      lines.push('_agent_logs 表无匹配记录。_');
      lines.push('');
      return lines.join('\n');
    }
    lines.push(`最近 ${rows.length} 条（倒序）：`);
    lines.push('');
    lines.push('| 时间 | agent | role | task | event | action |');
    lines.push('| --- | --- | --- | --- | --- | --- |');
    for (const r of rows) {
      const ts = typeof r.timestamp === 'number' ? new Date(r.timestamp).toISOString() : '-';
      lines.push(
        `| ${ts} | ${r.agent_name ?? r.agent_id ?? '-'} | ${r.agent_role ?? '-'} | ${r.task_id ?? '-'} | ${r.event_type ?? '-'} | ${r.action ?? '-'} |`,
      );
    }
    lines.push('');
    return lines.join('\n');
  } catch (err) {
    lines.push(`_读取 agent_logs 失败，已跳过：${err instanceof Error ? err.message : String(err)}_`);
    lines.push('');
    return lines.join('\n');
  } finally {
    if (db) {
      try {
        db.close();
      } catch {
        // ignore
      }
    }
  }
}

function buildRecentMemorySection(limit: number): { markdown: string; entries: LogEntry[] } {
  const lines: string[] = ['## 内存最近日志（环形缓冲）', ''];
  let entries: LogEntry[] = [];
  try {
    entries = getRecentLogEntries(limit);
  } catch {
    entries = [];
  }
  if (entries.length === 0) {
    lines.push('_内存缓冲无记录。_');
    lines.push('');
    return { markdown: lines.join('\n'), entries };
  }
  const safe = redactSensitive(entries);
  lines.push('```json');
  lines.push(JSON.stringify(safe, null, 2));
  lines.push('```');
  lines.push('');
  return { markdown: lines.join('\n'), entries: safe };
}

function buildRepro(): string {
  return [
    '## 复现说明',
    '',
    '<!-- 请补充以下信息 -->',
    '- 期望行为：',
    '- 实际行为：',
    '- 复现步骤：',
    '  1. ',
    '  2. ',
    '  3. ',
    '',
  ].join('\n');
}

// ─── Public API ───

/**
 * 构建诊断包。聚合环境 / 错误 / 日志信息为 markdown，
 * 并将 markdown 文件写到 ~/.lingxiao/logs/diagnostics-<时间戳>.md。
 * opts.zip 为 true 时尝试额外打包 zip（用 jszip）；打包失败则仅保留 markdown 文件并在结果中注明。
 *
 * best-effort：各 section 独立 try-catch，单点失败不影响整体产出。
 */
export async function buildDiagnosticsBundle(opts: DiagnosticsOptions = {}): Promise<DiagnosticsBundle> {
  const logTailBytes = opts.logTailBytes ?? DEFAULT_LOG_TAIL_BYTES;
  const logTailLines = opts.logTailLines ?? DEFAULT_LOG_TAIL_LINES;
  const agentLogLimit = opts.agentLogLimit ?? DEFAULT_AGENT_LOG_LIMIT;
  const recentLogLimit = opts.recentLogLimit ?? DEFAULT_RECENT_LOG_LIMIT;
  const dbPath = opts.dbPath ?? getDefaultDbPath();

  const envMd = buildEnvSection(opts.sessionId);
  const crash = buildCrashSection();
  const logTailMd = await buildLogTailSection({ logTailBytes, logTailLines });
  const agentMd = buildAgentLogSection(dbPath, opts.sessionId, agentLogLimit);
  const recent = buildRecentMemorySection(recentLogLimit);

  const notes: string[] = [];

  const markdown = [
    `# 凌霄诊断报告`,
    '',
    envMd,
    crash.markdown,
    logTailMd,
    agentMd,
    recent.markdown,
    buildRepro(),
  ].join('\n');

  const files: string[] = [];
  let bundlePath: string | undefined;

  // 写 markdown 文件
  const logDir = getLogDir();
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const mdPath = join(logDir, `diagnostics-${stamp}.md`);
  try {
    if (!existsSync(logDir)) mkdirSync(logDir, { recursive: true });
    writeFileSync(mdPath, markdown, 'utf-8');
    files.push(mdPath);
  } catch {
    // markdown 落盘失败不影响返回 markdown 字符串
  }

  if (opts.zip) {
    try {
      const { default: JSZip } = await import('jszip');
      const zip = new JSZip();
      zip.file('diagnostics.md', markdown);
      // 附带最新 crash 报告原文（已脱敏写盘）
      if (crash.latestCrashPath && existsSync(crash.latestCrashPath)) {
        try {
          const raw = readFileSync(crash.latestCrashPath, 'utf-8');
          zip.file('crash.json', redactSensitiveString(raw));
        } catch {
          // 跳过 crash 附件
        }
      }
      const buf = await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' });
      const zipPath = join(logDir, `diagnostics-${stamp}.zip`);
      writeFileSync(zipPath, buf);
      files.push(zipPath);
      bundlePath = zipPath;
    } catch (err) {
      notes.push(
        `> 注意：zip 打包失败，仅产出 markdown 文件。原因：${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  const finalMarkdown = notes.length > 0 ? `${markdown}\n\n${notes.join('\n')}\n` : markdown;

  return { markdown: finalMarkdown, files, bundlePath };
}

/**
 * ContractFormatParser — 黑板格式解析统一入口
 *
 * 将散落在 WorkerOutputParser、LeaderBlackboard 中的格式解析逻辑收拢为单一模块。
 *
 * 职责：
 * 1. `parseAndValidateGraphBlocks` — 统一入口，封装 3 层 fallback：
 *    a) 主解析：parseWorkerOutput 从 Worker 输出文本提取 graph_* 代码块
 *    b) Fallback 1：scanSessionFilesForContracts 从 session context/ 和 scratchpad/ 扫描文件
 *    c) Fallback 2：extractContractsFromTaskMetadata 从任务 orchestration.contract 元数据提取
 * 2. `extractContractsFromTaskMetadata` — 从任务元数据提取契约（独立导出）
 *
 * WorkerOutputParser.ts 和 schemas.ts 保持为内部实现细节，本模块调用它们。
 * 所有现有调用者（LeaderBlackboard applyOutput callback）不改行为。
 */

import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseManager } from '../Database.js';
import type { GraphNode, WorkerGraphOutput } from './types.js';
import { parseWorkerOutput, type ParseResult } from './WorkerOutputParser.js';
import { getContractsDir, getProjectContractsDir, sanitizeSurfaceForFilename } from '../ContractPack.js';

// ═══════════════════════════════════════════════════════════════
// 类型
// ═══════════════════════════════════════════════════════════════

/**
 * 解析选项——注入外部依赖，保持模块纯函数风格。
 * db 和 getActiveContract 仅 Fallback 2 需要，可选。
 */
export interface ParseAndValidateOptions {
  taskId: string;
  workspace: string;
  /** 数据库管理器，用于 Fallback 2 从任务元数据提取契约 */
  db?: DatabaseManager;
  /** 查询活跃契约的回调，用于 Fallback 2 去重 */
  getActiveContract?: (sessionId: string, surface: string) => GraphNode | null;
}

/**
 * 统一入口的增强解析结果——在 ParseResult 基础上增加 fallback 来源信息。
 */
export interface ContractFormatParseResult extends ParseResult {
  /** Fallback 来源标记，用于日志区分 */
  contractFallbackSource?: 'inline' | 'session-files' | 'task-metadata' | 'none';
}

// ═══════════════════════════════════════════════════════════════
// Fallback 1: 文件扫描
// ═══════════════════════════════════════════════════════════════

/**
 * Fallback 扫描：当 worker completion 提到了 graph_contract 但未内联代码块时,
 * 从 session 的 context/ 和 scratchpad/ 目录中搜索 graph_contract 块。
 *
 * 常见场景：architect worker 用 file_create 把 graph_contract 写到文件,
 * 用 team_message 广播描述,但 attempt_completion 文本只引用了文件路径。
 * 没有 fallback 时,contract 节点永远不会进入黑板图 → ContractPack 不构建 →
 * 项目级 .lingxiao/contracts/ 不创建 → Web UI 永远显示"无契约"。
 */
export function scanSessionFilesForContracts(
  sessionId: string,
  taskId: string,
  workspace: string,
): NonNullable<WorkerGraphOutput['newContracts']> {
  const result: NonNullable<WorkerGraphOutput['newContracts']> = [];
  const sessionDir = join(workspace, '.lingxiao', 'sessions', sessionId);

  for (const subdir of ['context', 'scratchpad'] as const) {
    const dir = join(sessionDir, subdir);
    if (!existsSync(dir)) continue;

    let files: string[];
    try {
      files = readdirSync(dir);
    } catch { continue; }

    for (const file of files) {
      // scratchpad 只扫描当前 task 的文件;context 扫描所有 contract-* 文件
      if (subdir === 'scratchpad') {
        const taskNum = taskId.startsWith('T-') ? taskId : `T-${taskId}`;
        if (!file.startsWith(taskNum)) continue;
      } else {
        if (!file.includes('contract')) continue;
      }

      const filePath = join(dir, file);
      try {
        const content = readFileSync(filePath, 'utf-8');
        const parsed = parseWorkerOutput(content, sessionId);
        if (parsed.output.newContracts && parsed.output.newContracts.length > 0) {
          result.push(...parsed.output.newContracts);
        }
      } catch { /* skip unreadable files */ }
    }
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════
// Fallback 2: 任务元数据提取
// ═══════════════════════════════════════════════════════════════

/**
 * 从任务的 orchestration.contract 元数据构造 contract 节点。
 *
 * 当 worker completion.result 不含 ```graph_contract 块时,检查任务自身的契约模板。
 * Leader 通过 create_task(contract={surface,title,content}) 创建契约任务时,
 * 契约正文存在 orchestration.contract 中。worker 完成后 attempt_completion
 * 的 contract_compliance 只是遵守证明(surface+status+evidence),不含正文。
 *
 * @returns contract 节点对象，或 null（无元数据 / 非 contract 任务 / 已有同 surface 活跃契约）
 */
/**
 * BUG FIX: 从磁盘加载已有的完整契约内容,防止薄模板覆盖 worker 写入的完整契约。
 *
 * Fallback 2 只在 worker completion 无 graph_contract 块时触发。
 * 如果此时磁盘已有同 surface 的契约文件,它一定是 worker 通过 file_create
 * 等方式写入的完整契约（模板只存在于 orchestration.contract 元数据中）。
 * 此时应该用磁盘文件内容构造 graph 节点,而非用薄模板——否则后续
 * persistContractPack 会用模板覆盖磁盘完整文件。
 *
 * 检查顺序: session-scoped contracts dir → project-level .lingxiao/contracts/
 */
function loadContractContentFromDisk(
  surface: string,
  sessionId: string,
  workspace: string,
): string | null {
  const fileName = `${sanitizeSurfaceForFilename(surface)}.json`;
  const searchDirs = [
    getContractsDir(sessionId, workspace),
    getProjectContractsDir(workspace),
  ];
  for (const dir of searchDirs) {
    const filePath = join(dir, fileName);
    if (!existsSync(filePath)) continue;
    try {
      const raw = readFileSync(filePath, 'utf8');
      const parsed = JSON.parse(raw) as unknown;
      if (parsed && typeof parsed === 'object'
        && typeof (parsed as Record<string, unknown>).content === 'string'
        && typeof (parsed as Record<string, unknown>).surface === 'string') {
        const fileContent = (parsed as Record<string, string>).content;
        const fileSurface = (parsed as Record<string, string>).surface;
        // surface 必须匹配,确保是同一契约
        if (fileSurface === surface && fileContent.length > 0) {
          return fileContent;
        }
      }
    } catch { /* skip unreadable */ }
  }
  return null;
}

export function extractContractsFromTaskMetadata(
  db: DatabaseManager,
  sessionId: string,
  taskId: string,
  getActiveContract?: (sessionId: string, surface: string) => GraphNode | null,
  workspace?: string,
): { sessionId: string; title: string; content: string; tags: string[]; createdBy: string } | null {
  try {
    const row = db.getDb().prepare(
      'SELECT orchestration FROM tasks WHERE id = ? AND session_id = ?'
    ).get(taskId, sessionId) as { orchestration?: string } | undefined;
    if (!row?.orchestration) return null;
    const orch = JSON.parse(row.orchestration) as {
      contract?: { surface?: string; title?: string; content?: string; version?: number };
      contractBinding?: { surface?: string; tag?: string; version?: number };
      nodeKind?: string;
    };
    // 只有 contract 产出者任务才适用（architect/contract 节点）
    if (orch.nodeKind !== 'contract') return null;
    const contract = orch.contract;
    if (!contract || typeof contract.surface !== 'string' || typeof contract.content !== 'string') return null;
    const surface = contract.surface;
    // 如果黑板已有同 surface 的活跃契约,跳过(避免重复)
    if (getActiveContract?.(sessionId, surface)) return null;
    const version = typeof contract.version === 'number' && contract.version > 0 ? contract.version : undefined;

    // BUG FIX: 在使用薄模板内容前,检查磁盘是否已有 worker 写入的完整契约文件。
    // 场景: architect worker 通过 file_create/write_work_note 写入完整契约 JSON,
    // 但 attempt_completion 文本未内联 graph_contract 块,触发 Fallback 2。
    // 此时磁盘文件就是 worker 的真实产出,应该用它而非模板构造 graph 节点。
    // 如果直接用模板内容,后续 persistContractPack 会用模板覆盖磁盘完整文件。
    let content = contract.content;
    const diskContent = loadContractContentFromDisk(surface, sessionId, workspace ?? process.cwd());
    if (diskContent !== null) {
      content = diskContent;
    }

    return {
      sessionId,
      title: contract.title ?? `Contract: ${surface}`,
      content,
      tags: Array.from(new Set([
        `contract:${surface}`,
        ...(version !== undefined ? [`v${version}`, `contract-version:${version}`] : []),
        'provenance:declared',
        `task:${taskId}`,
      ])),
      createdBy: taskId,
    };
  } catch {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════
// 统一入口
// ═══════════════════════════════════════════════════════════════

/**
 * 统一入口：解析并验证 Worker 输出中的 graph_* 代码块，含 3 层 fallback。
 *
 * 解析流程：
 * 1. 主解析：parseWorkerOutput 从输出文本提取 graph_fact/intent/contract/edge/supersede 块
 * 2. Fallback 1（仅 contract）：若输出提到 graph_contract 但未内联代码块，
 *    扫描 session 的 context/ 和 scratchpad/ 文件
 * 3. Fallback 2（仅 contract）：若仍无 contract，从任务 orchestration.contract 元数据提取
 *
 * 行为与原 LeaderBlackboard applyOutput callback 完全一致。
 */
export function parseAndValidateGraphBlocks(
  rawOutput: string,
  sessionId: string,
  options: ParseAndValidateOptions,
): ContractFormatParseResult {
  const { taskId, workspace, db, getActiveContract } = options;

  // Step 1: 主解析
  const parsed = parseWorkerOutput(rawOutput, sessionId);
  const { output: graphOutput, errors } = parsed;

  let contractFallbackSource: ContractFormatParseResult['contractFallbackSource'] = 'inline';

  // Step 2: Fallback 1 — 文件扫描
  // completion 提到了 graph_contract 但未内联代码块时（常见于 architect 把契约
  // 写到 file 而非 completion 文本），扫描 session 的 context/ 和 scratchpad/ 文件。
  if ((graphOutput.newContracts?.length ?? 0) === 0 && rawOutput.includes('graph_contract')) {
    const fileContracts = scanSessionFilesForContracts(sessionId, taskId, workspace);
    if (fileContracts.length > 0) {
      graphOutput.newContracts = fileContracts;
      contractFallbackSource = 'session-files';
    }
  }

  // Step 3: Fallback 2 — 任务元数据
  // worker completion 无 graph_contract 块，但任务自身有 contract 元数据
  // (Leader 通过 create_task(contract={...}) 创建契约任务时, orchestration.contract 存有契约正文)。
  if ((graphOutput.newContracts?.length ?? 0) === 0 && db) {
    const taskContract = extractContractsFromTaskMetadata(db, sessionId, taskId, getActiveContract, workspace);
    if (taskContract) {
      graphOutput.newContracts = [taskContract];
      contractFallbackSource = 'task-metadata';
    }
  }

  // 若无 contract fallback 发生且无 contract，标记为 none
  if ((graphOutput.newContracts?.length ?? 0) === 0) {
    contractFallbackSource = 'none';
  }

  return { output: graphOutput, errors, contractFallbackSource };
}

export const OFFICE_TOOL_NAMES = [
  // 固定 schema 的 generate_*/edit_*/inspect_*/html/slidev/canvas 工具已废弃；
  // office 产物改由 agent 用 shell 跑 node 脚本直调库（pptxgenjs/docx/exceljs/pdfkit）自由生成。
  // 仅保留验收 runtime 必需的 office_ops 与文件解析 parse_file。
  'office_ops',
  'parse_file',
] as const;

export type OfficeToolName = typeof OFFICE_TOOL_NAMES[number];

export const BUGHUNT_TOOL_NAMES = [
  'set_bughunt_dag',
  'upsert_bughunt_finding',
  'get_bughunt_ledger',
  'get_ready_dag_nodes',
  'verify_finding',
] as const;

export const BUGHUNT_SCAN_TOOL_NAMES = ['bughunt_full_scan'] as const;

export const BUGHUNT_MODE_TOOL_NAMES = [
  ...BUGHUNT_TOOL_NAMES,
  ...BUGHUNT_SCAN_TOOL_NAMES,
] as const;

export const WORKFLOW_TOOL_NAMES = ['workflow'] as const;

const OFFICE_TOOL_NAME_SET: ReadonlySet<string> = new Set(OFFICE_TOOL_NAMES);

export function isOfficeToolName(name: string): name is OfficeToolName {
  return OFFICE_TOOL_NAME_SET.has(name);
}


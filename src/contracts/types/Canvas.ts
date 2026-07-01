/**
 * Canvas — 剑阁可交互 Canvas 的核心契约类型
 *
 * 解决的根本问题：用户看到的是「成品」（HTML 渲染 / PPTX / 图片），
 * 要改的是「源码」（结构化 spec / 生成脚本 / HTML 源）。中间隔着一次渲染。
 * 本契约定义「源码 ↔ 成品」双向映射所需的全部数据结构。
 *
 * 映射采用 both_layered 分层模型：
 *   - spec 主干   产物由结构化 spec(JSON) 驱动，锚点指向 spec 节点路径，
 *                改动 = 改 spec 字段 → 重新装配。稳定、精确、可回滚、天然版本栈。
 *   - script 兜底 产物由自由脚本(gen_xx.cjs / 手写 HTML)生成，锚点指向源文件
 *                行号区间，改动 = LLM 改脚本对应行 → 重跑命令。覆盖自由产物。
 *
 * 核心模型：
 *   1. SourceProvenance —— 生成期注入到每个可交互单元的统一锚点（spec | script）
 *   2. CanvasSourceMap  —— 一份产物的完整映射表（nodeId ↔ 锚点）
 *   3. CanvasVersion    —— 版本快照（version_stack：每次改完入栈，可回退对比）
 *   4. SelectionIntent  —— 用户选区 + 自然语言意图，打包交给 LLM
 *   5. CanvasComment    —— 结构化批注（替代一次性纯文本 prompt）
 */

/** 锚点类型：both_layered 分层映射的两种源码锚定方式。 */
export type SourceAnchorKind = 'spec' | 'script';

/**
 * spec 锚点：指向结构化 spec 中的一个节点。
 * 落到 HTML 上即：
 *   data-node-id="slides.3.title" data-anchor="spec" data-spec-path="slides[3].title"
 */
export interface SpecAnchor {
  kind: 'spec';
  /** 稳定语义 ID，跨重新生成保持不变。如 "slides.3.title" */
  nodeId: string;
  /** spec 节点路径（JSON path 风格），如 "slides[3].title"。改这里就改产物 */
  specPath: string;
  /** 可选：语义角色，便于 LLM 理解，如 "title" | "subtitle" | "chart" */
  role?: string;
}

/**
 * script 锚点：指向自由脚本/手写 HTML 的源码行号区间。
 * 落到 HTML 上即：
 *   data-node-id="cover.title" data-anchor="script" data-src-file="gen_deck.cjs" data-src-range="84-88"
 */
export interface ScriptAnchor {
  kind: 'script';
  /** 稳定语义 ID。如 "cover.title" */
  nodeId: string;
  /** 生成它的源文件（相对 workspace 路径），如 "gen_deck.cjs" */
  srcFile: string;
  /** 对应源码行号区间 [startLine, endLine]，1-based，含两端 */
  srcRange: [number, number];
  /** 可选：语义角色 */
  role?: string;
}

/**
 * 溯源标记：生成期注入到每个可交互 DOM 单元的统一锚点（spec 或 script）。
 * 上层 Canvas 逻辑统一按 SourceProvenance 处理，按 kind 分派到对应回写路径。
 */
export type SourceProvenance = SpecAnchor | ScriptAnchor;

/**
 * 一份产物的完整源码映射表。持久化到
 * .lingxiao/canvas/<artifactId>/sourcemap.json
 */
export interface CanvasSourceMap {
  /** 产物稳定 ID（通常是产物相对路径的规范化形式） */
  artifactId: string;
  /** 当前产物文件路径（HTML 可交互源），相对 workspace */
  artifactPath: string;
  /** 该产物的主导锚点类型：spec 主干 或 script 兜底 */
  anchorKind: SourceAnchorKind;
  /** spec 主干模式下的 spec 文件（相对 workspace），如 "deck.spec.json" */
  specFile?: string;
  /** script 兜底模式下的生成脚本入口（相对 workspace），如 "gen_deck.cjs" */
  generatorFile?: string;
  /** 重新生成产物的命令（让回写闭环能自动重跑），如 "node gen_deck.cjs" */
  regenerateCommand?: string;
  /** 所有可交互单元的溯源标记 */
  nodes: SourceProvenance[];
  /** 生成时间戳 */
  generatedAt: number;
}

/** 版本快照在版本栈中的状态 */
export type CanvasVersionStatus = 'active' | 'superseded' | 'reverted';

/**
 * 版本快照：version_stack 的一个节点。
 * 每次 LLM 改完源码 + 重新生成，新产物作为新版本入栈，可回退到任意历史版本对比。
 */
export interface CanvasVersion {
  /** 版本号，从 1 递增 */
  version: number;
  /** 该版本产物快照路径（相对 workspace），如 ".lingxiao/canvas/<id>/versions/v2.html"。快照失败或首版可空 */
  snapshotPath?: string;
  /** 触发该版本的用户意图摘要（首版为空） */
  intent?: string;
  /** 相对上一版本改动的文件列表 */
  changedFiles?: string[];
  status: CanvasVersionStatus;
  createdAt: number;
}

/**
 * 选区意图：用户在 Canvas 上点选/框选一个元素 + 写下自然语言诉求，
 * 打包成这个结构交给 Leader/LLM。这是「成品交互」翻译成「源码修改」的桥梁。
 */
export interface SelectionIntent {
  /** 选中单元的稳定语义 ID（来自 SourceProvenance.nodeId） */
  nodeId: string;
  /** 选中单元的锚点（spec 或 script），决定 LLM 走哪条回写路径 */
  anchor: SourceProvenance;
  /** 选中单元当前的内容/文本快照（给 LLM 做定位佐证） */
  currentContent?: string;
  /** 用户的自然语言诉求，如 "标题再大一点，加一道金色描边" */
  userIntent: string;
  /** 产物 ID */
  artifactId: string;
  /** 可选：归一化选区框（0..1），用于在渲染图上回显高亮 */
  selectionBox?: { x: number; y: number; w: number; h: number };
  /** 发起时间 */
  createdAt: number;
}

/** 结构化批注的处理状态 */
export type CanvasCommentStatus = 'pending' | 'applied' | 'dismissed';

/**
 * 结构化批注：替代当前「一次性纯文本 prompt」。
 * 支持多条评论、定位回跳、状态追踪，持久化到
 * .lingxiao/canvas/<artifactId>/comments.json
 */
export interface CanvasComment {
  /** 批注唯一 ID */
  id: string;
  /** 锚定的产物 ID */
  artifactId: string;
  /** 锚定的语义单元 ID（可空，表示对整页的评论） */
  nodeId?: string;
  /** 锚定时产物的版本号 */
  version: number;
  /** 批注正文 */
  body: string;
  /** 归一化选区框，用于在 Canvas 上回显批注气泡位置 */
  selectionBox?: { x: number; y: number; w: number; h: number };
  status: CanvasCommentStatus;
  createdAt: number;
  updatedAt: number;
}

/**
 * 一份产物在 Canvas 中的完整状态聚合，对应
 * .lingxiao/canvas/<artifactId>/ 目录。
 */
export interface CanvasArtifactState {
  artifactId: string;
  sourceMap: CanvasSourceMap;
  versions: CanvasVersion[];
  activeVersion: number;
  comments: CanvasComment[];
}

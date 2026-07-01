import { z } from 'zod';
import { Tool, type ToolContext, type ToolResult } from '../Tool.js';
import { createReadStream } from 'fs';
import { access, readFile, stat as statFile } from 'fs/promises';
import { readBinaryProbeAsync, resolveWorkspacePath } from './utils.js';
import { basename, extname } from 'path';
import { tempDownloadRegistry } from '../../core/TempDownloadRegistry.js';
import { supportsVisionFromProvider } from '../../llm/model_capabilities.js';
import { ocrImage } from '../../llm/local_vision_fallback.js';
import { createInterface } from 'readline';

const FileReadSchema = z.object({
  path: z.string().describe('文件路径'),
  start_line: z.number().optional().describe('起始行号 1-based(可选)'),
  end_line: z.number().optional().describe('结束行号 (含，可选)'),
});

const MAX_LINES = 2000;
const MAX_LINE_CHARS = 2000;
const MAX_FILE_SIZE_WITHOUT_RANGE = 256 * 1024; // 256KB - CodeBuddy 启发的前置守卫
const ESTIMATED_CHARS_PER_TOKEN = 4; // 粗略估算：4 个字符 ≈ 1 token
const MAX_OUTPUT_TOKENS = parseInt(process.env.LINGXIAO_FILE_READ_MAX_TOKENS || '20000', 10);

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new Error('file_read aborted');
  }
}

async function readLineWindow(filePath: string, startLine: number, endLine: number, signal?: AbortSignal): Promise<{ lines: string[]; lastLine: number; hitLineLimit: boolean }> {
  const stream = createReadStream(filePath, { encoding: 'utf-8' });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });
  const result: string[] = [];
  let lineNumber = 0;
  let hitLineLimit = false;
  const onAbort = () => stream.destroy(new Error('file_read aborted'));
  signal?.addEventListener('abort', onAbort, { once: true });

  try {
    for await (const line of rl) {
      throwIfAborted(signal);
      lineNumber += 1;
      if (lineNumber === 1 && line.startsWith('\uFEFF')) {
        const displayLine = line.slice(1);
        if (startLine <= 1 && endLine >= 1) {
          result.push(`${String(1).padStart(6)}→${formatLine(displayLine)}`);
        }
        continue;
      }
      if (lineNumber < startLine) continue;
      if (lineNumber > endLine) break;

      result.push(`${String(lineNumber).padStart(6)}→${formatLine(line)}`);
      if (result.length >= MAX_LINES) {
        hitLineLimit = true;
        break;
      }
    }
  } finally {
    signal?.removeEventListener('abort', onAbort);
    rl.close();
    stream.destroy();
  }

  return { lines: result, lastLine: lineNumber, hitLineLimit };
}

function formatLine(line: string): string {
  return line.length > MAX_LINE_CHARS
    ? line.slice(0, MAX_LINE_CHARS) + `…[截断，该行共 ${line.length} 字符]`
    : line;
}

const PREVIEWABLE_BINARY_EXTENSIONS: Record<string, string> = {
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.bmp': 'image/bmp',
  '.svg': 'image/svg+xml',
  '.pdf': 'application/pdf',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm',
  '.mov': 'video/quicktime',
  '.mp3': 'audio/mpeg',
  '.wav': 'audio/wav',
};

// vision 模型可直接读取的位图格式（svg 是文本矢量、pdf/视频/音频不走图像协议）。
const VISION_IMAGE_EXTENSIONS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp']);
// 内联 base64 的图片大小上限：超过则回退下载链接，避免撑爆上下文 / 触发 provider 图片大小限制。
const MAX_IMAGE_INLINE_BYTES = 8 * 1024 * 1024;

// Code file extensions that should be wrapped in code blocks
const CODE_EXTENSIONS = new Set([
  '.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs',
  '.py', '.rb', '.go', '.rs', '.java', '.kt', '.scala',
  '.c', '.cpp', '.h', '.hpp',
  '.cs', '.fs', '.swift',
  '.php', '.html', '.htm', '.css', '.scss', '.sass', '.less',
  '.json', '.yaml', '.yml', '.toml', '.xml',
  '.sh', '.bash', '.zsh', '.fish',
  '.sql', '.graphql', '.md', '.astro', '.vue', '.svelte',
]);

/**
 * Detect if a file path is a code file based on extension
 */
function isCodeFile(path: string): boolean {
  const ext = extname(path).toLowerCase();
  return CODE_EXTENSIONS.has(ext);
}

/**
 * Guess language from file extension for syntax highlighting
 */
function guessLanguage(path: string): string {
  const ext = extname(path).toLowerCase();
  const langMap: Record<string, string> = {
    '.ts': 'typescript', '.tsx': 'tsx', '.js': 'javascript', '.jsx': 'jsx',
    '.py': 'python', '.rb': 'ruby', '.go': 'go', '.rs': 'rust',
    '.java': 'java', '.kt': 'kotlin', '.c': 'c', '.cpp': 'cpp',
    '.cs': 'csharp', '.swift': 'swift', '.php': 'php',
    '.html': 'html', '.htm': 'html', '.css': 'css', '.scss': 'scss',
    '.json': 'json', '.yaml': 'yaml', '.yml': 'yaml', '.toml': 'toml', '.xml': 'xml',
    '.sh': 'bash', '.bash': 'bash', '.zsh': 'zsh',
    '.sql': 'sql', '.md': 'markdown', '.astro': 'astro', '.vue': 'vue', '.svelte': 'svelte',
  };
  return langMap[ext] || 'text';
}

export class FileReadTool extends Tool {
  readonly name = 'file_read';
  readonly description = '读取文件内容，支持指定行号范围（含二进制检测和超大文件流式截断）';
  readonly parameters = FileReadSchema;

  async execute(args: unknown, context?: ToolContext): Promise<ToolResult> {
    const params = args as z.infer<typeof FileReadSchema>;
    let p: string;

    try {
      throwIfAborted(context?.abortSignal);
      p = resolveWorkspacePath(context?.workspace, params.path, context?.sessionId);
    } catch (error) {
      return {
        success: false,
        data: null,
        error: `ERROR: ${error instanceof Error ? error.message : String(error)}`,
      };
    }

    try {
      throwIfAborted(context?.abortSignal);
      await access(p);
    } catch {
      return {
        success: false,
        data: null,
        error: `ERROR: 文件不存在：${params.path}`,
      };
    }

    // 检查是否是文件（而非目录）
    throwIfAborted(context?.abortSignal);
    const stat = await statFile(p);
    if (!stat.isFile()) {
      return {
        success: false,
        data: null,
        error: `ERROR: 不是文件：${params.path}`,
      };
    }

    // 检测二进制文件
    try {
      throwIfAborted(context?.abortSignal);
      const probe = await readBinaryProbeAsync(p, context?.abortSignal);
      if (probe.isBinary) {
        const ext = extname(p).toLowerCase();
        const mimeType = PREVIEWABLE_BINARY_EXTENSIONS[ext];
        if (mimeType) {
          // 始终生成下载/预览卡片（前端 Artifact 面板预览用）
          const artifact = tempDownloadRegistry.create({
            path: p,
            name: basename(p),
            mimeType,
            sessionId: context?.sessionId,
          });

          // 图片格式：按当前模型能力把图像内容真正交付给 LLM，而非只给一个下载链接。
          //  - vision 模型 → image_url content part（走多模态协议，下游 externalize 成 blob 引用）
          //  - 非 vision 模型 → 本地 OCR 兜底
          // 仍附下载链接，前端可预览。与 ScreenshotTool 同源逻辑。
          if (VISION_IMAGE_EXTENSIONS.has(ext) && stat.size <= MAX_IMAGE_INLINE_BYTES) {
            const base64 = (await readFile(p)).toString('base64');
            const dataUri = `data:${mimeType};base64,${base64}`;
            const model = typeof context?.model === 'string' ? context.model : '';
            const visionCapable = model ? supportsVisionFromProvider(model) : false;
            const header = [
              `🖼 图片文件: ${p}`,
              `大小: ${(stat.size / 1024).toFixed(1)}KB · ${mimeType}`,
              `预览链接: ${artifact.url}`,
            ].join('\n');

            if (visionCapable) {
              return {
                success: true,
                data: [
                  { type: 'text', text: header },
                  { type: 'image_url', image_url: { url: dataUri, detail: 'auto' } },
                ],
              };
            }

            const ocrText = await ocrImage(dataUri, 1);
            const ocrSection = ocrText && ocrText.trim()
              ? ocrText
              : `[OCR 未提取到文字。当前模型 ${model || 'unknown'} 不支持 vision，图片已保存至 ${p}。]`;
            return {
              success: true,
              data: [
                header,
                '[System: 当前模型不支持图片输入，已用本地 OCR 替代图像内容。]',
                '',
                ocrSection,
              ].join('\n'),
            };
          }

          // 非图片可预览二进制（pdf/视频/音频/svg），或图片过大：返回下载/预览卡片。
          return {
            success: true,
            data: {
              ...artifact,
              preview_url: artifact.url,
              file_path: p,
              mode: 'binary_preview',
            },
          };
        }
        return {
          success: false,
          data: null,
          error: `ERROR: 二进制文件 (${probe.reason})，无法读取：${params.path}`,
        };
      }
    } catch (error) {
      return {
        success: false,
        data: null,
        error: `ERROR: ${error instanceof Error ? error.message : String(error)}`,
      };
    }

    try {
      // 参数验证
      throwIfAborted(context?.abortSignal);
      if (params.start_line !== undefined && params.start_line < 1) {
        return {
          success: false,
          data: null,
          error: `ERROR: start_line 必须 >= 1，当前值: ${params.start_line}`,
        };
      }
      if (params.end_line !== undefined && params.end_line < 1) {
        return {
          success: false,
          data: null,
          error: `ERROR: end_line 必须 >= 1，当前值: ${params.end_line}`,
        };
      }
      if (params.start_line !== undefined && params.end_line !== undefined && params.start_line > params.end_line) {
        return {
          success: false,
          data: null,
          error: `ERROR: 请让 start_line (${params.start_line}) 小于或等于 end_line (${params.end_line})`,
        };
      }

      // 前置守卫：大文件未指定范围时直接拒绝（学习 CodeBuddy）
      const hasRange = params.start_line !== undefined || params.end_line !== undefined;
      if (!hasRange && stat.size > MAX_FILE_SIZE_WITHOUT_RANGE) {
        const sizeMB = (stat.size / 1024 / 1024).toFixed(1);
        const maxKB = Math.round(MAX_FILE_SIZE_WITHOUT_RANGE / 1024);
        return {
          success: false,
          data: null,
          error: `ERROR: 文件过大 (${sizeMB}MB)，超过无范围读取上限 (${maxKB}KB)。\n` +
                 `提示：请使用 start_line 和 end_line 参数分段读取，例如：\n` +
                 `   - start_line=1, end_line=500（读取前500行）\n` +
                 `   - start_line=501, end_line=1000（读取501-1000行）`,
        };
      }

      const s = Math.max(1, params.start_line || 1);
      const e = params.end_line !== undefined ? params.end_line : Number.MAX_SAFE_INTEGER;
      const result = await readLineWindow(p, s, e, context?.abortSignal);
      const lines = [...result.lines];

      if (result.hitLineLimit) {
        lines.push(`...... (已读取 ${MAX_LINES} 行并截断。请使用 start_line=${result.lastLine + 1} 继续读取)`);
      }
      const fileContent = lines.join('\n') || '(空文本内容)';

      // 前置守卫：输出 token 估算超限时拒绝（学习 CodeBuddy）
      const estimatedTokens = Math.ceil(fileContent.length / ESTIMATED_CHARS_PER_TOKEN);
      if (estimatedTokens > MAX_OUTPUT_TOKENS) {
        return {
          success: false,
          data: null,
          error: `ERROR: 文件内容过长（估算约 ${estimatedTokens} tokens），超过上限 (${MAX_OUTPUT_TOKENS} tokens)。\n` +
                 `提示：请缩小读取范围：\n` +
                 `   - 当前已读 ${result.lines.length} 行，建议分多次读取\n` +
                 `   - 或使用 code_search 工具搜索特定内容`,
        };
      }

      return {
        success: true,
        data: fileContent,
      };
    } catch (error) {
      return {
        success: false,
        data: null,
        error: `ERROR: ${error instanceof Error ? error.message : String(error)}`,
      };
    }
  }
}

export default FileReadTool;

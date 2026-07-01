import type { JsonSchema, ToolContract, ToolContext, ToolResult, ToolScope } from '../contracts/types/Tool.js';

export type LazyToolLoader = () => Promise<ToolContract>;

export interface LazyToolProxyOptions {
  name: string;
  description: string;
  schema?: JsonSchema;
  scope?: ToolScope;
  loader: LazyToolLoader;
}

/**
 * Lightweight tool facade for heavy optional modules.
 *
 * The registry can synchronously expose name/description/schema to LLMs and
 * preflight without importing heavyweight dependencies such as Playwright,
 * tesseract, sharp, JSZip, docx, pptxgenjs or office parsers.  The real tool
 * module is loaded only on first execute() and then cached for the lifetime of
 * the registry instance.
 */
export class LazyToolProxy implements ToolContract {
  readonly name: string;
  readonly description: string;
  readonly scope?: ToolScope;
  readonly schema: JsonSchema;

  private readonly loader: LazyToolLoader;
  private loadedTool?: ToolContract;
  private loading?: Promise<ToolContract>;

  constructor(options: LazyToolProxyOptions) {
    this.name = options.name;
    this.description = options.description;
    this.scope = options.scope;
    this.schema = options.schema ?? {
      type: 'object',
      properties: {},
      additionalProperties: true,
    };
    this.loader = options.loader;
  }

  getSchema(): JsonSchema {
    return this.schema;
  }

  private async load(): Promise<ToolContract> {
    if (this.loadedTool) return this.loadedTool;
    this.loading ??= this.loader().then((tool) => {
      if (!tool || tool.name !== this.name || typeof tool.execute !== 'function') {
        throw new Error(`Lazy tool loader for ${this.name} returned an invalid tool contract`);
      }
      this.loadedTool = tool;
      return tool;
    }).finally(() => {
      this.loading = undefined;
    });
    return this.loading;
  }

  async execute(args: unknown, context?: ToolContext): Promise<ToolResult | unknown> {
    const tool = await this.load();
    return tool.execute(args, context);
  }
}

export function objectSchema(
  properties: Record<string, JsonSchema> = {},
  required: string[] = [],
  additionalProperties = true,
): JsonSchema {
  return {
    type: 'object',
    properties,
    ...(required.length > 0 ? { required } : {}),
    additionalProperties,
  };
}

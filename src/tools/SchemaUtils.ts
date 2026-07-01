import { z } from 'zod';

type JsonSchema = Record<string, unknown>;
type ZodInternalDef = Record<string, unknown>;

const OPTIONAL_LIKE_TYPES = new Set([
  'ZodOptional',
  'optional',
  'ZodDefault',
  'default',
  'ZodNullable',
  'nullable',
]);

export function zodToJsonSchema(schema: z.ZodTypeAny): JsonSchema {
  const zodWithJsonSchema = z as typeof z & {
    toJSONSchema?: (schema: z.ZodTypeAny) => JsonSchema;
  };
  if (typeof zodWithJsonSchema.toJSONSchema === 'function') {
    return stripDefaultedFromRequired(zodWithJsonSchema.toJSONSchema(schema));
  }
  return convert(schema);
}

/**
 * zod v4 的原生 toJSONSchema 把带 .default() 的字段算进 required（input 端语义：
 * default 只在缺省时补值，但字段仍被标 required）。这会污染暴露给 LLM 的 schema 和
 * 任何基于该 JSON schema 的 ajv 校验：例如 structured_patch 的 search hunk 里
 * replace_all = z.boolean().optional().default(false) 被列入 required，导致不带
 * replace_all 的 hunk 无法匹配 anyOf 任一 variant，报 `hunks: Invalid input`。
 *
 * 修正：递归遍历整个 schema 树，凡 properties[k] 自身带 default 的，从同级 required
 * 中剔除（default 字段天然可选）。覆盖 properties / items / oneOf / anyOf / allOf /
 * $defs / definitions 等所有嵌套位置，是所有工具 schema 的单点修复。
 */
function stripDefaultedFromRequired(schema: JsonSchema): JsonSchema {
  if (!schema || typeof schema !== 'object') return schema;
  if (Array.isArray(schema)) {
    return (schema as unknown[]).map((item) =>
      stripDefaultedFromRequired(item as JsonSchema)) as unknown as JsonSchema;
  }
  const out: JsonSchema = {};
  for (const [key, value] of Object.entries(schema)) {
    out[key] = (value && typeof value === 'object')
      ? stripDefaultedFromRequired(value as JsonSchema)
      : value;
  }
  const props = out.properties;
  if (props && typeof props === 'object' && !Array.isArray(props) && Array.isArray(out.required)) {
    const defaulted = new Set<string>();
    for (const [k, v] of Object.entries(props as Record<string, unknown>)) {
      if (v && typeof v === 'object' && !Array.isArray(v) && 'default' in (v as Record<string, unknown>)) {
        defaulted.add(k);
      }
    }
    if (defaulted.size > 0) {
      const nextRequired = (out.required as unknown[])
        .filter((r): r is string => typeof r === 'string' && !defaulted.has(r));
      if (nextRequired.length > 0) {
        out.required = nextRequired;
      } else {
        delete out.required;
      }
    }
  }
  return out;
}

function convert(schema: unknown): JsonSchema {
  const def = getZodDef(schema);
  const typeName = getTypeName(def);
  const description = getDescription(schema, def);
  const withDescription = (value: JsonSchema): JsonSchema => description ? { ...value, description } : value;

  switch (typeName) {
    case 'ZodObject':
    case 'object': {
      const shape = getObjectShape(def);
      const properties: Record<string, JsonSchema> = {};
      const required: string[] = [];
      for (const [key, value] of Object.entries(shape)) {
        properties[key] = convert(value);
        if (!isOptionalLike(value)) required.push(key);
      }
      return withDescription({
        type: 'object',
        properties,
        ...(required.length > 0 ? { required } : {}),
        additionalProperties: additionalPropertiesForObject(def),
      });
    }
    case 'ZodDiscriminatedUnion':
    case 'ZodUnion':
    case 'union': {
      const options = getUnionOptions(def).map(convert);
      return withDescription({ oneOf: options });
    }
    case 'ZodOptional':
    case 'optional':
    case 'ZodNullable':
    case 'nullable':
      return withDescription(convert(def.innerType));
    case 'ZodDefault':
    case 'default':
      return withDescription({ ...convert(def.innerType), default: getDefaultValue(def) });
    case 'ZodArray':
    case 'array':
      return withDescription({ type: 'array', items: convert(def.type && def.type !== 'array' ? def.type : def.element) });
    case 'ZodRecord':
    case 'record':
      return withDescription({ type: 'object', additionalProperties: convert(def.valueType) });
    case 'ZodEnum':
    case 'enum':
      return withDescription({ type: 'string', enum: getEnumValues(def) });
    case 'ZodLiteral':
    case 'literal': {
      const value = getLiteralValue(def);
      return withDescription({ const: value, type: typeof value });
    }
    case 'ZodString':
    case 'string':
      return withDescription({ type: 'string' });
    case 'ZodNumber':
    case 'number':
      return withDescription({ type: 'number' });
    case 'ZodBoolean':
    case 'boolean':
      return withDescription({ type: 'boolean' });
    case 'ZodUnknown':
    case 'unknown':
    case 'ZodAny':
    case 'any':
      return withDescription({});
    case 'ZodEffects':
      return withDescription(convert(def.schema));
    case 'ZodPipeline':
      return withDescription(convert(def.out));
    default:
      return withDescription({});
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function getZodDef(schema: unknown): ZodInternalDef {
  if (!isRecord(schema)) return {};
  const rawDef = schema._def ?? schema.def;
  return isRecord(rawDef) ? rawDef : {};
}

function getTypeName(def: ZodInternalDef): string | undefined {
  const typeName = def.typeName ?? def.type;
  return typeof typeName === 'string' ? typeName : undefined;
}

function getDescription(schema: unknown, def: ZodInternalDef): string | undefined {
  if (typeof def.description === 'string') return def.description;
  if (isRecord(schema) && typeof schema.description === 'string') return schema.description;
  return undefined;
}

function isShapeFactory(value: unknown): value is () => unknown {
  return typeof value === 'function';
}

function getObjectShape(def: ZodInternalDef): Record<string, unknown> {
  const shape = isShapeFactory(def.shape) ? def.shape() : def.shape;
  return isRecord(shape) ? shape : {};
}

function hasValuesMethod(value: unknown): value is { values: () => unknown } {
  return isRecord(value) && typeof value.values === 'function';
}

function isIterable(value: unknown): value is Iterable<unknown> {
  return typeof (value as { [Symbol.iterator]?: unknown } | null | undefined)?.[Symbol.iterator] === 'function';
}

function getUnionOptions(def: ZodInternalDef): unknown[] {
  const options = def.options;
  if (Array.isArray(options)) return options;
  if (hasValuesMethod(options)) {
    const values = options.values();
    if (isIterable(values)) return Array.from(values);
  }
  if (isIterable(options)) return Array.from(options);
  return [];
}

function isDefaultFactory(value: unknown): value is () => unknown {
  return typeof value === 'function';
}

function getDefaultValue(def: ZodInternalDef): unknown {
  return isDefaultFactory(def.defaultValue) ? def.defaultValue() : def.defaultValue;
}

function getEnumValues(def: ZodInternalDef): unknown[] {
  if (Array.isArray(def.values)) return def.values;
  if (isIterable(def.values)) return Array.from(def.values);
  return isRecord(def.entries) ? Object.values(def.entries) : [];
}

function getLiteralValue(def: ZodInternalDef): unknown {
  if (Array.isArray(def.values)) return def.values[0];
  if (isIterable(def.values)) return Array.from(def.values)[0];
  return def.value;
}

function additionalPropertiesForObject(def: ZodInternalDef): false | JsonSchema {
  const catchall = def?.catchall;
  if (!catchall) return false;
  const catchallDef = getZodDef(catchall);
  const catchallType = getTypeName(catchallDef);
  if (catchallType === 'ZodNever' || catchallType === 'never') return false;
  return convert(catchall);
}

function isOptionalLike(schema: unknown): boolean {
  const def = getZodDef(schema);
  const typeName = getTypeName(def);
  return typeName !== undefined && OPTIONAL_LIKE_TYPES.has(typeName);
}

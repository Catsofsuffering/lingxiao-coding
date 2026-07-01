/**
 * OfficeClient — v1.0.5 剑阁大改：直接 HTTP API 生成办公文件
 */

import { apiHeaders } from './headers';

export interface OfficeFormatInfo {
  format: 'pdf' | 'pptx' | 'docx' | 'xlsx';
  name: string;
  icon: string;
  description: string;
  extensions: string[];
}

export interface OfficeTemplate {
  id: string;
  name: string;
  group: string;
}

export interface OfficeGenerateResult {
  success: boolean;
  format: string;
  path?: string;
  downloadUrl?: string | null;
  [key: string]: unknown;
}
// ─── Office Preview Model 类型定义（与后端 OfficePreviewModel.ts 对齐） ───

export interface OfficePreviewBBox {
  x: number;
  y: number;
  w: number;
  h: number;
  unit: 'in';
}

export interface OfficePreviewSize {
  width: number;
  height: number;
  unit: 'in';
}

export interface OfficePreviewStyle {
  fontFace?: string;
  fontSizePt?: number;
  bold?: boolean;
  italic?: boolean;
  color?: string;
  fillColor?: string;
  lineColor?: string;
  paragraphStyle?: string;
  level?: number;
  align?: string;
}

export interface OfficePreviewAsset {
  id: string;
  relationshipId?: string;
  kind: 'image' | 'media' | 'ole' | 'external' | 'unknown';
  path?: string;
  target?: string;
  contentType?: string;
  extension?: string;
}

export interface OfficePreviewTableCell {
  id: string;
  text: string;
  rowSpan?: number;
  colSpan?: number;
  style?: OfficePreviewStyle;
}

export interface OfficePreviewTableRow {
  id: string;
  cells: OfficePreviewTableCell[];
}

export interface OfficePreviewElement {
  id: string;
  sourceId?: string;
  kind: 'text' | 'shape' | 'image' | 'table' | 'paragraph' | 'drawing' | 'pageBreak' | 'unknown';
  name?: string;
  text?: string;
  bbox?: OfficePreviewBBox;
  style?: OfficePreviewStyle;
  relationshipId?: string;
  assetId?: string;
  rows?: OfficePreviewTableRow[];
  children?: OfficePreviewElement[];
  metadata?: Record<string, unknown>;
}

export interface OfficePreviewPage {
  id: string;
  index: number;
  name?: string;
  entryPath?: string;
  size: OfficePreviewSize;
  elements: OfficePreviewElement[];
  notes?: string;
  metadata?: Record<string, unknown>;
}

export interface OfficePreviewTheme {
  name?: string;
  headFontFace?: string;
  bodyFontFace?: string;
  majorFontFace?: string;
  minorFontFace?: string;
  defaultFontFace?: string;
}

export interface OfficePreviewModel {
  schema: 'lingxiao.office.preview.v1';
  kind: 'pptx' | 'docx';
  renderer: 'office-preview-structure';
  pageSize: OfficePreviewSize;
  theme: OfficePreviewTheme;
  pages: OfficePreviewPage[];
  assets: OfficePreviewAsset[];
  warnings: string[];
  stats: {
    pageCount: number;
    elementCount: number;
    textElementCount: number;
    imageCount: number;
    tableCount: number;
  };
}


async function readJson<T>(res: Response): Promise<T> {
  const json = await res.json().catch(() => null);
  if (!res.ok) {
    throw new Error(json?.error || `HTTP ${res.status}`);
  }
  return json?.data as T;
}

export const officeClient = {
  async getFormats(): Promise<OfficeFormatInfo[]> {
    const res = await fetch('/api/v1/office/formats', { headers: apiHeaders() });
    return readJson<OfficeFormatInfo[]>(res);
  },

  async getTemplates(): Promise<OfficeTemplate[]> {
    const res = await fetch('/api/v1/office/templates', { headers: apiHeaders() });
    return readJson<OfficeTemplate[]>(res);
  },

  async generate(
    format: 'pdf' | 'pptx' | 'docx' | 'xlsx',
    params: Record<string, unknown>,
    options?: { outputPath?: string; createDownloadLink?: boolean },
  ): Promise<OfficeGenerateResult> {
    const res = await fetch('/api/v1/office/generate', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        format,
        params,
        outputPath: options?.outputPath,
        createDownloadLink: options?.createDownloadLink ?? true,
      }),
    });
    return readJson<OfficeGenerateResult>(res);
  },

  /** 获取办公文件的结构化预览模型 */
  async getPreview(
    path: string,
    options?: { format?: 'pptx' | 'docx'; slideLimit?: number },
  ): Promise<OfficePreviewModel> {
    const res = await fetch('/api/v1/office/preview', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        path,
        format: options?.format,
        slideLimit: options?.slideLimit,
      }),
    });
    return readJson<OfficePreviewModel>(res);
  },

  /** 编辑 PPTX 文件（增量操作） */
  async editPptx(
    path: string,
    operations: Array<Record<string, unknown>>,
    options?: { outputPath?: string; overwrite?: boolean; createDownloadLink?: boolean },
  ): Promise<OfficeGenerateResult> {
    const res = await fetch('/api/v1/office/generate', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        format: 'pptx',
        params: {
          __editMode: true,
          path,
          operations,
          output_path: options?.outputPath,
          overwrite: options?.overwrite ?? false,
          create_download_link: options?.createDownloadLink ?? true,
        },
      }),
    });
    return readJson<OfficeGenerateResult>(res);
  },
};

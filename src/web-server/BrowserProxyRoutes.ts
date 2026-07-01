/**
 * BrowserProxyRoutes — v1.0.5 剑阁原生浏览器体验
 *
 * 核心原理：后端做反向代理，把目标页面内容抓回来，
 * 去掉 X-Frame-Options / Content-Security-Policy 头，
 * 通过我们自己的路由 serve 给前端 iframe。
 * 这样 iframe 加载的是同源 URL，不受 X-Frame-Options 限制，
 * 用户在 iframe 里的所有操作都是原生的（点击、输入、滚动）。
 *
 * 同时通过 Playwright CDP session 做输入转发，
 * 让 iframe 里的操作同步到后端 Playwright 页面。
 */

import type { FastifyInstance, FastifyReply } from 'fastify';
import type { AuthFn } from './types.js';
import { BrowserRuntime } from '../core/BrowserRuntime.js';

interface ProxyDeps {
  requireServerToken: AuthFn;
  browserRuntime: BrowserRuntime;
}

function sendError(reply: FastifyReply, status: number, error: unknown): void {
  reply.status(status).send({ error: error instanceof Error ? error.message : String(error) });
}

export function registerBrowserProxyRoutes(fastify: FastifyInstance, deps: ProxyDeps): void {
  const { requireServerToken, browserRuntime } = deps;

  /**
   * GET /api/v1/browser/proxy/:sessionId/*
   *
   * 反向代理目标页面。iframe 加载这个 URL 即可显示真实页面。
   * 后端用 Playwright 的 page 内容 + 去除安全头。
   */
  fastify.get('/api/v1/browser/proxy/:sessionId', async (request, reply) => {
    if (!requireServerToken(request, reply)) return;
    const { sessionId } = request.params as { sessionId: string };
    const query = request.query as { token?: string };

    try {
      // 获取页面 HTML
      const html = await browserRuntime.getHtml(sessionId);
      const url = html.url;

      // 重写页面内的相对路径为代理路径
      let processedHtml = html.html;

      // 注入 base 标签让相对资源正确加载
      if (url && url !== 'about:blank') {
        const urlObj = new URL(url);
        const baseTag = `<base href="${urlObj.origin}${urlObj.pathname}">`;
        // 注入到 <head> 后面
        if (processedHtml.includes('<head>')) {
          processedHtml = processedHtml.replace('<head>', `<head>${baseTag}`);
        } else if (processedHtml.includes('<html>')) {
          processedHtml = processedHtml.replace('<html>', `<html><head>${baseTag}</head>`);
        } else {
          processedHtml = `<head>${baseTag}</head>` + processedHtml;
        }
      }

      // 设置响应头 — 不设 X-Frame-Options，允许 iframe 嵌入
      reply
        .header('Content-Type', 'text/html; charset=utf-8')
        .header('Cache-Control', 'no-cache')
        .removeHeader('X-Frame-Options')
        .removeHeader('Content-Security-Policy');

      return reply.send(processedHtml);
    } catch (error) {
      sendError(reply, 500, error);
    }
  });

  /**
   * GET /api/v1/browser/proxy-info/:sessionId
   * 返回当前页面 URL 和 title，前端用于判断是否可以代理
   */
  fastify.get('/api/v1/browser/proxy-info/:sessionId', async (request, reply) => {
    if (!requireServerToken(request, reply)) return;
    const { sessionId } = request.params as { sessionId: string };
    try {
      const info = await browserRuntime.getPageInfo(sessionId);
      return { data: info };
    } catch (error) {
      sendError(reply, 500, error);
    }
  });
}

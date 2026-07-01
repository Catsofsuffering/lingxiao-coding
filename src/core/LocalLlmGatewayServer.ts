/**
 * LocalLlmGatewayServer —— 本地 LLM 网关的进程绑定式随机端口监听器。
 *
 * 设计目标（v2 重构）：
 * - **进程绑定**：每个 lingxiao 进程启动自己的网关监听器，随进程生灭。
 *   不再共享复用——消除了"旧进程事件循环已死但进程 sleeping 不退出，
 *   新进程复用死网关导致全部请求超时"的僵尸问题。
 * - **随机端口**：port=0 让 OS 分配可用端口，避免固定端口的 EADDRINUSE 漂移问题。
 *   与 Web UI 端口随机化一致。
 * - **运行时端点注入**：绑定后通过 setRuntimeGatewayEndpoint() 把实际 host:port
 *   写入 LocalLlmGateway 模块，resolveLocalLlmGateway() 优先读取运行时端点，
 *   确保 env 注入和 prompt 生成都用正确的地址。
 * - 不再写 gateway.json 归属文件——无共享即无竞态。
 *
 * 路由处理器（callLocalGateway）用网关配置模型、不绑定会话，故复用 deps 即可，无需 IPC。
 */

import Fastify from 'fastify';
import { getConfigValue } from '../config.js';
import { registerCleanup } from './CleanupRegistry.js';
import { registerLocalLlmGatewayRoutes, type GatewayDeps } from '../web-server/LocalLlmGatewayRoutes.js';
import { normalizeHost, readPositiveInt, setRuntimeGatewayEndpoint, clearRuntimeGatewayEndpoint } from './LocalLlmGateway.js';
import { coreLogger } from './Log.js';

export interface LocalLlmGatewayEndpoint {
  host: string;
  port: number;
  origin: string;
  openaiBaseUrl: string;
  anthropicBaseUrl: string;
}

export interface LocalLlmGatewayHandle extends LocalLlmGatewayEndpoint {
  /**
   * 关闭本进程绑定的监听器（幂等）。
   * 进程退出由 cleanup 自动调用；测试可显式调用释放端口。
   */
  close(): Promise<void>;
}

function endpointOf(host: string, port: number): LocalLlmGatewayEndpoint {
  const origin = `http://${host}:${port}`;
  return {
    host,
    port,
    origin,
    openaiBaseUrl: `${origin}/llm/openai/v1`,
    anthropicBaseUrl: `${origin}/llm/anthropic`,
  };
}

/**
 * 启动本地 LLM 网关的进程绑定式随机端口监听器。
 *
 * @returns 监听句柄（含 close()）；网关未启用时返回 null（无监听/路由）。
 */
export async function startLocalLlmGatewayServer(deps: GatewayDeps): Promise<LocalLlmGatewayHandle | null> {
  if (getConfigValue('llm_gateway.enabled') !== true) return null;

  const host = normalizeHost(String(getConfigValue('llm_gateway.host') || '127.0.0.1'));
  // port=0：让 OS 分配随机可用端口，与 Web UI 端口随机化一致。
  // 配置的 llm_gateway.port 仅作"首选端口"尝试，失败则随机回退。
  const preferredPort = readPositiveInt('llm_gateway.port', 62000);

  const gateway = Fastify({ logger: false });
  registerLocalLlmGatewayRoutes(gateway, deps);

  let actualPort: number;
  try {
    // 先尝试配置的首选端口，EADDRINUSE 则回退到随机端口（port=0）
    await gateway.listen({ host, port: preferredPort });
    actualPort = preferredPort;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException | undefined)?.code;
    if (code === 'EADDRINUSE') {
      // 首选端口被占用（可能是上一个进程还没完全释放，或被其他程序占用）
      // → 回退到随机端口，绝不 fail-loud
      await gateway.listen({ host, port: 0 });
      actualPort = (gateway.server.address() as { port: number }).port;
    } else {
      throw err;
    }
  }

  // 注入运行时端点：resolveLocalLlmGateway() 和 MemoryEmbedding 会读取
  setRuntimeGatewayEndpoint(host, actualPort);

  let closed = false;
  const close = async (): Promise<void> => {
    if (closed) return;
    closed = true;
    clearRuntimeGatewayEndpoint();
    try {
      await gateway.close();
    } catch {
      /* 关闭中，忽略 */
    }
  };
  registerCleanup(close, 3.5);

  const model = String(getConfigValue('llm_gateway.model') || '').trim();
  coreLogger.info(`[LocalLlmGateway] 网关监听已启动: ${host}:${actualPort} (model: ${model || '<未配置>'}${actualPort !== preferredPort ? `, 首选端口 ${preferredPort} 被占用, 随机回退` : ''})`);

  return { ...endpointOf(host, actualPort), close };
}

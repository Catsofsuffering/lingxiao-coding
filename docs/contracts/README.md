# 凌霄剑域 · 架构契约文档

> 本目录包含凌霄各层之间的接口契约、数据流契约和事件契约的权威定义。
>
> 维护者修改跨层接口时**必须**同步更新本目录对应文档。

## 文档索引

| 文档 | 内容 | 适用范围 |
|------|------|----------|
| [system-architecture.md](./system-architecture.md) | 系统分层架构图、层间依赖规则 | 全局 |
| [core-engine.md](./core-engine.md) | 核心引擎层模块接口与契约 | `src/core/` |
| [agent-orchestration.md](./agent-orchestration.md) | Agent 编排层模块接口与契约 | `src/agents/` |
| [autonomy-governor.md](./autonomy-governor.md) | Autonomy Mode、Grant、RuntimeGate、LoopBreaker、PromptSegment 与 Web/TUI 同步契约 | Leader harness + Agent runtime + Web/TUI |
| [autonomy-governor-views.md](./autonomy-governor-views.md) | Autonomy Governor 在 Web UI、TUI、Agent、Task、Permission、Cost、Trace 视图中的可视化契约 | Web/TUI + runtime events |
| [web-server.md](./web-server.md) | Web 服务器层 API 端点与认证契约 | `src/web-server/` |
| [sse-events.md](./sse-events.md) | SSE 事件名映射表与数据结构 | 跨层（后端→前端） |
| [frontend.md](./frontend.md) | 前端状态管理与事件消费契约 | `web/src/` + `src/tui/` |
| [cli.md](./cli.md) | CLI 命令、配置与升级流程契约 | `src/cli.ts` + `src/config.ts` |
| [process-lifecycle.md](./process-lifecycle.md) | 进程启动/关闭顺序与清理优先级 | 全局 |

## 契约变更原则

1. **跨层接口变更**必须更新对应文档 + 相关层文档
2. **新增模块**必须在对应层文档中补充接口定义
3. **删除/重命名接口**必须在文档中标注 deprecated → removed 时间线
4. **SSE 事件变更**必须同步更新 `sse-events.md` 和 `frontend.md`

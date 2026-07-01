# 维护文档索引

> 本目录包含凌霄代码库的稳定性问题、修复路线图和重构计划。

## 文档清单

| 文档 | 内容 |
|------|------|
| [stability-issues.md](./stability-issues.md) | 全部 P0/P1/P2 问题清单，按层分类，含修复优先级 |
| [security-hardening.md](./security-hardening.md) | 安全加固指南：路径遍历、XSS、SSRF、认证修复 |
| [timer-cleanup.md](./timer-cleanup.md) | 定时器 unref 修复和资源清理指南 |
| [mega-file-refactoring.md](./mega-file-refactoring.md) | 巨型文件拆分计划 |
| [autonomy-governor-roadmap.md](./autonomy-governor-roadmap.md) | Autonomy Governor 落地路线图：自主边界、模式同步、Agent loop 止损、PromptSegment 与计费改造 |

## 问题统计

| 严重级 | 数量 | 处理时限 |
|--------|------|----------|
| P0 | 10 | 立即修复 |
| P1 | 28 | 下一个版本 |
| P2 | 32 | 排期处理 |

## 按层分布

| 层 | P0 | P1 | P2 | 小计 |
|----|----|----|----|----|
| 核心引擎 `src/core/` | 2 | 6 | 8 | 16 |
| Agent 系统 `src/agents/` | 2 | 5 | 6 | 13 |
| Web 服务器 `src/web-server/` | 2 | 5 | 4 | 11 |
| 前端 `web/src/` + `src/tui/` | 2 | 5 | 6 | 13 |
| CLI/工程化 | 2 | 7 | 8 | 17 |
| **合计** | **10** | **28** | **32** | **70** |

## 修复路线图

### 阶段一：P0 安全 + 稳定性（立即）
1. GitIntegrationApi 路径遍历修复
2. window.__LINGXIAO_TOKEN__ XSS 链修复
3. 定时器 unref（3 处跨层）
4. 配置文件损坏恢复机制
5. 升级中断回滚机制

### 阶段二：P1 功能 + 可靠性（下一版本）
1. MessageBus.unregister 泄漏修复
2. 认证补齐（TempDownloadRoutes）
3. SSRF 防护（URL 参数过滤）
4. LLM 超时/重试策略
5. SSE 重连竞态修复

### 阶段三：P2 可维护性（排期）
1. 巨型文件拆分（5 个文件 > 1000 行）
2. 错误处理统一化
3. 配置参数外部化
4. i18n 补全

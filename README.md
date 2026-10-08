# Streaming Agent Gateway（参考实现）

可观察 · 可取消 · 可恢复 · 可审计 的流式 Agent 网关。**零依赖**，仅用 Node 内置 `http` + `fetch`（Node ≥ 22）。

## 运行

```bash
npm run demo   # 自包含演示：起网关 + 跑 A/B/C/D 四个场景
npm start      # 仅启动网关（默认 :3000），可用自己的客户端连
npm run web    # 启动网关 + 托管 Web 客户端（默认 :8080）→ 打开 http://localhost:8080
npm run cli    # 启动交互式命令行客户端（默认连 :3000）
```

## Web 客户端

零依赖单文件（`public/index.html`），浏览器原生 `EventSource` 订阅事件流：

- **事件流（server→client）**：`EventSource('/runs/:id/stream')`。事件带 `id=seq`，**浏览器断线自动带 `Last-Event-ID` 重连**，服务端按 seq 纯补发缺失事件（Replay，不重执行）。
- **命令（client→server）**：独立 `POST` 端点（创建/取消/暂停/恢复/重试），与事件流**双通道**分离。
- 界面：流式打字渲染、`message_complete` 作为最终真相覆盖、TTFT/tokens 统计、实时事件日志、状态徽章与按钮按状态联动（`waiting_client` 时启用"重试"）。

```bash
npm run web          # 然后浏览器打开 http://localhost:8080
```

## CLI 客户端

交互式命令行（`examples/cli.js`），基于共享 SDK 的 `fetch` 流式订阅 + 手动游标重连：

```bash
npm run cli                                    # 默认连 http://localhost:3000
npm run cli --url http://localhost:8080 "提示词"  # 指定网关与提示词
```

运行中可用按键（演示各项能力）：

| 按键 | 作用 | 对应端点 |
|---|---|---|
| `c` | 取消（混合取消：AbortSignal 掐在途流 + 显式 `run_terminated`） | `POST /cancel` |
| `p` | 暂停（边界停止 + 落 checkpoint，状态 `paused`） | `POST /pause` |
| `r` | 恢复（从 checkpoint 续跑，状态 `running`） | `POST /resume` |
| `y` | 重试（仅 `waiting_client` 时，触发 `incident` 的 `remediation=retry`） | `POST /retry` |
| `x` | **模拟断线并重连**：abort 当前订阅，2 秒后按 `seq` 游标重新订阅，服务端纯补发 | `GET /stream?since=` |
| `q` | 退出 | — |

> 非 TTY 环境（管道/CI）下自动降级为"创建 Run → 流式到结束即退出"，仍可验证全链路。

## 验收清单（对应你提的 5 条需求）

| 需求 | 是否覆盖 | 落点 |
|---|---|---|
| 创建独立 Run | ✅ | `POST /runs` → `store.createRun` |
| 流式调用模型 | ✅ | `StreamingModel.stream()` + `loop._loop` |
| 转换统一 Harness Event | ✅ | `adapter.js`（原生→统一事件，异常→`incident`） |
| SSE 推送 | ✅ | `GET /runs/:id/stream`（事件带 `id=seq`） |
| 用户取消 | ✅ | `POST /cancel` + 混合取消（AbortSignal + 边界） |
| 断线恢复 | ✅ | Replay：`Last-Event-ID` / `?since=` 按 seq 补发（不重执行） |
| checkpoint 保存 | ✅ | `run_checkpoints` 单版本 upsert，轮次边界落点 |
| TTFT 统计 | ✅ | `usage` 事件带 `ttft_ms` |

> 额外覆盖：显式 `pause` / `resume` / `retry`、`incident` 失败协议、`audit_log` 防篡改 hash 链。

## 目录

```
src/
  events.js     RunEvent 统一信封 + 受控枚举 + SSE 编码
  store.js      RunStore：runs / run_events / run_checkpoints + 审计 hash 链（内存）
  bus.js        进程内 pub/sub（事件实时推送给 SSE）
  model.js      StreamingModel 抽象 + Mock 流式模型 + 可取消工具 + CancellationToken
  adapter.js    Harness 转换层（原生→统一事件、异常→incident）
  loop.js       Agent Loop：混合取消 / checkpoint / Option Y 恢复 / TTFT / incident-retry
  gateway.js    HTTP 网关：SSE 推送 + 命令通道（双通道）
  client.js     共享客户端 SDK（Web EventSource / Node fetch 流式）
  schema.sql    生产持久化 schema（操作日志与审计分开存）
examples/
  demo.js         四个场景：常规流式 / 取消 / 断线重连补发 / incident+retry
  cli.js          交互式命令行客户端（fetch 流式 + 手动游标重连）
  web-server.js   托管 Web 客户端静态页 + 网关（共用端口）
public/
  index.html      零依赖 Web 客户端（原生 EventSource + 命令 POST）
```

## 审查结论落地的关键设计

- **RunEvent 统一信封 + 受控枚举**（含 `usage` / `incident`）；`seq` 服务端生成、按 run 单调。
- **混合取消**：`AbortSignal` 掐在途流 + 安全点查 `requested`/`pauseRequested`；工具执行中可立即终止。
- **Replay ≠ Checkpoint**：Replay=客户端断线补事件（不重执行）；Checkpoint=服务端崩溃/暂停后加载快照续跑（真执行）。
- **Checkpoint 是缓存，事件日志为恢复权威源**（每边界落点 ⇒ 直接用 `checkpoint.messages`，无重跑/无 seq 撞车）。
- **命令通道独立于事件流**：`POST cancel/pause/resume/retry` + `GET /stream`（SSE）。单订阅者。
- **可追踪（操作日志）与可审计 Trace 分开存**：`run_events` vs `audit_log`（hash 链、长保留）。
- **失败协议**：任何失败发 `incident`（severity/category/retryable/remediation）；`retry` 等客户端 `POST /retry`。
- **客户端**：共享 SDK；原生 `Last-Event-ID` replay；`message_complete` 为真相，token 渐进增强。

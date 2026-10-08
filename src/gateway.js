// gateway.js —— HTTP 网关：SSE 事件推送 + 命令通道（双通道）
//
// 审查结论：
//  - 事件（server→client）：GET /runs/:id/stream（SSE），事件带 id=seq，
//    断线用 Last-Event-ID 自动补发（Replay，不重执行）
//  - 命令（client→server）：独立 POST 端点
//      POST /runs             创建独立 Run 并启动
//      POST /runs/:id/cancel  用户取消（混合取消）
//      POST /runs/:id/pause   显式暂停（边界停止 + 落 checkpoint）
//      POST /runs/:id/resume  从 checkpoint 续跑（显式，重连只读不自动）
//      POST /runs/:id/retry   remediation=retry 时由客户端触发重试
//  - 单订阅者：一个 run 一条 SSE 连接即可；本实现仍支持多连接（各自独立游标）
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { bus } from './bus.js';
import { RunStore } from './store.js';
import { AgentLoop, TERMINAL } from './loop.js';
import { MockStreamingModel, MockTool, OpenAICompatibleModel, CancellationToken } from './model.js';
import { RunStatus, toSSE } from './events.js';

const DEFAULT_TEXT =
  '你好，我是流式 Agent。这串文字用于演示 SSE 推送、用户取消与断线恢复。';

export function createGateway({ store, port = 3000, staticRoot = null } = {}) {
  store = store ?? new RunStore();
  const tokens = new Map(); // run_id -> CancellationToken
  const loops = new Map(); // run_id -> AgentLoop
  const staticDir = staticRoot ? path.resolve(staticRoot) : null;

  function getLoop(runId, config = {}) {
    if (!loops.has(runId)) {
      const mcfg = config.model ?? {};
      const provider = mcfg.provider ?? 'mock';
      let model;
      let tool = null;
      if (provider === 'mock') {
        model = new MockStreamingModel(config.prompt ?? DEFAULT_TEXT, {
          delayMs: config.streamDelayMs ?? 8,
        });
        tool = new MockTool('demo_tool', {
          failMode: config.toolFailMode ?? 'never',
          sideEffecting: false,
        });
      } else {
        // 真实模型：DeepSeek / 通义千问（OpenAI 兼容协议）
        const presets = {
          deepseek: { baseURL: 'https://api.deepseek.com', model: 'deepseek-chat' },
          qwen: {
            baseURL: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
            model: 'qwen-plus',
          },
        };
        const p = presets[provider];
        if (!p) throw new Error(`unknown model provider: ${provider}`);
        const envKey =
          provider === 'deepseek'
            ? process.env.DEEPSEEK_API_KEY
            : process.env.DASHSCOPE_API_KEY;
        model = new OpenAICompatibleModel({
          baseURL: mcfg.baseURL ?? p.baseURL,
          apiKey: mcfg.apiKey ?? envKey ?? '',
          model: mcfg.model ?? p.model,
          temperature: mcfg.temperature,
        });
        // 真实模型不附带 mock 工具（避免构造非法 messages；incident/retry 由 mock 演示覆盖）
      }
      loops.set(runId, new AgentLoop(store, model, {
        maxTurns: config.maxTurns ?? 1,
        tool,
      }));
    }
    return loops.get(runId);
  }

  function ensureToken(runId) {
    if (!tokens.has(runId)) tokens.set(runId, new CancellationToken());
    return tokens.get(runId);
  }

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const parts = url.pathname.split('/').filter(Boolean);
    try {
      // 静态文件服务（Web 客户端与 SSE 网关共用一个端口）
      if (staticDir && req.method === 'GET' && parts[0] !== 'runs') {
        if (await serveStatic(res, url, staticDir)) return;
        // 未命中静态资源 → 落到下面的 404
      }

      // 创建独立 Run
      if (req.method === 'POST' && parts[0] === 'runs' && parts.length === 1) {
        const runId = 'run_' + randomUUID().slice(0, 8);
        const body = await readBody(req);
        const config = body ? JSON.parse(body) : {};
        store.createRun(runId, config);
        const loop = getLoop(runId, config);
        const token = ensureToken(runId);
        const messages = config.messages ?? [
          { role: 'user', content: config.prompt ?? '请介绍一下你自己。' },
        ];
        loop.start(runId, messages, token); // 后台执行，不阻塞响应
        sendJSON(res, 200, { run_id: runId, status: 'running' });
        return;
      }

      if (parts[0] === 'runs' && parts.length === 3) {
        const runId = parts[1];
        const action = parts[2];
        const meta = store.getRun(runId);
        if (!meta) {
          sendJSON(res, 404, { error: 'unknown run' });
          return;
        }

        // SSE 事件推送 + 断线重连补发
        if (req.method === 'GET' && action === 'stream') {
          return handleStream(req, res, store, runId, url);
        }

        if (req.method === 'POST' && action === 'cancel') {
          const t = ensureToken(runId);
          t.request();
          sendJSON(res, 200, {
            accepted: true,
            mode: 'hybrid',
            hardkill: ['model_stream' /* 在途流被 AbortSignal 掐断 */],
            boundary: ['tool' /* 不可取消工具降级到边界 */],
          });
          return;
        }

        if (req.method === 'POST' && action === 'pause') {
          const t = ensureToken(runId);
          t.requestPause();
          sendJSON(res, 200, { accepted: true });
          return;
        }

        if (req.method === 'POST' && action === 'resume') {
          const st = meta.status;
          if (st === RunStatus.RUNNING || st === RunStatus.FINISHED) {
            sendJSON(res, 409, { error: 'cannot resume', status: st });
            return;
          }
          getLoop(runId).resume(runId, ensureToken(runId));
          sendJSON(res, 200, { accepted: true });
          return;
        }

        if (req.method === 'POST' && action === 'retry') {
          const st = meta.status;
          if (st !== RunStatus.WAITING_CLIENT) {
            sendJSON(res, 409, { error: 'not awaiting retry', status: st });
            return;
          }
          getLoop(runId).resume(runId, ensureToken(runId));
          sendJSON(res, 200, { accepted: true });
          return;
        }
      }

      // 临时调试：列出某 run 的所有事件类型
      if (req.method === 'GET' && parts[0] === 'debug' && parts.length === 2) {
        const evs = store.getEventsSince(parts[1], 0);
        sendJSON(res, 200, { types: evs.map((e) => e.type) });
        return;
      }

      // 临时调试：列出某 run 的所有事件类型
      if (req.method === 'GET' && parts[0] === 'debug' && parts.length === 2) {
        const evs = store.getEventsSince(parts[1], 0);
        sendJSON(res, 200, { types: evs.map((e) => e.type) });
        return;
      }

      sendJSON(res, 404, { error: 'not found' });
    } catch (e) {
      sendJSON(res, 500, { error: e.message });
    }
  });

  function handleStream(req, res, store, runId, url) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });

    // 重连游标：优先 Last-Event-ID（浏览器自动带），回退 ?since=
    const since = Number(
      req.headers['last-event-id'] ?? url.searchParams.get('since') ?? 0,
    );

    // 1) 先补发 since 之后的历史事件（Replay —— 纯重投，不重执行）
    const status = store.getRun(runId).status;
    for (const e of store.getEventsSince(runId, since)) res.write(toSSE(e));

    // 已结束 / 已终止 / 已失败的 run：重连只做幂等观测，补发后直接关闭
    if ([RunStatus.FINISHED, RunStatus.TERMINATED, RunStatus.FAILED].includes(status)) {
      res.end();
      return;
    }

    // 2) 再实时订阅新事件
    const handler = (e) => {
      if (e.run_id !== runId) return;
      res.write(toSSE(e));
      if (TERMINAL.has(e.type)) {
        clearInterval(heartbeat);
        res.end();
      }
    };
    bus.on('run-event', handler);
    const heartbeat = setInterval(() => res.write(': ping\n\n'), 15000);
    req.on('close', () => {
      clearInterval(heartbeat);
      bus.off('run-event', handler);
    });
  }

  server.listen(port, () => {
    console.log(`[gateway] listening on http://localhost:${port}`);
  });

  return { server, store };
}

function readBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => resolve(data));
  });
}

// 静态文件服务：仅用于托管 Web 客户端；带目录穿越防护
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};
async function serveStatic(res, url, root) {
  let rel = decodeURIComponent(url.pathname);
  if (rel === '/' || rel === '') rel = '/index.html';
  const filePath = path.normalize(path.join(root, rel));
  if (!filePath.startsWith(root)) {
    sendJSON(res, 403, { error: 'forbidden' });
    return true;
  }
  try {
    const data = await readFile(filePath);
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] ?? 'application/octet-stream' });
    res.end(data);
    return true;
  } catch {
    return false; // 交给后续 404
  }
}

function sendJSON(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

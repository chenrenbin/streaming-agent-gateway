// cli.js —— 交互式命令行客户端（与 Web 客户端共用同一套网关契约）
//
// 运行：npm run cli [--url http://localhost:3000] ["你的提示词"]
//
// 交互按键（流式进行中可用）：
//   c  取消(cancel)      p  暂停(pause)      r  恢复(resume)
//   y  重试(retry)       x  模拟断线并重连    q  退出
//
// 传输说明（呼应审查"双通道" + 断线恢复）：
//  - 订阅：fetch 流式读 SSE（Node 无原生 EventSource），手动维护 lastSeq 游标
//  - 命令：独立 POST 端点（cancel/pause/resume/retry）
//  - 断线恢复：按 seq 游标重新 GET /stream?since=  → 服务端纯补发缺失事件（Replay，不重执行）
import { GatewayClient } from '../src/client.js';
import readline from 'node:readline';

const TERMINAL = new Set(['run_finished', 'run_terminated', 'run_failed']);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 简单 ANSI 着色
const c = {
  dim: (s) => `\x1b[2m${s}\x1b[0m`,
  ok: (s) => `\x1b[32m${s}\x1b[0m`,
  warn: (s) => `\x1b[33m${s}\x1b[0m`,
  err: (s) => `\x1b[31m${s}\x1b[0m`,
  cyan: (s) => `\x1b[36m${s}\x1b[0m`,
};
const hasColor = process.stdout.isTTY;
const paint = (s) => (hasColor ? s : s.replace(/\x1b\[[0-9;]*m/g, ''));

function parseArgs(argv) {
  let url = 'http://localhost:3000';
  let provider = 'mock';
  let model = null;
  let apiKey = null;
  const rest = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--url') url = argv[++i];
    else if (argv[i] === '--provider') provider = argv[++i];
    else if (argv[i] === '--model') model = argv[++i];
    else if (argv[i] === '--api-key') apiKey = argv[++i];
    else rest.push(argv[i]);
  }
  const prompt = rest.join(' ') || '请介绍一下你自己，并演示流式输出。';
  const modelCfg = { provider };
  if (model) modelCfg.model = model;
  if (apiKey) modelCfg.apiKey = apiKey;
  return { url, prompt, modelCfg };
}

// 渲染单个事件：token 内联"打字"，结构事件单独成行
function render(ev) {
  if (ev.type === 'token') {
    process.stdout.write(ev.data.delta);
    return;
  }
  process.stdout.write('\n'); // 结束当前行的打字输出
  let line = '';
  switch (ev.type) {
    case 'run_started': line = paint(c.cyan(`▶ run_started`)); break;
    case 'message_complete': line = paint(c.ok(`✔ message_complete (len=${ev.data.text.length})`)); break;
    case 'usage': line = paint(c.dim(`⏱ usage: ttft=${ev.data.ttft_ms}ms, tokens=${ev.data.tokens}`)); break;
    case 'turn_start': line = paint(c.dim(`↻ turn_start #${ev.data.iteration}`)); break;
    case 'turn_end': line = paint(c.dim(`↻ turn_end #${ev.data.iteration}`)); break;
    case 'tool_call_start': line = paint(c.ok(`🔧 tool_call_start: ${ev.data.name}`)); break;
    case 'tool_result': line = paint(c.dim(`   tool_result: ${ev.data.result}`)); break;
    case 'tool_call_end': line = paint(c.dim(`   tool_call_end: ${ev.data.status}`)); break;
    case 'checkpoint': line = paint(c.dim(`💾 checkpoint saved (seq_at=${ev.seq})`)); break;
    case 'incident':
      line = paint(c.warn(`⚠ incident: ${ev.data.category} retryable=${ev.data.retryable} remediation=${ev.data.remediation}`));
      if (ev.data.remediation === 'retry' && ev.data.retryable) line += paint(c.warn('  → 等待客户端决策，按 y 重试'));
      break;
    case 'paused': line = paint(c.warn(`⏸ paused (${ev.data.reason})`)); break;
    case 'resumed': line = paint(c.cyan(`⏵ resumed`)); break;
    case 'run_terminated': line = paint(c.err(`■ run_terminated (用户取消)`)); break;
    case 'run_finished': line = paint(c.ok(`✅ run_finished`)); break;
    case 'run_failed': line = paint(c.err(`❌ run_failed: ${ev.data.message}`)); break;
    default: line = paint(c.dim(`· ${ev.type}`));
  }
  console.log(`  ${line}`);
}

async function main() {
  const { url, prompt, modelCfg } = parseArgs(process.argv.slice(2));
  const client = new GatewayClient(url);

  console.log(paint(c.cyan(`\n=== Streaming Agent Gateway · CLI 客户端 ===`)));
  console.log(paint(c.dim(`网关: ${url}`)));
  console.log(paint(c.dim(`提示词: ${prompt}`)));
  if (modelCfg.provider !== 'mock')
    console.log(paint(c.dim(`模型: ${modelCfg.provider}/${modelCfg.model ?? '(默认)'} ${modelCfg.apiKey ? '(含 API Key)' : '(用网关环境变量)'}`)));

  const { run_id, status } = await client.createRun({ prompt, model: modelCfg });
  console.log(paint(c.ok(`\n▶ 已创建 Run: ${run_id} (status=${status})`)));
  console.log(paint(c.dim(`按键: c=取消 p=暂停 r=恢复 y=重试 x=断线重连 q=退出\n`)));

  let activeAc = null;
  let stop = false;
  let lastStatus = 'running';

  // 订阅 + 自动重连（演示断线恢复）。仅在 run 仍在进行且非主动退出时重连。
  async function watch() {
    while (!stop) {
      const ac = new AbortController();
      activeAc = ac;
      let terminal = false;
      try {
        for await (const ev of client.subscribe(run_id, {
          since: client._cursor,
          signal: ac.signal,
        })) {
          render(ev);
          if (['paused', 'waiting_client'].includes(ev.type)) lastStatus = ev.type;
          if (TERMINAL.has(ev.type)) { terminal = true; break; }
        }
      } catch (e) {
        if (e.name !== 'AbortError') { console.error(paint(c.err(`订阅错误: ${e.message}`))); break; }
      }
      if (terminal || stop) break;
      // 连接被断开但 run 未终态 → 按 seq 游标重连补发（Replay）
      console.log(paint(c.warn(`\n⟳ 连接断开，2 秒后按 seq=${client._cursor} 重连补发缺失事件…`)));
      await sleep(2000);
    }
  }

  const watcher = watch();

  // 交互按键
  if (process.stdin.isTTY) {
    readlineKeypress(async (ch) => {
      switch (ch) {
        case 'c': await client.cancel(run_id); console.log(paint(c.err(`\n» 已发送 cancel`))); break;
        case 'p': await client.pause(run_id); console.log(paint(c.warn(`\n» 已发送 pause`))); break;
        case 'r':
          if (lastStatus === 'paused' || lastStatus === 'crashed') {
            await client.resume(run_id); console.log(paint(c.cyan(`\n» 已发送 resume`)));
          } else console.log(paint(c.dim(`\n» 当前状态不可 resume (${lastStatus})`)));
          break;
        case 'y':
          if (lastStatus === 'waiting_client') {
            await client.retry(run_id); console.log(paint(c.cyan(`\n» 已发送 retry`)));
          } else console.log(paint(c.dim(`\n» 当前无待重试 incident (${lastStatus})`)));
          break;
        case 'x':
          console.log(paint(c.warn(`\n» 模拟断线（abort 当前订阅）…`)));
          activeAc?.abort(); // watcher 会检测到断开并重连
          break;
        case 'q':
          stop = true; activeAc?.abort();
          console.log(paint(c.dim(`\n» 退出`)));
          break;
      }
    });
  }

  await watcher;
  console.log(paint(c.dim(`\n— 会话结束 —\n`)));
  process.exit(0);
}

// 轻量 keypress：TTY raw 模式单键触发
function readlineKeypress(handler) {
  readline.emitKeypressEvents(process.stdin);
  try { process.stdin.setRawMode(true); } catch {}
  process.stdin.resume();
  process.stdin.on('keypress', (str, key) => {
    if (key && key.ctrl && key.name === 'c') { process.exit(0); }
    handler(str);
  });
}

main().catch((e) => { console.error(e); process.exit(1); });

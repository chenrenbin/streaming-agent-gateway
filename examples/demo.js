// demo.js —— 自包含演示：覆盖 v1 验收清单全部场景
//   · 创建独立 Run        · 流式调用模型        · 转换统一 Harness Event
//   · SSE 推送            · 用户取消            · 断线恢复(Replay)
//   · checkpoint 保存     · TTFT 统计           · incident + retry（额外）
//
// 运行：npm run demo   （本文件会自己起一个网关，无需先 npm start）
import { createGateway } from '../src/gateway.js';
import { GatewayClient } from '../src/client.js';

const PORT = 3210;
const BASE = `http://localhost:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const TERMINAL = new Set(['run_finished', 'run_terminated', 'run_failed']);

// 一段足够长、能在 200ms 内被"取消/断线"打断的文本
const LONG =
  '这是一段较长的演示文本，用来验证流式推送、用户取消以及断线后的事件重放。' +
  '每一个字符都是一个 token，客户端会实时收到；当我们在中途取消或断开连接时，' +
  '服务端应当能够掐断在途的流式请求，或者让重新连接的客户端按序号补发缺失的事件。';

function log(...a) {
  console.log(...a);
}

async function main() {
  const { store } = createGateway({ port: PORT });
  await sleep(200); // 等监听就绪
  const client = new GatewayClient(BASE);

  // ── 场景 A：创建 Run → 流式 → SSE → 收到 usage(TTFT) → 正常结束 ──
  log('\n=== 场景 A：创建 Run + 流式 + SSE + TTFT ===');
  {
    const { run_id } = await client.createRun({ prompt: '请介绍一下你自己。' });
    log('created run:', run_id);
    let ttft = null;
    let tokenCount = 0;
    for await (const ev of client.subscribe(run_id)) {
      if (ev.type === 'token') tokenCount++;
      if (ev.type === 'usage') ttft = ev.data.ttft_ms;
      if (!['token'].includes(ev.type)) {
        log(`  · ${ev.type}`, ev.type === 'usage' ? `ttft_ms=${ev.data.ttft_ms}` : JSON.stringify(ev.data).slice(0, 64));
      }
      if (TERMINAL.has(ev.type)) break;
    }
    log(`  => tokens=${tokenCount}, TTFT=${ttft}ms`);
  }

  // ── 场景 B：用户取消（混合取消：AbortSignal 掐在途流 + 边界终止）──
  log('\n=== 场景 B：用户取消（流式进行中打断） ===');
  {
    const { run_id } = await client.createRun({ prompt: LONG, streamDelayMs: 18 });
    const ac = new AbortController();
    const sub = client.subscribe(run_id, { signal: ac.signal });
    let got = 0;
    const p = (async () => {
      for await (const ev of sub) {
        if (ev.type === 'token') got++;
        else log(`  · ${ev.type}`);
        if (TERMINAL.has(ev.type)) break;
      }
    })();
    await sleep(220); // 等流式进行到一半
    const ack = await client.cancel(run_id); // 命令通道：POST /cancel
    log(`  (已收 ${got} 个 token) cancel_ack:`, JSON.stringify(ack));
    await p;
  }

  // ── 场景 C：断线恢复（Replay）—— 断开后用游标重新订阅，补发缺失事件 ──
  log('\n=== 场景 C：断线恢复 (Replay) ===');
  {
    const { run_id } = await client.createRun({ prompt: LONG, streamDelayMs: 6 });
    const ac = new AbortController();
    let firstSeen = 0;
    const sub = client.subscribe(run_id, { signal: ac.signal });
    const p = (async () => {
      for await (const ev of sub) {
        if (ev.type === 'token') firstSeen++;
        if (TERMINAL.has(ev.type)) break;
      }
    })();
    await sleep(220);
    log(`  → 模拟断线（已收 ${firstSeen} 个 token，游标=${client._cursor}）`);
    ac.abort(); // 模拟网络断开
    await p;
    await sleep(1300); // 等 run 真正结束（此后重连只会"补发"，不会重执行）
    log('  → 用游标重新订阅（run 已结束），服务端按 seq 补发缺失事件：');
    let replayTokens = 0;
    let replayStructural = 0;
    for await (const ev of client.subscribe(run_id, { since: client._cursor })) {
      if (ev.type === 'token') replayTokens++;
      else {
        replayStructural++;
        log(`  [重连补发] ${ev.type}`);
      }
      if (TERMINAL.has(ev.type)) break;
    }
    log(`  => 重连后补发 token=${replayTokens}, 结构事件=${replayStructural}`);
  }

  // ── 场景 D：incident + retry（remediation=retry 等客户端决策）──
  log('\n=== 场景 D：incident + retry ===');
  {
    const { run_id } = await client.createRun({ toolFailMode: 'once', streamDelayMs: 12 });
    const ac = new AbortController();
    let gotIncident = false;
    const sub = client.subscribe(run_id, { signal: ac.signal });
    const p = (async () => {
      for await (const ev of sub) {
        log(`  · ${ev.type}`, ev.type === 'incident' ? `retryable=${ev.data.retryable} remediation=${ev.data.remediation}` : '');
        if (ev.type === 'incident') gotIncident = true;
        if (TERMINAL.has(ev.type)) break;
      }
    })();
    while (!gotIncident) await sleep(30);
    ac.abort();
    await p;
    log('  → 客户端决策：重试（POST /retry）');
    await client.retry(run_id);
    for await (const ev of client.subscribe(run_id)) {
      log(`  · ${ev.type}`, ev.type === 'incident' ? `retryable=${ev.data.retryable}` : '');
      if (TERMINAL.has(ev.type)) break;
    }
  }

  // ── 审计链完整性校验（可审计：防篡改 hash 链）──
  log('\n=== 审计链校验 ===');
  log('  audit records:', store.audit.length, '| chain valid:', store.verifyAuditChain());

  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

// real-models.js —— 接真实模型（DeepSeek / 通义千问）的示例
//
// 通过网关创建 Run，验证 OpenAICompatibleModel 适配器：流式调用 / SSE 推送 / 取消 /
// 失败协议 全部复用，无需改动网关其余管道。
//
// 用法：
//   node examples/real-models.js deepseek   # 需要 DEEPSEEK_API_KEY
//   node examples/real-models.js qwen       # 需要 DASHSCOPE_API_KEY
//   node examples/real-models.js            # 默认 deepseek
//
// 未设置 API Key 时，会真实发起请求并触发"缺凭证 → incident → run_failed"，
// 用于演示失败协议路径（而非报错崩溃）。
import { createGateway } from '../src/gateway.js';
import { GatewayClient } from '../src/client.js';

const PORT = 3220;
const BASE = `http://localhost:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const TERMINAL = new Set(['run_finished', 'run_terminated', 'run_failed']);

const provider = process.argv[2] ?? 'deepseek'; // deepseek | qwen
const envKey =
  provider === 'deepseek' ? process.env.DEEPSEEK_API_KEY : process.env.DASHSCOPE_API_KEY;

if (!envKey) {
  console.log(`\n[提示] 未检测到 ${provider} 的 API Key。`);
  console.log(
    `       环境变量：${provider === 'deepseek' ? 'DEEPSEEK_API_KEY' : 'DASHSCOPE_API_KEY'}`,
  );
  console.log(
    `       设置后重跑即可看到真实流式输出：\n` +
      `         export ${provider === 'deepseek' ? 'DEEPSEEK_API_KEY' : 'DASHSCOPE_API_KEY'}=sk-xxx && node examples/real-models.js ${provider}\n`,
  );
  console.log(`       下面仍会发起请求，演示"缺凭证 → incident → run_failed"失败协议路径。\n`);
}

const { store } = createGateway({ port: PORT });
await sleep(200);

const client = new GatewayClient(BASE);
const { run_id } = await client.createRun({
  prompt: '用一句话介绍你自己。',
  model: { provider },
});
console.log(`▶ 创建 Run: ${run_id} (provider=${provider})`);

for await (const ev of client.subscribe(run_id)) {
  if (ev.type === 'token') process.stdout.write(ev.data.delta);
  else if (ev.type === 'usage')
    console.log(
      `\n  ⏱ usage: ttft=${ev.data.ttft_ms}ms llm_tokens=${JSON.stringify(ev.data.llm_tokens)}`,
    );
  else if (ev.type === 'incident')
    console.log(
      `\n  ⚠ incident: ${ev.data.category} retryable=${ev.data.retryable} remediation=${ev.data.remediation} — ${ev.data.message}`,
    );
  else if (ev.type !== 'token') console.log(`  · ${ev.type}`);
  if (TERMINAL.has(ev.type)) break;
}

console.log(`\n  audit chain valid: ${store.verifyAuditChain()}`);
process.exit(0);

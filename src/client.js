// client.js —— 共享客户端 SDK（Web / CLI 共用同一套语义）
//
// 审查结论（分支 6）：
//  - Web 用浏览器原生 EventSource；CLI / Node 用 fetch 流式读 SSE（本 SDK 用 fetch 版）
//  - 重连 replay：维护 lastSeq 游标，断开后用 ?since= / Last-Event-ID 自动补发
//  - 客户端以 message_complete 为最终真相，token 只是渐进增强
import { toSSE } from './events.js';

export class GatewayClient {
  constructor(baseUrl) {
    this.base = baseUrl.replace(/\/$/, '');
    this._cursor = 0;
  }

  async createRun(config) {
    const r = await fetch(`${this.base}/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    return r.json();
  }
  async cancel(runId) {
    return (await fetch(`${this.base}/runs/${runId}/cancel`, { method: 'POST' })).json();
  }
  async pause(runId) {
    return (await fetch(`${this.base}/runs/${runId}/pause`, { method: 'POST' })).json();
  }
  async resume(runId) {
    return (await fetch(`${this.base}/runs/${runId}/resume`, { method: 'POST' })).json();
  }
  async retry(runId) {
    return (await fetch(`${this.base}/runs/${runId}/retry`, { method: 'POST' })).json();
  }

  // 订阅事件流（Node 端 fetch 流式）。signal 可用于主动断开（模拟断线）。
  // 生产客户端应在连接断开且 run 仍进行时，用 this._cursor 自动重连补发。
  async *subscribe(runId, { since = 0, signal } = {}) {
    const res = await fetch(
      `${this.base}/runs/${runId}/stream?since=${since}`,
      { headers: { 'Last-Event-ID': String(since) }, signal },
    );
    const decoder = new TextDecoder();
    let buf = '';
    try {
      // 用 for await 而非手动 reader.read()：规避 Node undici 偶发返回
      // { done:false, value:undefined } 导致的崩溃，流式更稳健
      for await (const chunk of res.body) {
        buf += decoder.decode(chunk, { stream: true });
        const frames = buf.split('\n\n');
        buf = frames.pop();
        for (const f of frames) {
          const ev = parseSSE(f);
          if (!ev) continue;
          if (ev.seq != null) this._cursor = ev.seq; // 维护续传游标
          yield ev;
        }
      }
    } catch (e) {
      if (e.name !== 'AbortError') throw e; // 主动断开（模拟断线）时静默结束
    }
  }
}

// 解析单个 SSE 帧：返回完整事件对象（含 type / data / seq / id 等）
export function parseSSE(frame) {
  let id = null;
  let eventType = null;
  let data = '';
  for (const line of frame.split('\n')) {
    if (line.startsWith('id:')) id = line.slice(3).trim();
    else if (line.startsWith('event:')) eventType = line.slice(6).trim();
    else if (line.startsWith('data:')) data += line.slice(5).trim();
  }
  if (!data) return null;
  try {
    const obj = JSON.parse(data);
    return { ...obj, type: obj.type ?? eventType, _id: id ?? obj.seq };
  } catch {
    return { id, type: eventType, data };
  }
}

// 便捷：把事件对象编码回 SSE（调试用，与 gateway 端 toSSE 一致）
export { toSSE };

// model.js —— StreamingModel 抽象 + Adapter 接入 + 可取消工具
//
// 审查结论（第 4 步"接入模型 Adapter"被改写）：
//  - 任何可执行单元（模型流式调用 / 工具）都必须收一个 CancellationToken
//  - 工具强制自报 capabilities { cancellable, sideEffecting }
//  - sideEffecting && cancellable 的工具必须幂等/可补偿（本参考实现用 Mock 体现契约）
import { RunStatus } from './events.js';

// ── CancellationToken：混合取消的核心 ────────────────────────────
//  - requested      : 安全点（轮次边界 / 工具执行前）检查的布尔
//  - signal         : AbortSignal，透传到模型/工具的底层请求，真·掐断在途流
//  - pauseRequested : 显式暂停标志（边界停止 + 落 checkpoint）
export class CancellationToken {
  constructor() {
    this.requested = false;
    this.pauseRequested = false;
    this.controller = new AbortController();
    this.signal = this.controller.signal;
  }
  request() {
    this.requested = true;
    // 取消 = 硬杀在途流：abort 透传到模型/工具底层请求（一次性，不可逆；run 终止后不会再 resume）
    try {
      this.controller.abort();
    } catch {}
  }
  requestPause() {
    // 暂停 = 边界优雅停：只置标志，不 abort。
    // 因为 AbortController 一次性不可复原，若这里 abort，恢复时流式会因 signal 已 aborted 直接空转。
    this.pauseRequested = true;
  }
  // 恢复时调用：替换一个新的（未中止的）controller/signal，供恢复后的流式与后续取消使用
  resetSignal() {
    this.controller = new AbortController();
    this.signal = this.controller.signal;
  }
}

// ── StreamingModel 抽象 ─────────────────────────────────────────
// 子类实现 async function* stream(messages, signal)，逐 token 产出。
export class StreamingModel {
  capabilities = { cancellable: true, sideEffecting: false };
  async *stream(_messages, _signal) {
    throw new Error('stream() not implemented');
  }
}

// 演示用 Mock：把一段文本按字流式吐出，遵守 AbortSignal（协作式停止）
// 多轮场景下给每轮加「第 N 轮」标记，便于在 Web 端观察 pause/恢复 的边界
export class MockStreamingModel extends StreamingModel {
  constructor(text, { delayMs = 8 } = {}) {
    super();
    this.text = text;
    this.delayMs = delayMs;
    this._call = 0;
  }
  async *stream(_messages, signal) {
    this._call += 1;
    const text = this._call > 1 ? `${this.text}\n（第${this._call}轮响应）` : this.text;
    // 按码点切分（CJK 也能逐字流式），便于演示 SSE 推送
    const pieces = [...text];
    for (const w of pieces) {
      if (signal?.aborted) return; // 在途流被 abort → 立即停
      await new Promise((r) => setTimeout(r, this.delayMs));
      yield w;
    }
  }
}

// 真实模型：OpenAI 兼容流式适配（DeepSeek / 通义千问 DashScope 等均兼容此协议）
//
// 用法：new OpenAICompatibleModel({ baseURL, apiKey, model, temperature? })
//  - DeepSeek : baseURL=https://api.deepseek.com,            model=deepseek-chat / deepseek-reasoner
//  - 通义千问 : baseURL=https://dashscope.aliyuncs.com/compatible-mode/v1, model=qwen-plus / qwen-max / qwen-turbo
// 关键：fetch 原生支持 AbortSignal → 取消时真·掐断在途流（与"混合取消"完全一致）
export class OpenAICompatibleModel extends StreamingModel {
  constructor({ baseURL, apiKey, model, temperature = 0.7, topP, maxTokens, extra = {} } = {}) {
    super();
    if (!baseURL || !model) {
      throw new Error('OpenAICompatibleModel 需要 baseURL / model');
    }
    this.baseURL = baseURL.replace(/\/$/, '');
    this.apiKey = apiKey ?? ''; // 允许为空：缺凭证将在"请求时"失败 → 触发 incident → run_failed
    this.model = model;
    this.temperature = temperature;
    this.topP = topP;
    this.maxTokens = maxTokens;
    this.extra = extra;
    this.capabilities = { cancellable: true, sideEffecting: false };
    this.timeoutMs = 30000; // 防止无外网/超时导致网关挂死
  }

  async *stream(messages, signal) {
    const body = {
      model: this.model,
      messages,
      stream: true,
      stream_options: { include_usage: true }, // 请求真实 token 用量（审计/usage 事件用）
      temperature: this.temperature,
      ...(this.topP != null ? { top_p: this.topP } : {}),
      ...(this.maxTokens != null ? { max_tokens: this.maxTokens } : {}),
      ...this.extra,
    };
    // 组合信号：取消(token.signal) 或 超时 任一触发即 abort（fetch 抛 AbortError）
    const ctrl = new AbortController();
    const onAbort = () => ctrl.abort();
    if (signal) signal.addEventListener('abort', onAbort, { once: true });
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      ctrl.abort();
    }, this.timeoutMs);
    let res;
    try {
      res = await fetch(`${this.baseURL}/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.apiKey}`,
        },
        body: JSON.stringify(body),
        signal: ctrl.signal, // 取消即真·掐断在途流；超时也会 abort
      });
    } catch (e) {
      clearTimeout(timer);
      if (signal) signal.removeEventListener('abort', onAbort);
      // 超时（非用户取消）→ 抛普通 Error，让 loop 走"失败协议"(incident → run_failed)
      if (timedOut) throw new Error('request timeout');
      // 否则（用户取消 / 网络错误）原样上抛，由 loop 区分处理
      throw e;
    }
    clearTimeout(timer);
    if (signal) signal.removeEventListener('abort', onAbort);

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`${this.model} 请求失败 HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop();
        for (const line of lines) {
          const t = line.trim();
          if (!t || !t.startsWith('data:')) continue;
          const data = t.slice(5).trim();
          if (data === '[DONE]') return;
          let json;
          try {
            json = JSON.parse(data);
          } catch {
            continue; // 跳过心跳/注释行
          }
          yield json; // 原始 provider chunk → adapter.adaptModelChunk 归一化成 Harness Event
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {}
    }
  }
}

// 演示用工具：带 capabilities + 可失败，用于演练 incident / retry
export class MockTool {
  constructor(name, opts = {}) {
    this.name = name;
    this.capabilities = {
      cancellable: opts.cancellable ?? true,
      sideEffecting: opts.sideEffecting ?? false,
    };
    this.failMode = opts.failMode ?? 'never'; // 'never' | 'once' | 'always'
    this._attempts = 0;
  }
  async execute(args, token) {
    this._attempts += 1;
    if (
      this.failMode === 'always' ||
      (this.failMode === 'once' && this._attempts === 1)
    ) {
      throw new Error(`${this.name} failed (attempt ${this._attempts})`);
    }
    // 模拟耗时，并支持取消（cancellable=true 时收到 abort 立即 reject）
    await new Promise((resolve, reject) => {
      const t = setTimeout(resolve, 40);
      if (token?.signal) {
        token.signal.addEventListener('abort', () => {
          clearTimeout(t);
          const e = new Error('tool aborted');
          e.name = 'AbortError';
          reject(e);
        });
      }
    });
    return { ok: true, result: `${this.name}(${JSON.stringify(args)}) => done` };
  }
}

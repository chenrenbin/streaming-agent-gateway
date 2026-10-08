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
    try {
      this.controller.abort();
    } catch {}
  }
  requestPause() {
    this.pauseRequested = true;
    try {
      this.controller.abort();
    } catch {}
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

// 演示用 Mock：把一段文本按词流式吐出，遵守 AbortSignal（协作式停止）
export class MockStreamingModel extends StreamingModel {
  constructor(text, { delayMs = 8 } = {}) {
    super();
    this.text = text;
    this.delayMs = delayMs;
  }
  async *stream(_messages, signal) {
    // 按码点切分（CJK 也能逐字流式），便于演示 SSE 推送
    const pieces = [...this.text];
    for (const w of pieces) {
      if (signal?.aborted) return; // 在途流被 abort → 立即停
      await new Promise((r) => setTimeout(r, this.delayMs));
      yield w;
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

// adapter.js —— Harness 转换层
//
// 审查结论（"转换统一 Harness Event"）：
//  - 各 provider 的"原生流式输出"在此统一成 Harness 事件（这里原生即字符串 token，
//    真实 Adapter 会解析各家的 SSE / JSON 流）
//  - 异常统一转换为结构化 incident（失败协议）
import { RunEventType } from './events.js';

// provider 原生 chunk → 统一 Harness chunk
export function adaptModelChunk(chunk) {
  // 真实场景：不同模型返回 {choices:[{delta:{content}}]} 等，这里归一化
  return { kind: 'token', delta: chunk };
}

export function adaptToolResult(name, args, result) {
  return { kind: 'tool_result', name, args, result };
}

// 异常 → 结构化 incident（失败协议）。任何失败都发 incident，靠 retryable 区分。
export function toIncident(err, ctx = {}) {
  const retryable = ctx.retryable ?? false;
  return {
    incident_id: `inc_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    severity: ctx.severity ?? 'error',
    category: ctx.category ?? 'unknown',
    message: err?.message ?? String(err),
    span_id: ctx.span_id ?? null,
    retryable,
    remediation: ctx.remediation ?? (retryable ? 'retry' : 'abort'),
    retry_hint: retryable ? { max_attempts: 3, backoff_ms: 500 } : undefined,
    context: ctx.context ?? {},
  };
}

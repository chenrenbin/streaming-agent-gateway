// events.js —— RunEvent 统一事件契约（Harness Event）
//
// 审查结论：
//  - 统一信封 + 受控 type 枚举（含 usage / incident）
//  - 身份/血缘字段：run_id / seq / span_id / parent_span_id
//  - seq 由 RunStore 在 emit 时生成（按 run 单调递增），同时充当
//    持久化游标、Replay 重排键、断线续传游标

export const RunEventType = {
  // 生命周期（可观察基座）
  RUN_STARTED: 'run_started',
  RUN_FINISHED: 'run_finished',
  RUN_TERMINATED: 'run_terminated', // 用户取消 / 显式终止
  RUN_FAILED: 'run_failed',
  PAUSED: 'paused', // 显式 pause 或等待客户端 retry
  RESUMED: 'resumed',

  // Agent Loop 轮次（每个 turn 一个 span）
  TURN_START: 'turn_start',
  TURN_END: 'turn_end',

  // 模型流式输出（可观察）
  TOKEN: 'token',
  MESSAGE_COMPLETE: 'message_complete',

  // 工具调用
  TOOL_CALL_START: 'tool_call_start',
  TOOL_CALL_END: 'tool_call_end',
  TOOL_RESULT: 'tool_result',

  // 恢复（可恢复）
  CHECKPOINT: 'checkpoint',

  // 审计 / 失败协议（可审计）
  USAGE: 'usage', // 成本 / 用量 / TTFT
  ERROR: 'error', // 非致命
  AUDIT: 'audit',
  INCIDENT: 'incident', // 结构化失败，含 remediation
};

export const RunStatus = {
  QUEUED: 'queued',
  RUNNING: 'running',
  PAUSED: 'paused',
  WAITING_CLIENT: 'waiting_client', // remediation=retry 等客户端决策
  TERMINATED: 'terminated',
  FINISHED: 'finished',
  FAILED: 'failed',
};

// 构造一个事件对象（seq / id 通常由 store 回填）
export function makeEvent(runId, type, data, meta = {}) {
  return {
    id: meta.id ?? `${runId}:${meta.seq ?? '?'}`,
    run_id: runId,
    type,
    seq: meta.seq ?? null,
    ts: meta.ts ?? Date.now(),
    span_id: meta.span_id ?? null,
    parent_span_id: meta.parent_span_id ?? null,
    data: data ?? {},
  };
}

// 编码为 SSE 帧：事件带 id=seq，浏览器 EventSource 断线会自动带 Last-Event-ID 重连
export function toSSE(event) {
  const lines = [];
  if (event.seq != null) lines.push(`id: ${event.seq}`);
  lines.push(`event: ${event.type}`);
  lines.push(`data: ${JSON.stringify(event)}`);
  return lines.join('\n') + '\n\n';
}

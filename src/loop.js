// loop.js —— Agent Loop 执行引擎（核心）
//
// 审查落地的关键点：
//  1) 混合取消：AbortSignal 掐在途流 + 安全点（轮次边界/工具前）查 requested/pauseRequested
//  2) 工具执行中可立即终止：AbortSignal 透传到工具；不可取消的工具降级到边界（cancel_ack 如实标注）
//  3) checkpoint 在轮次边界落点（单版本 upsert），作为"状态缓存"
//  4) 恢复（Option Y）：加载最新 checkpoint + 续 seq 真执行。
//     因为我们每个边界都落 checkpoint 且 checkpoint.messages 已含全部已完成轮次，
//     所以直接用 checkpoint.messages 即可，无需重放事件（若 checkpoint 更稀疏才需重放追上）。
//  5) TTFT：首 token 相对轮次开始的时间，落在 usage 事件里
//  6) 失败协议：任何失败发 incident；remediation=retry 时进入 WAITING_CLIENT，等客户端 POST /retry
import { RunEventType, RunStatus } from './events.js';
import { bus } from './bus.js';
import { adaptModelChunk, adaptToolResult, toIncident } from './adapter.js';

function newSpan(parent = null) {
  return {
    span_id: 'span_' + Math.random().toString(36).slice(2, 10),
    parent_span_id: parent,
  };
}

const TERMINAL = new Set([
  RunEventType.RUN_FINISHED,
  RunEventType.RUN_TERMINATED,
  RunEventType.RUN_FAILED,
]);

export class AgentLoop {
  constructor(store, model, opts = {}) {
    this.store = store;
    this.model = model;
    this.maxTurns = opts.maxTurns ?? 1;
    this.tool = opts.tool ?? null; // 可选 MockTool，用于演示工具事件与 incident/retry
  }

  _publish(event) {
    bus.emit('run-event', event);
  }

  _emit(runId, type, data, meta) {
    const ev = this.store.appendEvent(runId, type, data, meta);
    this._publish(ev);
    return ev;
  }

  // 启动一次全新 run
  async start(runId, initialMessages, token) {
    this.store.setStatus(runId, RunStatus.RUNNING);
    this._emit(runId, RunEventType.RUN_STARTED, { messages: initialMessages });
    await this._loop(runId, [...initialMessages], 0, token);
  }

  // 从 checkpoint 恢复（Option Y）：加载最新快照 + 续 seq 真执行，绝不重跑已完成的轮次
  async resume(runId, token) {
    // 清除暂停标志：恢复即代表"已接受暂停、继续"，避免 resume 后在边界被再次暂停
    token.pauseRequested = false;
    // 替换新的（未中止的）signal：pause 不会 abort，但 cancel 会；恢复后需一个干净信号供后续流式/取消使用
    token.resetSignal();
    const cp = this.store.getLatestCheckpoint(runId);
    if (!cp) throw new Error('no checkpoint to resume from');
    const messages = structuredClone(cp.messages);
    const iteration = cp.iteration;
    this.store.setStatus(runId, RunStatus.RUNNING);
    this._emit(runId, RunEventType.RESUMED, {
      from_seq: cp.seq_at,
      note: 'Option Y: checkpoint.messages used directly (per-boundary checkpoint ⇒ rebuild trivial)',
    });
    await this._loop(runId, messages, iteration, token);
  }

  async _loop(runId, messages, iteration, token) {
    try {
      while (iteration < this.maxTurns) {
        // 安全点：先查暂停 / 取消（只在这些边界切换状态）
        if (token.pauseRequested) {
          this._doPause(runId, messages, iteration);
          return;
        }
        if (token.requested) {
          this._terminate(runId);
          return;
        }

        const span = newSpan();
        this._emit(runId, RunEventType.TURN_START, { iteration }, span);
        // checkpoint 在轮次边界落点（缓存）
        this.store.saveCheckpoint(runId, {
          messages: structuredClone(messages),
          iteration,
          phase: 'AWAIT_LLM',
        });

        // 流式调用模型
        const turnStart = Date.now();
        let firstTokenTs = null;
        let fullText = '';
        let usageInfo = null; // 真实模型在末块回传的 token 用量
        try {
          for await (const chunk of this.model.stream(messages, token.signal)) {
            if (token.requested) break; // 收到取消 → 停止收集 token
            const adapted = adaptModelChunk(chunk);
            if (adapted.kind === 'token') {
              if (firstTokenTs == null) firstTokenTs = Date.now();
              fullText += adapted.delta;
              this._emit(runId, RunEventType.TOKEN, { delta: adapted.delta }, span);
            } else if (adapted.kind === 'usage') {
              usageInfo = adapted.usage; // prompt/completion/total tokens
            }
          }
        } catch (err) {
          // 在途流被 abort：按 pause / cancel 分流
          if (err?.name === 'AbortError' || token.requested || token.pauseRequested) {
            if (token.pauseRequested) {
              this._doPause(runId, messages, iteration);
            } else {
              this._terminate(runId);
            }
            return;
          }
          this._fail(runId, err, span);
          return;
        }

        // 取消可能在收集完 token 之后才被观察到
        if (token.requested) {
          this._terminate(runId);
          return;
        }

        const ttft = firstTokenTs != null ? firstTokenTs - turnStart : null;
        this._emit(runId, RunEventType.MESSAGE_COMPLETE, { text: fullText }, span);
        messages.push({ role: 'assistant', content: fullText });

        // 审计：usage（含 TTFT + 真实 token 用量）
        const llmTokens = usageInfo
          ? {
              prompt: usageInfo.prompt_tokens,
              completion: usageInfo.completion_tokens,
              total: usageInfo.total_tokens,
            }
          : undefined;
        this._emit(runId, RunEventType.USAGE, {
          ttft_ms: ttft,
          tokens: fullText.length,
          turn: iteration,
          llm_tokens: llmTokens,
        }, span);
        this.store.appendAudit({
          run_id: runId,
          kind: 'usage',
          ttft_ms: ttft,
          tokens: fullText.length,
          turn: iteration,
          llm_tokens: llmTokens,
        });

        // 可选工具步骤（演示 incident / retry）
        if (this.tool && iteration === 0) {
          const toolSpan = newSpan(span.span_id);
          this._emit(runId, RunEventType.TOOL_CALL_START, { name: this.tool.name }, toolSpan);
          try {
            const args = { q: fullText.slice(0, 16) };
            const res = await this.tool.execute(args, token);
            this._emit(
              runId,
              RunEventType.TOOL_RESULT,
              adaptToolResult(this.tool.name, args, res.result),
              toolSpan,
            );
            this._emit(
              runId,
              RunEventType.TOOL_CALL_END,
              { name: this.tool.name, status: 'ok' },
              toolSpan,
            );
            messages.push({ role: 'tool', name: this.tool.name, content: res.result });
          } catch (err) {
            const incident = toIncident(err, {
              severity: 'error',
              category: 'tool_error',
              span_id: toolSpan.span_id,
              retryable: this.tool.capabilities.cancellable, // 可取消工具 → 允许 retry
              remediation: this.tool.capabilities.cancellable ? 'retry' : 'abort',
            });
            this._emit(runId, RunEventType.INCIDENT, incident, toolSpan);
            this.store.appendAudit({ run_id: runId, kind: 'incident', ...incident });
            if (incident.remediation === 'retry') {
              // 等客户端决策：进入 WAITING_CLIENT，客户端发 POST /retry 触发 resume
              this.store.saveCheckpoint(runId, {
                messages: structuredClone(messages), // 不含失败的 tool 结果
                iteration,
                phase: 'AWAIT_RETRY',
              });
              this.store.setStatus(runId, RunStatus.WAITING_CLIENT);
              this._emit(runId, RunEventType.PAUSED, { reason: 'await_retry' });
              return;
            }
            this._fail(runId, err, toolSpan);
            return;
          }
        }

        this._emit(runId, RunEventType.TURN_END, { iteration }, span);
        iteration += 1;
        this.store.saveCheckpoint(runId, {
          messages: structuredClone(messages),
          iteration,
          phase: 'AWAIT_LLM',
        });
      }

      this._emit(runId, RunEventType.RUN_FINISHED, {});
      this.store.setStatus(runId, RunStatus.FINISHED);
    } catch (err) {
      this._fail(runId, err, null);
    }
  }

  _doPause(runId, messages, iteration) {
    this.store.saveCheckpoint(runId, {
      messages: structuredClone(messages),
      iteration,
      phase: 'PAUSED',
    });
    this.store.setStatus(runId, RunStatus.PAUSED);
    this._emit(runId, RunEventType.PAUSED, { reason: 'user_pause' });
    this.store.appendAudit({ run_id: runId, kind: 'paused', reason: 'user_pause' });
  }

  _terminate(runId) {
    this._emit(runId, RunEventType.RUN_TERMINATED, { reason: 'user_cancel' });
    this.store.setStatus(runId, RunStatus.TERMINATED);
    this.store.appendAudit({ run_id: runId, kind: 'terminated', reason: 'user_cancel' });
  }

  _fail(runId, err, span) {
    const incident = toIncident(err, {
      severity: 'critical',
      category: 'unknown',
      span_id: span?.span_id ?? null,
    });
    this._emit(runId, RunEventType.INCIDENT, incident, span);
    this._emit(runId, RunEventType.RUN_FAILED, { message: err?.message });
    this.store.setStatus(runId, RunStatus.FAILED);
    this.store.appendAudit({ run_id: runId, kind: 'failed', message: err?.message });
  }
}

export { TERMINAL };

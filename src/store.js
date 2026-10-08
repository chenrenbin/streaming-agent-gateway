// store.js —— Run Store（内存实现）
//
// 审查结论的持久化形态（生产替换为 schema.sql 的三张表 + 审计表）：
//  - runs           : run 元数据 / 状态
//  - run_events     : 追加写事件日志（既是 Replay 源，也是审计源；结构事件永久）
//  - run_checkpoints: 最新一份状态快照（单版本 upsert；Option Y 里它是"缓存"）
//  - audit_log      : 防篡改、hash 链式、长保留的审计 Sink（与操作日志分开存）
import { createHash } from 'node:crypto';
import { makeEvent, RunStatus } from './events.js';

export class RunStore {
  constructor() {
    this.runs = new Map(); // run_id -> meta
    this.events = new Map(); // run_id -> RunEvent[]
    this.checkpoints = new Map(); // run_id -> { state, seq_at, ts }
    this.audit = []; // hash-chained audit records
    this._auditPrev = '0'.repeat(64);
    this._seq = new Map(); // run_id -> last seq
  }

  createRun(runId, config = {}) {
    const now = Date.now();
    this.runs.set(runId, {
      run_id: runId,
      parent_run_id: config.parent_run_id ?? null,
      status: RunStatus.QUEUED,
      config,
      owner: config.owner ?? 'anon',
      trace_root: config.trace_root ?? runId,
      created_at: now,
      updated_at: now,
    });
    this.events.set(runId, []);
    this._seq.set(runId, 0);
    return this.runs.get(runId);
  }

  getRun(runId) {
    return this.runs.get(runId);
  }

  setStatus(runId, status) {
    const r = this.runs.get(runId);
    if (!r) throw new Error('unknown run ' + runId);
    r.status = status;
    r.updated_at = Date.now();
  }

  // seq 按 run 单调递增，由 store 生成
  nextSeq(runId) {
    return (this._seq.get(runId) ?? 0) + 1;
  }
  maxSeq(runId) {
    return this._seq.get(runId) ?? 0;
  }

  // 追加一条事件（Replay / 审计的权威源）
  appendEvent(runId, type, data, meta = {}) {
    const seq = this.nextSeq(runId);
    this._seq.set(runId, seq);
    const event = makeEvent(runId, type, data, {
      ...meta,
      seq,
      id: `${runId}:${seq}`,
    });
    this.events.get(runId).push(event);
    return event;
  }

  getEventsSince(runId, sinceSeq) {
    return this.events.get(runId).filter((e) => e.seq > sinceSeq);
  }

  getAllEvents(runId) {
    return this.events.get(runId);
  }

  // checkpoint：单版本 upsert（只保留最新）。它只是"物化状态缓存"，
  // 恢复权威源是事件日志（见 loop.js resume 的 Option Y 说明）。
  saveCheckpoint(runId, state) {
    this.checkpoints.set(runId, {
      ...state,
      seq_at: this.maxSeq(runId),
      ts: Date.now(),
    });
  }
  getLatestCheckpoint(runId) {
    return this.checkpoints.get(runId);
  }

  // 防篡改审计 Sink：每一行带 prev_hash，WORM（只追加），长保留
  appendAudit(record) {
    const payload = JSON.stringify(record);
    const hash = createHash('sha256')
      .update(this._auditPrev + '|' + payload)
      .digest('hex');
    const entry = { ...record, prev_hash: this._auditPrev, hash, ts: Date.now() };
    this.audit.push(entry);
    this._auditPrev = hash;
    return entry;
  }

  verifyAuditChain() {
    let prev = '0'.repeat(64);
    for (const e of this.audit) {
      if (e.prev_hash !== prev) return false;
      const payload = JSON.stringify({
        ...e,
        prev_hash: undefined,
        hash: undefined,
        ts: undefined,
      });
      const h = createHash('sha256').update(prev + '|' + payload).digest('hex');
      if (h !== e.hash) return false;
      prev = e.hash;
    }
    return true;
  }
}

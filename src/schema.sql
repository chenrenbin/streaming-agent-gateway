-- schema.sql —— 生产持久化 schema（内存实现见 store.js）
-- 审查结论落地的表结构：操作日志(run_events) 与 审计(audit_log) 分开存。
-- 注意：token 等高频事件按"结构事件永久 + token TTL"处理，
--       这里 run_events 存全部事件，生产可对 token 类事件设 TTL/分区。

-- 1) run 元数据 / 状态
CREATE TABLE runs (
  run_id         TEXT PRIMARY KEY,
  parent_run_id  TEXT,
  status         TEXT NOT NULL,            -- queued/running/paused/waiting_client/terminated/finished/failed
  config_json    JSONB,
  owner          TEXT,
  trace_root     TEXT,                     -- 可审计 Trace 根（OTel 兼容）
  created_at     BIGINT,
  updated_at     BIGINT
);

-- 2) 事件日志：追加写，既是 Replay 源也是审计源
--    (run_id, seq) 唯一 → 天然幂等（重放只读）
CREATE TABLE run_events (
  run_id         TEXT    NOT NULL,
  seq            INTEGER NOT NULL,
  id             TEXT,
  type           TEXT    NOT NULL,         -- RunEventType 枚举
  ts             BIGINT,
  span_id        TEXT,                    -- Trace 关联
  parent_span_id TEXT,
  data_json      JSONB,
  PRIMARY KEY (run_id, seq)
);
CREATE INDEX idx_run_events_seq ON run_events (run_id, seq);

-- 3) checkpoint：单版本 upsert（只保留最新）。它是"状态缓存"，
--    恢复权威源是 run_events（Option Y）。如需要时间旅行可改成 (run_id, checkpoint_seq) 多版本。
CREATE TABLE run_checkpoints (
  run_id      TEXT PRIMARY KEY,
  state_json  JSONB,                        -- { messages, iteration, phase }
  seq_at      INTEGER,                      -- 对应 run_events 的 seq
  ts          BIGINT
);

-- 4) 审计 Sink：防篡改、hash 链式、长保留（与操作日志不同保留期/消费者）
CREATE TABLE audit_log (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id       TEXT,
  kind         TEXT,                         -- usage / incident / terminated / paused / failed
  payload_json JSONB,
  prev_hash    TEXT NOT NULL,                 -- 上一行 hash
  hash         TEXT NOT NULL,                 -- 本行 hash = sha256(prev_hash | payload)
  ts           BIGINT
);
CREATE INDEX idx_audit_run ON audit_log (run_id);

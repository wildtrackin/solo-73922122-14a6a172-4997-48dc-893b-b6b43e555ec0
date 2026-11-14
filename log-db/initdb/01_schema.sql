-- ============================================================
-- 日志库：独立于源库，保存逻辑解码得到的完整变更日志
-- 设计要点：
--   1. source_commit 以 (slot_name, commit_lsn) 唯一，
--      同一事务重放（落库后、确认前崩溃恢复）由 ON CONFLICT 幂等吞掉，
--      保证“不漏记、不重复”。
--   2. 一个 source_commit 对应多条有序 result_change，
--      同事务多次修改同一行各占一条记录，按 change_seq 保序。
--   3. 整笔事务在一个本地事务里写入：未提交时查询完全不可见。
-- ============================================================

CREATE TABLE IF NOT EXISTS source_commit (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slot_name       TEXT        NOT NULL,
    xid             BIGINT      NOT NULL,                 -- 源事务号 xid
    commit_lsn      PG_LSN      NOT NULL,                 -- 源提交位置 end_lsn
    begin_lsn       PG_LSN,
    commit_time     TIMESTAMPTZ NOT NULL,                 -- 源端提交时间
    change_count    INTEGER     NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (slot_name, commit_lsn)
);

CREATE TABLE IF NOT EXISTS result_change (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slot_name       TEXT       NOT NULL,
    xid             BIGINT     NOT NULL,
    commit_lsn      PG_LSN     NOT NULL,
    change_seq      INTEGER    NOT NULL,                  -- 事务内顺序，从 1 开始
    action          CHAR(1)    NOT NULL CHECK (action IN ('I','U','D')),
    table_name      TEXT       NOT NULL DEFAULT 'public.results',
    row_lsn         PG_LSN,                               -- 该条行变更的 lsn
    row_identity    JSONB,                                -- 旧行标识/完整旧值
    old_values      JSONB,                                -- UPDATE/DELETE 完整旧值
    new_values      JSONB,                                -- INSERT/UPDATE 新值
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (slot_name, commit_lsn, change_seq)
);

CREATE INDEX IF NOT EXISTS idx_result_change_commit
    ON result_change (slot_name, commit_lsn, change_seq);
CREATE INDEX IF NOT EXISTS idx_result_change_sample_new
    ON result_change (((new_values ->> 'sample_no')::text));
CREATE INDEX IF NOT EXISTS idx_result_change_sample_old
    ON result_change (((old_values ->> 'sample_no')::text));

-- 采集器自身状态（单行，按 slot_name 区分）
CREATE TABLE IF NOT EXISTS slot_state (
    slot_name        TEXT PRIMARY KEY,
    persisted_lsn    PG_LSN,        -- 已持久提交到日志库的最新事务提交位置
    confirmed_lsn    PG_LSN,        -- 已向源槽确认（flush feedback）的位置
    last_xid         BIGINT,
    last_change_at   TIMESTAMPTZ,   -- 最近一次处理消息的时间（心跳刷新）
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 采集器存活心跳，状态 API 据此判断采集进程是否在运行
CREATE TABLE IF NOT EXISTS collector_heartbeat (
    slot_name      TEXT PRIMARY KEY,
    heartbeat_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    pid            INTEGER
);

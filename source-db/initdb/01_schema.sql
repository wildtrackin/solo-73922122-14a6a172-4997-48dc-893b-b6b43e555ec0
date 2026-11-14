-- ============================================================
-- 源库检验结果表
-- 注意：逻辑复制槽只能拿到建槽之后产生的 WAL 变更，
--       本脚本写入的初始数据在采集器建槽之前完成，属于基线数据，
--       不会（也不需要）出现在变更日志中。
-- ============================================================

CREATE TABLE IF NOT EXISTS results (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_no   TEXT        NOT NULL,            -- 样本号
    test_item   TEXT        NOT NULL,            -- 检测项目
    result      TEXT,                            -- 结果
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 更正(UPDATE)和删除(DELETE)都要保留完整旧值：
-- REPLICA IDENTITY FULL 让 wal2json 的 oldkeys 携带全部旧列
ALTER TABLE results REPLICA IDENTITY FULL;

-- 基线虚构检验数据
INSERT INTO results (sample_no, test_item, result) VALUES
    ('S2026-0001', '血常规-白细胞', '6.2'),
    ('S2026-0001', '血常规-红细胞', '4.55'),
    ('S2026-0002', '血糖',         '5.4'),
    ('S2026-0002', '糖化血红蛋白', '5.9%'),
    ('S2026-0003', '谷丙转氨酶',   '21'),
    ('S2026-0003', '肌酐',         '78'),
    ('S2026-0004', '血钾',         '4.1'),
    ('S2026-0004', '血钠',         '140');

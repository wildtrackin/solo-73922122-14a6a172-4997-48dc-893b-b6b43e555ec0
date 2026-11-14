-- 采集停顿期间源库照常提交：停掉 collector 后执行本脚本，
-- 复制槽在源库保留 WAL；重启 collector 后应补采且顺序不变。
BEGIN;
INSERT INTO results (sample_no, test_item, result)
VALUES ('S2026-0006', '总胆红素', '12.4');
UPDATE results SET result = '4.3'
 WHERE sample_no = 'S2026-0004' AND test_item = '血钾';
COMMIT;

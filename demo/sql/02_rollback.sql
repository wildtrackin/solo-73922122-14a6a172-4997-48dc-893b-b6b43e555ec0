-- 回滚事务：ROLLBACK 后不得出现在变更日志中
BEGIN;
UPDATE results SET result = '999.0'
 WHERE sample_no = 'S2026-0001' AND test_item = '血常规-白细胞';
DELETE FROM results
 WHERE sample_no = 'S2026-0003';
ROLLBACK;

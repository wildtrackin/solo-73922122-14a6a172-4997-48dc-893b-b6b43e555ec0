-- 崩溃演示事务：采集器将在该事务落库后、确认源位置前退出
BEGIN;
INSERT INTO results (sample_no, test_item, result)
VALUES ('S2026-0007', '肌钙蛋白I', '0.02');
UPDATE results SET result = '80'
 WHERE sample_no = 'S2026-0003' AND test_item = '肌酐';
COMMIT;

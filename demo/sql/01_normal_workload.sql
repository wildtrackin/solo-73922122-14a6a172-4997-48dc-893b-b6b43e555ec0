-- 正常工作负载：同事务多次修改同一行 + 更正 + 删除（验证旧值保留与保序）
BEGIN;

-- 同一事务内对同一行连续两次更正：两条都必须保留且有序
UPDATE results SET result = '5.8', updated_at = now()
 WHERE sample_no = 'S2026-0002' AND test_item = '血糖';
UPDATE results SET result = '5.6', updated_at = now()
 WHERE sample_no = 'S2026-0002' AND test_item = '血糖';

-- 更正另一项目
UPDATE results SET result = '24', updated_at = now()
 WHERE sample_no = 'S2026-0003' AND test_item = '谷丙转氨酶';

-- 新增样本结果
INSERT INTO results (sample_no, test_item, result)
VALUES ('S2026-0005', 'C反应蛋白', '3.1');

-- 删除一行：需要完整旧值
DELETE FROM results
 WHERE sample_no = 'S2026-0004' AND test_item = '血钠';

COMMIT;

#!/usr/bin/env bash
# ============================================================
# 端到端可控演示：
#   A. 正常事务（同事务同行多次修改、更正、删除完整旧值）
#   B. 回滚不记录
#   C. 采集停顿期间源库照常提交，恢复后续采
#   D. 落库后、确认前崩溃 -> 重启自动续采，不漏不重
# 用法：./demo/demo.sh
# ============================================================
set -uo pipefail
cd "$(dirname "$0")/.."

# 读取 .env
set -a; . ./.env; set +a

FAIL=0
section() { printf '\n==================== %s ====================\n' "$1"; }
info()    { printf '\n--- %s\n' "$1"; }
check() { # check "描述" 期望 实际
  if [ "$2" = "$3" ]; then printf '  [PASS] %s (= %s)\n' "$1" "$3";
  else printf '  [FAIL] %s: 期望 %s, 实际 %s\n' "$1" "$2" "$3"; FAIL=$((FAIL+1)); fi
}

PSQL_SRC=(docker compose exec -T source-db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -At)
PSQL_LOG=(docker compose exec -T log-db   psql -U "$LOG_DB_USER"    -d "$LOG_DB_NAME"    -v ON_ERROR_STOP=1 -At)
API=(docker compose exec -T api curl -fsS)

src_sql()  { "${PSQL_SRC[@]}" "$@"; }
log_sql()  { "${PSQL_LOG[@]}" "$@"; }
api_get()  { "${API[@]}" "http://localhost:8000$1"; }
api_json() { api_get "$1" | python3 -m json.tool; }

section "0. 启动环境（构建镜像 + 初始化两个库 + 启动采集器）"
docker compose up -d --build

info "等待 API 健康检查通过..."
for i in $(seq 1 60); do
  if docker compose exec -T api curl -fsS http://localhost:8000/health >/dev/null 2>&1; then break; fi
  sleep 2
done
api_get /health >/dev/null || { echo "API 未就绪"; exit 1; }

info "等待采集器建槽并进入流式状态..."
for i in $(seq 1 30); do
  active=$(src_sql -c "SELECT active FROM pg_replication_slots WHERE slot_name='$SLOT_NAME';" 2>/dev/null || echo t)
  [ "$active" = "t" ] && break
  sleep 1
done
api_json /status

# ------------------------------------------------------------ A 正常事务
section "A. 正常工作负载（同事务同行两次更正 + 更正 + 插入 + 删除）"
src_sql < demo/sql/01_normal_workload.sql
sleep 3

info "提交列表（按提交位置翻页首页）"
api_json "/commits?limit=10"

COMMITS=$(log_sql -c "SELECT count(*) FROM source_commit WHERE slot_name='$SLOT_NAME';")
check "完整事务数" "1" "$COMMITS"
CHANGES=$(log_sql -c "SELECT count(*) FROM result_change WHERE slot_name='$SLOT_NAME';")
check "行变更条数（2+1+1+1=5，同行两次更正都保留）" "5" "$CHANGES"

info "血糖样本两次更正的有序前后值"
log_sql -c "SELECT change_seq, action, old_values->>'result' AS old_result,
                   new_values->>'result' AS new_result
              FROM result_change
             WHERE slot_name='$SLOT_NAME' AND new_values->>'test_item'='血糖'
             ORDER BY change_seq;"
SEQ1=$(log_sql -c "SELECT change_seq FROM result_change
                    WHERE slot_name='$SLOT_NAME' AND new_values->>'result'='5.8' LIMIT 1;")
SEQ2=$(log_sql -c "SELECT change_seq FROM result_change
                    WHERE slot_name='$SLOT_NAME' AND new_values->>'result'='5.6' LIMIT 1;")
check "第一次更正排在第二次之前" "true" "$([ "${SEQ1:-0}" -lt "${SEQ2:-0}" ] && echo true || echo false)"

info "删除保留完整旧值（old_values 含全部列，new_values 为空）"
log_sql -c "SELECT action, old_values, new_values FROM result_change
             WHERE slot_name='$SLOT_NAME' AND action='D';"
DEL_OLD=$(log_sql -c "SELECT old_values->>'sample_no' FROM result_change
                       WHERE slot_name='$SLOT_NAME' AND action='D'
                         AND old_values->>'test_item'='血钠' LIMIT 1;")
check "删除记录旧值中的样本号" "S2026-0004" "$DEL_OLD"
DEL_OLD_COLS=$(log_sql -c "SELECT (SELECT count(*) FROM jsonb_object_keys(old_values))
                             FROM result_change
                            WHERE slot_name='$SLOT_NAME' AND action='D' LIMIT 1;")
check "删除旧值包含全部 4 列（REPLICA IDENTITY FULL）" "4" "$DEL_OLD_COLS"
DEL_NEW=$(log_sql -c "SELECT count(*) FROM result_change
                       WHERE slot_name='$SLOT_NAME' AND action='D' AND new_values IS NULL;")
check "删除记录无新值" "1" "$DEL_NEW"

# ------------------------------------------------------------ B 回滚
section "B. 回滚事务不记录"
COMMITS_BEFORE=$(log_sql -c "SELECT count(*) FROM source_commit WHERE slot_name='$SLOT_NAME';")
src_sql < demo/sql/02_rollback.sql
sleep 2
# 回滚里把白细胞改成 999.0；该值在任何已提交事务中都不应出现
RB_CHANGES=$(log_sql -c "SELECT count(*) FROM result_change
                          WHERE slot_name='$SLOT_NAME'
                            AND COALESCE(old_values->>'result', new_values->>'result')='999.0';")
check "回滚内容未进入日志" "0" "$RB_CHANGES"
COMMITS_AFTER=$(log_sql -c "SELECT count(*) FROM source_commit WHERE slot_name='$SLOT_NAME';")
check "回滚不产生事务记录" "$COMMITS_BEFORE" "$COMMITS_AFTER"
CUR_WBC=$(src_sql -c "SELECT result FROM results
                       WHERE sample_no='S2026-0001' AND test_item='血常规-白细胞';")
check "源表白细胞结果未变" "6.2" "$CUR_WBC"

# ------------------------------------------------------------ C 停顿续采
section "C. 采集停顿时源库继续提交，恢复后补采"
docker compose stop collector
info "采集器已停止，源库继续写入..."
src_sql < demo/sql/03_paused.sql
sleep 2
DURING_PAUSE=$(log_sql -c "SELECT count(*) FROM result_change
                            WHERE slot_name='$SLOT_NAME'
                              AND new_values->>'sample_no'='S2026-0006';")
check "停顿期间日志库看不到新事务" "0" "$DURING_PAUSE"
RETAINED=$(api_get /status | python3 -c "import sys,json; print(json.load(sys.stdin)['retained_wal_bytes'])")
info "复制槽保留 WAL 字节数（>0 证明未扫描当前表、靠槽续采）: $RETAINED"

info "重启采集器..."
docker compose start collector
sleep 4
AFTER_RESUME=$(log_sql -c "SELECT count(*) FROM result_change
                            WHERE slot_name='$SLOT_NAME'
                              AND new_values->>'sample_no'='S2026-0006';")
check "恢复后补采到停顿期间事务" "1" "$AFTER_RESUME"
TOTAL_COMMITS=$(log_sql -c "SELECT count(*) FROM source_commit WHERE slot_name='$SLOT_NAME';")
check "累计完整事务数（A + C = 2，回滚不计）" "2" "$TOTAL_COMMITS"

# ------------------------------------------------------------ D 崩溃恢复
section "D. 落库后、确认前退出：重启不漏不重"
docker compose stop collector
# 标记一次性崩溃点（命名卷在容器之间共享）
docker compose run --rm --no-deps --entrypoint sh collector -c 'touch /var/lib/lab-cdc/crash-once && ls -l /var/lib/lab-cdc'
CRASH_ENV=$(grep -E '^CRASH_AFTER_PERSIST=' .env | cut -d= -f2)
if [ "$CRASH_ENV" != "1" ]; then
  info "临时以 CRASH_AFTER_PERSIST=1 启动采集器..."
  CRASH_AFTER_PERSIST=1 docker compose up -d --no-deps collector
else
  docker compose up -d --no-deps collector
fi

info "写入将触发崩溃的事务..."
src_sql < demo/sql/04_crash_txn.sql

info "等待采集器崩溃退出（最多 30s）..."
EXITED=no
for i in $(seq 1 30); do
  state=$(docker compose inspect collector 2>/dev/null \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['State']['Status'])" 2>/dev/null || echo running)
  if [ "$state" = "exited" ]; then EXITED=yes; break; fi
  sleep 1
done
check "采集器在确认前退出" "yes" "$EXITED"

info "崩溃点状态：日志已落库，但源槽确认位置未推进"
api_json /status
PERSISTED=$(log_sql -c "SELECT count(*) FROM source_commit WHERE slot_name='$SLOT_NAME';")
check "崩溃事务已在日志库（完整可见）" "3" "$PERSISTED"
CRASH_ROWS=$(log_sql -c "SELECT count(*) FROM result_change
                          WHERE slot_name='$SLOT_NAME'
                            AND new_values->>'sample_no'='S2026-0007';")
check "崩溃事务的行变更已落库" "1" "$CRASH_ROWS"

info "以正常模式重启采集器（自动从持久槽位置续采）..."
docker compose up -d --no-deps collector
sleep 5
api_json /status

FINAL_COMMITS=$(log_sql -c "SELECT count(*) FROM source_commit WHERE slot_name='$SLOT_NAME';")
check "重启后事务不重复（仍为 3 笔）" "3" "$FINAL_COMMITS"
FINAL_ROWS=$(log_sql -c "SELECT count(*) FROM result_change
                          WHERE slot_name='$SLOT_NAME'
                            AND new_values->>'sample_no'='S2026-0007';")
check "崩溃事务行变更不重复（仍为 1 条）" "1" "$FINAL_ROWS"
CONFIRMED=$(log_sql -c "SELECT (confirmed_lsn = persisted_lsn) FROM slot_state
                         WHERE slot_name='$SLOT_NAME';")
check "续采后确认位置追平持久位置" "t" "$CONFIRMED"

# ------------------------------------------------------------ 翻页演示
section "E. API 翻页与样本检索"
info "第一页（limit=1）"
api_json "/commits?limit=1"
CURSOR=$(api_get "/commits?limit=1" | python3 -c "import sys,json; print(json.load(sys.stdin)['next_cursor'])")
info "用 next_cursor 翻下一页: cursor=$CURSOR"
api_json "/commits?limit=1&cursor=$CURSOR"
info "按样本号检索（S2026-0002，看到两次更正的前后值）"
api_json "/changes?sample_no=S2026-0002"

section "结果"
if [ "$FAIL" -eq 0 ]; then
  echo "全部断言通过 ✅  Swagger 文档: http://localhost:${API_PORT}/docs"
else
  echo "有 $FAIL 条断言失败 ❌"
  exit 1
fi

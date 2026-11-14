"""
基于 wal2json 流式逻辑复制的变更采集器。

确认语义（不漏记、不重复的核心）：
  1. 通过复制协议长连接从持久复制槽读取 WAL 变更，整笔事务（B ... C）缓存后
     在日志库的一个本地事务中原子写入 source_commit + result_change。
  2. 日志库 COMMIT 成功（落盘）之后，才向源库发送 standby status 反馈
     （flush/write LSN = 该事务提交 LSN），源槽 restart_lsn 随后推进，
     该事务的 WAL 才可能被回收。
  3. 若在“日志库已提交、反馈尚未发出/生效”之间退出：源槽仍停留在旧位置，
     重启后该事务会被重新解码；日志库以 (slot_name, commit_lsn) 唯一约束
     幂等跳过，因此既不会漏记也不会重复。
  4. 回滚事务根本不会产生逻辑解码消息（wal2json 只输出已提交事务），
     无需额外处理即天然不记录。
  5. 采集停顿时复制槽保留 WAL，源库可继续提交；恢复后按源提交顺序补采，
     日志库只暴露完整事务（写入是原子的），不依赖扫描当前 results 表补历史。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time

import psycopg2

from .config import Settings, get_settings
from .db import log_conn

logger = logging.getLogger("collector")

# wal2json 解码选项：
#   include-transaction=true  -> 输出 BEGIN(B)/COMMIT(C) 信封，携带 xid/lsn/时间
#   include-lsn=true          -> 每条行变更携带 lsn
#   include-timestamp=true    -> 携带事务时间戳
#   include-pk / include-old-keys + REPLICA IDENTITY FULL -> 更正、删除保留完整旧值
#   add-tables                -> 固定只采集 public.results
WAL2JSON_OPTIONS = {
    "include-xids": "true",
    "include-transaction": "true",
    "include-timestamp": "true",
    "include-lsn": "true",
    "include-pk": "true",
    "include-old-keys": "true",
    "add-tables": "public.results",
    "actions": "insert,update,delete",
}


class Collector:
    def __init__(self, settings: Settings):
        self.s = settings
        self._buf: dict[int, dict] = {}   # xid -> 事务缓存
        self._last_msg_at = time.time()
        self._stop = threading.Event()

    # ------------------------------------------------------------------ 建槽
    def ensure_slot(self) -> None:
        """创建持久逻辑复制槽（仅首次）。建槽之后的 results 变更才会被采集。"""
        conn = psycopg2.connect(self.s.source_repl_dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        try:
            cur = conn.cursor()
            try:
                cur.create_replication_slot(self.s.slot_name, output_plugin=self.s.plugin)
                logger.info("created replication slot %s", self.s.slot_name)
            except psycopg2.errors.DuplicateObject:
                logger.info("replication slot %s already exists, resume from persisted position",
                            self.s.slot_name)
                conn.close()
                conn = None
        finally:
            if conn is not None:
                conn.close()

        with log_conn(self.s) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO slot_state (slot_name, started_at)
                VALUES (%s, now())
                ON CONFLICT (slot_name) DO NOTHING
                """,
                (self.s.slot_name,),
            )
            conn.commit()

    # ------------------------------------------------------------------ 心跳
    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                with log_conn(self.s) as conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO collector_heartbeat (slot_name, heartbeat_at, pid)
                        VALUES (%s, now(), %s)
                        ON CONFLICT (slot_name) DO UPDATE
                          SET heartbeat_at = now(), pid = EXCLUDED.pid
                        """,
                        (self.s.slot_name, os.getpid()),
                    )
                    conn.commit()
            except Exception:
                logger.exception("heartbeat update failed")
            self._stop.wait(self.s.heartbeat_interval)

    # ------------------------------------------------------- 单条行变更落库
    @staticmethod
    def _column_map(names, values):
        """把 wal2json 的并行列名/列值数组合成有序 dict（保留源列顺序）。"""
        if not names or not values:
            return None
        return dict(zip(names, values))

    def _persist_transaction(self, payload: dict) -> None:
        xid = payload["xid"]
        commit_lsn = payload["commit_lsn"]
        changes = payload["changes"]
        if not changes:
            logger.info("xid %s committed with no captured results change, skip", xid)
            return

        rows = []
        for seq, ch in enumerate(changes, start=1):
            oldkeys = ch.get("oldkeys") or {}
            # REPLICA IDENTITY FULL：keynames/keyvalues 覆盖整行 -> 完整旧值
            old_values = self._column_map(oldkeys.get("keynames"), oldkeys.get("keyvalues"))
            rows.append({
                "slot": self.s.slot_name,
                "xid": xid,
                "commit_lsn": commit_lsn,
                "seq": seq,
                "action": ch["action"][0].upper(),       # I / U / D
                "table": ch.get("table", "public.results"),
                "row_lsn": ch.get("lsn"),
                # 原始旧键结构一并留存，便于审计
                "identity": json.dumps(oldkeys, ensure_ascii=False) if oldkeys else None,
                "old": json.dumps(old_values, ensure_ascii=False) if old_values is not None else None,
                "new": json.dumps(ch.get("identity"), ensure_ascii=False)
                if ch.get("identity") else None,
            })

        with log_conn(self.s) as conn, conn.cursor() as cur:
            # 崩溃恢复重放：该事务可能已经完整落库，幂等跳过整笔
            cur.execute(
                "SELECT 1 FROM source_commit WHERE slot_name = %s AND commit_lsn = %s",
                (self.s.slot_name, commit_lsn),
            )
            if cur.fetchone() is not None:
                logger.warning("transaction xid=%s lsn=%s already persisted, skipping replay",
                               xid, commit_lsn)
            else:
                cur.execute(
                    """
                    INSERT INTO source_commit
                        (slot_name, xid, commit_lsn, begin_lsn, commit_time, change_count)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        self.s.slot_name, xid, commit_lsn,
                        payload.get("begin_lsn"), payload["commit_time"], len(changes),
                    ),
                )
                cur.executemany(
                    """
                    INSERT INTO result_change
                        (slot_name, xid, commit_lsn, change_seq, action, table_name,
                         row_lsn, row_identity, old_values, new_values)
                    VALUES (%(slot)s, %(xid)s, %(commit_lsn)s, %(seq)s, %(action)s,
                            %(table)s, %(row_lsn)s, %(identity)s, %(old)s, %(new)s)
                    ON CONFLICT (slot_name, commit_lsn, change_seq) DO NOTHING
                    """,
                    rows,
                )

            # 同一本地事务内推进“已持久位置”；与业务写入同生共死
            cur.execute(
                """
                INSERT INTO slot_state (slot_name, persisted_lsn, last_xid, last_change_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (slot_name) DO UPDATE
                  SET persisted_lsn = GREATEST(slot_state.persisted_lsn, EXCLUDED.persisted_lsn),
                      last_xid = EXCLUDED.last_xid,
                      last_change_at = now()
                """,
                (self.s.slot_name, commit_lsn, xid),
            )
            conn.commit()   # 落盘后才进入确认阶段

    def _maybe_crash(self, commit_lsn: str) -> None:
        """演示用：日志库提交之后、源位置确认之前退出。

        触发一次后把标记改名归档，保证重启续采时不会反复崩溃。
        """
        if not self.s.crash_after_persist:
            return
        marker = self.s.crash_marker_path
        if os.path.exists(marker):
            archived = f"{marker}.fired-at-{commit_lsn.replace('/', '_')}"
            os.replace(marker, archived)
            logger.error(
                "DEMO CRASH: log DB committed at %s but source slot NOT confirmed; "
                "marker archived to %s, exiting now (os._exit, no feedback sent)",
                commit_lsn, archived,
            )
            os._exit(86)

    def _mark_confirmed(self, conn, commit_lsn: str) -> None:
        """日志库落盘之后，向源槽确认位置；同时记录 confirmed_lsn。"""
        from .lsn import lsn_to_int

        conn.send_feedback(flush_lsn=lsn_to_int(commit_lsn))
        with log_conn(self.s) as lconn, lconn.cursor() as cur:
            cur.execute(
                """
                UPDATE slot_state
                   SET confirmed_lsn = GREATEST(confirmed_lsn, %s::pg_lsn)
                 WHERE slot_name = %s
                """,
                (commit_lsn, self.s.slot_name),
            )
            lconn.commit()
        logger.info("confirmed source slot up to %s", commit_lsn)

    # --------------------------------------------------------- 复制消息回调
    def on_message(self, msg) -> None:
        self._last_msg_at = time.time()
        payload = json.loads(msg.payload)
        kind = payload.get("action")

        if kind == "B":  # BEGIN：开启事务缓存
            self._buf[payload["xid"]] = {
                "xid": payload["xid"],
                "begin_lsn": payload.get("lsn"),       # wal2json >=2.5 的 BEGIN 携带 lsn
                "changes": [],
            }
            return

        if kind in ("I", "U", "D"):  # 行变更，按到达顺序累积（wal2json 保证事务内有序）
            xid = payload["xid"]
            tx = self._buf.get(xid)
            if tx is None:
                # 理论上不会发生：没有 BEGIN 信封时直接报错，避免静默丢数据
                raise RuntimeError(f"row change without BEGIN envelope: xid={xid}")
            tx["changes"].append(payload)
            return

        if kind == "C":  # COMMIT：原子落库 -> （可选崩溃点）-> 确认源位置
            xid = payload["xid"]
            tx = self._buf.pop(xid, None)
            if tx is None:
                raise RuntimeError(f"COMMIT without BEGIN envelope: xid={xid}")
            # wal2json >=2.5 为 endlsn，旧版本为 lsn
            tx["commit_lsn"] = payload.get("endlsn") or payload.get("lsn")
            tx["commit_time"] = payload["timestamp"]
            if not tx["commit_lsn"]:
                raise RuntimeError(f"COMMIT message without endlsn/lsn: {payload}")

            self._persist_transaction(tx)
            self._maybe_crash(tx["commit_lsn"])
            self._mark_confirmed(msg.cursor.connection, tx["commit_lsn"])
            return

        # 其他消息类型（如 T 截断等，本配置不会出现）忽略并记录
        logger.info("ignore replication message kind=%s", kind)

    # ---------------------------------------------------------------- 主循环
    def run(self) -> None:
        self.ensure_slot()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        while not self._stop.is_set():
            conn = None
            try:
                conn = psycopg2.connect(
                    self.s.source_repl_dsn,
                    connection_factory=psycopg2.extras.LogicalReplicationConnection,
                )
                cur = conn.cursor()
                logger.info("start streaming from slot %s", self.s.slot_name)
                cur.start_replication(
                    slot_name=self.s.slot_name,
                    options=WAL2JSON_OPTIONS,
                    decode=True,
                    keepalive_interval=self.s.keepalive_interval,
                )
                cur.consume_stream(self.on_message)
            except Exception:
                logger.exception("replication stream interrupted, retry in 3s")
                time.sleep(3)
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    Collector(get_settings()).run()


if __name__ == "__main__":
    main()

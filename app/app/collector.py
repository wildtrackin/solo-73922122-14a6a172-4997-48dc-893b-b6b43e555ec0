"""
基于 wal2json（format-version 2）流式逻辑复制的变更采集器。

wal2json 默认 format-version=1 会把整笔事务封装成一条 JSON 消息；
本采集器显式设置 format-version=2，插件按逐条消息输出
B（BEGIN）/ I、U、D（行变更）/ C（COMMIT），与下面的逐条处理模型一致。

确认语义（不漏记、不重复的核心）：
  1. 通过复制协议长连接从持久复制槽读取 WAL 变更，整笔事务（B ... C）缓存后
     在日志库的一个本地事务中原子写入 source_commit + result_change。
  2. 日志库 COMMIT 成功（落盘）之后，才向源库发送 standby status 反馈
     （flush LSN = 该事务提交位置），源槽 restart_lsn 随后推进，
     该事务的 WAL 才可能被回收。
  3. 若在“日志库已提交、反馈尚未发出/生效”之间退出：源槽仍停留在旧位置，
     重启后该事务会被重新解码；日志库以 (slot_name, commit_lsn) 唯一约束
     幂等跳过，因此既不会漏记也不会重复。
  4. 解析或落库失败：回调直接抛错断流，确认反馈只在落库成功后发出；
     重连后从槽的已确认位置重放，失败的事务不会被越过（宁可停流重试，
     也不跳过任何一笔事务）。
  5. 回滚事务根本不会产生逻辑解码消息（wal2json 只输出已提交事务），
     无需额外处理即天然不记录。
  6. 采集停顿时复制槽保留 WAL，源库可继续提交；恢复后按源提交顺序补采，
     日志库只暴露完整事务（写入是原子的），不依赖扫描当前 results 表补历史。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time

import psycopg2
import psycopg2.extras

from .config import Settings, get_settings
from .db import log_conn
from .lsn import int_to_lsn, lsn_to_int

logger = logging.getLogger("collector")

# wal2json 解码选项（选项名与取值已对照 wal2json 2.5/2.6 源码核实，未知选项会让
# START_REPLICATION 直接报错，务必只使用下列已知选项）：
#   format-version=2     -> 逐条输出 B/I/U/D/C 消息（默认 1 会把整笔事务封装成一条 JSON）
#   include-xids         -> 每条消息携带源事务号 xid（默认 false）
#   include-lsn          -> B/C 信封携带 lsn/nextlsn，行变更携带 lsn（默认 false）
#   include-timestamp    -> 携带事务提交时间戳（默认 false）
#   include-transaction  -> 输出 BEGIN(B)/COMMIT(C) 信封（默认 true，显式声明）
#   add-tables           -> 固定只采集 public.results
#   actions              -> 只采集增改删（不含 TRUNCATE）
# 说明：v2 消息中 columns=完整新值（I/U），identity=旧行标识（U/D）；
#       配合源表 REPLICA IDENTITY FULL，identity 即更正、删除的完整旧值。
WAL2JSON_OPTIONS = {
    "format-version": "2",
    "include-xids": "true",
    "include-lsn": "true",
    "include-timestamp": "true",
    "include-transaction": "true",
    "add-tables": "public.results",
    "actions": "insert,update,delete",
}


class Collector:
    def __init__(self, settings: Settings):
        self.s = settings
        self._buf: dict[int, dict] = {}   # xid -> 事务缓存
        self._repl_cur = None             # 当前复制游标（确认反馈挂在游标上）
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
    def _columns_to_map(columns):
        """wal2json v2 列数组 [{"name","type","value"}, ...] -> 有序 dict（保留源列顺序）。"""
        if not columns:
            return None
        return {c["name"]: c["value"] for c in columns}

    def _persist_transaction(self, tx: dict) -> bool:
        """整笔事务在日志库一个本地事务中原子落库。

        返回是否确有变更落库（空事务只确认、不写入，也不触发崩溃演示点）。
        """
        xid = tx["xid"]
        commit_lsn = tx["commit_lsn"]
        changes = tx["changes"]
        if not changes:
            logger.info("xid %s committed with no captured results change, confirm only", xid)
            return False

        rows = []
        for seq, ch in enumerate(changes, start=1):
            # v2：columns = 完整新值（I/U）；identity = 旧行标识（U/D），
            # REPLICA IDENTITY FULL 时即完整旧值
            new_values = self._columns_to_map(ch.get("columns"))
            old_values = self._columns_to_map(ch.get("identity"))
            rows.append({
                "slot": self.s.slot_name,
                "xid": xid,
                "commit_lsn": commit_lsn,
                "seq": seq,
                "action": ch["action"],                    # I / U / D
                "table": f'{ch.get("schema", "public")}.{ch.get("table", "results")}',
                "row_lsn": ch.get("row_lsn"),
                # 原始旧键结构一并留存，便于审计
                "identity": json.dumps(ch["identity"], ensure_ascii=False)
                if ch.get("identity") else None,
                "old": json.dumps(old_values, ensure_ascii=False) if old_values is not None else None,
                "new": json.dumps(new_values, ensure_ascii=False) if new_values is not None else None,
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
                        tx.get("begin_lsn"), tx["commit_time"], len(changes),
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

        logger.info("persisted xid=%s commit_lsn=%s changes=%d", xid, commit_lsn, len(changes))
        return True

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

    def _mark_confirmed(self, commit_lsn: str) -> None:
        """日志库落盘之后，向源槽确认位置；同时记录 confirmed_lsn。

        send_feedback 是复制游标（而非连接）上的方法；force=True 立即发出，
        不等下一个 status 周期。
        """
        cur = self._repl_cur
        if cur is None or cur.closed:
            raise RuntimeError("replication cursor unavailable, cannot confirm source position")
        cur.send_feedback(flush_lsn=lsn_to_int(commit_lsn), force=True)
        with log_conn(self.s) as lconn, lconn.cursor() as lcur:
            lcur.execute(
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
        """逐条处理 wal2json v2 消息；任何解析/落库失败都抛错断流，绝不越过。"""
        self._last_msg_at = time.time()
        try:
            payload = json.loads(msg.payload)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"cannot parse replication message at LSN {msg.data_start}: {exc}"
            ) from exc
        kind = payload.get("action")

        if kind == "B":  # BEGIN：开启事务缓存
            xid = payload.get("xid")
            if xid is None:
                raise RuntimeError(f"BEGIN without xid: {payload!r}")
            self._buf[xid] = {
                "xid": xid,
                "begin_lsn": payload.get("lsn"),       # include-lsn 时 BEGIN 携带 lsn
                "changes": [],
            }
            return

        if kind in ("I", "U", "D"):  # 行变更，按到达顺序累积（wal2json 保证事务内有序）
            xid = payload.get("xid")
            tx = self._buf.get(xid) if xid is not None else None
            if tx is None:
                # 没有 BEGIN 信封时直接报错，避免静默丢数据
                raise RuntimeError(f"row change without BEGIN envelope: {payload!r}")
            # 行变更的 WAL 位置：优先消息内 lsn 字段，兜底取复制协议 data_start
            row_lsn = payload.get("lsn")
            if not row_lsn and msg.data_start:
                row_lsn = int_to_lsn(msg.data_start)
            payload["row_lsn"] = row_lsn
            tx["changes"].append(payload)
            return

        if kind == "C":  # COMMIT：原子落库 -> （可选崩溃点）-> 确认源位置
            xid = payload.get("xid")
            tx = self._buf.pop(xid, None) if xid is not None else None
            if tx is None:
                raise RuntimeError(f"COMMIT without BEGIN envelope: {payload!r}")
            # 提交位置：nextlsn（事务 end_lsn）优先，兼容旧字段 endlsn / lsn，
            # 最后兜底用 COMMIT 消息自身的复制协议位置
            commit_lsn = (payload.get("nextlsn") or payload.get("endlsn")
                          or payload.get("lsn"))
            if not commit_lsn and msg.data_start:
                commit_lsn = int_to_lsn(msg.data_start)
            if not commit_lsn:
                raise RuntimeError(f"COMMIT message without commit LSN: {payload!r}")
            commit_time = payload.get("timestamp")
            if not commit_time:
                raise RuntimeError(f"COMMIT message without timestamp: {payload!r}")
            tx["commit_lsn"] = commit_lsn
            tx["commit_time"] = commit_time

            persisted = self._persist_transaction(tx)
            if persisted:
                # 崩溃演示点只作用于“确有落库”的事务（空事务不存在确认窗口）
                self._maybe_crash(commit_lsn)
            self._mark_confirmed(commit_lsn)
            return

        # 未知消息类型：抛错断流重试，绝不静默越过
        # （当前选项下只会出现 B/I/U/D/C；TRUNCATE 已被 actions 过滤）
        raise RuntimeError(f"unexpected replication message action={kind!r}: {payload!r}")

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
                self._repl_cur = cur
                # 断流重连后事务从 BEGIN 重新解码，丢弃残留缓存避免串档
                self._buf.clear()
                logger.info("start streaming from slot %s", self.s.slot_name)
                cur.start_replication(
                    slot_name=self.s.slot_name,
                    options=WAL2JSON_OPTIONS,
                    decode=True,
                    status_interval=self.s.keepalive_interval,
                )
                cur.consume_stream(self.on_message)
            except Exception:
                logger.exception("replication stream interrupted, retry in 3s")
                time.sleep(3)
            finally:
                self._repl_cur = None
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

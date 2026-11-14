"""
变更日志查询 API（FastAPI）。

只查询独立日志库中已完整提交的事务；采集器按事务原子写入，
因此本接口永远不会读到“半笔事务”。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from psycopg2.errors import InvalidTextRepresentation, UndefinedObject
from pydantic import BaseModel

from .config import get_settings
from .db import log_query, source_query
from .lsn import lsn_valid

app = FastAPI(
    title="检验结果变更日志 API",
    description=(
        "基于 PostgreSQL 16 逻辑复制（wal2json）的 results 表变更日志。\n\n"
        "- 仅含建槽之后的 INSERT/UPDATE/DELETE；\n"
        "- 更正、删除保留完整旧值，同事务同行多次修改逐条保序；\n"
        "- 按源提交位置（commit_lsn）翻页，顺序与源提交一致；\n"
        "- 回滚事务不会出现。"
    ),
    version="1.0.0",
)
settings = get_settings()


# ----------------------------------------------------------------- 响应模型
class Change(BaseModel):
    change_seq: int
    action: str
    table_name: str
    row_lsn: str | None
    old_values: dict[str, Any] | None
    new_values: dict[str, Any] | None


class Commit(BaseModel):
    xid: int
    commit_lsn: str
    begin_lsn: str | None
    commit_time: datetime
    change_count: int
    ingested_at: datetime
    changes: list[Change]


class CommitPage(BaseModel):
    items: list[Commit]
    next_cursor: str | None
    has_more: bool
    limit: int


class FlatChange(BaseModel):
    commit_lsn: str
    xid: int
    commit_time: datetime
    change_seq: int
    action: str
    sample_no: str | None
    test_item: str | None
    old_values: dict[str, Any] | None
    new_values: dict[str, Any] | None


class ChangePage(BaseModel):
    items: list[FlatChange]
    next_cursor: str | None
    has_more: bool
    limit: int


class Status(BaseModel):
    slot_name: str
    collector_alive: bool
    heartbeat_age_seconds: float | None
    persisted_lsn: str | None
    confirmed_lsn: str | None
    last_xid: int | None
    last_change_at: datetime | None
    source_slot_active: bool | None
    source_restart_lsn: str | None
    source_confirmed_flush_lsn: str | None
    source_pg_current_lsn: str | None
    retained_wal_bytes: int | None


@app.get("/", include_in_schema=False)
def root():
    return {"service": "lab-results-cdc", "docs": "/docs"}


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


# ------------------------------------------------------------- 提交位置翻页
@app.get("/commits", response_model=CommitPage, summary="按提交位置翻页查询完整事务")
def list_commits(
    cursor: str | None = Query(None, description="上一页返回的 next_cursor（提交位置 LSN），首页不传"),
    limit: int = Query(20, ge=1, le=200),
    direction: Literal["forward", "backward"] = Query("forward"),
):
    if cursor is not None and not lsn_valid(cursor):
        raise HTTPException(status_code=400, detail=f"invalid cursor LSN: {cursor!r}")

    order = "ASC" if direction == "forward" else "DESC"
    cmp_op = ">" if direction == "forward" else "<"
    params: list[Any] = [settings.slot_name]
    where = "slot_name = %s"
    if cursor:
        where += f" AND commit_lsn {cmp_op} %s::pg_lsn"
        params.append(cursor)

    # 多取一条用于判断 has_more
    sql = f"""
        SELECT xid, commit_lsn::text AS commit_lsn, begin_lsn::text AS begin_lsn,
               commit_time, change_count, ingested_at
          FROM source_commit
         WHERE {where}
      ORDER BY commit_lsn {order}
         LIMIT %s
    """
    try:
        rows = log_query(settings, sql, params + [limit + 1])
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid cursor LSN")

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = page[-1]["commit_lsn"] if has_more and page else None

    if page:
        details = log_query(
            settings,
            """
            SELECT commit_lsn::text AS commit_lsn, change_seq, action, table_name,
                   row_lsn::text AS row_lsn, old_values, new_values
              FROM result_change
             WHERE slot_name = %s AND commit_lsn = ANY(%s::pg_lsn[])
             ORDER BY commit_lsn, change_seq
            """,
            [settings.slot_name, [r["commit_lsn"] for r in page]],
        )
        grouped: dict[str, list] = {}
        for d in details:
            grouped.setdefault(d["commit_lsn"], []).append(d)
        for r in page:
            r["changes"] = grouped.get(r["commit_lsn"], [])

    return CommitPage(items=page, next_cursor=next_cursor, has_more=has_more, limit=limit)


@app.get("/commits/{commit_lsn}", response_model=Commit, summary="按提交位置取单笔事务及全部前后值")
def get_commit(commit_lsn: str):
    if not lsn_valid(commit_lsn):
        raise HTTPException(status_code=400, detail=f"invalid LSN: {commit_lsn!r}")
    try:
        head = log_query(
            settings,
            """
            SELECT xid, commit_lsn::text AS commit_lsn, begin_lsn::text AS begin_lsn,
                   commit_time, change_count, ingested_at
              FROM source_commit
             WHERE slot_name = %s AND commit_lsn = %s::pg_lsn
            """,
            [settings.slot_name, commit_lsn],
        )
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid LSN")
    if not head:
        raise HTTPException(status_code=404, detail="commit not found")

    changes = log_query(
        settings,
        """
        SELECT change_seq, action, table_name, row_lsn::text AS row_lsn,
               old_values, new_values
          FROM result_change
         WHERE slot_name = %s AND commit_lsn = %s::pg_lsn
      ORDER BY change_seq
        """,
        [settings.slot_name, commit_lsn],
    )
    head[0]["changes"] = changes
    return head[0]


# ------------------------------------------------------------- 行变更检索
@app.get("/changes", response_model=ChangePage, summary="按样本号/动作检索行级变更（含前后值）")
def list_changes(
    sample_no: str | None = Query(None, description="样本号，匹配 new/old 两侧，支持更正与删除追溯"),
    action: Literal["I", "U", "D"] | None = Query(None),
    cursor: str | None = Query(None, description="提交位置 LSN 游标"),
    limit: int = Query(50, ge=1, le=500),
):
    if cursor is not None and not lsn_valid(cursor):
        raise HTTPException(status_code=400, detail=f"invalid cursor LSN: {cursor!r}")

    where = ["rc.slot_name = %s"]
    params: list[Any] = [settings.slot_name]
    if sample_no:
        where.append(
            "COALESCE(rc.new_values ->> 'sample_no', rc.old_values ->> 'sample_no') = %s"
        )
        params.append(sample_no)
    if action:
        where.append("rc.action = %s")
        params.append(action)
    if cursor:
        where.append("rc.commit_lsn > %s::pg_lsn")
        params.append(cursor)

    sql = f"""
        SELECT rc.commit_lsn::text AS commit_lsn, sc.xid, sc.commit_time,
               rc.change_seq, rc.action,
               COALESCE(rc.new_values ->> 'sample_no', rc.old_values ->> 'sample_no') AS sample_no,
               COALESCE(rc.new_values ->> 'test_item', rc.old_values ->> 'test_item') AS test_item,
               rc.old_values, rc.new_values
          FROM result_change rc
          JOIN source_commit sc
            ON sc.slot_name = rc.slot_name AND sc.commit_lsn = rc.commit_lsn
         WHERE {' AND '.join(where)}
      ORDER BY rc.commit_lsn ASC, rc.change_seq ASC
         LIMIT %s
    """
    try:
        rows = log_query(settings, sql, params + [limit + 1])
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid cursor LSN")

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = page[-1]["commit_lsn"] if has_more and page else None
    return ChangePage(items=page, next_cursor=next_cursor, has_more=has_more, limit=limit)


# ----------------------------------------------------------------- 采集状态
@app.get("/status", response_model=Status, summary="采集状态、续采位置与源槽 WAL 保留情况")
def get_status():
    state_rows = log_query(
        settings,
        """
        SELECT s.persisted_lsn::text AS persisted_lsn,
               s.confirmed_lsn::text AS confirmed_lsn,
               s.last_xid, s.last_change_at,
               EXTRACT(EPOCH FROM (now() - h.heartbeat_at))::float AS heartbeat_age_seconds
          FROM slot_state s
          LEFT JOIN collector_heartbeat h ON h.slot_name = s.slot_name
         WHERE s.slot_name = %s
        """,
        [settings.slot_name],
    )
    state = state_rows[0] if state_rows else {}
    alive = (state.get("heartbeat_age_seconds") is not None
             and state["heartbeat_age_seconds"] < 30)

    slot = {}
    try:
        slot_rows = source_query(
            settings,
            """
            SELECT active,
                   restart_lsn::text AS restart_lsn,
                   confirmed_flush_lsn::text AS confirmed_flush_lsn
              FROM pg_replication_slots
             WHERE slot_name = %s
            """,
            [settings.slot_name],
        )
        if slot_rows:
            slot = slot_rows[0]
        current = source_query(settings, "SELECT pg_current_wal_lsn()::text AS lsn")
        current_lsn = current[0]["lsn"]
        retained = source_query(
            settings,
            "SELECT pg_wal_lsn_diff(%s, %s)::bigint AS bytes",
            [current_lsn, slot.get("restart_lsn") or current_lsn],
        )[0]["bytes"]
    except UndefinedObject:
        current_lsn, retained = None, None
    except Exception:
        # 源库暂不可达时仍返回日志库侧状态
        current_lsn, retained = None, None

    return Status(
        slot_name=settings.slot_name,
        collector_alive=bool(alive),
        heartbeat_age_seconds=state.get("heartbeat_age_seconds"),
        persisted_lsn=state.get("persisted_lsn"),
        confirmed_lsn=state.get("confirmed_lsn"),
        last_xid=state.get("last_xid"),
        last_change_at=state.get("last_change_at"),
        source_slot_active=slot.get("active"),
        source_restart_lsn=slot.get("restart_lsn"),
        source_confirmed_flush_lsn=slot.get("confirmed_flush_lsn"),
        source_pg_current_lsn=current_lsn,
        retained_wal_bytes=retained,
    )

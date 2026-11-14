"""LSN（PostgreSQL 日志序列号）工具：'0/16B3748' <-> 整数。"""
import re

_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")


def lsn_valid(lsn: str | None) -> bool:
    return bool(lsn and _LSN_RE.match(lsn))


def lsn_to_int(lsn: str) -> int:
    """'0/16B3748' -> 整数，便于按提交位置做游标比较。"""
    high, low = lsn.split("/")
    return (int(high, 16) << 32) | int(low, 16)

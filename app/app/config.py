"""应用配置：全部通过环境变量注入（见 .env / docker-compose.yml）。"""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", case_sensitive=False)

    # 源库（普通连接，用于状态查询）
    source_db_dsn: str
    # 源库复制连接（replication=database），用于 wal2json 流式解码
    source_repl_dsn: str

    # 独立日志库
    log_db_dsn: str

    slot_name: str = "lab_results_slot"
    plugin: str = "wal2json"

    # 复制协议 keepalive 间隔（秒），采集停顿期间持续向源库报告存活与确认位置
    keepalive_interval: int = 10
    heartbeat_interval: int = 5

    # 崩溃演示开关：置 1 后，采集器在“日志库已提交、源位置尚未确认”的窗口退出
    crash_after_persist: bool = False
    crash_marker_path: str = "/var/lib/lab-cdc/crash-once"


@lru_cache
def get_settings() -> Settings:
    return Settings()

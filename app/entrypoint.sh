#!/bin/sh
# 同一镜像两种角色：api（查询服务）/ collector（逻辑复制采集器）
set -e
case "$1" in
  api)
    exec uvicorn app.main:app --host 0.0.0.0 --port 8000
    ;;
  collector)
    exec python -m app.collector
    ;;
  *)
    exec "$@"
    ;;
esac

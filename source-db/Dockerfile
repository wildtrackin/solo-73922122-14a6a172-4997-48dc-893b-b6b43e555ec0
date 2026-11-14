# 源库：PostgreSQL 16 + wal2json 逻辑解码插件
FROM postgres:16

# 安装 PGDG 仓库中的 wal2json（官方 postgres 镜像基于 Debian，自带 PGDG apt 源）
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-wal2json \
    && rm -rf /var/lib/apt/lists/*

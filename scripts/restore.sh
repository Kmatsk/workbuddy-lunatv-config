#!/bin/sh
# 把仓库里的配置写回 Kvrocks（换机/重装后恢复用）
# 用法: sh scripts/restore.sh [--restart]   --restart 会重启 lunatv-core 让配置立即生效
set -e

REPO=${REPO:-/vol1/1000/docker/LunaTV-backup}
KV_HOST=${KV_HOST:-127.0.0.1}
KV_PORT=${KV_PORT:-6666}

python3 "$REPO/scripts/redact.py" restore --repo "$REPO" --host "$KV_HOST" --port "$KV_PORT" "$@"

#!/bin/sh
# 从 Kvrocks 导出 LunaTV 配置 → 脱敏写入仓库（不需要 docker 权限）
# 用法: sh scripts/export.sh
set -e

REPO=${REPO:-/vol1/1000/docker/LunaTV-backup}
LUNA_DIR=${LUNA_DIR:-/vol1/1000/docker/LunaTV}
# 优先读仓库里保存的 .env 副本（部署目录的 .env 属 root，普通用户读不到）
LUNA_ENV=${LUNA_ENV:-$REPO/secrets/.env.local}
[ -r "$LUNA_ENV" ] || LUNA_ENV="$LUNA_DIR/.env"

KV_HOST=${KV_HOST:-127.0.0.1}
KV_PORT=${KV_PORT:-6666}

python3 "$REPO/scripts/redact.py" export --repo "$REPO" --env "$LUNA_ENV" \
  --host "$KV_HOST" --port "$KV_PORT"

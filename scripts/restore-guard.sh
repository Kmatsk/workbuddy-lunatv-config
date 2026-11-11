#!/bin/sh
# 从仓库还原 tvbox-guard：拷代码/配置 → 生成 .env → 重建容器
# 需要 root（部署目录属 root，且要调 docker）
# 用法: sudo sh scripts/restore-guard.sh
set -e

REPO=${REPO:-/vol1/1000/docker/LunaTV-backup}
GUARD_DIR=${GUARD_DIR:-/vol1/1000/docker/tvbox-guard}
SRC=$REPO/tvbox-guard
SECRETS=$REPO/secrets/guard.env
[ -r "$SECRETS" ] || SECRETS=$SRC/.env.example

mkdir -p "$GUARD_DIR/logo" "$GUARD_DIR/backups" "$GUARD_DIR/cache"
for f in guard.py admin.html docker-compose.yml config.json; do
  cp -f "$SRC/$f" "$GUARD_DIR/$f"
done
cp -f "$SECRETS" "$GUARD_DIR/.env"
chmod 600 "$GUARD_DIR/.env"
chmod 755 "$GUARD_DIR/guard.py"

cd "$GUARD_DIR"
docker compose up -d --force-recreate
docker ps --filter name=tvbox-guard --format '{{.Names}} {{.Status}}'

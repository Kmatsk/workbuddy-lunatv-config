#!/bin/sh
# 把 tvbox-guard 部署目录里的代码/配置同步进仓库
# config.json 由后台页面维护，属于运行状态，所以每次备份都重新拷一份
# .env（密码 / TMDB Token）只在可读时同步到 secrets/guard.env，不入库
# 用法: sh scripts/export-guard.sh
set -e

REPO=${REPO:-/vol1/1000/docker/LunaTV-backup}
GUARD_DIR=${GUARD_DIR:-/vol1/1000/docker/tvbox-guard}
DST=$REPO/tvbox-guard

mkdir -p "$DST"

if [ ! -r "$GUARD_DIR/guard.py" ]; then
  echo "[guard] 跳过：$GUARD_DIR 不可读（用 sudo 跑，或调整目录权限）" >&2
  exit 0
fi

for f in guard.py admin.html docker-compose.yml config.json; do
  [ -r "$GUARD_DIR/$f" ] || continue
  cp -f "$GUARD_DIR/$f" "$DST/$f"
done

if [ -r "$GUARD_DIR/.env" ]; then
  mkdir -p "$REPO/secrets"
  cp -f "$GUARD_DIR/.env" "$REPO/secrets/guard.env"
  chmod 600 "$REPO/secrets/guard.env"
fi

chmod 755 "$DST/guard.py"
echo "[guard] 已导出 → $DST"

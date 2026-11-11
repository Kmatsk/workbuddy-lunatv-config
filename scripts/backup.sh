#!/bin/sh
# 导出配置 → 有变化才提交 → 推送 GitHub
# 用法: sh scripts/backup.sh [提交说明]
set -e

REPO=${REPO:-/vol1/1000/docker/LunaTV-backup}
cd "$REPO"

sh "$REPO/scripts/export.sh"
sh "$REPO/scripts/export-guard.sh"

if [ -z "$(git status --porcelain)" ]; then
  echo "[backup] 配置无变化，跳过提交"
  exit 0
fi

git add -A
git -c user.name="${GIT_AUTHOR_NAME:-LunaTV Backup}" \
    -c user.email="${GIT_AUTHOR_EMAIL:-lunatv@local}" \
    commit -q -m "${1:-sync: LunaTV 配置自动备份 $(date '+%Y-%m-%d %H:%M')}"

echo "[backup] 已提交: $(git log -1 --pretty=%h) $(git log -1 --pretty=%s)"

if git remote get-url origin >/dev/null 2>&1; then
  if git push -q origin HEAD 2>/dev/null; then
    echo "[backup] 已推送 origin"
  else
    echo "[backup] 推送失败（网络或凭据问题），提交已留在本地" >&2
    exit 1
  fi
else
  echo "[backup] 未配置 origin，仅本地提交"
fi

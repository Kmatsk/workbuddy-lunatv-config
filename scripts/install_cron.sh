#!/bin/sh
# 安装每日自动备份的定时任务（默认每天 04:20）
# 用法: sh scripts/install_cron.sh [小时:分钟]   例: sh scripts/install_cron.sh 03:30
#       LUNA_USER=Kmatsk sh scripts/install_cron.sh   # 指定以哪个用户运行
set -e

REPO=${REPO:-/vol1/1000/docker/LunaTV-backup}
RUNAS=${LUNA_USER:-Kmatsk}
WHEN=${1:-04:20}
HH=${WHEN%%:*}
MM=${WHEN##*:}
CMD="REPO=$REPO sh $REPO/scripts/backup.sh >> $REPO/cron.log 2>&1"

chmod +x "$REPO"/scripts/*.sh 2>/dev/null || true

# root 身份优先写 /etc/cron.d（可指定运行用户，NAS 的计划任务也读这里）
if [ "$(id -u)" = "0" ] && [ -d /etc/cron.d ]; then
  {
    echo "# LunaTV 配置自动备份（由 scripts/install_cron.sh 生成）"
    echo "SHELL=/bin/sh"
    echo "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    echo "$MM $HH * * * $RUNAS $CMD"
  } > /etc/cron.d/lunatv-backup
  chmod 644 /etc/cron.d/lunatv-backup
  echo "[cron] 已写入 /etc/cron.d/lunatv-backup：每天 ${HH}:${MM}，以 ${RUNAS} 运行"
  cat /etc/cron.d/lunatv-backup
elif command -v crontab >/dev/null 2>&1; then
  ( crontab -l 2>/dev/null | grep -v "LunaTV-backup/scripts/backup.sh" ; echo "$MM $HH * * * $CMD" ) | crontab -
  echo "[cron] 已写入 $(id -un) 的 crontab：每天 ${HH}:${MM}"
  crontab -l | grep backup.sh
else
  echo "[cron] 当前系统没有 crontab，请在 NAS 计划任务里添加：" >&2
  echo "       每天 ${HH}:${MM} 执行  $CMD" >&2
  exit 1
fi

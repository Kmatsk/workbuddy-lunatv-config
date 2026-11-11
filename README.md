# LunaTV 配置备份

LunaTV（DaoTV 增强版）在 NAS 上的部署文件与运行配置，用于换机、重装或回滚时快速还原。

- 部署位置：`/vol1/1000/docker/LunaTV`（NAS 192.168.3.100）
- 访问地址：http://192.168.3.100:3030
- 存储：Kvrocks（Redis 协议兼容），播放配置全部放在 `admin:config` 这个键里
- 备份仓库：`/vol1/1000/docker/LunaTV-backup`（就是本目录）
- Kvrocks 额外绑了 `127.0.0.1:6666`，只给备份脚本直连用，不对外暴露；脚本因此不需要 docker 权限，普通用户与 cron 都能跑

## 备份什么

| 内容 | 来源 | 是否入库 |
|---|---|---|
| `docker-compose.yml` | 部署目录 | ✅ |
| `kvrocks.conf` | 部署目录（已调 block_cache=32MB，适配 3.7G 内存的 NAS） | ✅ |
| `.env.example` | `.env` 模板，密码为占位符 | ✅ |
| `config/admin-config.json` | Kvrocks `admin:config`，敏感字段已脱敏 | ✅ |
| `config/user-history.json` | `u:<用户>:sh` 搜索历史 | ✅ |
| `meta.json` | 导出时间、源数量、脱敏字段清单、配置哈希 | ✅ |
| `secrets/secrets.json` | 密码 / TMDB Key 等明文 | ❌ 只留 NAS，已 gitignore |
| `.env` | 真实环境变量 | ❌ 已 gitignore |
| `video-cache/`、`cache:*`、`calendar:*` | 视频与接口缓存 | ❌ 可随时重建 |

`admin:config` 里已包含：片源列表（key/name/api/weight/disabled）、站点名称与公告、搜索与 TMDB 设置、网盘搜索、短剧源、豆瓣/B站/YouTube 开关、首页模块开关等。

## 目录

```
├── docker-compose.yml
├── kvrocks.conf
├── .env.example
├── config/
│   ├── admin-config.json     # 脱敏后的完整配置
│   └── user-history.json
├── secrets/secrets.json      # 不入库
├── meta.json
└── scripts/
    ├── export.sh             # 从 Kvrocks 导出 → 脱敏写盘
    ├── backup.sh             # export + 有变化才 commit + push
    ├── restore.sh            # 仓库 → Kvrocks 写回
    ├── redact.py             # 脱敏 / 合并逻辑
    └── install_cron.sh       # 注册每日定时任务
```

## 日常使用

```sh
# 手动备份一次（会顺手推送到 GitHub）
sh /vol1/1000/docker/LunaTV-backup/scripts/backup.sh

# 只导出看变化，不提交
sh /vol1/1000/docker/LunaTV-backup/scripts/export.sh
git -C /vol1/1000/docker/LunaTV-backup diff config/admin-config.json
```

配置没变化时不会产生空提交。

## 换机 / 重装后还原

```sh
# 1. 拉仓库（含敏感值就带上 secrets/ 目录，或稍后手工补）
git clone <本仓库地址> /vol1/1000/docker/LunaTV-backup

# 2. 部署目录只放 compose 与配置
mkdir -p /vol1/1000/docker/LunaTV && cd /vol1/1000/docker/LunaTV
cp /vol1/1000/docker/LunaTV-backup/docker-compose.yml .
cp /vol1/1000/docker/LunaTV-backup/kvrocks.conf .

# 3. 起服务（首次会自己建 kvrocks 空库）
#    .env 若不存在，restore 会用 .env.example + secrets 自动生成
docker compose up -d

# 4. 写回配置（--restart 让 LunaTV 立即读新配置）
sh /vol1/1000/docker/LunaTV-backup/scripts/restore.sh --restart
```

## 敏感信息怎么处理

- 入库的 `admin-config.json` 里，`TMDBApiKey`、`apiKey`、`token`、`password`、`tavilyApiKeys`、`NetDiskConfig.username` 等一律替换成 `"<SECRET>"`
- 明文集中在 `secrets/secrets.json`（权限 600，`.gitignore` 排除），换机时单独用 U 盘/SFTP 拷过去
- `restore.sh` 会自动把明文按路径填回占位符；缺 secrets 时敏感字段置空并告警，其它配置照常还原
- 想改密码：改 NAS 上的 `.env`，重建容器，再跑一次 `backup.sh`（secrets 会同步更新）

## 定时任务

```sh
sh /vol1/1000/docker/LunaTV-backup/scripts/install_cron.sh 04:20
```

已安装在 `/etc/cron.d/lunatv-backup`，每天 04:20 以 `Kmatsk` 身份执行 `backup.sh`，日志写到 `cron.log`。改用别的时间就再跑一次脚本传参。

## 已实测

- 导出：5 个源、站点名、全部开关正确落盘；`TMDBApiKey` 在入库文件里是 `"<SECRET>"`
- 幂等：连跑两次 `export.sh`，`git status` 无改动，`backup.sh` 输出「配置无变化，跳过提交」
- 还原：`restore.sh` 写回后 `admin:config` 的 JSON 与原值 **完全一致**，TMDB Key 正确回填
- 权限：以 `Kmatsk` 用户跑 `backup.sh` 全程无需 sudo（不依赖 docker 命令）


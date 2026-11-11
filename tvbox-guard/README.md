# tvbox-guard

TVBox / 影视 App 的中间网关，跑在 NAS 上，端口 `3032`。App 里配的接口地址指向它，它再转发到 LunaTV（`3030`）的 CMS 接口，并在中间做一层加工：

| 能力 | 说明 |
|---|---|
| 片源同步 | 定时从 LunaTV 拉片源列表，App 只认网关这一个地址，换源/改源不用改 App |
| 聚合搜索 | 主源 + 其余启用源并行搜索，`额外聚合源数量` 控制最多并入几个**真正有结果**的源 |
| 内容过滤 | 分类黑名单 + 标题黑名单，开关关闭即只按原样透传；后台可实时改词，保存即生效 |
| 封面缓存 | 海报图本地落盘并限容，关掉开关就 302 回源站原图 |
| TMDB 补图 | 无封面时按标题/年份查 TMDB，命中有缓存；未收录也记账，避免重复打接口 |
| 统计 | TMDB 命中 / 未收录 / 分类拦截 / 标题拦截，按月落盘，只留最近 1 个月 |
| 在线设备 | 按来源 IP 统计实时在线（超过空闲阈值不算在线）、今日接入、本月接入，历史保留 31 天 / 6 个月 |

## 文件

```
tvbox-guard/
├── guard.py            # 全部逻辑（单文件，无第三方依赖，镜像用 python:3.12-alpine）
├── admin.html          # 后台页面（/admin）
├── docker-compose.yml  # 部署定义，敏感值走 .env
├── config.json         # 过滤词与占位分类规则，后台改了会被回写
├── .env.example        # 环境变量模板
└── README.md
```

## 环境变量

| 变量 | 用途 |
|---|---|
| `LUNA_USER` / `LUNA_PASS` | LunaTV 管理员账号，网关要登录后台读片源列表 |
| `TMDB_TOKEN` | TMDB 只读 Token，用于补封面 |
| `LUNA_BASE` / `SELF_BASE` | 上游与自身地址，默认 `http://192.168.3.100:3030` / `:3032` |
| `GUARD_PORT` | 监听端口，默认 3032；compose 里以 `3032:3032` 映射到宿主机（桥接网络，Docker 面板可见该端口） |

真实值放同目录 `.env`（权限 600，`.gitignore` 排除），仓库里只留占位符。

## 部署 / 还原

```sh
mkdir -p /vol1/1000/docker/tvbox-guard
cd /vol1/1000/docker/tvbox-guard
git clone <仓库地址> /tmp/repo && cp -r /tmp/repo/tvbox-guard/* .
cp .env.example .env && vi .env      # 填账号与 Token
docker compose up -d
```

或直接在备份仓库里用 `sudo sh scripts/restore-guard.sh`，会顺带把 `secrets/guard.env` 里的明文填回去。

## 运行时目录

`logo/`（自定义台标）、`backups/`（配置自动备份）、`cache/`（封面与接口缓存）都在宿主机上，属运行数据，**不入库**。

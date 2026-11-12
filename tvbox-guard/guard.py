#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 过滤网关 (tvbox-guard)
- /            管理页面（状态 / 词库编辑 / 拦截测试）
- /api/tvbox   过滤后的 TVBox 订阅配置（分类剔除 + CMS 走代理）
- /cms/<key>   代理 CMS 源请求，过滤分类与视频列表
- /img?u=      封面图缓存代理（本地缓存 + 后台预热，机顶盒秒开）
- /api/status /api/config /api/test /health

配置文件: /app/config.json （页面保存后立即热加载，无需重启）
"""
import json, os, re, time, base64, gzip, copy, threading, hashlib, urllib.request, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor, as_completed

LUNA = os.environ.get("LUNA_BASE", "http://192.168.3.100:3030")
SELF = os.environ.get("SELF_BASE", "http://192.168.3.100:3032")
LISTEN_PORT = int(os.environ.get("GUARD_PORT", "3032"))
CONFIG_PATH = os.environ.get("GUARD_CONFIG", "/app/config.json")
START_TS = time.time()
try:
    SELF_HOST = (urllib.parse.urlsplit(SELF).hostname or "").strip()
except Exception:
    SELF_HOST = ""
try:
    LUNA_HOST = (urllib.parse.urlsplit(LUNA).hostname or "").strip()
except Exception:
    LUNA_HOST = ""
# 网关本身与同机 LunaTV 服务的请求都属于「服务流量」，不计入客户端设备
DEFAULT_IGNORE_DEVS = ["192.168.3.100", SELF_HOST, LUNA_HOST]
# 172.17.0.1：容器走桥接网络时，宿主机自己发起的请求会以 docker0 网关地址出现
LOCAL_HOSTS = {"127.0.0.1", "::1", "localhost", "172.17.0.1", SELF_HOST, LUNA_HOST}

DEFAULT_CATS = ["伦理","倫理","里番","成人","福利","情色","色情","写真","激情","三级",
                "av","女优","男优","18禁","限制级","r18","人妖","巨乳","自拍","偷拍",
                "凸凸","啪啪","做爱","援交","夜色","花边"]
DEFAULT_TITLES = ["伦理","倫理","里番","情色","色情","福利","成人","激情","三级","女优",
                  "男优","18禁","限制级","人妖","巨乳","凸凸","啪啪","做爱","援交"]
DEFAULT_PLACEHOLDER = r"^(分类|子类)\s*\d*$"

_f = {"cats": list(DEFAULT_CATS), "titles": list(DEFAULT_TITLES),
      "placeholder": re.compile(DEFAULT_PLACEHOLDER), "mtime": 0.0,
      "disabled": set(), "order": [],
      "sync_min": 5, "backup": True, "keep": 5, "tvcache_min": 5,
      "src_limit": 0, "src_active": "", "tmdb_on": True,
      "fo_on": True, "fo_thresh": 3, "fo_list": [],
      "cats_on": True, "titles_on": True, "filter_on": True,
      "search_all": True, "search_limit": 0,
      "agg_on": True, "agg_browse": 8, "agg_search": 0, "agg_tvbox": False, "agg_tvsub": False,
      "agg_cat_off": set(), "agg_cat_order": [],
      "agg_home": "", "agg_page": 20, "agg_top_pg": 1, "agg_sub_pg": 1, "agg_sub_src": 12,
      "agg_fill": True, "agg_fill_max": 20,
      # 采集调度：定时/间隔预采集，采完即停；客户端触发采集可单独关
      "agg_auto_on": True, "agg_auto_sec": 3600, "agg_auto_at": "",
      "agg_auto_scope": "hot", "agg_auto_n": 12, "agg_auto_max": 40,
      "agg_auto_budget": 300, "agg_lazy": True,
      "sync_on": True, "img_on": True, "idle_min": 5, "ignore_dev": set(DEFAULT_IGNORE_DEVS),
      # 缓存容量与保留期：封面/TMDB/总容量都可调，配合每日自动清理防止占满磁盘
      "img_max_mb": 256, "img_keep_days": 30, "tmdb_keep_days": 60,
      "cache_max_mb": 512, "cache_auto_clean": True,
      "sh_on": True, "sh_sec": 600, "sh_fail": 3, "sh_ok": 2, "sh_to": 10,
      "asort_on": False, "asort_mode": "avg", "asort_min": 2,
      "asort_scope": "all", "asort_fail_last": True}
_stats = {"requests": 0, "blocked": 0, "bcat": 0, "btitle": 0, "thit": 0, "tmiss": 0}
STAT_FIELDS = ("cat", "title", "hit", "miss")
STATS_KEEP_MONTHS = max(1, int(os.environ.get("STATS_KEEP_MONTHS", "1")))   # 月度明细只留最近 1 个月，到期自动覆盖
def stats_path():
    return os.path.join(CACHE_DIR, "stats.json")   # CACHE_DIR 是 NAS 上的挂载目录
_stats_p = {"since": 0.0, "months": {}, "dirty": False, "ts": 0.0, "lock": threading.Lock()}
_seen = {}   # 缓存条目 -> [分类已计, 标题已计, 时间]（重启不虚增）


def stats_month_key(t=None):
    return time.strftime("%Y-%m", time.localtime(t if t is not None else time.time()))


def month_rec(mk=None):
    """取本月记录（四个字段齐全），不存在则初始化"""
    mk = mk or stats_month_key()
    with _stats_p["lock"]:
        m = _stats_p["months"].setdefault(mk, {})
        for k in STAT_FIELDS:
            m[k] = int(m.get(k) or 0)
        return m


def stats_load():
    """读取磁盘上的长期统计（重启不丢）"""
    try:
        with open(stats_path(), encoding="utf-8") as f:
            d = json.load(f)
    except FileNotFoundError:
        _stats_p["since"] = time.time()
        return
    except Exception as e:
        print("[stats] load fail:", e, flush=True)
        _stats_p["since"] = time.time()
        return
    _stats["bcat"] = max(0, int(d.get("cat") or 0))
    _stats["btitle"] = max(0, int(d.get("title") or 0))
    _stats["thit"] = max(0, int(d.get("tmdbHit") or 0))
    _stats["tmiss"] = max(0, int(d.get("tmdbMiss") or 0))
    _stats["blocked"] = _stats["bcat"] + _stats["btitle"]
    _stats_p["since"] = float(d.get("since") or time.time())
    months = {}
    for k, v in (d.get("months") or {}).items():
        if isinstance(v, dict):
            months[k] = {f: max(0, int(v.get(f) or 0)) for f in STAT_FIELDS}
    _stats_p["months"] = months
    for k, v in (d.get("seen") or {}).items():
        if isinstance(v, list) and len(v) >= 2:
            try:
                _seen[k] = [int(v[0]), int(v[1]), float(v[2]) if len(v) > 2 else time.time()]
            except Exception:
                pass
    print("[stats] loaded cat=%d title=%d tmdb=%d/%d since=%s" %
          (_stats["bcat"], _stats["btitle"], _stats["thit"], _stats["tmiss"],
           time.strftime("%Y-%m-%d %H:%M", time.localtime(_stats_p["since"]))), flush=True)


def stats_save(force=False):
    """落盘：累计值 + 本月明细（更早的月份自动被覆盖）"""
    with _stats_p["lock"]:
        if not _stats_p["dirty"] and not force:
            return
        if not force and time.time() - float(_stats_p["ts"] or 0) < 20:
            return
        _stats_p["dirty"] = False
        _stats_p["ts"] = time.time()
        mk = stats_month_key()
        months = dict(_stats_p["months"])
        months.setdefault(mk, {f: 0 for f in STAT_FIELDS})
        keep = sorted(months)[-STATS_KEEP_MONTHS:]
        months = {k: {f: int((months[k] or {}).get(f) or 0) for f in STAT_FIELDS} for k in keep}
        _stats_p["months"] = months
        cutoff = time.time() - 7 * 86400
        seen = {k: v for k, v in _seen.items() if float(v[2] or 0) >= cutoff}
        if len(seen) > 400:
            seen = dict(sorted(seen.items(), key=lambda kv: -float(kv[1][2] or 0))[:400])
        data = {"since": _stats_p["since"], "cat": _stats["bcat"], "title": _stats["btitle"],
                "tmdbHit": _stats["thit"], "tmdbMiss": _stats["tmiss"],
                "updated": time.time(), "month": mk, "months": months, "seen": seen}
    try:
        tmp = stats_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=1))
        os.replace(tmp, stats_path())
    except Exception as e:
        print("[stats] save fail:", e, flush=True)


def bump(kind, n=1):
    """kind: cat（分类拦截）/ title（标题拦截）/ hit（TMDB 命中）/ miss（TMDB 未收录）"""
    if n <= 0 or kind not in STAT_FIELDS:
        return
    with _stats_p["lock"]:
        if kind == "cat":
            _stats["bcat"] += n
            _stats["blocked"] += n
        elif kind == "title":
            _stats["btitle"] += n
            _stats["blocked"] += n
        elif kind == "hit":
            _stats["thit"] += n
        else:
            _stats["tmiss"] += n
        m = _stats_p["months"].setdefault(stats_month_key(), {})
        m[kind] = int(m.get(kind) or 0) + n
        _stats_p["dirty"] = True


def bump_key(key, kind, cur):
    """同一条缓存记录只累计一次：重启后重新预热不会把数字刷高"""
    try:
        cur = int(cur or 0)
    except Exception:
        return
    if cur <= 0 or not key:
        return
    idx = 0 if kind == "cat" else 1
    with _stats_p["lock"]:
        prev = _seen.get(key) or [0, 0, 0.0]
        if cur <= int(prev[idx] or 0):
            prev[2] = time.time()
            _seen[key] = prev
            return
        delta = cur - int(prev[idx] or 0)
        prev[idx] = cur
        prev[2] = time.time()
        _seen[key] = prev
    _stats_p["dirty"] = True
    bump(kind, delta)


def stats_reset():
    with _stats_p["lock"]:
        _stats["bcat"] = _stats["btitle"] = _stats["blocked"] = 0
        _stats["thit"] = _stats["tmiss"] = 0
        _stats["requests"] = 0
        _stats_p["since"] = time.time()
        _stats_p["months"] = {stats_month_key(): {f: 0 for f in STAT_FIELDS}}
        _seen.clear()
        _stats_p["dirty"] = True
    stats_save(True)
    print("[stats] reset", flush=True)
# ---------------- 搜索统计 ----------------
# 每个关键词记录「总次数 / 当日次数 / 命中屏蔽次数 / 最近搜索时间」
# 落盘 cache/searchstats.json，重启不清零，只保留次数最多的 500 个关键词
SEARCH_KEEP = 500
_search_stats = {}          # word -> {"n","dayn","day","blocked","first","last"}
_ss_lock = threading.Lock()
_ss_meta = {"dirty": False, "ts": 0.0}


def searchstats_path():
    return os.path.join(CACHE_DIR, "searchstats.json")


def searchstats_load():
    global _search_stats
    try:
        with open(searchstats_path(), "r", encoding="utf-8") as fp:
            d = json.load(fp)
        if isinstance(d, dict):
            _search_stats = {str(k)[:50]: v for k, v in d.items() if isinstance(v, dict)}
    except Exception:
        pass
    print("[searchstats] loaded %d words" % len(_search_stats), flush=True)


def searchstats_save(force=False):
    with _ss_lock:
        if not _ss_meta["dirty"] and not force:
            return
        if not force and time.time() - float(_ss_meta["ts"] or 0) < 15:
            return
        _ss_meta["dirty"] = False
        _ss_meta["ts"] = time.time()
        data = dict(sorted(_search_stats.items(),
                           key=lambda kv: -int((kv[1] or {}).get("n") or 0))[:SEARCH_KEEP])
    try:
        tmp = searchstats_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False))
        os.replace(tmp, searchstats_path())
    except Exception as e:
        print("[searchstats] save fail:", e, flush=True)


def search_bump(word, blocked=False):
    w = (word or "").strip()[:50]
    if not w:
        return
    today = day_key(time.time())
    with _ss_lock:
        r = _search_stats.get(w)
        if r is None:
            r = _search_stats[w] = {"n": 0, "dayn": 0, "day": today, "blocked": 0,
                                    "first": time.time(), "last": 0.0}
        if r.get("day") != today:      # 跨天：当日计数归零
            r["day"], r["dayn"] = today, 0
        r["n"] = int(r.get("n") or 0) + 1
        r["dayn"] = int(r.get("dayn") or 0) + 1
        r["last"] = time.time()
        if blocked:
            r["blocked"] = int(r.get("blocked") or 0) + 1
        _ss_meta["dirty"] = True
        if len(_search_stats) > SEARCH_KEEP + 100:   # 只留次数最多的关键词
            for k in sorted(_search_stats,
                            key=lambda k: int((_search_stats[k] or {}).get("n") or 0)
                            )[:len(_search_stats) - SEARCH_KEEP]:
                _search_stats.pop(k, None)


def searchstats_snapshot():
    with _ss_lock:
        today = day_key(time.time())
        items = []
        for w, r in _search_stats.items():
            d = dict(r)
            d["word"] = w
            d["today"] = int(d.get("dayn") or 0) if d.get("day") == today else 0
            items.append(d)
        items.sort(key=lambda x: (-int(x.get("n") or 0), x["word"]))
        since = min([float(x.get("first") or 0) for x in items] or [0])
        return {"ok": True, "day": today,
                "words": len(items),
                "total": sum(int(x.get("n") or 0) for x in items),
                "today": sum(int(x.get("today") or 0) for x in items),
                "since": since, "items": items[:200]}


def searchstats_reset():
    with _ss_lock:
        _search_stats.clear()
        _ss_meta["dirty"] = True
    searchstats_save(True)
    print("[searchstats] reset", flush=True)


# ---------------- 在线设备统计 ----------------
# 每个设备记录「当日 / 本月 / 累计」请求次数与最后活跃时间；超过阈值没有数据流就不算在线
# 明细按「日」「月」两级汇总落盘 cache/devices.json：日保留 31 天、月保留 6 个月，到期自动覆盖
DEV_KEEP_DAYS = 31
DEV_KEEP_MONTHS = 6
_clients = {}
_clients_lock = threading.Lock()
_clients_meta = {"dirty": False, "ts": 0.0}
_hist = {"days": {}, "months": {}}


def dev_path():
    return os.path.join(CACHE_DIR, "devices.json")


def day_key(t=None):
    return time.strftime("%Y-%m-%d", time.localtime(t if t is not None else time.time()))


def month_key(t=None):
    return time.strftime("%Y-%m", time.localtime(t if t is not None else time.time()))


def _is_docker_gw(ip):
    """172.16/12 里的 x.x.x.1：docker 网桥网关，宿主机/容器自身流量的来源，不是客户端"""
    try:
        a, b, c, d = [int(x) for x in str(ip).split(".")]
    except Exception:
        return False
    return a == 172 and 16 <= b <= 31 and d == 1


def ignore_hosts():
    """网关自身 + NAS/LunaTV 服务的请求不算客户端设备"""
    return set(LOCAL_HOSTS) | {str(x) for x in (_f.get("ignore_dev") or set()) if str(x).strip()}


# ---------------- 设备 MAC ----------------
# 容器在 bridge 网络里，自己的 /proc/net/arp 只有网桥网关；
# compose 把宿主机 /proc/1/net/arp 挂到 /host/arp（只读），从这里读宿主机 ARP 表拿真实客户端 MAC。
ARP_FILES = [os.environ.get("GUARD_ARP", "/host/arp"), "/proc/net/arp"]
_mac = {"ts": 0.0, "map": {}}
_mac_lock = threading.Lock()


def arp_refresh(min_gap=15):
    """读取宿主机 ARP 表（15 秒内不重复读）；同一 IP 只取第一条有效 MAC"""
    now = time.time()
    with _mac_lock:
        if now - float(_mac["ts"] or 0) < min_gap:
            return _mac["map"]
        m = {}
        for fp in ARP_FILES:
            try:
                with open(fp, "r", encoding="utf-8", errors="replace") as f:
                    lines = f.read().splitlines()
            except Exception:
                continue
            for ln in lines[1:]:
                parts = ln.split()
                if len(parts) < 4:
                    continue
                ip, hw = parts[0], parts[3].lower()
                if hw in ("00:00:00:00:00:00", "incomplete") or hw.count(":") != 5:
                    continue
                m.setdefault(ip, hw)
            if m:
                break        # 宿主机 ARP 表优先，读到了就不再退回去看容器内的
        _mac["map"] = m
        _mac["ts"] = now
        return m


def mac_of(ip):
    """ip → MAC（读不到就返回空，界面上显示 —）"""
    if not ip:
        return ""
    try:
        return arp_refresh().get(str(ip), "")
    except Exception:
        return ""


def _hist_prune():
    """只保留最近 31 天与最近 6 个月，过期自动覆盖"""
    days = _hist["days"]
    for k in sorted(days)[:-DEV_KEEP_DAYS]:
        days.pop(k, None)
    months = _hist["months"]
    for k in sorted(months)[:-DEV_KEEP_MONTHS]:
        months.pop(k, None)


def devices_load():
    global _hist
    try:
        with open(dev_path(), encoding="utf-8") as f:
            d = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        print("[dev] load fail:", e, flush=True)
        return
    now = time.time()
    today, mon = day_key(now), month_key(now)
    with _clients_lock:
        for ip, r in (d.get("devices") or {}).items():
            if not isinstance(r, dict):
                continue
            if float(r.get("ts") or 0) < now - 3 * 86400:
                continue
            _clients[ip] = {"n": int(r.get("n") or 0), "first": float(r.get("first") or now),
                            "ts": float(r.get("ts") or 0), "ua": str(r.get("ua") or ""),
                            "path": str(r.get("path") or ""), "day": str(r.get("day") or ""),
                            "dayn": int(r.get("dayn") or 0),
                            "mac": str(r.get("mac") or ""),
                            "dayFirst": float(r.get("dayFirst") or 0)}
        h = d.get("hist") or {}
        if isinstance(h.get("days"), dict) or isinstance(h.get("months"), dict):
            _hist = {"days": {k: v for k, v in (h.get("days") or {}).items() if isinstance(v, dict)},
                     "months": {k: v for k, v in (h.get("months") or {}).items() if isinstance(v, dict)}}
        else:
            # 老版本只有设备计数：用当日/累计值补一份日与月的底账
            for ip, r in _clients.items():
                dn = int(r.get("dayn") or 0)
                if dn <= 0:
                    continue
                _hist["days"].setdefault(today, {"n": 0, "dev": {}})
                _hist["months"].setdefault(mon, {"n": 0, "dev": {}})
                _hist["days"][today]["n"] += dn
                _hist["months"][mon]["n"] += dn
                _hist["days"][today]["dev"][ip] = _hist["days"][today]["dev"].get(ip, 0) + dn
                _hist["months"][mon]["dev"][ip] = _hist["months"][mon]["dev"].get(ip, 0) + dn
    _hist_prune()
    print("[dev] loaded %d devices, %d days, %d months" %
          (len(_clients), len(_hist["days"]), len(_hist["months"])), flush=True)


def devices_save(force=False):
    with _clients_lock:
        if not _clients_meta["dirty"] and not force:
            return
        if not force and time.time() - float(_clients_meta["ts"] or 0) < 30:
            return
        _clients_meta["dirty"] = False
        _clients_meta["ts"] = time.time()
        snap = {k: dict(v) for k, v in _clients.items()}
        hist = {"days": copy.deepcopy(_hist["days"]), "months": copy.deepcopy(_hist["months"])}
    try:
        tmp = dev_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps({"updated": time.time(), "devices": snap, "hist": hist}, ensure_ascii=False))
        os.replace(tmp, dev_path())
    except Exception as e:
        print("[dev] save fail:", e, flush=True)


def devices_reset():
    """手动清零：清空设备列表与日/月历史（设备下次请求会重新接入统计）"""
    with _clients_lock:
        _clients.clear()
        _hist["days"].clear()
        _hist["months"].clear()
        _clients_meta["dirty"] = True
    devices_save(True)
    print("[devices] reset", flush=True)


def dev_touch(ip, ua, path):
    """一次数据流：更新设备当日/本月/累计次数、最后活跃时间，以及日/月汇总"""
    now = time.time()
    today, mon = day_key(now), month_key(now)
    with _clients_lock:
        rec = _clients.get(ip)
        if rec is None:
            rec = _clients[ip] = {"n": 0, "first": now, "ts": 0.0, "ua": ua, "path": path,
                                  "day": today, "dayn": 0, "dayFirst": now, "mac": mac_of(ip)}
        if not rec.get("mac"):
            rec["mac"] = mac_of(ip)
        if rec.get("day") != today:      # 跨天：今日计数归零，累计保留
            rec["day"], rec["dayn"], rec["dayFirst"] = today, 0, now
        rec["n"] = int(rec.get("n") or 0) + 1
        rec["dayn"] = int(rec.get("dayn") or 0) + 1
        rec["ts"] = now
        rec["ua"] = ua
        rec["path"] = path
        for bucket, k in ((_hist["days"], today), (_hist["months"], mon)):
            e = bucket.setdefault(k, {"n": 0, "dev": {}})
            e["n"] = int(e.get("n") or 0) + 1
            e["dev"][ip] = int((e["dev"].get(ip) or 0)) + 1
        _hist_prune()
        _clients_meta["dirty"] = True
        if len(_clients) > 80:           # 清掉 3 天没动静的设备
            for k in [k for k, v in _clients.items() if now - float(v.get("ts") or 0) > 3 * 86400]:
                _clients.pop(k, None)
    devices_save()


def devices_snapshot():
    """在线判定 + 当日/本月统计（空转超过 onlineIdleMinutes 即离线）"""
    now = time.time()
    idle = max(60, min(86400, int(_f.get("idle_min") or 5) * 60))
    today, mon = day_key(now), month_key(now)
    skip = ignore_hosts()
    def _sk(ip):
        return ip in skip or _is_docker_gw(ip)
    with _clients_lock:
        items = [(k, dict(v)) for k, v in _clients.items()]
        hday = copy.deepcopy(_hist["days"].get(today) or {"n": 0, "dev": {}})
        hmon = copy.deepcopy(_hist["months"].get(mon) or {"n": 0, "dev": {}})
        trend = []
        for k, v in sorted(_hist["days"].items())[-7:]:
            dev = {i: c for i, c in ((v or {}).get("dev") or {}).items() if not _sk(i)}
            trend.append((k, sum(int(c or 0) for c in dev.values()), len(dev)))
    out = []
    fd = flow_snapshot().get("devices") or {}
    for ip, r in items:
        if _sk(ip):
            continue
        ago = now - float(r.get("ts") or 0)
        if ago > 86400:
            continue          # 超过 24 小时没动静才移出列表；跨天不清空，避免凌晨设备集体消失
        is_today = r.get("day") == today
        fb = fd.get(ip) or {}
        out.append({"ip": ip, "mac": r.get("mac") or mac_of(ip),
                    "online": ago <= idle, "ago": round(ago, 1),
                    "n": int((hday.get("dev") or {}).get(ip) or (r.get("dayn") or 0 if is_today else 0)),
                    "month": int((hmon.get("dev") or {}).get(ip) or 0),
                    "total": int(r.get("n") or 0),
                    "nowB": float(fb.get("now") or 0),
                    "dayB": int(fb.get("day") or 0),
                    "monthB": int(fb.get("month") or 0),
                    "totalB": int(fb.get("total") or 0),
                    "path": r.get("path") or "", "ua": r.get("ua") or "",
                    "seenAt": float(r.get("dayFirst") or 0)})
    out.sort(key=lambda x: (not x["online"], -x["n"]))
    return {"idleMin": idle // 60, "idleSec": idle, "day": today, "month": mon,
            "online": sum(1 for x in out if x["online"]), "today": len(out),
            "monthDevices": len([k for k in (hmon.get("dev") or {}) if not _sk(k)]),
            "dayRequests": sum(int(c or 0) for i, c in (hday.get("dev") or {}).items() if not _sk(i)),
            "monthRequests": sum(int(c or 0) for i, c in (hmon.get("dev") or {}).items() if not _sk(i)),
            "devices": out[:30],
            "trend": [{"day": d, "n": n, "devices": c} for d, n, c in trend],
            "serverTime": now}


# ---------------- 流量统计 ----------------
# 只计量「网关 → 机顶盒」方向的响应字节（订阅配置 / 分类列表 / 详情 / 封面），
# 管理页自身与探测流量不计入，避免刷管理页把数字刷高。
# 日 / 月 / 累计落盘 cache/traffic.json，重启不清零；实时速率用内存滚动窗口，不落盘。
FLOW_KEEP = 120        # 全局速率窗口（秒）：120 秒足够算「实时」与「近 1 分钟平均」
FLOW_DEV_KEEP = 30     # 单设备速率窗口（秒）
_flow = {"total": 0, "day": "", "dayB": 0, "month": "", "monthB": 0,
         "since": time.time(), "dev": {}, "win": []}
_flow_lock = threading.Lock()
_flow_meta = {"dirty": False, "ts": 0.0}


def flow_path():
    return os.path.join(CACHE_DIR, "traffic.json")


def _flow_roll(tk, mk):
    """跨天 / 跨月归零（调用方持锁）"""
    if _flow["day"] != tk:
        _flow["day"], _flow["dayB"] = tk, 0
    if _flow["month"] != mk:
        _flow["month"], _flow["monthB"] = mk, 0


def flow_load():
    try:
        with open(flow_path(), encoding="utf-8") as f:
            d = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        print("[flow] load fail:", e, flush=True)
        return
    now = time.time()
    with _flow_lock:
        _flow["total"] = int(d.get("total") or 0)
        _flow["day"] = str(d.get("day") or day_key(now))
        _flow["dayB"] = int(d.get("dayB") or 0)
        _flow["month"] = str(d.get("month") or month_key(now))
        _flow["monthB"] = int(d.get("monthB") or 0)
        _flow["since"] = float(d.get("since") or now)
        for ip, r in (d.get("dev") or {}).items():
            if not isinstance(r, dict):
                continue
            _flow["dev"][str(ip)] = {"t": int(r.get("t") or 0),
                                     "d": int(r.get("d") or 0),
                                     "day": str(r.get("day") or _flow["day"]),
                                     "m": int(r.get("m") or 0),
                                     "mon": str(r.get("mon") or _flow["month"]),
                                     "win": []}
        _flow_roll(day_key(now), month_key(now))
        total = _flow["total"]
        n = len(_flow["dev"])
    print("[flow] loaded %.1f MB / %d devices" % (total / 1048576.0, n), flush=True)


def flow_save(force=False):
    with _flow_lock:
        if not _flow_meta["dirty"] and not force:
            return
        if not force and time.time() - float(_flow_meta["ts"] or 0) < 30:
            return
        _flow_meta["dirty"] = False
        _flow_meta["ts"] = time.time()
        snap = {"updated": time.time(), "total": int(_flow["total"]),
                "day": _flow["day"], "dayB": int(_flow["dayB"]),
                "month": _flow["month"], "monthB": int(_flow["monthB"]),
                "since": float(_flow["since"]),
                "dev": {ip: {"t": int(r["t"]), "d": int(r["d"]), "day": r["day"],
                             "m": int(r["m"]), "mon": r["mon"]}
                        for ip, r in _flow["dev"].items()}}
    try:
        tmp = flow_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(snap, ensure_ascii=False))
        os.replace(tmp, flow_path())
    except Exception as e:
        print("[flow] save fail:", e, flush=True)


def flow_note(ip, nb):
    """记一笔出向字节；ip 为服务流量（自身/LunaTV/网桥）时只进总量、不进设备明细"""
    try:
        nb = int(nb or 0)
    except Exception:
        return
    if nb <= 0:
        return
    now = time.time()
    tk, mk = day_key(now), month_key(now)
    svc = (not ip) or ip in ignore_hosts() or _is_docker_gw(ip)
    if svc:      # 网关自身 / NAS LunaTV / 网桥：既不算设备，也不进总量，保证「总量 = 设备合计」
        return
    with _flow_lock:
        _flow_roll(tk, mk)
        _flow["total"] += nb
        _flow["dayB"] += nb
        _flow["monthB"] += nb
        w = _flow["win"]
        w.append((now, nb))
        if len(w) > 4096:
            del w[:len(w) - 4096]
        r = _flow["dev"].get(ip)
        if r is None:
            r = _flow["dev"][ip] = {"t": 0, "d": 0, "day": tk, "m": 0, "mon": mk, "win": []}
        if r["day"] != tk:
            r["day"], r["d"] = tk, 0
        if r["mon"] != mk:
            r["mon"], r["m"] = mk, 0
        r["t"] += nb
        r["d"] += nb
        r["m"] += nb
        rw = r["win"]
        rw.append((now, nb))
        if len(rw) > 512:
            del rw[:len(rw) - 512]
        _flow_meta["dirty"] = True
    flow_save()


def flow_reset():
    """清零流量统计（跟「清零统计」按钮一起用）"""
    now = time.time()
    with _flow_lock:
        _flow["total"] = 0
        _flow["day"], _flow["dayB"] = day_key(now), 0
        _flow["month"], _flow["monthB"] = month_key(now), 0
        _flow["since"] = now
        _flow["dev"].clear()
        _flow["win"] = []
        _flow_meta["dirty"] = True
    flow_save(True)
    print("[flow] reset", flush=True)


def flow_snapshot():
    """总量 + 实时速率 + 最近 1 分钟走势（30 根 2 秒柱）+ 每设备用量"""
    now = time.time()
    tk, mk = day_key(now), month_key(now)
    with _flow_lock:
        _flow_roll(tk, mk)
        w = _flow["win"]
        cut = now - FLOW_KEEP
        if w and w[0][0] < cut:
            del w[:next((i for i, x in enumerate(w) if x[0] >= cut), len(w))]
        r10 = sum(b for t, b in w if t >= now - 10) / 10.0
        r60 = sum(b for t, b in w if t >= now - 60) / 60.0
        spark = [0] * 30
        for t, b in w:
            i = 29 - int((now - t) // 2)
            if 0 <= i < 30:
                spark[i] += b
        dev = {}
        dc = now - FLOW_DEV_KEEP
        for ip, r in _flow["dev"].items():
            rw = r["win"]
            if rw and rw[0][0] < dc:
                del rw[:next((i for i, x in enumerate(rw) if x[0] >= dc), len(rw))]
            dev[ip] = {"now": round(sum(b for _, b in rw) / float(FLOW_DEV_KEEP), 1),
                       "day": int(r["d"]) if r["day"] == tk else 0,
                       "month": int(r["m"]) if r["mon"] == mk else 0,
                       "total": int(r["t"])}
        return {"ok": True, "port": LISTEN_PORT, "now": round(r10, 1), "avg": round(r60, 1),
                "day": int(_flow["dayB"]), "month": int(_flow["monthB"]),
                "total": int(_flow["total"]), "since": float(_flow["since"]),
                "dayKey": tk, "monthKey": mk, "spark": spark,
                "devices": dev, "serverTime": now}


def read_config_file():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception:
        return {"categories": list(DEFAULT_CATS), "titles": list(DEFAULT_TITLES),
                "placeholderPattern": DEFAULT_PLACEHOLDER,
                "disabledSources": [], "sourceOrder": [],
                "sourceSyncMinutes": 5, "backupOnSave": True, "backupKeep": 5,
                "tvboxRefreshMinutes": 5, "sourceLimit": 0, "activeSource": "",
                "backupSources": [], "autoFailover": True, "failoverThreshold": 3}


def apply_config(c):
    _f["cats"] = [str(w).lower() for w in (c.get("categories") or DEFAULT_CATS) if str(w).strip()]
    _f["titles"] = [str(w).lower() for w in (c.get("titles") or DEFAULT_TITLES) if str(w).strip()]
    _f["placeholder"] = re.compile(c.get("placeholderPattern") or DEFAULT_PLACEHOLDER)
    _f["disabled"] = {str(k) for k in (c.get("disabledSources") or []) if str(k).strip()}
    _f["order"] = [str(k) for k in (c.get("sourceOrder") or []) if str(k).strip()]
    _f["sync_min"] = max(1, min(60, int(c.get("sourceSyncMinutes") or 5)))
    _f["backup"] = bool(c.get("backupOnSave", True))
    _f["keep"] = max(1, min(50, int(c.get("backupKeep") or 5)))
    _f["tvcache_min"] = max(1, min(120, int(c.get("tvboxRefreshMinutes") or 5)))
    _f["src_limit"] = max(0, min(50, int(c.get("sourceLimit") or 0)))
    _f["src_active"] = str(c.get("activeSource") or "").strip()
    _f["fo_on"] = bool(c.get("autoFailover", True))
    _f["fo_list"] = [str(k).strip() for k in (c.get("backupSources") or []) if str(k).strip()]
    try:
        _th = int(c.get("failoverThreshold") or 3)
    except Exception:
        _th = 3
    _f["fo_thresh"] = max(1, min(10, _th))
    _f["tmdb_on"] = bool(c.get("tmdbEnabled", True))
    _f["cats_on"] = bool(c.get("categoryFilterEnabled", True))
    _f["titles_on"] = bool(c.get("titleFilterEnabled", True))
    _f["filter_on"] = bool(c.get("filterEnabled", True))
    if ADULT_MODE:      # 成人专用实例：屏蔽永远关闭，配置里就算打开也不生效
        _f["cats_on"] = _f["titles_on"] = _f["filter_on"] = False
    _f["search_all"] = bool(c.get("searchAllSources", True))
    _f["search_limit"] = max(0, min(50, int(c.get("searchLimit") or 0)))
    _f["agg_on"] = bool(c.get("aggEnabled", True))
    _f["agg_browse"] = max(1, min(20, int(c.get("aggBrowseSources") or 8)))
    _f["agg_search"] = max(0, min(50, int(c.get("aggSearchSources") or 0)))
    _f["agg_tvbox"] = bool(c.get("aggInTvbox", False))
    _f["agg_tvsub"] = bool(c.get("aggTvSubs", False))
    _f["agg_cat_off"] = {str(x).strip() for x in (c.get("aggCatOff") or []) if str(x).strip()}
    _f["agg_cat_order"] = [str(x).strip() for x in (c.get("aggCatOrder") or []) if str(x).strip()]
    _f["agg_home"] = str(c.get("aggHomeCat") or "").strip()
    _f["agg_page"] = max(10, min(60, int(c.get("aggPageSize") or 20)))
    _f["agg_top_pg"] = max(1, min(5, int(c.get("aggTopPages") or 1)))
    _f["agg_sub_pg"] = max(1, min(5, int(c.get("aggSubPages") or 1)))
    _f["agg_sub_src"] = max(1, min(20, int(c.get("aggSubSources") or 12)))
    # 空白源补位：名额内的源没采到数据就跳过，用后面的源补足「有效源数量」
    _f["agg_fill"] = bool(c.get("aggFillEnabled", True))
    _f["agg_fill_max"] = max(1, min(40, int(c.get("aggFillMax") or 20)))
    # 采集调度：自动预采集（到点跑一轮，采完就停，不在后台常驻循环抓取）
    _f["agg_auto_on"] = bool(c.get("aggAutoCollectEnabled", True))
    _f["agg_auto_sec"] = max(300, min(86400, int(c.get("aggAutoInterval") or 3600)))
    _at = str(c.get("aggAutoAt") or "").strip()
    m = re.match(r"^(\d{1,2})\s*[:：]\s*(\d{1,2})$", _at) if _at else None
    _f["agg_auto_at"] = ("%02d:%02d" % (max(0, min(23, int(m.group(1)))), max(0, min(59, int(m.group(2)))))) if m else ""
    _sc = str(c.get("aggAutoScope") or "hot").strip().lower()
    _f["agg_auto_scope"] = _sc if _sc in ("top", "hot", "all") else "hot"
    _f["agg_auto_n"] = max(1, min(80, int(c.get("aggAutoHotCount") or 12)))
    _f["agg_auto_max"] = max(1, min(400, int(c.get("aggAutoMaxCats") or 40)))
    _f["agg_auto_budget"] = max(60, min(3600, int(c.get("aggAutoBudget") or 300)))
    # 客户端触发采集：关掉后分类页只用已有缓存（过期也不回源），完全没人访问就不产生流量
    _f["agg_lazy"] = bool(c.get("aggLazyCollectEnabled", True))
    _f["sync_on"] = bool(c.get("sourceSyncEnabled", True))
    _f["img_on"] = bool(c.get("imageCacheEnabled", True))
    # 缓存容量与保留期（防止封面/TMDB 越攒越大、老文件长期占盘）
    _f["img_max_mb"] = max(32, min(4096, int(c.get("imageCacheMaxMB") or 256)))
    _f["img_keep_days"] = max(1, min(365, int(c.get("imageCacheKeepDays") or 30)))
    _f["tmdb_keep_days"] = max(1, min(365, int(c.get("tmdbCacheKeepDays") or 60)))
    _f["cache_max_mb"] = max(64, min(8192, int(c.get("cacheMaxMB") or 512)))
    _f["cache_auto_clean"] = bool(c.get("cacheAutoCleanEnabled", True))
    try:
        _iv = int(c["onlineIdleMinutes"])
    except Exception:
        _iv = 5
    _f["idle_min"] = max(1, min(1440, _iv))
    ig = c.get("ignoreDevices")
    if ig is None:
        ig = DEFAULT_IGNORE_DEVS
    _f["ignore_dev"] = {str(x).strip() for x in ig if str(x).strip()}
    # 源健康检测：失败到阈值自动停用，恢复后按间隔自动启用
    _f["sh_on"] = bool(c.get("sourceHealthEnabled", True))
    _f["sh_sec"] = max(60, min(86400, int(c.get("sourceHealthInterval") or 600)))
    _f["sh_fail"] = max(1, min(20, int(c.get("sourceHealthFail") or 3)))
    _f["sh_ok"] = max(1, min(10, int(c.get("sourceHealthRecover") or 2)))
    _f["sh_to"] = max(3, min(60, int(c.get("sourceHealthTimeout") or 10)))
    # 按响应时间自动排序：延时小的源排最前（影响下发顺序 + 聚合采集顺序）
    _f["asort_on"] = bool(c.get("sourceAutoSortEnabled", False))
    _f["asort_mode"] = "recent" if str(c.get("sourceAutoSortMode") or "avg") == "recent" else "avg"
    _f["asort_min"] = max(1, min(20, int(c.get("sourceAutoSortMinProbes") or 2)))
    _f["asort_scope"] = str(c.get("sourceAutoSortScope") or "all")
    if _f["asort_scope"] not in ("all", "deliver", "agg"):
        _f["asort_scope"] = "all"
    _f["asort_fail_last"] = bool(c.get("sourceAutoSortFailLast", True))


def reload_config(force=False):
    try:
        mt = os.path.getmtime(CONFIG_PATH)
    except OSError:
        return
    if not force and mt == _f["mtime"]:
        return
    try:
        apply_config(read_config_file())
        _f["mtime"] = mt
        print("[config] loaded cats=%d titles=%d" % (len(_f["cats"]), len(_f["titles"])), flush=True)
        # 词库/源开关变更后，旧缓存可能含未过滤内容，直接清空
        try:
            with _cms_lock:
                _cms_cache.clear()
        except Exception:
            pass
    except Exception as e:
        print("[config] load failed:", e, flush=True)


def save_config(c):
    old = read_config_file()
    if _f["backup"]:
        make_backup()

    def pick(key, default):
        return c[key] if key in c else old.get(key, default)

    data = {
        "_说明": "分类黑名单匹配分类名；标题黑名单额外过滤视频标题；修改保存后自动生效，无需重启容器",
        "categories": [str(w).strip() for w in pick("categories", list(DEFAULT_CATS)) or [] if str(w).strip()],
        "titles": [str(w).strip() for w in pick("titles", list(DEFAULT_TITLES)) or [] if str(w).strip()],
        "placeholderPattern": c.get("placeholderPattern") or DEFAULT_PLACEHOLDER,
        "disabledSources": [str(k).strip() for k in pick("disabledSources", []) or [] if str(k).strip()],
        "sourceOrder": [str(k).strip() for k in pick("sourceOrder", []) or [] if str(k).strip()],
        "sourceSyncMinutes": max(1, min(60, int(pick("sourceSyncMinutes", 5) or 5))),
        "backupOnSave": bool(pick("backupOnSave", True)),
        "backupKeep": max(1, min(50, int(pick("backupKeep", 5) or 5))),
        "tvboxRefreshMinutes": max(1, min(120, int(pick("tvboxRefreshMinutes", 5) or 5))),
        "sourceLimit": max(0, min(50, int(pick("sourceLimit", 0) or 0))),
        "activeSource": str(pick("activeSource", "") or "").strip(),
        "autoFailover": bool(pick("autoFailover", True)),
        "failoverThreshold": max(1, min(10, int(pick("failoverThreshold", 3) or 3))),
        "backupSources": [str(k).strip() for k in pick("backupSources", []) or [] if str(k).strip()],
        "tmdbEnabled": bool(pick("tmdbEnabled", True)),
        "categoryFilterEnabled": False if ADULT_MODE else bool(pick("categoryFilterEnabled", True)),
        "titleFilterEnabled": False if ADULT_MODE else bool(pick("titleFilterEnabled", True)),
        "filterEnabled": False if ADULT_MODE else bool(pick("filterEnabled", True)),
        "searchAllSources": bool(pick("searchAllSources", True)),
        "searchLimit": max(0, min(50, int(pick("searchLimit", 0) or 0))),
        "aggEnabled": bool(pick("aggEnabled", True)),
        "aggBrowseSources": max(1, min(20, int(pick("aggBrowseSources", 8) or 8))),
        "aggSearchSources": max(0, min(50, int(pick("aggSearchSources", 0) or 0))),
        "aggInTvbox": bool(pick("aggInTvbox", False)),
        "aggTvSubs": bool(pick("aggTvSubs", False)),
        "aggCatOff": [str(x).strip() for x in pick("aggCatOff", []) or [] if str(x).strip()],
        "aggCatOrder": [str(x).strip() for x in pick("aggCatOrder", []) or [] if str(x).strip()],
        "aggHomeCat": str(pick("aggHomeCat", "") or "").strip(),
        "aggPageSize": max(10, min(60, int(pick("aggPageSize", 20) or 20))),
        "aggTopPages": max(1, min(5, int(pick("aggTopPages", 1) or 1))),
        "aggSubPages": max(1, min(5, int(pick("aggSubPages", 1) or 1))),
        "aggSubSources": max(1, min(20, int(pick("aggSubSources", 12) or 12))),
        "aggFillEnabled": bool(pick("aggFillEnabled", True)),
        "aggFillMax": max(1, min(40, int(pick("aggFillMax", 20) or 20))),
        "aggAutoCollectEnabled": bool(pick("aggAutoCollectEnabled", True)),
        "aggAutoInterval": max(300, min(86400, int(pick("aggAutoInterval", 3600) or 3600))),
        "aggAutoAt": str(pick("aggAutoAt", "") or "").strip()[:5],
        "aggAutoScope": str(pick("aggAutoScope", "hot") or "hot"),
        "aggAutoHotCount": max(1, min(80, int(pick("aggAutoHotCount", 12) or 12))),
        "aggAutoMaxCats": max(1, min(400, int(pick("aggAutoMaxCats", 40) or 40))),
        "aggAutoBudget": max(60, min(3600, int(pick("aggAutoBudget", 300) or 300))),
        "aggLazyCollectEnabled": bool(pick("aggLazyCollectEnabled", True)),
        "sourceSyncEnabled": bool(pick("sourceSyncEnabled", True)),
        "imageCacheEnabled": bool(pick("imageCacheEnabled", True)),
        "imageCacheMaxMB": max(32, min(4096, int(pick("imageCacheMaxMB", 256) or 256))),
        "imageCacheKeepDays": max(1, min(365, int(pick("imageCacheKeepDays", 30) or 30))),
        "tmdbCacheKeepDays": max(1, min(365, int(pick("tmdbCacheKeepDays", 60) or 60))),
        "cacheMaxMB": max(64, min(8192, int(pick("cacheMaxMB", 512) or 512))),
        "cacheAutoCleanEnabled": bool(pick("cacheAutoCleanEnabled", True)),
        "onlineIdleMinutes": max(1, min(1440, int(pick("onlineIdleMinutes", 5)))),
        "ignoreDevices": list(dict.fromkeys([str(x).strip() for x in pick("ignoreDevices", DEFAULT_IGNORE_DEVS) or [] if str(x).strip()])),
        "sourceHealthEnabled": bool(pick("sourceHealthEnabled", True)),
        "sourceHealthInterval": max(60, min(86400, int(pick("sourceHealthInterval", 600) or 600))),
        "sourceHealthFail": max(1, min(20, int(pick("sourceHealthFail", 3) or 3))),
        "sourceHealthRecover": max(1, min(10, int(pick("sourceHealthRecover", 2) or 2))),
        "sourceHealthTimeout": max(3, min(60, int(pick("sourceHealthTimeout", 10) or 10))),
        "sourceAutoSortEnabled": bool(pick("sourceAutoSortEnabled", False)),
        "sourceAutoSortMode": "recent" if str(pick("sourceAutoSortMode", "avg")) == "recent" else "avg",
        "sourceAutoSortMinProbes": max(1, min(20, int(pick("sourceAutoSortMinProbes", 2) or 2))),
        "sourceAutoSortScope": str(pick("sourceAutoSortScope", "all") or "all"),
        "sourceAutoSortFailLast": bool(pick("sourceAutoSortFailLast", True)),
    }
    # 单文件 bind mount 不支持 rename，直接截断写入
    with open(CONFIG_PATH, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
        fp.flush()
    apply_config(data)
    _f["mtime"] = os.path.getmtime(CONFIG_PATH)
    # 用户手动改了主源/下发数量/启停/顺序 → 放弃自动切换状态，回到配置主源
    if any(k in c for k in ("activeSource", "sourceLimit", "disabledSources", "sourceOrder")):
        fo_reset(silent=True)
    # 开关变化会影响封面改写与屏蔽结果，旧缓存一并丢弃（含聚合源分类页合并缓存）
    try:
        with _cms_lock:
            _cms_cache.clear()
        with _search_lock:
            _search_cache.clear()
        with _agg["lock"]:
            _agg["pool"].clear()
    except Exception:
        pass
    # 停用名单变了：立刻把停用源从聚合采集源（分类映射 + 分类树）里摘掉
    if "disabledSources" in c:
        try:
            agg_src_prune()
        except Exception as e:
            print("[agg] 停用源刷新失败: %s" % str(e)[:60], flush=True)


def bad_cat(s):
    if not _f.get("filter_on", True):    # 屏蔽总开关关闭：全部放行
        return False
    if not s:
        return True
    s = str(s).strip()
    if not s or _f["placeholder"].match(s):
        return True
    if not _f.get("cats_on", True):      # 开关关闭：分类原样放行
        return False
    low = s.lower()
    return any(w in low for w in _f["cats"])


def bad_cat_item(s):
    """列表/搜索里的条目：type_name 缺失或占位符时不判命中（只有明确命中词库才过滤）"""
    if not _f.get("filter_on", True) or not _f.get("cats_on", True):
        return False
    s = str(s or "").strip()
    if not s or _f["placeholder"].match(s):
        return False
    low = s.lower()
    return any(w in low for w in _f["cats"])


def bad_title(s):
    if not _f.get("filter_on", True):    # 屏蔽总开关关闭：全部放行
        return False
    if not s:
        return False
    if not _f.get("titles_on", True):    # 开关关闭：标题不做二次过滤
        return False
    low = str(s).lower()
    return any(w in low for w in _f["titles"])


_cookie = {"v": None}


def get_cookie():
    if _cookie["v"]:
        return _cookie["v"]
    req = urllib.request.Request(
        LUNA + "/api/login",
        data=json.dumps({"username": os.environ.get("LUNA_USER", ""),
                         "password": os.environ.get("LUNA_PASS", "")}).encode(),
        headers={"Content-Type": "application/json"})
    resp = urllib.request.urlopen(req, timeout=15)
    _cookie["v"] = resp.headers.get("Set-Cookie", "").split(";")[0].strip()
    return _cookie["v"]


_sources = {"data": None, "ts": 0.0}


def self_api(u):
    """该 api 是不是网关自己的聚合采集源（LunaTV 里加了聚合源时，不能再去聚合它自己）"""
    u = str(u or "")
    if not u:
        return False
    if "/agg" in u or "provide/vod" in u:
        try:
            host = (urllib.parse.urlsplit(u).hostname or "").strip()
        except Exception:
            host = ""
        if host in LOCAL_HOSTS or host == SELF_HOST:
            return True
    return False


def sources_cached(force=False):
    ttl = max(1, int(_f.get("sync_min") or 5)) * 60
    # 源同步开关关闭：只用手上的列表，不再去 LunaTV 读源（手动「立即同步」仍然可用）
    if not force and _sources["data"] is not None and not _f.get("sync_on", True):
        return _sources["data"]
    if force or _sources["data"] is None or time.time() - _sources["ts"] > ttl:
        try:
            req = urllib.request.Request(LUNA + "/api/admin/config", headers={"Cookie": get_cookie()})
            cfg = json.loads(urllib.request.urlopen(req, timeout=20).read().decode())
            m = {s["key"]: {"api": s["api"], "name": s.get("name") or s["key"],
                            "disabled": bool(s.get("disabled"))}
                 for s in (cfg["Config"].get("SourceConfig") or [])
                 if s.get("api") and not self_api(s.get("api"))}
            if m:
                _sources["data"], _sources["ts"] = m, time.time()
        except Exception as e:
            print("[warn] fetch sources failed:", e, flush=True)
    return _sources["data"] or {}


def src_off(k):
    """源关闭 = 网关里手动停用 / 健康检测自动停用 / LunaTV 里已停用（任一即不下发、不参与聚合）"""
    return (k in _f["disabled"] or k in _sh["auto"]
            or bool((sources_cached().get(k) or {}).get("disabled")))


def src_on(k):
    """源可用：不在任何一份停用名单里（搜索源 / 聚合采集源统一用它过滤）"""
    return not src_off(k)


ADMIN_PATH = os.environ.get("GUARD_ADMIN", "/app/admin.html")
LOGO_PATH = os.environ.get("GUARD_LOGO", "/app/logo/logo.png")
BACKUP_DIR = os.environ.get("GUARD_BACKUP", "/app/backups")
CACHE_DIR = os.environ.get("GUARD_CACHE", "/app/cache")
CACHE_FILE = os.path.join(CACHE_DIR, "tvbox-config.json")

# ---------------- 源健康检测（自动停用 / 自动恢复） ----------------
# 每隔 sh_sec 秒并发探一次每个源的 ac=class：
#   连续失败达 sh_fail 次 → 自动停用（效果等同片源列表里关闭，不下发、不参与聚合搜索）
#   已停用的源仍继续被探测，连续成功 sh_ok 次 → 自动恢复启用
SH_FILE = os.path.join(CACHE_DIR, "srchealth.json")
SH_HIST = 20           # 每源保留最近 20 次探测结果，用于算成功率
SH_WORKERS = 4         # 并发探测数（别太高，源站会限流导致误判）
SH_REFIRE = 1.5        # 首次失败后隔多久再探一次，两次都失败才计数
SH_BOOT_DELAY = 40     # 启动后等聚合建树跑完再开始首轮探测
_sh = {"items": {}, "auto": set(), "log": [], "next": 0.0, "ts": 0.0,
       "running": False, "dirty": False, "lock": threading.Lock()}


def sh_conf():
    return {"on": bool(_f.get("sh_on", True)),
            "sec": max(60, min(86400, int(_f.get("sh_sec") or 600))),
            "fail": max(1, min(20, int(_f.get("sh_fail") or 3))),
            "ok": max(1, min(10, int(_f.get("sh_ok") or 2))),
            "to": max(3, min(60, int(_f.get("sh_to") or 10)))}


def sh_load():
    """恢复自动停用状态与历史，重启容器后机顶盒不会突然多回一堆挂掉的源"""
    try:
        with open(SH_FILE, "r", encoding="utf-8") as fp:
            d = json.load(fp)
        items = d.get("items") or {}
        with _sh["lock"]:
            _sh["items"] = {str(k): v for k, v in items.items() if isinstance(v, dict)}
            _sh["auto"] = {str(k) for k in (d.get("auto") or [])}
            _sh["log"] = [x for x in (d.get("log") or []) if isinstance(x, dict)][-20:]
            _sh["ts"] = float(d.get("ts") or 0)
        if _sh["auto"]:
            print("[srchealth] 恢复自动停用 %d 个源: %s" % (len(_sh["auto"]), ",".join(sorted(_sh["auto"]))), flush=True)
    except Exception:
        pass


def sh_save(force=False):
    if not force and not _sh.get("dirty"):
        return
    with _sh["lock"]:
        data = {"ts": time.time(), "auto": sorted(_sh["auto"]),
                "items": {k: v for k, v in _sh["items"].items()}, "log": _sh["log"][-20:]}
        _sh["dirty"] = False
    tmp = SH_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False)
        os.replace(tmp, SH_FILE)
    except Exception as e:
        print("[srchealth] save fail:", e, flush=True)


def sh_item(key):
    it = _sh["items"].get(key)
    if not it:
        it = {"ok": None, "fail": 0, "pass": 0, "ms": 0, "msh": [], "ts": 0.0,
              "ok_ts": 0.0, "hist": [], "err": "", "auto": 0.0, "auto_ts": 0.0,
              "probe_ts": 0.0, "fails": 0, "total": 0}
        _sh["items"][key] = it
    return it


def sh_log(text, key="", kind=""):
    _sh["log"].append({"ts": time.time(), "key": key, "kind": kind, "text": text})
    _sh["log"] = _sh["log"][-20:]
    _sh["dirty"] = True


def sh_sort_key(k):
    """自动排序键：响应快的排最前

    第 0 档：测够次数且最近一次正常 → 按平均/最近延时升序
    第 1 档：还没测够次数（没数据）→ 保持用户手动顺序，排在已测源之后
    第 2 档：最近一次探测失败 → 排最末（可在设置里关掉）
    """
    it = _sh["items"].get(k) or {}
    hist = [int(x) for x in (it.get("msh") or []) if x]
    ts = float(it.get("ts") or 0)
    if ts and not it.get("ok") and _f.get("asort_fail_last", True):
        return (2, 0)
    if str(_f.get("asort_mode") or "avg") == "recent":
        ms = int(it.get("ms") or (hist[-1] if hist else 0))
        enough = bool(ms)
    else:
        need = max(1, int(_f.get("asort_min") or 2))
        enough = len(hist) >= need
        ms = int(sum(hist) / len(hist)) if hist else 0
    if not enough:
        return (1, 0)
    return (0, ms)


def sh_drop_cache():
    """启停变化后清掉聚合池与 CMS 缓存，机顶盒下次请求立即用新源列表"""
    try:
        with _agg["lock"]:
            _agg["pool"].clear()
    except Exception:
        pass
    try:
        with _cms_lock:
            _cms_cache.clear()
    except Exception:
        pass
    # 搜索结果同样带来源前缀，缓存里的旧结果也要丢掉，并把停用源从聚合映射里摘掉
    try:
        with _search_lock:
            _search_cache.clear()
    except Exception:
        pass
    try:
        agg_src_prune()
    except Exception as e:
        print("[agg] 源启停刷新失败: %s" % str(e)[:60], flush=True)


def sh_probe(key, timeout=None):
    """探测单个源：拉一次 ac=class 并计时，返回 (ok, ms, detail)"""
    meta = sources_cached().get(key)
    if not meta:
        return False, 0, "源不存在"
    to = timeout or sh_conf()["to"]
    t0 = time.time()
    try:
        d = _upstream_json(meta, "ac=class", to)
        cls = (d.get("class") or []) if isinstance(d, dict) else []
        lst = (d.get("list") or []) if isinstance(d, dict) else []
        ok = bool(cls or lst)
        detail = ("分类 %d 个" % len(cls)) if cls else ("列表 %d 条" % len(lst) if lst else "返回空数据")
    except Exception as e:
        ok, detail = False, str(e)[:70]
    return ok, int((time.time() - t0) * 1000), detail


def sh_note(key, ok, ms=0, detail="", probe=True, persist=True):
    """记一次探测结果，按阈值自动停用 / 自动恢复"""
    c = sh_conf()
    now = time.time()
    dropped = recovered = ""
    with _sh["lock"]:
        it = sh_item(key)
        if probe:
            it["probe_ts"] = now
        it["ts"] = now
        it["ok"] = bool(ok)
        it["ms"] = int(ms or 0)
        it["err"] = "" if ok else str(detail or "失败")[:70]
        it["total"] = int(it.get("total") or 0) + 1
        it["hist"] = (it.get("hist") or [])[-(SH_HIST - 1):]
        it["hist"].append(1 if ok else 0)
        if ok:
            it["fail"] = 0
            it["ok_ts"] = now
            it["pass"] = int(it.get("pass") or 0) + 1
            if ms:
                msh = (it.get("msh") or [])[-9:]
                msh.append(int(ms))
                it["msh"] = msh
            if key in _sh["auto"] and it["pass"] >= c["ok"]:
                _sh["auto"].discard(key)
                it["auto"] = 0.0
                it["pass"] = 0
                recovered = key
        else:
            it["pass"] = 0
            it["fail"] = int(it.get("fail") or 0) + 1
            it["fails"] = int(it.get("fails") or 0) + 1
            manual_off = key in _f["disabled"] or bool((sources_cached().get(key) or {}).get("disabled"))
            if not manual_off and key not in _sh["auto"] and it["fail"] >= c["fail"]:
                _sh["auto"].add(key)
                it["auto"] = now
                it["auto_ts"] = now
                dropped = key
        if recovered:
            sh_log("恢复启用 · 连续 %d 次探测正常" % c["ok"], recovered, "on")
        if dropped:
            sh_log("自动停用 · 连续 %d 次探测失败（%s）" % (it["fail"], it["err"]), dropped, "off")
        _sh["dirty"] = True
    if dropped:
        print("[srchealth] 自动停用 %s：连续 %d 次失败" % (dropped, c["fail"]), flush=True)
        sh_drop_cache()
    if recovered:
        print("[srchealth] 自动恢复 %s" % recovered, flush=True)
        sh_drop_cache()
    if persist:
        sh_save()
    return {"dropped": dropped, "recovered": recovered}


def sh_probe_retry(key):
    """探测一次；失败就隔 1.5 秒再来一次。两次都失败才算真失败，
    避免源站限流 / 偶发抖动把正常源判死（并发探测时最容易误判）"""
    ok, ms, detail = sh_probe(key)
    if ok:
        return ok, ms, detail
    time.sleep(SH_REFIRE)
    ok2, ms2, detail2 = sh_probe(key)
    if ok2:
        return True, ms2, detail2
    return False, ms2 or ms, detail2 or detail


def sh_probe_all(keys=None, auto=False):
    """并发探测全部源（含已自动停用的源，否则永远恢复不回来）"""
    if _sh.get("running"):
        return {"ok": True, "skipped": True}
    srcs = sources_cached()
    if keys:
        targets = [k for k in keys if k in srcs]
    else:
        targets = [k for k in ordered_source_keys()]
        targets += [k for k in _sh["auto"] if k in srcs and k not in targets]
    if not targets:
        return {"ok": True, "probed": 0, "alive": 0, "dead": 0}
    _sh["running"] = True
    t0 = time.time()
    alive = dead = 0
    try:
        with ThreadPoolExecutor(max_workers=min(SH_WORKERS, len(targets))) as ex:
            futs = {ex.submit(sh_probe_retry, k): k for k in targets}
            for fu in as_completed(futs):
                k = futs[fu]
                try:
                    ok, ms, detail = fu.result()
                except Exception as e:
                    ok, ms, detail = False, 0, str(e)[:70]
                r = sh_note(k, ok, ms, detail, probe=True, persist=False)
                if ok:
                    alive += 1
                else:
                    dead += 1
                if r.get("dropped") or r.get("recovered"):
                    print("[srchealth] %s %s" % (k, "已恢复" if r.get("recovered") else "已停用"), flush=True)
    finally:
        _sh["running"] = False
        _sh["ts"] = time.time()
        _sh["dirty"] = True
        if not auto:
            sh_save(True)
        else:
            sh_save()
    print("[srchealth] 探测完成 %d 源 · 正常 %d / 异常 %d · %.1fs"
          % (len(targets), alive, dead, time.time() - t0), flush=True)
    return {"ok": True, "probed": len(targets), "alive": alive, "dead": dead,
            "sec": round(time.time() - t0, 1)}


def sh_tick():
    """由 warm_loop 每 15 秒调一次：到点就起一轮全量探测"""
    c = sh_conf()
    if not c["on"] or _sh.get("running"):
        return
    now = time.time()
    if now < float(_sh.get("next") or 0):
        return
    _sh["next"] = now + c["sec"]
    threading.Thread(target=sh_probe_all, daemon=True).start()


def sh_auto_keys():
    return sorted(_sh["auto"])


def sh_snapshot():
    """给「源检测」页的完整快照"""
    c = sh_conf()
    srcs = sources_cached()
    order = {k: i for i, k in enumerate(_f["order"])}
    keys = list(srcs.keys())
    keys.sort(key=lambda k: order.get(k, len(order) + 100))
    eff = {k: i + 1 for i, k in enumerate(ordered_source_keys("deliver"))}   # 实际下发排位
    now = time.time()
    items = []
    for k in keys:
        it = _sh["items"].get(k) or {}
        hist = it.get("hist") or []
        msh = it.get("msh") or []
        auto_ts = float(it.get("auto") or 0)
        manual = k in _f["disabled"]
        luna_off = bool((srcs.get(k) or {}).get("disabled"))
        if auto_ts:
            state = "auto_off"
        elif manual:
            state = "manual_off"
        elif luna_off:
            state = "luna_off"
        elif not hist:
            state = "unknown"
        elif it.get("ok"):
            state = "ok"
        else:
            state = "bad"
        items.append({
            "key": k, "name": (srcs.get(k) or {}).get("name") or k, "state": state,
            "alive": bool(it.get("ok")), "ms": int(it.get("ms") or 0),
            "avgMs": int(sum(msh) / len(msh)) if msh else 0,
            "rate": int(round(100.0 * sum(hist) / len(hist))) if hist else None,
            "fail": int(it.get("fail") or 0), "pass": int(it.get("pass") or 0),
            "fails": int(it.get("fails") or 0), "total": int(it.get("total") or 0),
            "lastTs": float(it.get("ts") or 0), "okTs": float(it.get("ok_ts") or 0),
            "autoTs": auto_ts, "err": str(it.get("err") or ""),
            "hist": hist[-12:],
            "pos": int(eff.get(k) or 0),       # 自动排序后的实际顺序（0 = 未参与下发）
            "sortMs": int(sum(msh) / len(msh)) if msh else int(it.get("ms") or 0),
        })
    rank = {"auto_off": 0, "bad": 1, "manual_off": 2, "luna_off": 3, "unknown": 4, "ok": 5}
    items.sort(key=lambda x: (rank.get(x["state"], 9), x["key"]))
    total = len(items)
    ok_n = len([x for x in items if x["state"] == "ok"])
    bad_n = len([x for x in items if x["state"] == "bad"])
    auto_n = len([x for x in items if x["state"] == "auto_off"])
    rates = [x["rate"] for x in items if x["rate"] is not None]
    mss = [x["avgMs"] for x in items if x["avgMs"]]
    return {"ok": True, "items": items, "total": total, "alive": ok_n, "bad": bad_n,
            "autoOff": auto_n, "autoKeys": sorted(_sh["auto"]),
            "offline": len([x for x in items if x["state"] in ("auto_off", "manual_off", "luna_off")]),
            "rate": int(round(sum(rates) / len(rates))) if rates else None,
            "avgMs": int(sum(mss) / len(mss)) if mss else 0,
            "conf": {"on": c["on"], "sec": c["sec"], "fail": c["fail"], "ok": c["ok"], "to": c["to"]},
            "asort": {"on": bool(_f.get("asort_on")), "mode": str(_f.get("asort_mode") or "avg"),
                      "min": int(_f.get("asort_min") or 2), "scope": str(_f.get("asort_scope") or "all"),
                      "failLast": bool(_f.get("asort_fail_last", True))},
            "running": bool(_sh.get("running")),
            "lastTs": float(_sh.get("ts") or 0), "nextTs": float(_sh.get("next") or 0),
            "nextIn": max(0, int(float(_sh.get("next") or 0) - now)),
            "log": list(_sh["log"])[-12:][::-1],
            "failover": {"active": fo_active(), "primary": fo_primary(),
                         "conf": fo_conf_primary(), "since": float(_fo.get("since") or 0),
                         "switches": int(_fo.get("switches") or 0)}}


def sh_set_enabled(key, on):
    """手动启用/停用（启用时同时清掉自动停用标记）"""
    if not key or key not in sources_cached():
        return {"ok": False, "error": "源不存在"}
    keys = set(_f["disabled"])
    if on:
        keys.discard(key)
        with _sh["lock"]:
            was_auto = key in _sh["auto"]
            _sh["auto"].discard(key)
            it = sh_item(key)
            it["auto"] = 0.0
            it["fail"] = 0
            it["pass"] = 0
            if was_auto:
                sh_log("手动恢复启用", key, "on")
            _sh["dirty"] = True
    else:
        keys.add(key)
        with _sh["lock"]:
            it = sh_item(key)
            it["auto"] = 0.0
            _sh["auto"].discard(key)
            sh_log("手动停用", key, "off")
            _sh["dirty"] = True
    save_config({"disabledSources": sorted(keys)})
    sh_save(True)
    sh_drop_cache()
    return {"ok": True, "key": key, "enabled": bool(on),
            "off": bool(src_off(key)), "state": _src_state(key)}


def _src_state(key):
    srcs = sources_cached()
    if key in _sh["auto"]:
        return "auto_off"
    if key in _f["disabled"]:
        return "manual_off"
    if (srcs.get(key) or {}).get("disabled"):
        return "luna_off"
    return "on"

_admin = {"html": None, "mtime": 0.0}


def render_admin():
    try:
        mt = os.path.getmtime(ADMIN_PATH)
        if _admin["html"] is None or mt != _admin["mtime"]:
            with open(ADMIN_PATH, "r", encoding="utf-8") as fp:
                _admin["html"] = fp.read().replace("__SUB__", SELF + "/api/tvbox")
            _admin["mtime"] = mt
        return _admin["html"]
    except Exception as e:
        return "<h1>管理页面缺失</h1><p>%s</p>" % e


def logo_updated():
    try:
        return int(os.path.getmtime(LOGO_PATH))
    except OSError:
        return 0


def make_backup():
    if not os.path.exists(CONFIG_PATH):
        return
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        name = time.strftime("config-%Y%m%d-%H%M%S.json")
        with open(CONFIG_PATH, "r", encoding="utf-8") as src:
            content = src.read()
        with open(os.path.join(BACKUP_DIR, name), "w", encoding="utf-8") as dst:
            dst.write(content)
        keep = max(1, int(_f.get("keep") or 5))
        files = sorted(f for f in os.listdir(BACKUP_DIR) if f.startswith("config-"))
        for f in files[:-keep]:
            os.remove(os.path.join(BACKUP_DIR, f))
    except Exception as e:
        print("[backup] failed:", e, flush=True)


_luna = {"cfg": None, "ts": 0.0, "dur": 0.0, "err": None, "building": False,
         "hash": None, "changed": True}
_luna_lock = threading.Lock()
_cms_cache = {}          # (key, query) -> (ts, body, ctype)
CMS_BUDGET = max(4, int(os.environ.get("CMS_BUDGET_MB", "48"))) * 1024 * 1024
_cms_lock = threading.Lock()
CMS_TTL = 240            # 内容列表缓存秒数
CMS_TTL_CLASS = 600      # 分类列表缓存秒数（几乎不变）


def save_cfg_disk(cfg):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump(cfg, fp, ensure_ascii=False)
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        print("[cache] save failed:", e, flush=True)


def load_cfg_disk():
    """启动时加载上次的配置，避免冷启动等 LunaTV 构建"""
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as fp:
            cfg = json.load(fp)
        if isinstance(cfg, dict) and cfg.get("sites"):
            _luna["cfg"] = cfg
            _luna["ts"] = os.path.getmtime(CACHE_FILE)
            print("[cache] disk config loaded, sites=%d" % len(cfg["sites"]), flush=True)
            return True
    except Exception:
        pass
    return False


def fetch_luna_cfg():
    """拉取并缓存 LunaTV 原生 TVBox 配置（慢操作，只在后台/首次执行）"""
    t0 = time.time()
    try:
        raw = urllib.request.urlopen(LUNA + "/api/tvbox", timeout=90).read().decode("utf-8", "ignore")
        cfg = json.loads(raw)
        h = hashlib.sha1(json.dumps(cfg, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        _luna["changed"] = (h != _luna.get("hash"))    # 内容真变了才算变化， ts 每次拉取都会更新
        _luna["hash"] = h
        _luna["cfg"] = cfg
        _luna["ts"] = time.time()
        _luna["dur"] = time.time() - t0
        _luna["err"] = None
        save_cfg_disk(cfg)
        print("[tvbox] refreshed in %.2fs, sites=%d" % (_luna["dur"], len(cfg.get("sites") or [])), flush=True)
        return cfg
    except Exception as e:
        _luna["err"] = str(e)
        print("[tvbox] refresh failed:", e, flush=True)
        return None
    finally:
        _luna["building"] = False


def refresh_async():
    """后台刷新，避免阻塞机顶盒请求"""
    if _luna["building"]:
        return
    if not _luna_lock.acquire(blocking=False):
        return
    try:
        if _luna["building"]:
            return
        _luna["building"] = True
        threading.Thread(target=fetch_luna_cfg, daemon=True).start()
    finally:
        _luna_lock.release()


def luna_cfg():
    """取配置：有缓存立刻返回，过期则触发后台刷新；无缓存才同步等一次"""
    ttl = max(60, int(_f.get("tvcache_min") or 5) * 60)
    age = time.time() - _luna["ts"] if _luna["ts"] else 1e9
    if _luna["cfg"] is not None:
        if _f.get("sync_on", True) and age > ttl:
            refresh_async()
        return _luna["cfg"]
    with _luna_lock:
        if _luna["cfg"] is None:
            fetch_luna_cfg()
    return _luna["cfg"]


_pf = {"done": False, "ts": 0.0}
_warm_full = {"done": False}
TMDB_WARM_MAX = int(os.environ.get("TMDB_WARM_MAX", "400"))


def prefetch_cms():
    """后台预热各源分类列表，机顶盒进入站点时直接命中缓存"""
    _pf["ts"] = time.time()
    try:
        srcs = sources_cached()
        keys = ordered_source_keys("deliver")   # 自动排序开启时优先热快的源
        act0 = fo_active() or _f.get("src_active") or ""
        if act0:
            keys = [k for k in keys if k == act0] or keys[:1]
        elif int(_f.get("src_limit") or 0) > 0:
            keys = keys[: int(_f["src_limit"])]
        if not keys:
            return

        tasks = []
        for k in keys:
            tasks.append((k, "?ac=list"))
            tasks.append((k, "?ac=videolist&pg=1"))

        def one(t):
            k, q = t
            try:
                urllib.request.urlopen(SELF + "/cms/" + k + q, timeout=60).read()
            except Exception:
                pass

        workers = min(8, max(3, (len(keys) + 1) // 2))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(one, tasks))
        print("[cms] prefetched %d sources / %d tasks (workers=%d)" % (len(keys), len(tasks), workers), flush=True)
        _pf["done"] = True
        # 分类内容探测（后台，静默剔除点开空的分类）
        for k in keys:
            if cat_probe_stale(k):
                threading.Thread(target=probe_cats, args=(k,), daemon=True).start()
        # 封面预热 + 缓存体积控制（首次启动全量预热，此后只热前几个分类）
        # 首次只热首页 + 前 8 个分类、最多 20 个源；其余封面由 /img 按需抓（避免开机狂抓几百张图）
        warm_images(keys if _warm_full["done"] else keys[:20],
                    ncat=(6 if _warm_full["done"] else 8))
        _warm_full["done"] = True
        img_cleanup()
    except Exception as e:
        print("[cms] prefetch failed:", e, flush=True)


CAT_PROBE = os.path.join(CACHE_DIR, "cat-probe.json")
CAT_PROBE_TTL = 6 * 3600
JUNK = re.compile(r"(请勿删除|請勿刪除|同步图片|同步圖片|占位|测试片|test\s*only)", re.I)
_cat_probe = {"data": {}, "running": set()}
_cat_probe_lock = threading.Lock()


CLASS_MAP = os.path.join(CACHE_DIR, "class-map.json")
_cls_map = {}
_cls_lock = threading.Lock()


def cls_map_load():
    try:
        with open(CLASS_MAP, encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict):
                    _cls_map[k] = v
        print("[classmap] loaded %d sources" % len(_cls_map), flush=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        print("[classmap] load fail:", e, flush=True)


def cls_map_save():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = CLASS_MAP + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_cls_map, f, ensure_ascii=False)
        os.replace(tmp, CLASS_MAP)
    except Exception as e:
        print("[classmap] save fail:", e, flush=True)


def cls_map_learn(key, classes):
    """记住 分类名 -> type_id，TAI 这类源偶尔只返回几个分类时用来补"""
    with _cls_lock:
        m = _cls_map.setdefault(key, {})
        changed = False
        for c in classes:
            if not isinstance(c, dict):
                continue
            nm = str(c.get("type_name") or "").strip()
            tid = c.get("type_id")
            if nm and tid is not None and str(m.get(nm)) != str(tid):
                m[nm] = tid
                changed = True
        if changed:
            cls_map_save()
        return dict(m)


def cls_map_fill(key, classes):
    """上游分类列表明显缩水时，用历史映射补齐顺序与 id"""
    have = {str(c.get("type_name") or "").strip() for c in classes if isinstance(c, dict)}
    with _cls_lock:
        m = dict(_cls_map.get(key) or {})
    if len(m) < 8 or len(classes) >= len(m) * 0.7:
        return classes
    out = list(classes)
    for nm, tid in m.items():
        if nm and nm not in have:
            out.append({"type_id": tid, "type_name": nm})
    if len(out) > len(classes):
        print("[classmap] %s degraded %d -> %d" % (key, len(classes), len(out)), flush=True)
    return out


def cat_probe_load():
    try:
        with open(CAT_PROBE, "r", encoding="utf-8") as fp:
            d = json.load(fp)
        for k, v in (d.get("data") or {}).items():
            _cat_probe["data"][k] = v
        print("[probe] loaded %d source probes" % len(_cat_probe["data"]), flush=True)
    except Exception:
        pass


def cat_probe_save():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = CAT_PROBE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump({"data": _cat_probe["data"]}, fp, ensure_ascii=False)
        os.replace(tmp, CAT_PROBE)
    except Exception as e:
        print("[warn] save cat probe:", e, flush=True)


def cat_probe_stale(key):
    d = _cat_probe["data"].get(key)
    return not d or time.time() - float(d.get("ts") or 0) > CAT_PROBE_TTL


def cat_empty_set(key):
    """返回该源内容为空的分类名集合（探测失败/样本不足时返回空集合，不裁剪）"""
    d = _cat_probe["data"].get(key)
    if not d or time.time() - float(d.get("ts") or 0) > CAT_PROBE_TTL:
        return set()
    totals = d.get("totals") or {}
    usable = d.get("usable") or {}
    known = [v for v in totals.values() if v is not None]
    if len(known) < max(3, int(len(totals) * 0.8)):
        return set()
    if len([n for n, v in totals.items() if v]) < min(5, max(2, len(totals) // 2)):
        return set()
    empty = set(n for n, v in totals.items() if v is not None and v <= 3)
    # 首页全是占位/垃圾条目的分类一并剔除（如「电影」只有 3 条“同步图片专用”）
    for n, u in usable.items():
        if u == 0 and totals.get(n):
            empty.add(n)
    return empty


def probe_cats(key):
    """后台探测某源各分类的实际内容量，用于剔除机顶盒上点开空空如也的分类"""
    with _cat_probe_lock:
        if key in _cat_probe["running"]:
            return
        _cat_probe["running"].add(key)
    try:
        meta = sources_cached().get(key)
        if not meta:
            return
        api = meta["api"]

        def up(q):
            u = api + ("&" if "?" in api else "?") + q
            req = urllib.request.Request(u, headers={"User-Agent": "TVBox/1.0.0"})
            return json.loads(urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore"))

        d = up("ac=list")
        pairs = [(str(c.get("type_id")), c.get("type_name")) for c in (d.get("class") or [])
                 if c.get("type_id") is not None and c.get("type_name")]
        if not pairs:
            return

        def one(p):
            tid, name = p
            try:
                r = up("ac=videolist&t=" + urllib.parse.quote(tid) + "&pg=1")
                t = r.get("total")
                lst = r.get("list") or []
                n = len(lst)
                try:
                    t = int(t)
                except Exception:
                    t = n
                good = 0
                for v in lst:
                    nm = v.get("vod_name") or ""
                    if bad_cat(v.get("type_name") or "") or bad_title(nm) or JUNK.search(nm or ""):
                        continue
                    good += 1
                return name, (t, good)
            except Exception:
                return name, (None, None)

        totals, usable = {}, {}
        with ThreadPoolExecutor(max_workers=5) as ex:
            for name, (tot, good) in ex.map(one, pairs):
                totals[name] = tot
                if good is not None:
                    usable[name] = good
        _cat_probe["data"][key] = {"ts": time.time(), "totals": totals, "usable": usable}
        cat_probe_save()
        empt = sorted(n for n, v in totals.items() if v == 0 or (usable.get(n) == 0 and v))
        print("[probe] %s %d cats, empty=%d %s" % (key, len(totals), len(empt), ",".join(empt[:8])), flush=True)
    except Exception as e:
        print("[probe] %s failed: %s" % (key, e), flush=True)
    finally:
        with _cat_probe_lock:
            _cat_probe["running"].discard(key)


def warm_loop():
    """后台定时预热，保证缓存始终是热的"""
    time.sleep(1)
    while True:
        try:
            sync_on = bool(_f.get("sync_on", True))
            if _luna["cfg"] is None or (sync_on and time.time() - _luna["ts"] > max(60, int(_f.get("tvcache_min") or 5) * 60) * 0.9):
                before = _luna.get("hash")
                fetch_luna_cfg()
                # 只有配置内容真的变了才重新预热全部源（原来每次刷新都预热，上游流量翻好几倍）
                if _luna.get("changed") and _luna.get("hash") != before \
                        and time.time() - float(_pf.get("ts") or 0) > 1200:
                    prefetch_cms()
            elif not _pf["done"]:
                prefetch_cms()
            stats_save()
            devices_save()
            searchstats_save()
            fo_probe()
            fo_save()
            sh_tick()
            sh_save()
        except Exception as e:
            print("[warm] error:", e, flush=True)
        time.sleep(15)


# ---------------- 封面图缓存代理 ----------------
IMG_DIR = os.path.join(CACHE_DIR, "img")
IMG_TTL_DEF = 30 * 86400
IMG_BUDGET_DEF = int(float(os.environ.get("IMG_BUDGET_MB", "256")) * 1048576)


def img_ttl():
    """封面保留天数（可在「本地缓存」里改，1 ~ 365 天）"""
    return max(1, min(365, int(_f.get("img_keep_days") or 30))) * 86400


def img_budget():
    """封面缓存容量上限（可在「本地缓存」里改，32MB ~ 4GB）"""
    # 没配过就用环境变量 / 默认 256MB，兼容老部署
    mb = int(_f.get("img_max_mb") or 0)
    return (max(32, min(4096, mb)) * 1048576) if mb else IMG_BUDGET_DEF

CT_BY_EXT = {".webp": "image/webp", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
             ".png": "image/png", ".gif": "image/gif", ".avif": "image/avif"}
_img_locks = {}
_img_locks_guard = threading.Lock()
_img_stats = {"ts": 0.0, "n": 0, "b": 0}
_img_fail = {}           # url -> (连续失败次数, 禁止重试到的时间戳)
_img_fail_lock = threading.Lock()
IMG_FAIL_BASE = 600      # 首败冷却 10 分钟，连败翻倍，最长 1 小时；防止挂掉的封面被反复重抓浪费流量


def img_fail_skip(url):
    with _img_fail_lock:
        _f, until = _img_fail.get(url, (0, 0.0))
    return time.time() < until


def img_fail_note(url, ok=False):
    with _img_fail_lock:
        if ok:
            _img_fail.pop(url, None)
            return
        f = int(_img_fail.get(url, (0, 0.0))[0]) + 1
        _img_fail[url] = (f, time.time() + min(3600, IMG_FAIL_BASE * f))
        if len(_img_fail) > 3000:
            _img_fail.clear()


def _img_key(url):
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:24]


def _img_guess_ct(url):
    path = urllib.parse.urlparse(url).path.lower()
    for ext, ct in CT_BY_EXT.items():
        if path.endswith(ext):
            return ct
    return "image/jpeg"


def img_cached(url):
    fp = os.path.join(IMG_DIR, _img_key(url))
    try:
        st = os.stat(fp)
        if st.st_size > 512 and time.time() - st.st_mtime < img_ttl():
            return fp, st.st_size
    except OSError:
        pass
    return None, 0


def ensure_img(url):
    """确保本地有这张图，返回 (路径, content-type)；失败返回 (None, None)"""
    if img_fail_skip(url):          # 刚失败过的不再立刻重试，避免反复打上游
        return None, None
    fp, _ = img_cached(url)
    if fp:
        return fp, _img_guess_ct(url)
    with _img_locks_guard:
        lock = _img_locks.get(url)
        if lock is None:
            lock = _img_locks[url] = threading.Lock()
    with lock:
        fp, _ = img_cached(url)
        if fp:
            return fp, _img_guess_ct(url)
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"})
            r = urllib.request.urlopen(req, timeout=20)
            data = r.read()
            if len(data) < 512:
                raise ValueError("too small (%d)" % len(data))
            ct = (r.headers.get("Content-Type") or "").split(";")[0].strip()
            if not ct.startswith("image/"):
                ct = _img_guess_ct(url)
            os.makedirs(IMG_DIR, exist_ok=True)
            dest = os.path.join(IMG_DIR, _img_key(url))
            tmp = dest + ".tmp"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, dest)
            img_fail_note(url, ok=True)
            return dest, ct
        except Exception as e:
            img_fail_note(url)
            print("[img] fail %-40s %s" % (str(e)[:40], url[-50:]), flush=True)
            return None, None
        finally:
            with _img_locks_guard:
                _img_locks.pop(url, None)


def _sniff_ct(b):
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "image/webp"
    if b[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if b[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if b[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if b[4:12] == b"ftypavif":
        return "image/avif"
    return None


def img_stats():
    if time.time() - float(_img_stats["ts"] or 0) < 30:
        return _img_stats
    n = b = 0
    try:
        with os.scandir(IMG_DIR) as it:
            for e in it:
                try:
                    if e.is_file():
                        n += 1
                        b += e.stat().st_size
                except OSError:
                    pass
    except FileNotFoundError:
        pass
    except Exception:
        pass
    _img_stats.update(ts=time.time(), n=n, b=b)
    return _img_stats


def img_cleanup():
    """按「保留天数 + 容量上限」清理封面缓存，返回 (删除数, 释放字节)"""
    budget = img_budget()
    ttl = img_ttl()
    now = time.time()
    n = freed = 0
    try:
        files = []
        total = 0
        expired = []
        with os.scandir(IMG_DIR) as it:
            for e in it:
                try:
                    st = e.stat()
                except OSError:
                    continue
                if now - st.st_mtime > ttl:
                    expired.append(e.path)        # 超过保留期的老图直接删，不占名额
                    continue
                files.append((st.st_mtime, st.st_size, e.path))
                total += st.st_size
    except FileNotFoundError:
        return 0, 0
    except Exception as e:
        print("[img] cleanup scan error:", e, flush=True)
        return 0, 0
    for fp in expired:
        try:
            sz = os.path.getsize(fp)
            os.remove(fp)
            n += 1
            freed += sz
        except OSError:
            pass
    if total > budget:
        files.sort()                              # 最旧的先删，直到降到上限的 80%
        for mt, sz, fp in files:
            if total <= budget * 0.8:
                break
            try:
                os.remove(fp)
                total -= sz
                n += 1
                freed += sz
            except OSError:
                pass
        print("[img] trimmed to %.1fMB（上限 %dMB）" % (total / 1048576.0, budget // 1048576), flush=True)
    else:
        print("[img] cache %d files / %.1fMB（上限 %dMB · 保留 %d 天）"
              % (len(files), total / 1048576.0, budget // 1048576, ttl // 86400), flush=True)
    if n:
        _img_stats.update(ts=0.0)                 # 让统计下次重算
    return n, freed


# ---------------- TMDB 封面兜底 ----------------
TMDB_TOKEN = os.environ.get("TMDB_TOKEN") or ""
TMDB_LANG = os.environ.get("TMDB_LANG", "zh-CN")
TMDB_FILE = os.path.join(CACHE_DIR, "tmdb.json")
TMDB_TTL_DEF = 60 * 86400
TMDB_MISS_TTL = 12 * 3600


def tmdb_ttl():
    """TMDB 结果保留天数（可在「本地缓存」里改，1 ~ 365 天）"""
    return max(1, min(365, int(_f.get("tmdb_keep_days") or 60))) * 86400
TMDB_IMG = "https://image.tmdb.org/t/p/w500"
_tmdb = {"map": {}, "lock": threading.Lock(), "dirty": False, "ts": 0.0, "err": 0}
_tmdb_gate = {"t": 0.0, "lock": threading.Lock()}


def _clean_name(name):
    """把「狂飙(2023)更新至39集」这类名字洗成可搜索的片名"""
    s = str(name or "").strip()
    if not s:
        return ""
    s = re.sub(r"[\(\[【][^\)\]】]{0,40}[\)\]】]", " ", s)
    s = re.sub(r"(更新至|更新到|更至|全)\s*\d+\s*[集话期]?", " ", s)
    s = re.sub(r"\b(4K|1080P?|720P|2160P|HD|BD|WEB-?DL|TC|TS|DVD|HDR)\b", " ", s, flags=re.I)
    s = re.sub(r"(国语|粤语|英语|中字|双语|高清|超清|完整版|未删减|抢先版|电影版|电视剧|动漫版|合集|大结局)\s*$", " ", s)
    s = re.sub(r"(?<=[^\d\s])\s*(19|20)\d{2}\s*$", " ", s)   # 片名后粘连的年份
    s = re.sub(r"[·•:：\-_\s]+", " ", s).strip()
    return s[:40]


def tmdb_load():
    try:
        with open(TMDB_FILE, encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            _tmdb["map"] = {k: v for k, v in d.items() if isinstance(v, dict)}
            print("[tmdb] loaded %d entries" % len(_tmdb["map"]), flush=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        print("[tmdb] load fail:", e, flush=True)


def tmdb_save(force=False):
    with _tmdb["lock"]:
        if not _tmdb["dirty"]:
            return
        if not force and time.time() - float(_tmdb["ts"] or 0) < 20:
            return
        _tmdb["dirty"] = False
        _tmdb["ts"] = time.time()
        data = dict(_tmdb["map"])
    try:
        tmp = TMDB_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False))
        os.replace(tmp, TMDB_FILE)
    except Exception as e:
        print("[tmdb] save fail:", e, flush=True)


def tmdb_prune():
    """删掉超过保留期的 TMDB 记录，防止 tmdb.json 只涨不消"""
    ttl = tmdb_ttl()
    now = time.time()
    with _tmdb["lock"]:
        old = [k for k, v in _tmdb["map"].items()
               if not isinstance(v, dict) or now - float(v.get("ts") or 0) > ttl]
        for k in old:
            _tmdb["map"].pop(k, None)
        if old:
            _tmdb["dirty"] = True
    if old:
        tmdb_save(True)
        print("[tmdb] pruned %d expired entries" % len(old), flush=True)
    return len(old)


def _tmdb_api(path, **kw):
    q = urllib.parse.urlencode(kw)
    req = urllib.request.Request("https://api.themoviedb.org/3" + path + "?" + q,
                                 headers={"Authorization": "Bearer " + TMDB_TOKEN,
                                          "Accept": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=6).read().decode("utf-8", "ignore"))


def _tmdb_hit_cached(name, year=""):
    nm = _clean_name(name)
    if not nm:
        return True
    key = nm + ("@" + str(year)[:4] if year else "")
    with _tmdb["lock"]:
        e = _tmdb["map"].get(key)
    if not isinstance(e, dict):
        return False
    ttl = tmdb_ttl() if e.get("p") else TMDB_MISS_TTL
    return time.time() - float(e.get("ts") or 0) < ttl


def tmdb_poster(name, year=""):
    """按片名查 TMDB 海报，返回完整图片地址；查不到返回空串（结果落盘，失败短缓存）"""
    if not TMDB_TOKEN or not _f.get("tmdb_on", True):
        return ""
    nm = _clean_name(name)
    if not nm:
        return ""
    if not year:                                   # 名字里带的年份当线索
        m = re.search(r"(19|20)\d{2}", str(name))
        if m and re.search(r"[^\d\s]", str(name)):
            year = m.group(0)
    key = nm + ("@" + str(year)[:4] if year else "")
    now = time.time()
    cached = None
    with _tmdb["lock"]:
        e = _tmdb["map"].get(key)
        if isinstance(e, dict):
            ttl = tmdb_ttl() if e.get("p") else TMDB_MISS_TTL
            if now - float(e.get("ts") or 0) < ttl:
                cached = e.get("p") or ""
    if cached is not None:
        bump("hit" if cached else "miss")
        return (TMDB_IMG + cached) if cached else ""
    if now < float(_tmdb.get("down_until") or 0):
        return ""
    # 温柔限速：TMDB 50 req/s，这里最密 25 req/s
    with _tmdb_gate["lock"]:
        wait = _tmdb_gate["t"] - time.time()
        if wait > 0:
            time.sleep(min(wait, 0.5))
        _tmdb_gate["t"] = time.time() + 0.04
    try:
        d = _tmdb_api("/search/multi", query=nm, language=TMDB_LANG, include_adult="false")
    except Exception as e:
        _tmdb["err"] += 1
        _tmdb["down_until"] = time.time() + 300   # 熔断 5 分钟，避免每张图都卡超时
        print("[tmdb] search fail %-20s %s" % (nm, str(e)[:40]), flush=True)
        return ""
    cands = []
    for x in (d.get("results") or []):
        if not x.get("poster_path"):
            continue
        t = x.get("title") or x.get("name") or ""
        ot = x.get("original_title") or x.get("original_name") or ""
        score = 0.0
        if t == nm or ot == nm:
            score += 100
        elif nm and (nm in t or t in nm):
            score += 30
        if year:
            yy = str(x.get("release_date") or x.get("first_air_date") or "")[:4]
            if yy == str(year)[:4]:
                score += 25
            elif yy and abs(int(yy) - int(str(year)[:4] or 0)) <= 1:
                score += 10
        try:
            score += min(int(x.get("vote_count") or 0), 8000) / 8000.0 * 12
        except Exception:
            pass
        try:
            score += min(float(x.get("popularity") or 0), 200) / 200.0 * 3
        except Exception:
            pass
        cands.append((score, x.get("poster_path"), t))
    best = max(cands, key=lambda z: z[0]) if cands else None
    p = best[1] if best else ""
    with _tmdb["lock"]:
        _tmdb["map"][key] = {"p": p, "ts": now, "t": (best[2] if best else "")}
        _tmdb["dirty"] = True
    bump("hit" if p else "miss")
    tmdb_save()
    print("[tmdb] %-22s -> %s" % (nm, (p or "未收录")), flush=True)
    return TMDB_IMG + p if p else ""


def img_clear():
    """手动清空封面缓存（测试用，清完下次访问会重新拉）"""
    n = freed = 0
    try:
        with os.scandir(IMG_DIR) as it:
            for e in it:
                try:
                    if e.is_file():
                        sz = e.stat().st_size
                        os.remove(e.path)
                        n += 1
                        freed += sz
                except OSError:
                    pass
    except FileNotFoundError:
        pass
    except Exception as e:
        print("[img] clear fail:", e, flush=True)
    _img_stats.update(ts=0.0, n=0, b=0)
    print("[img] cleared %d files / %.1fMB" % (n, freed / 1048576.0), flush=True)
    return n, freed


def proxify_pic(u, name="", year=""):
    if not _f.get("tmdb_on", True):
        name = ""
    extra = ""
    if name:
        extra = "&n=" + urllib.parse.quote(str(name)[:60], safe="")
        if year:
            extra += "&y=" + urllib.parse.quote(str(year)[:4], safe="")
    if not _f.get("img_on", True):
        # 封面缓存关闭：源图原样交给机顶盒，网关不中转、不落盘
        if isinstance(u, str) and u.strip():
            return u
        return SELF + "/img?u=" + extra if extra else ""
    if not isinstance(u, str) or not u or u.startswith(SELF):
        return u if u else (SELF + "/img?u=" + extra if extra else "")
    if not (u.startswith("http://") or u.startswith("https://")):
        return SELF + "/img?u=" + extra if extra else u
    try:
        host = urllib.parse.urlsplit(u).netloc
    except Exception:
        return SELF + "/img?u=" + extra if extra else ""
    if not host or ".." in host or host.startswith(".") or host.endswith(":") or "#err" in u:
        return SELF + "/img?u=" + extra if extra else ""
    return SELF + "/img?u=" + urllib.parse.quote(u, safe="") + extra


def abs_pic(u, base=""):
    """把源站返回的相对封面路径补成完整地址（如 zzoc 的 upload/vod/xxx.jpg）"""
    if not isinstance(u, str):
        return u
    u = u.strip()
    if not u or u.startswith(SELF):
        return u
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if not base:
        return u
    try:
        p = urllib.parse.urlsplit(base if "//" in base else "http://" + base)
        origin = (p.scheme or "https") + "://" + p.netloc
    except Exception:
        return u
    if not p.netloc or "." not in p.netloc:
        return u
    return origin + (u if u.startswith("/") else "/" + u)


def proxify_item(v, base=""):
    name = v.get("vod_name") or ""
    year = v.get("vod_year") or ""
    for k in ("vod_pic", "vod_pic_thumb", "vod_pic_slide", "vod_pic_screenshot"):
        val = v.get(k)
        if not val or not isinstance(val, str):
            continue
        parts = [proxify_pic(abs_pic(x, base), name, year) for x in val.split("$$")]
        v[k] = "$$".join([p for p in parts if p])
    if not v.get("vod_pic") and name and _f.get("tmdb_on", True):
        v["vod_pic"] = proxify_pic("", name, year)


def _pics_from_payload(raw):
    """返回 [(原图地址, 片名, 年份)]，地址可能为空（交给 TMDB 兜底）"""
    out = []
    try:
        d = json.loads(raw)
    except Exception:
        return out
    for v in (d.get("list") or []):
        if not isinstance(v, dict):
            continue
        name = v.get("vod_name") or ""
        pic = v.get("vod_pic") or ""
        if isinstance(pic, str) and pic.startswith(SELF):
            try:
                qs = urllib.parse.parse_qs(urllib.parse.urlsplit(pic).query)
                pic = (qs.get("u") or [""])[0]
            except Exception:
                pic = ""
        if not (isinstance(pic, str) and pic.startswith("http")):
            pic = ""
        if pic or name:
            out.append((pic, name, str(v.get("vod_year") or "")))
    return out


def warm_images(keys, ncat=6):
    """把首页与各分类页的封面先抓到本地，机顶盒打开时直接命中"""
    if not _f.get("img_on", True):     # 封面缓存关闭：不再预热
        return
    for k in keys:
        try:
            pages = ["?ac=videolist&pg=1"]
            try:
                cls = json.loads(urllib.request.urlopen(SELF + "/cms/" + k + "?ac=list", timeout=30).read())
                pages += ["?ac=videolist&t=" + urllib.parse.quote(str(c.get("type_id"))) + "&pg=1"
                          for c in (cls.get("class") or [])[:ncat] if c.get("type_id") is not None]
            except Exception:
                pass

            def getp(q):
                try:
                    return _pics_from_payload(urllib.request.urlopen(SELF + "/cms/" + k + q, timeout=60).read())
                except Exception:
                    return []

            with ThreadPoolExecutor(max_workers=4) as ex:
                lists = list(ex.map(getp, pages))
            items, seen = [], set()
            for sub in lists:
                for u, n, y in sub:
                    kk = u or ("\x00" + str(n))
                    if kk in seen:
                        continue
                    seen.add(kk)
                    items.append((u, n, y))
            if not items:
                continue

            def need(t):
                u, n, y = t
                if u:
                    return not img_cached(u)[0]
                return bool(n) and not _tmdb_hit_cached(n, y)

            todo = [t for t in items if need(t)]
            tmdb_only = [t for t in todo if not t[0]][:TMDB_WARM_MAX] if _f.get("tmdb_on", True) else []
            if _f.get("tmdb_on", True):
                todo = [t for t in todo if t[0]] + tmdb_only
            else:
                todo = [t for t in todo if t[0]]

            def warm_one(t):
                u, n, y = t
                q = "u=" + urllib.parse.quote(u, safe="")
                if n:
                    q += "&n=" + urllib.parse.quote(str(n)[:60], safe="")
                if y:
                    q += "&y=" + urllib.parse.quote(str(y)[:4], safe="")
                try:
                    urllib.request.urlopen(SELF + "/img?" + q, timeout=40).read()
                except Exception:
                    pass

            if todo:
                with ThreadPoolExecutor(max_workers=8) as ex:
                    list(ex.map(warm_one, todo))
            try:
                tmdb_save(True)
            except Exception:
                pass
            print("[img] %s pages=%d covers=%d fetched=%d (tmdb %d)" %
                  (k, len(pages), len(items), len(todo), len(tmdb_only)), flush=True)
        except Exception as e:
            print("[img] warm %s failed: %s" % (k, e), flush=True)


SEP = "~"                      # 聚合结果里的来源标记：<key>~<上游 id>
_search_cache = {}             # query -> (ts, payload)
_search_lock = threading.Lock()
SEARCH_TTL = 150
SEARCH_MAX_PER_NAME = 2        # 同名最多保留几个来源，避免刷屏


def query_key(query):
    """去掉 _ 时间戳等易变参数，得到稳定的缓存键"""
    parts = []
    for kv in str(query or "").split("&"):
        if not kv:
            continue
        k = kv.split("=", 1)[0]
        if k in ("_", "extend", "quick"):
            continue
        parts.append(kv)
    parts.sort()
    return "&".join(parts)


def ordered_source_keys(scope="all"):
    """已启用源，按配置顺序；开了「按响应时间自动排序」时，快的排最前

    scope: deliver = 下发机顶盒的顺序；agg = 聚合采集源的取源顺序；all = 两者
    自动排序只在网关内部生效，不会改写用户手动拖出来的 sourceOrder
    """
    srcs = sources_cached()
    keys = [k for k in srcs if not src_off(k)]
    order = {k: i for i, k in enumerate(_f["order"])}
    keys.sort(key=lambda k: order.get(k, len(order) + 100))
    sc = str(_f.get("asort_scope") or "all")
    if _f.get("asort_on") and (sc == "all" or sc == scope):
        keys.sort(key=sh_sort_key)      # 稳定排序：同档位内保留手动顺序
    return keys


def pick_delivered(keys):
    """实际下发给机顶盒的源（与 handle_tvbox 一致）"""
    act = _f.get("src_active") or ""
    lim = int(_f.get("src_limit") or 0)
    ovr = fo_active()          # 主源异常时自动顶上的备源
    if ovr:
        rest = [k for k in keys if k != ovr]
        if act:
            return [ovr]
        if lim > 0:
            return ([ovr] + rest)[:lim]
        return [ovr] + rest
    if act:
        one = [k for k in keys if k == act]
        return one or keys[:1]
    if lim > 0:
        return keys[:lim]
    return list(keys)


def search_primary():
    """负责聚合的主源：下发列表里的第一个（与订阅顺序一致）"""
    keys = ordered_source_keys()
    if not keys:
        return ""
    delivered = pick_delivered(keys)
    return delivered[0] if delivered else keys[0]


def search_extras(base_key=None):
    """可参与聚合的「额外源」：除主源以外的全部启用源（已下发的也算）"""
    keys = ordered_source_keys()
    primary = base_key or search_primary()
    return [k for k in keys if k != primary]


# ---------------- 备源 / 主源自动切换 ----------------
# 主源连续失败到阈值就把备源顶到第一位；备源也挂就按备源顺序继续往下切；原主源恢复后自动切回
FO_FILE = os.path.join(CACHE_DIR, "failover.json")
FO_COOL = 600        # 失败计数窗口：超过 10 分钟重新计数
FO_PROBE_SEC = 180   # 挂在备源上时，每隔 3 分钟探测一次原主源
_fo = {"active": "", "fail": {}, "log": [], "since": 0.0, "switches": 0,
       "probe_ts": 0.0, "probe_ok": 0, "dirty": False, "lock": threading.Lock()}


def fo_load():
    try:
        with open(FO_FILE, "r", encoding="utf-8") as fp:
            d = json.load(fp)
        _fo["active"] = str(d.get("active") or "")
        _fo["since"] = float(d.get("since") or 0)
        _fo["switches"] = int(d.get("switches") or 0)
        _fo["log"] = [x for x in (d.get("log") or []) if isinstance(x, dict)][-20:]
        if _fo["active"]:
            print("[failover] 恢复上次自动切换状态: %s" % _fo["active"], flush=True)
    except Exception:
        pass


def fo_save(force=False):
    if not force and not _fo.get("dirty"):
        return
    tmp = FO_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump({"active": _fo["active"], "since": _fo["since"],
                       "switches": _fo["switches"], "log": _fo["log"][-20:]},
                      fp, ensure_ascii=False)
        os.replace(tmp, FO_FILE)
        _fo["dirty"] = False
    except Exception as e:
        print("[failover] save fail:", e, flush=True)


def fo_conf_primary():
    """配置里的主源：activeSource 优先，否则是片源顺序里的第一个启用源"""
    keys = ordered_source_keys()
    if not keys:
        return ""
    act = _f.get("src_active") or ""
    if act:
        one = [k for k in keys if k == act]
        return one[0] if one else keys[0]
    return keys[0]


def fo_active():
    """自动切换后真正生效的主源（空 = 用配置主源）"""
    act = _fo.get("active") or ""
    if not act:
        return ""
    if act not in sources_cached() or src_off(act):
        return ""
    return act


def fo_primary():
    return fo_active() or fo_conf_primary()


def fo_thresh():
    return max(1, min(10, int(_f.get("fo_thresh") or 3)))


def fo_backups():
    """备源顺序：用户指定的优先；没指定就自动用其它启用源（按片源顺序）"""
    cand = [k for k in (_f.get("fo_list") or []) if k in sources_cached() and not src_off(k)]
    if not cand:
        cand = ordered_source_keys()
    seen, out = set(), []
    for k in cand:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def fo_candidates():
    """当前主源之后可切的目标：按备源顺序，跳过熔断中/屡次失败的"""
    cur = fo_primary()
    th = fo_thresh()
    now = time.time()
    out = []
    for k in fo_backups():
        if k == cur:
            continue
        r = _fo["fail"].get(k) or {}
        if int(r.get("n") or 0) >= th and now - float(r.get("ts") or 0) < FO_COOL:
            continue
        if src_cooling(k):
            continue
        out.append(k)
    return out


def fo_reset(silent=False):
    """回到配置里的主源"""
    with _fo["lock"]:
        was = _fo.get("active") or ""
        _fo["active"] = ""
        _fo["since"] = 0.0
        _fo["fail"] = {}
        _fo["probe_ok"] = 0
        _fo["dirty"] = True
        if was and not silent:
            _fo["log"].append({"ts": time.time(), "from": was,
                               "to": fo_conf_primary(), "why": "手动切回配置主源"})
            _fo["log"] = _fo["log"][-20:]
    if was:
        print("[failover] 切回配置主源", flush=True)
        fo_save(True)


def fo_switch(why="上游连续失败"):
    """把备源顶上来；没有可用备源时保持原样并记录"""
    cur = fo_primary()
    cands = fo_candidates()
    conf = fo_conf_primary()
    if not cands:
        with _fo["lock"]:
            _fo["log"].append({"ts": time.time(), "from": cur, "to": "", "why": why + " · 无可用备源"})
            _fo["log"] = _fo["log"][-20:]
            _fo["dirty"] = True
        print("[failover] %s 无可用备源" % (cur or "-"), flush=True)
        fo_save(True)
        return ""
    nxt = cands[0]
    with _fo["lock"]:
        _fo["active"] = "" if nxt == conf else nxt
        _fo["since"] = time.time()
        _fo["switches"] = int(_fo.get("switches") or 0) + 1
        _fo["probe_ok"] = 0
        _fo["log"].append({"ts": time.time(), "from": cur, "to": nxt, "why": why})
        _fo["log"] = _fo["log"][-20:]
        _fo["dirty"] = True
    print("[failover] 主源切换 %s -> %s (%s)" % (cur or "-", nxt, why), flush=True)
    fo_save(True)
    return nxt


def fo_note(key, ok, reason=""):
    """上报一次上游结果：只有当前主源的失败累计，达到阈值就切备源"""
    if not _f.get("fo_on", True) or not key:
        return
    key = str(key)
    if key != fo_primary():
        if ok:
            _fo["fail"].pop(key, None)
        return
    if ok:
        if _fo["fail"].pop(key, None) is not None:
            _fo["dirty"] = True
        return
    now = time.time()
    with _fo["lock"]:
        r = _fo["fail"].get(key) or {"n": 0, "ts": now}
        if now - float(r.get("ts") or 0) > FO_COOL:
            r = {"n": 0, "ts": now}
        r["n"] = int(r.get("n") or 0) + 1
        r["ts"] = now
        r["why"] = str(reason or "")[:60]
        _fo["fail"][key] = r
        cnt = r["n"]
        _fo["dirty"] = True
    if cnt >= fo_thresh():
        fo_switch(reason or "上游连续失败")


def fo_probe(force=False):
    """挂在备源上时定期探测原主源，连续 2 次正常就切回"""
    if not _f.get("fo_on", True) or not fo_active():
        return None
    now = time.time()
    if not force and now - float(_fo.get("probe_ts") or 0) < FO_PROBE_SEC:
        return None
    _fo["probe_ts"] = now
    key = fo_conf_primary()
    meta = sources_cached().get(key)
    if not meta:
        return None
    try:
        d = _upstream_json(meta, "ac=class", 8)
        ok = isinstance(d, dict) and bool(d.get("class") or d.get("list"))
    except Exception as e:
        ok = False
        print("[failover] 原主源 %s 仍不可用: %s" % (key, str(e)[:50]), flush=True)
    with _fo["lock"]:
        _fo["probe_ok"] = (int(_fo.get("probe_ok") or 0) + 1) if ok else 0
        cnt = _fo["probe_ok"]
        if cnt >= 2:
            _fo["log"].append({"ts": time.time(), "from": fo_active(),
                               "to": key, "why": "原主源已恢复 · 自动切回"})
            _fo["log"] = _fo["log"][-20:]
            _fo["active"] = ""
            _fo["since"] = 0.0
            _fo["fail"] = {}
            _fo["probe_ok"] = 0
            _fo["dirty"] = True
    if cnt >= 2:
        print("[failover] 原主源恢复，切回 %s" % key, flush=True)
        fo_save(True)
    return ok


# 搜索失败熔断：连续失败 2 次的源临时退出聚合，避免每次冷搜都白等 10 秒，到期自动恢复
_src_fail = {}
_src_fail_lock = threading.Lock()
SRC_FAIL_COOL = 180


def src_fail_note(key, ok):
    with _src_fail_lock:
        if ok:
            _src_fail.pop(key, None)
            return
        r = _src_fail.get(key) or {}
        _src_fail[key] = {"n": int(r.get("n") or 0) + 1, "ts": time.time()}


def src_cooling(key):
    with _src_fail_lock:
        r = _src_fail.get(key)
    return bool(r) and int(r.get("n") or 0) >= 2 and time.time() - float(r.get("ts") or 0) < SRC_FAIL_COOL


def search_targets(base_key):
    """聚合搜索要打哪些上游：主源 + 额外源（数量受 searchLimit 限制）

    「额外源」指除主源之外的启用源，与是否已下发无关：
    机顶盒每个站点都会各自搜索，聚合只是让主源这一路的结果更全。
    """
    if not ordered_source_keys():
        return [base_key]
    if base_key != search_primary():   # 只有主源负责聚合，其它站点各搜各的，避免 N×N
        return [base_key]
    if not _f.get("search_all", True):
        return [base_key]
    # 某些源没有该片源（甚至挂掉）时不占用「额外源数量」的额度，
    # 由 merge_search 只并入前 N 个真正有结果的额外源；失败熔断中的源直接跳过
    extras = [k for k in search_extras(base_key) if not src_cooling(k)]
    lim = int(_f.get("search_limit") or 0)
    cand = extras if lim <= 0 else extras[:max(8, lim * 3)]
    return [base_key] + cand


def _search_breadth():
    """当前设置下主源这一路最多覆盖几个源（1 + 额外源数量）"""
    if not _f.get("search_all", True):
        return 1
    n = len(search_extras())
    lim = int(_f.get("search_limit") or 0)
    return 1 + (min(lim, n) if lim > 0 else n)


def _upstream_json(meta, query, timeout=20):
    url = meta["api"] + ("?" + query if query else "")
    req = urllib.request.Request(url, headers={"User-Agent": "TVBox/1.0.0"})
    raw = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")
    return json.loads(raw)


def merge_search(base_key, query, targets=None, limit=None, min_targets=2, cap=80,
                 timeout=10, workers=None, deadline=None):
    """主源 + 其它源并行搜索，合并成一个列表（id 带来源前缀，详情可回溯）

    targets/limit 不传时按「搜索范围」设置走；聚合采集源会显式传入自己的源组合。
    """
    targets = list(targets) if targets is not None else search_targets(base_key)
    # 停用源不进入搜索范围（手动停用 / 自动停用 / LunaTV 停用都算）
    targets = [k for k in targets if src_on(k)]
    srcs = sources_cached()
    if not targets or len(targets) < min_targets:
        return None
    t0 = time.time()

    def one(k):
        meta = srcs.get(k)
        if not meta:
            return k, None
        try:
            d = _upstream_json(meta, query, timeout)
            src_fail_note(k, isinstance(d, dict))
            fo_note(k, isinstance(d, dict), "搜索无有效返回")
            return k, d
        except Exception as e:
            src_fail_note(k, False)
            fo_note(k, False, "搜索超时/失败")
            print("[search] %s fail: %s" % (k, str(e)[:60]), flush=True)
            return k, None

    nw = workers or min(6, max(2, len(targets)))
    ex = ThreadPoolExecutor(max_workers=min(len(targets), nw))
    order = {k: i for i, k in enumerate(targets)}
    parts = []
    try:
        futs = [ex.submit(one, k) for k in targets]
        if deadline:
            # 聚合源有硬性等待上限：到点先返回已经拿到的结果，慢源让它在后台自己跑完
            try:
                for fu in as_completed(futs, timeout=deadline):
                    parts.append(fu.result())
            except Exception:
                pass
        else:
            parts = [fu.result() for fu in as_completed(futs)]
    finally:
        ex.shutdown(wait=False)
    parts.sort(key=lambda x: order.get(x[0], 999))
    items, seen, hit = [], {}, 0
    lim = int(_f.get("search_limit") or 0) if limit is None else int(limit)     # 0 = 不限
    extra_used, used = 0, []
    for k, d in parts:
        if not isinstance(d, dict):
            continue
        if src_off(k):          # 停用源（含缓存里残留的）不并入搜索结果
            continue
        added = 0
        for v in (d.get("list") or []):
            if not isinstance(v, dict):
                continue
            if bad_cat_item(v.get("type_name") or ""):
                bump("cat", 1)
                continue
            if bad_title(v.get("vod_name") or ""):
                bump("title", 1)
                continue
            nm = re.sub(r"\s+", "", str(v.get("vod_name") or "")) or str(v.get("vod_id"))
            if seen.get(nm, 0) >= SEARCH_MAX_PER_NAME:
                continue
            if len(items) >= cap:      # 结果太多反而难翻，够用就行
                break
            seen[nm] = seen.get(nm, 0) + 1
            v["vod_id"] = str(k) + SEP + str(v.get("vod_id"))
            proxify_item(v, (srcs.get(k) or {}).get("api") or "")
            items.append(v)
            hit += 1
            added += 1
        if k != base_key and added:
            extra_used += 1
            used.append(k)
        # 额度按「真正有结果的额外源」算：前 N 个有结果的源并进来，后面即使是活的也不再并入
        if lim > 0 and extra_used >= lim:
            break
    out = {"code": 1, "msg": "data list", "page": 1, "pagecount": 1,
           "limit": len(items), "total": len(items), "list": items}
    print("[search] %s -> %d sources, %d items, %.2fs (extra used: %s)"
          % ("+".join(targets), len(targets), hit, time.time() - t0,
             "+".join(used) if used else "-"), flush=True)
    return out


def merge_detail(query, ids_raw, timeout=25):
    """详情请求里带来源前缀时，按来源拆开分别取，再合并回一个列表"""
    srcs = sources_cached()
    qs = urllib.parse.parse_qs(query)
    ac = (qs.get("ac") or ["detail"])[0] or "detail"
    groups, order = {}, []
    for token in str(ids_raw).split(","):
        token = token.strip()
        if not token:
            continue
        if SEP in token:
            k, vid = token.split(SEP, 1)
        else:
            k, vid = "", token
        if not k:
            continue
        if src_off(k):          # 停用源不再提供详情（历史收藏点开会提示源已停用）
            continue
        groups.setdefault(k, []).append(vid)
        order.append(k)

    def one(k):
        meta = srcs.get(k)
        if not meta:
            return k, None
        ids = ",".join(groups[k])
        q = "ac=" + urllib.parse.quote(ac) + "&ids=" + urllib.parse.quote(ids, safe=",")
        try:
            return k, _upstream_json(meta, q, timeout)
        except Exception as e:
            print("[detail] %s fail: %s" % (k, str(e)[:60]), flush=True)
            return k, None

    with ThreadPoolExecutor(max_workers=min(6, max(2, len(groups)))) as ex:
        parts = list(ex.map(one, list(groups.keys())))
    items = []
    for k, d in parts:
        if not isinstance(d, dict):
            continue
        for v in (d.get("list") or []):
            if not isinstance(v, dict):
                continue
            if bad_cat_item(v.get("type_name") or ""):
                bump("cat", 1)
                continue
            if bad_title(v.get("vod_name") or ""):
                bump("title", 1)
                continue
            v["vod_id"] = str(k) + SEP + str(v.get("vod_id"))
            proxify_item(v, (srcs.get(k) or {}).get("api") or "")
            items.append(v)
    print("[detail] %d groups -> %d items" % (len(groups), len(items)), flush=True)
    return {"code": 1, "msg": "data list", "page": 1, "pagecount": 1,
            "limit": len(items), "total": len(items), "list": items}


# ---------------- 自建聚合采集源（一个 CMS 接口聚合全部源） ----------------
# LunaTV / TVBox 把 http://NAS:3032/agg 当成一个普通苹果 CMS 源即可：
# 搜索走全部源、详情按 id 前缀回溯上游、分类浏览按「电影/电视剧/动漫…」跨源合并。
AGG_FILE = os.path.join(CACHE_DIR, "aggcats.json")
AGG_POOL_TTL = 600          # 分类页合并结果缓存
AGG_SUB_SRC_MAX = 12        # 每个细分类最多挂多少个源（按源顺序取前 N）
AGG_CAT_TTL = 1800          # 分类树缓存
AGG_CAP_PER_NAME = 1        # 同名只留一条，分类页不再出现同一部片的两三个来源
AGG_SCHEMA = 2              # 2 = 两级分类（大分类 + 细分类）+ 条件筛选
_agg = {"cats": [], "ids": {}, "map": {}, "top": set(), "ts": 0.0,
        "pool": {}, "empty": {}, "lock": threading.Lock()}


def _agg_norm(s):
    """分类名归一：去掉图标、空格与标点，让各站的「电影」「🎬电影」能对上"""
    s = str(s or "")
    s = re.sub(r"[\U0001F000-\U0001FAFF\u2190-\u2BFF\u2600-\u27BF\uFE0F\u200d]", "", s)
    s = re.sub(r"[\s\-_/|·,，、。()（）\[\]【】]+", "", s)
    return (s or str(s or "")).strip().lower()


def item_src(v):
    """条目来源 key：聚合/搜索结果里的 vod_id 形如 <key>~<上游id>"""
    s = str((v or {}).get("vod_id") or "")
    return s.split(SEP, 1)[0] if SEP in s else ""


def agg_map_on(tid):
    """某个分类下仍然启用的源映射（停用源一律不参与采集）"""
    return {k: v for k, v in (_agg["map"].get(str(tid)) or {}).items() if src_on(k)}


def agg_src_prune(rebuild=True):
    """源启停后立刻生效：把停用源从分类映射、分类树与结果缓存里摘掉，并后台重建树"""
    dropped = 0
    dead = set()                         # 源全停用 → 分类直接下架
    for _tid, mp in list(_agg["map"].items()):
        if not isinstance(mp, dict):
            continue
        before = len(mp)
        for k in list(mp.keys()):
            if not src_on(k):
                mp.pop(k, None)
                dropped += 1
        if before and not mp:            # 该分类的源全停用了：映射整条删掉
            _agg["map"].pop(_tid, None)
            dead.add(_tid)
    with _agg["lock"]:
        _agg["pool"].clear()
        if dead:
            _agg["cats"] = [c for c in _agg["cats"] if str(c.get("type_id")) not in dead]
            _agg["top"] = {str(c["type_id"]) for c in _agg["cats"] if not c.get("type_pid")}
    if dropped:
        print("[agg] 源启停：从分类映射摘除 %d 处，分类缓存立即失效" % dropped, flush=True)
        agg_ids_save()
        if rebuild:
            threading.Thread(target=_agg_rebuild_later, daemon=True).start()
    return dropped


def _agg_rebuild_later(delay=6):
    """等一会儿（避开当前的机顶盒请求）再重建分类树，把停用源彻底清出去"""
    try:
        time.sleep(delay)
        agg_build(force=True)
    except Exception as e:
        print("[agg] 后台重建失败: %s" % str(e)[:60], flush=True)



def agg_ids_load():
    try:
        with open(AGG_FILE, encoding="utf-8") as fp:
            d = json.load(fp)
        _agg["ids"] = {str(k): int(v) for k, v in (d.get("ids") or {}).items()}
        _agg["cats"] = [c for c in (d.get("cats") or []) if isinstance(c, dict)]
        _agg["map"] = {str(k): v for k, v in (d.get("map") or {}).items() if isinstance(v, dict)}
        _agg["ts"] = float(d.get("ts") or 0)
        _agg["top"] = {str(c["type_id"]) for c in _agg["cats"]
                       if c.get("type_id") is not None and not c.get("type_pid")}
        if int(d.get("v") or 0) != AGG_SCHEMA:
            _agg["ts"] = 0          # 结构升级过，下次请求自动重建
        print("[agg] 分类缓存载入 %d 类" % len(_agg["cats"]), flush=True)
    except Exception:
        pass


def agg_ids_save():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = AGG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump({"v": AGG_SCHEMA, "ids": _agg["ids"], "cats": _agg["cats"],
                       "map": _agg["map"], "ts": _agg["ts"]}, fp, ensure_ascii=False)
        os.replace(tmp, AGG_FILE)
    except Exception as e:
        print("[agg] 分类缓存保存失败:", e, flush=True)


def _agg_tid(name):
    """同一个分类名永远拿到同一个 type_id，客户端收藏/分类不会错位"""
    ids = _agg["ids"]
    if name in ids:
        return ids[name]
    used = set(ids.values())
    tid = 1000 + (int(hashlib.md5(name.encode("utf-8")).hexdigest()[:6], 16) % 8000)
    while tid in used:
        tid = 1000 + ((tid - 1000 + 13) % 8000)
    ids[name] = tid
    return tid


def agg_browse_keys():
    keys = ordered_source_keys("agg")
    if not keys:
        return []
    prim = fo_primary() or search_primary()
    keys = ([prim] if prim in keys else []) + [k for k in keys if k != prim]
    return keys[:max(1, int(_f.get("agg_browse") or 8))]


def agg_search_keys():
    keys = ordered_source_keys("agg")
    if not keys:
        return []
    prim = fo_primary() or search_primary()
    keys = ([prim] if prim in keys else []) + [k for k in keys if k != prim]
    lim = int(_f.get("agg_search") or 0)
    keys = keys if lim <= 0 else keys[:1 + lim]
    # 连续失败冷却中的源不参与，避免每次都等它超时（主源除外）
    return [k for i, k in enumerate(keys) if i == 0 or not src_cooling(k)]


AGG_ORDER = ["电影", "电视剧", "综艺", "动漫", "纪录片", "短剧", "体育"]
# 一级分类：只有这些名字算「大分类」入口
AGG_ALIAS = {
    "电影": ["电影", "电影片", "影片", "电影频道", "高清电影", "4K电影", "邵氏电影", "预告片", "影视解说"],
    "电视剧": ["电视剧", "连续剧", "剧集", "电视连续剧"],
    "综艺": ["综艺", "综艺片", "娱乐", "综艺节目", "演唱会"],
    "动漫": ["动漫", "动画", "动画片", "动漫片", "二次元"],
    "纪录片": ["纪录片", "记录片", "记录"],
    "短剧": ["短剧", "微短剧", "爽文短剧", "竖屏短剧", "AI短剧", "AI漫剧", "短剧大全"],
    "体育": ["体育", "体育赛事", "赛事"],
}
AGG_GROUP_OF = {alias: g for g, names in AGG_ALIAS.items() for alias in map(_agg_norm, names)}
# 二级分类：多数站用 type_pid 挂在父类下；少数站（百度云/红牛）是扁平结构，按名字归组
AGG_SUB_ALIAS = {
    "电影": ["动作片", "喜剧片", "爱情片", "科幻片", "恐怖片", "剧情片", "战争片", "惊悚片",
             "悬疑片", "犯罪片", "古装片", "历史片", "冒险片", "奇幻片", "武侠片", "枪战片",
             "灾难片", "伦理片", "家庭片", "同性片", "歌舞片", "西部片", "喜剧", "动作", "科幻"],
    "电视剧": ["国产剧", "大陆剧", "内地剧", "香港剧", "港澳剧", "港剧", "台湾剧", "台剧",
              "韩国剧", "韩剧", "日剧", "日本剧", "欧美剧", "海外剧", "美剧", "英剧", "泰剧",
              "其他剧", "其它剧", "港台剧"],
    "综艺": ["大陆综艺", "国内综艺", "港台综艺", "日韩综艺", "欧美综艺", "韩国综艺",
             "日本综艺", "台湾综艺", "香港综艺"],
    "动漫": ["国产动漫", "日韩动漫", "欧美动漫", "港台动漫", "海外动漫", "有声动漫",
             "日本动漫", "韩国动漫", "国漫", "动漫电影", "动画电影", "漫剧"],
    "短剧": ["反转爽剧", "古装仙侠", "脑洞悬疑", "现代都市", "女频恋爱", "年代穿越",
             "都市脑洞", "战神赘婿", "逆袭", "AI漫剧"],
    "体育": ["篮球", "足球", "斯诺克", "台球", "网球", "其他赛事", "NBA", "CBA", "英超", "西甲"],
}
AGG_SUB_OF = {alias: g for g, names in AGG_SUB_ALIAS.items() for alias in map(_agg_norm, names)}
# 同一个细分类在各站叫法不一，统一成一个名字
SUB_CANON = {
    "韩国剧": "韩剧", "韩国电视剧": "韩剧",
    "香港剧": "港剧", "港澳剧": "港剧", "香港电视剧": "港剧",
    "台湾剧": "台剧", "台湾电视剧": "台剧",
    "大陆剧": "国产剧", "内地剧": "国产剧", "国产电视剧": "国产剧",
    "日本剧": "日剧", "海外剧": "欧美剧", "美剧": "欧美剧", "英剧": "欧美剧", "美国剧": "欧美剧",
    "泰国剧": "泰剧", "记录片": "纪录片",
    "喜剧": "喜剧片", "动作": "动作片", "科幻": "科幻片", "家庭篇": "家庭片",
    "动漫电影": "动画电影",
    # 短剧的细分类各站叫法不一：带「短剧」前缀的、爽文类统一收进来
    "爽文短剧": "反转爽剧", "短剧年代穿越": "年代穿越", "短剧古装仙侠": "古装仙侠",
    "短剧反转爽剧": "反转爽剧", "短剧女频恋爱": "女频恋爱", "短剧成长逆袭": "成长逆袭",
    "短剧脑洞悬疑": "脑洞悬疑", "短剧现代都市": "现代都市",
    # 同义/近义细分类合并，避免一个分类出现三四条
    "马泰剧": "泰剧", "港台剧": "港剧", "中国动漫": "国产动漫",
    "反转爽文": "反转爽剧", "穿越年代": "年代穿越", "重生民国": "年代穿越",
    "言情总裁": "女频恋爱", "女恋总裁": "女频恋爱", "现代言情": "女频恋爱",
    "闪婚离婚": "女频恋爱",
}
AGG_SUB_BAD = {"全部", "更多", "最新", "热门", "推荐", "其它", "其他", "未知", "无",
               "专题", "排行", "全部影片", "全部视频", "未分类", "默认"}
# 条件筛选
AGG_AREAS = ["大陆", "香港", "台湾", "美国", "韩国", "日本", "泰国", "英国", "法国", "印度"]
AGG_LANGS = ["国语", "粤语", "英语", "韩语", "日语", "泰语"]
AGG_YEARS = [str(y) for y in range(time.localtime().tm_year, time.localtime().tm_year - 11, -1)]


def _agg_root_group(c, byid):
    """向上找最近的可归组祖先（最多 3 层），找不到返回 None"""
    pid = str(c.get("type_pid") or 0)
    depth = 0
    while pid and pid not in ("0", "", "-1", "None", "none") and depth < 3:
        p = byid.get(pid)
        if not p:
            return None
        g = AGG_GROUP_OF.get(_agg_norm(p.get("type_name")))
        if g:
            return g
        pid = str(p.get("type_pid") or 0)
        depth += 1
    return None


AGG_SUB_ADULT = ("伦理", "里番", "黄漫", "无码", "有码", "中字", "中文字幕", "激情", "情色", "色情",
                 "成人", "三级", "自拍", "偷拍", "探花", "传媒", "主播", "人妻", "熟女", "少妇",
                 "乱伦", "调教", "换脸", "吃瓜", "黑料", "写真", "套图", "福利", "约炮", "裸",
                 "AV", "SM", "TS", "VR", "写真", "视频区", "理论片", "擦边", "重口", "巨乳",
                 "丝袜", "美腿", "学生", "萝莉", "同人动漫", "剧情故事")

# 全分类实例（GUARD_OPEN=1，3035 那套）：屏蔽全部关闭，聚合树保留全部细分类
# （含成人/福利类，按原站归属挂在电影/动漫/电视剧下，不单独建「成人」大分类）
OPEN_MODE = str(os.environ.get("GUARD_OPEN", "") or os.environ.get("GUARD_ADULT", "")).strip().lower() \
    in ("1", "true", "yes", "on")
ADULT_MODE = OPEN_MODE      # 兼容旧变量名


def _agg_is_adult(name):
    """分类名是否属于成人类（只看名字里有没有成人关键词）"""
    up = str(name or "").upper()
    return any(w.upper() in up for w in AGG_SUB_ADULT)


# 细分类名前缀（「动漫-黄漫」这类），全分类实例里剥掉，避免分类栏出现重复前缀
AGG_NAME_PREFIX = re.compile(r"^(动漫|电影|电视剧|短剧|综艺|纪录片|动作|喜剧)\s*[-·_/|]\s*(.+)$")


def _agg_sub_ok(name, grp):
    nm = str(name or "")
    if not nm or nm in AGG_SUB_BAD:
        return False
    if _agg_norm(nm) == _agg_norm(grp):
        return False
    if _agg_norm(nm) in AGG_GROUP_OF:
        return False
    if len(nm) < 2 or len(nm) > (14 if OPEN_MODE else 8):
        return False
    if re.fullmatch(r"[\d\-\s\.]+", nm):
        return False
    # 成人/灌水类细分类不进分类栏（这些源本来也会被屏蔽词拦截）；全分类实例反过来，全部保留
    up = nm.upper()
    if not OPEN_MODE and any(w.upper() in up for w in AGG_SUB_ADULT):
        return False
    return True


def agg_build(force=False):
    """并行读各源的 ac=class，按 type_pid 合并成「大分类 + 细分类」两级树"""
    if not _f.get("agg_on", True):
        return
    if not force and _agg["cats"] and time.time() - _agg["ts"] < agg_cat_ttl():
        return
    # 客户端触发采集关闭：分类树只在完全没有时建一次，过期也用旧的（不在浏览时打几十个源）
    if not force and _agg["cats"] and not _f.get("agg_lazy", True):
        return
    # 大分类浏览仍按「分类浏览源数量」控制；细分类的源映射扫全部启用源，
    # 否则前几个源恰好没内容的细分类（如海外动漫/斯诺克）会变成空分类
    keys = ordered_source_keys("agg") or agg_browse_keys()
    bset = set(agg_browse_keys())
    srcs = sources_cached()
    if not keys:
        return

    def one(k):
        meta = srcs.get(k)
        if not meta:
            return k, []
        try:
            out, _ = cms_build(k, "ac=class", meta, True, 8)
            d = json.loads(out.decode("utf-8", "ignore"))
            return k, (d.get("class") or [])
        except Exception as e:
            print("[agg] class %s fail: %s" % (k, str(e)[:50]), flush=True)
            return k, []

    with ThreadPoolExecutor(max_workers=min(16, len(keys))) as ex:
        parts = list(ex.map(one, keys))

    groups = {}     # 大分类名 -> {源 key: 该站顶层 tid}
    subs = {}       # (大分类名, 归一细类名) -> {"cnt": {原名: 次数}, "src": {源: tid}}
    for k, cls in parts:
        rows = [c for c in cls if isinstance(c, dict) and c.get("type_id") is not None]
        if not rows:
            continue
        byid = {str(c["type_id"]): c for c in rows}
        # 顺手把父子关系写进 cls_children 的缓存，分类页取子分类时不用再打一次 ac=class
        with _cls_pid_lock:
            _cls_pid[k] = {str(c["type_id"]): str(c.get("type_pid") or 0) for c in rows}
        for c in rows:
            raw = str(c.get("type_name") or "").strip()
            if OPEN_MODE:
                m0 = AGG_NAME_PREFIX.match(raw)
                if m0:
                    raw = m0.group(2).strip()
            nm = _agg_norm(raw)
            if not nm:
                continue
            tid = str(c["type_id"])
            g0 = AGG_GROUP_OF.get(nm)
            grp = _agg_root_group(c, byid)
            if g0 and not grp:          # 顶层大分类：只收浏览源，控制首页加载量
                if k in bset:
                    groups.setdefault(g0, {}).setdefault(k, tid)
                continue
            grp = grp or AGG_SUB_OF.get(nm)   # 扁平结构的老站按名字归组
            if not grp:
                continue
            canon = SUB_CANON.get(nm, nm)
            # 名字本身属于另一个大分类时归回自己的组（如「爽文短剧」有的站挂在电视剧下）
            home = AGG_SUB_OF.get(canon)
            if home:
                grp = home
            if not _agg_sub_ok(canon, grp):
                continue
            it = subs.setdefault((grp, canon), {"cnt": {}, "src": {}})
            it["cnt"][raw] = it["cnt"].get(raw, 0) + 1
            it["src"].setdefault(k, tid)
    if not groups and not subs:
        return

    cats, amap = [], {}
    for g in AGG_ORDER:
        rank = [_agg_norm(x) for x in AGG_SUB_ALIAS.get(g, [])]
        items = sorted([(k2, v) for k2, v in subs.items() if k2[0] == g],
                       key=lambda x: (-len(x[1]["src"]),
                                      rank.index(x[0][1]) if x[0][1] in rank else 99))
        if g not in groups and not items:
            continue
        gid = _agg_tid(g)
        cats.append({"type_id": gid, "type_pid": 0, "type_name": g})
        if groups.get(g):
            amap[str(gid)] = dict(groups[g])
        for (gg, cn), it in items[:(30 if OPEN_MODE else 20)]:
            name = max(it["cnt"], key=it["cnt"].get) if it["cnt"] else cn
            sid = _agg_tid(g + "|" + cn)
            cats.append({"type_id": sid, "type_pid": gid, "type_name": name})
            # 细分类源映射存前 20 个（实际抓取数按后台「细分类源数量」再截）
            amap[str(sid)] = dict(list(it["src"].items())[:20])
        if str(gid) not in amap and items:      # 该站只有细分类、没有父入口时兜底
            amap[str(gid)] = dict(list(items[0][1]["src"].items())[:20])
    _agg["cats"] = cats
    _agg["map"] = amap
    _agg["top"] = {str(c["type_id"]) for c in cats if not c.get("type_pid")}
    _agg["ts"] = time.time()
    with _agg["lock"]:      # 分类 id 稳定，旧缓存能留就留，避免每次重建后首访变慢
        _agg["pool"] = {k: v for k, v in _agg["pool"].items() if k.split("|")[0] in amap}
        _agg["empty"] = {}                       # 重建后重新验证哪些细分类是空的
    agg_ids_save()
    print("[agg] 分类树就绪: %d 个（大分类 %d / 细分类 %d）/ 来自 %d 个源"
          % (len(cats), len(_agg["top"]), len(cats) - len(_agg["top"]), len(keys)), flush=True)


def agg_resolve_tid(v):
    """TVBox 有时把分类名（中文）直接当 type_id 发过来，统一换成我们的数字 id"""
    v = str(v or "").strip()
    if not v:
        return ""
    if v.isdigit():
        return v
    # 兼容「电影-动作片」「电影/动作片」这类带前缀的写法
    for sep2 in ("-", "·", "/", "_", "|"):
        if sep2 in v:
            tail = v.split(sep2)[-1].strip()
            if tail:
                v = tail
            break
    nm = _agg_norm(v)
    for c in _agg["cats"]:
        if _agg_norm(c.get("type_name")) == nm:
            return str(c["type_id"])
    return v


def agg_filters():
    """分类页顶部的筛选条：类型（细分类）+ 地区 + 年份 + 排序"""
    vis = agg_cats_visible()
    out = {}
    for c in vis:
        if c.get("type_pid"):
            continue
        tid = str(c["type_id"])
        subs = [x for x in vis if str(x.get("type_pid") or "") == tid]
        f = []
        if subs:
            f.append({"key": "class", "name": "类型",
                      "value": [{"n": "全部", "v": ""}] +
                               [{"n": x["type_name"], "v": str(x["type_id"])} for x in subs]})
        f.append({"key": "area", "name": "地区",
                  "value": [{"n": "全部", "v": ""}] + [{"n": a, "v": a} for a in AGG_AREAS]})
        f.append({"key": "year", "name": "年份",
                  "value": [{"n": "全部", "v": ""}] + [{"n": y, "v": y} for y in AGG_YEARS] +
                           [{"n": "更早", "v": "更早"}]})
        f.append({"key": "by", "name": "排序",
                  "value": [{"n": "最新", "v": "time"}, {"n": "最热", "v": "hits"},
                            {"n": "评分", "v": "score"}]})
        out[tid] = f
        out[str(c["type_name"])] = f      # 有的 App 直接把分类名当 type_id，两套 key 都认
    return out


def agg_cats_visible():
    """按后台「分类管理」的排序和显隐开关处理后的分类表（关大分类连子类一起藏）"""
    idx = {nm: i for i, nm in enumerate(_f.get("agg_cat_order") or [])}
    cats = sorted(_agg["cats"], key=lambda c: idx.get(c["type_name"], 9999)) if idx else list(_agg["cats"])
    # 屏蔽开启时，命中分类黑名单的（伦理片/里番动漫…）直接不进分类栏，免得点开全空
    if _f.get("filter_on", True) and _f.get("cats_on", True):
        cats = [c for c in cats if not bad_cat(c["type_name"])]
    # 实测为空的细分类 6 小时内不再出现在分类栏（重建分类后重新验证）
    emp = _agg.get("empty") or {}
    if emp:
        cats = [c for c in cats
                if not (c.get("type_pid") and emp.get(str(c["type_id"]))
                        and time.time() - emp[str(c["type_id"])] < 21600)]
    off = _f.get("agg_cat_off") or set()
    if not off:
        return cats
    top_off = {str(c["type_id"]) for c in cats
               if not c.get("type_pid") and c["type_name"] in off}
    return [c for c in cats
            if c["type_name"] not in off and str(c.get("type_pid") or "") not in top_off]


def agg_tvbox_cats():
    """机顶盒分类栏：默认只列 7 个大分类；勾了「细分类也放进去」再把细分类铺开"""
    vis = agg_cats_visible()
    tops = [c for c in vis if not c.get("type_pid")]
    names = [c["type_name"] for c in tops]
    if not _f.get("agg_tvsub"):
        return names
    used = set(names)
    by_id = {str(c["type_id"]): c for c in tops}
    for c in vis:
        if not c.get("type_pid"):
            continue
        nm = c["type_name"]
        if nm in used:
            p = by_id.get(str(c.get("type_pid")))
            nm = ((p.get("type_name") if p else "") + "-" + nm)
        used.add(nm)
        names.append(nm)
    return names


def _agg_num(v):
    try:
        return float(re.sub(r"[^\d\.]", "", str(v or "0")) or 0)
    except Exception:
        return 0.0


def _agg_match(v, flt):
    """条件筛选的本地兜底：上游不认 area/year/lang 时按字段自己筛"""
    if not flt:
        return True
    a = str(v.get("vod_area") or "")
    w = flt.get("area")
    if w and a and w not in a:
        return False
    y = str(v.get("vod_year") or "")
    w = flt.get("year")
    if w and y:
        yy = y[:4]
        if w in ("更早", "older"):
            if yy.isdigit() and int(yy) >= 2015:
                return False
        elif w not in y:
            return False
    lg = str(v.get("vod_lang") or "")
    w = flt.get("lang")
    if w and lg and w not in lg:
        return False
    return True


def agg_flt_query(flt):
    """筛选条件拼成上游 query；各站支持不一，认不出的交给本地兜底"""
    pairs = []
    for k in ("area", "year", "lang"):
        v = (flt or {}).get(k)
        if v and v not in ("更早", "older"):
            pairs.append("%s=%s" % (k, urllib.parse.quote(str(v))))
    return "&".join(pairs)


def agg_pool_ttl():
    """分类页缓存有效期：自动采集开着时至少撑到下一轮，这样客户端浏览不用回源"""
    t = AGG_POOL_TTL
    if _f.get("agg_auto_on"):
        t = max(t, min(86400, int(_f.get("agg_auto_sec") or 3600) + 300))
    return t


def agg_cat_ttl():
    """分类树缓存有效期：同理，别让浏览时又去挨个源拉 ac=class"""
    t = AGG_CAT_TTL
    if _f.get("agg_auto_on"):
        t = max(t, min(86400, int(_f.get("agg_auto_sec") or 3600) + 300))
    return t


def agg_cat_pool(tid, flt=None, force=False):
    """某个分类下的跨源合并结果（第一页各源合并，本地过滤 + 排序 + 分页）"""
    tid = str(tid)
    flt = {k: v for k, v in (flt or {}).items() if v}
    if flt.get("by") == "time":
        flt.pop("by", None)          # 默认就是最新，和没选排序共用缓存
    fq = agg_flt_query(flt)
    ckey = tid + "|" + "&".join("%s=%s" % (k, flt[k]) for k in sorted(flt))
    lazy = bool(_f.get("agg_lazy", True)) or force     # 预采集走 force，无视按需开关
    with _agg["lock"]:
        hit = _agg["pool"].get(ckey)
    # 客户端触发采集关掉时：缓存过期也继续用（宁可旧一点，也不在浏览时回源）
    if hit and (time.time() - hit[0] < agg_pool_ttl() or not lazy):
        # 缓存里可能还有刚被停用源的条目，出池前再按来源过一遍
        live = [v for v in hit[1] if src_on(item_src(v))]
        if not live and not flt and tid not in _agg.get("top", set()):
            with _agg["lock"]:
                _agg["empty"].setdefault(tid, time.time())
        return live
    if not lazy:
        # 没有缓存又关了按需采集：直接返回空，由「立即采集一次 / 定时预采集」来填
        return []
    srcs = sources_cached()
    tids = (_agg["map"].get(tid) or {})
    if not tids:
        return []
    is_top = tid in _agg.get("top", set())
    # 拉取数量可在后台调：源数量 × 每源页数（大分类/细分类分开设置）
    if is_top:
        max_src = max(1, int(_f.get("agg_browse") or 8))
        pages = max(1, min(5, int(_f.get("agg_top_pg") or 1)))
    else:
        max_src = max(1, int(_f.get("agg_sub_src") or 12))
        pages = max(1, min(5, int(_f.get("agg_sub_pg") or 1)))
    # 先剔掉停用源再按数量截断，避免停用源白占名额（否则分类会莫名变少）
    tids = {k: v for k, v in tids.items() if src_on(k) and k in srcs}
    if not tids:
        if not is_top and not flt:
            with _agg["lock"]:
                _agg["empty"].setdefault(tid, time.time())   # 源全停用了：分类栏先下架
        return []
    # 取源顺序跟「片源顺序 / 自动排序」一致：自动排序开启时优先打响应快的源
    aord = {k: i for i, k in enumerate(ordered_source_keys("agg"))}
    tids = dict(sorted(tids.items(), key=lambda kv: aord.get(kv[0], 9999)))
    # 空白源补位：名额内的源没采到数据时跳过它，用后面的源补足「有效数量」；
    # 关闭补位 = 严格只取前 N 个源，采不到就少几条
    fill_on = bool(_f.get("agg_fill", True))
    fill_cap = int(_f.get("agg_fill_max") or 20)
    fill_cap = max_src if not fill_on else max(max_src, min(40, fill_cap))

    def src_tasks(k, lt):
        """单个源要打的页（大分类：父类 + 前两个子类并行，整棵子树太慢）"""
        meta = srcs.get(k)
        if not meta or src_off(k):
            return []
        if not is_top:
            return [(k, lt, pg2) for pg2 in range(1, pages + 1)]
        try:
            kids = cls_children(k, meta, lt)
        except Exception:
            kids = []
        return [(k, t, pg2) for t in (kids[:2] if kids else [lt]) for pg2 in range(1, pages + 1)]

    def fetch(k, t, q):
        meta = srcs.get(k) or {}
        try:
            out, _ = cms_build(k, q, meta, True, 6)
            d = json.loads(out.decode("utf-8", "ignore"))
            return [v for v in (d.get("list") or []) if isinstance(v, dict)]
        except Exception as e:
            print("[agg] cat %s/%s fail: %s" % (k, t, str(e)[:40]), flush=True)
            return []

    def one(item):
        k, t, pg2 = item
        base = "ac=videolist&t=%s&pg=%d" % (t, pg2)
        rows = fetch(k, t, base + ("&" + fq if fq else ""))
        if fq:
            keep = [v for v in rows if _agg_match(v, flt)]
            if len(keep) < 3:          # 上游不认筛选参数：回退到原始页自己筛
                seen0 = {str(v.get("vod_id")) for v in keep}
                for v in fetch(k, t, base):
                    if str(v.get("vod_id")) not in seen0 and _agg_match(v, flt):
                        keep.append(v)
            rows = keep
        for v in rows:
            v["vod_id"] = str(k) + SEP + str(v.get("vod_id"))
        return rows

    # 按「有效源数量」取数：一轮取够 max_src 个有数据的源就停；空白源自动由后面的源补位
    items, good = [], set()
    order_keys = list(tids.keys())[:fill_cap]
    pos = 0
    while pos < len(order_keys) and len(good) < max_src:
        need = max_src - len(good)
        size = need if fill_on else max_src
        wave = order_keys[pos: pos + size]
        pos += len(wave)
        if not wave:
            break
        tasks = []
        for k in wave:
            tasks.extend(src_tasks(k, tids[k]))
        if not tasks:
            continue
        with ThreadPoolExecutor(max_workers=min(16, max(1, len(tasks)))) as ex:
            for (k, _t, _p), part in zip(tasks, ex.map(one, tasks)):
                if part:
                    good.add(k)
                    items.extend(part)
        if not fill_on:
            break
    seen, out = {}, []
    for v in items:
        if not isinstance(v, dict):
            continue
        if not src_on(item_src(v)):     # 抓取途中源被停用：结果同样丢弃
            continue
        nm = re.sub(r"\s+", "", str(v.get("vod_name") or "")) or str(v.get("vod_id"))
        if seen.get(nm, 0) >= AGG_CAP_PER_NAME:
            continue
        seen[nm] = seen.get(nm, 0) + 1
        out.append(v)
    by = (flt or {}).get("by") or "time"
    if by == "hits":
        out.sort(key=lambda v: _agg_num(v.get("vod_hits")), reverse=True)
    elif by == "score":
        out.sort(key=lambda v: _agg_num(v.get("vod_score")), reverse=True)
    else:
        out.sort(key=lambda v: str(v.get("vod_time") or ""), reverse=True)
    with _agg["lock"]:
        if len(_agg["pool"]) > 24:
            _agg["pool"].clear()
        _agg["pool"][ckey] = (time.time(), out)
    print("[agg] 分类 %s%s 有效 %d 个源（试 %d）→ %d 条"
          % (tid, (" 「%s」" % " ".join("%s=%s" % (k, v) for k, v in flt.items())) if flt else "",
             len(good), pos, len(out)), flush=True)
    # 细分类实测为空（映射到的源都没内容）时记下来，分类栏自动下架，别让用户点开空壳
    if not out and not is_top and not flt:
        with _agg["lock"]:
            _agg["empty"][tid] = time.time()
    return out


def agg_list_page(tid=None, pg=1, flt=None):
    agg_build()
    vis = agg_cats_visible()
    if tid is None:
        # 无 t 的请求（推荐页/首页）：返回后台设置的「推荐分类」，没设就用第一个大分类
        home = str(_f.get("agg_home") or "").strip()
        tid = ""
        if home:
            for c in vis:
                if not c.get("type_pid") and c["type_name"] == home:
                    tid = str(c["type_id"])
                    break
        if not tid and vis:
            tid = str(vis[0]["type_id"])
    pool = agg_cat_pool(tid, flt)
    pg = max(1, int(pg or 1))
    size = max(10, min(60, int(_f.get("agg_page") or 20)))
    total = len(pool)
    page = pool[(pg - 1) * size: pg * size]
    return {"code": 1, "msg": "数据列表", "page": pg,
            "pagecount": max(1, (total + size - 1) // size), "limit": str(size),
            "total": total, "class": vis, "list": page}


def agg_search_page(query):
    keys = agg_search_keys()
    if not keys:
        return {"code": 0, "msg": "没有可用源", "page": 1, "pagecount": 1,
                "limit": "20", "total": 0, "list": []}
    lim = int(_f.get("agg_search") or 0)
    out = merge_search(keys[0], query, targets=keys, limit=lim,
                       min_targets=1, cap=120, timeout=4, workers=32, deadline=4.5)
    if out is None:
        return {"code": 0, "msg": "搜索失败", "page": 1, "pagecount": 1,
                "limit": "20", "total": 0, "list": []}
    # 聚合源已经合并好一页，告诉客户端只有一页，避免它再翻 20 页重复打源
    out.update({"page": 1, "pagecount": 1, "limit": len(out.get("list") or [])})
    return out


def agg_detail_page(ids_raw, ac="videolist"):
    try:
        out = merge_detail("ac=%s&ids=%s" % (ac, ids_raw), ids_raw, timeout=8)
    except Exception as e:
        print("[agg] detail fail:", e, flush=True)
        out = {"code": 0, "msg": "详情获取失败", "list": [], "total": 0}
    return out


def agg_empty(msg="无数据"):
    return {"code": 0, "msg": msg, "page": 1, "pagecount": 1, "limit": "20",
            "total": 0, "list": []}


# ---------------- 采集调度：定时预采集，采完即停 ----------------
# 之前的模式是「客户端一浏览就回源 + 缓存 10 分钟」，机器一多就变成不停重复抓取。
# 这里改成两段式：①到点跑一轮预算好的预采集，跑完立刻退出线程（不常驻、不循环）；
# ②客户端浏览只在「按需采集」打开时才回源，关掉就只读已有缓存。
ACQ_FILE = os.path.join(CACHE_DIR, "aggcollect.json")
ACQ_LOG = 20
ACQ_WORKERS = 2            # 预采集并发（压低，避免整轮把宽带打满）
ACQ_BOOT_DELAY = 90        # 启动后等分类树建好再开始首轮预采集
_acq = {"running": False, "started": 0.0, "finished": 0.0, "next": 0.0,
        "stop": False, "cur": "", "done": 0, "total": 0, "cats": 0, "items": 0,
        "failed": 0, "abort": "", "trigger": "", "last": {}, "log": [],
        "dirty": False, "lock": threading.Lock()}


def acq_load():
    try:
        with open(ACQ_FILE, "r", encoding="utf-8") as fp:
            d = json.load(fp)
        with _acq["lock"]:
            _acq["last"] = d.get("last") or {}
            _acq["log"] = [x for x in (d.get("log") or []) if isinstance(x, dict)][-ACQ_LOG:]
            _acq["next"] = float(d.get("next") or 0)
    except Exception:
        pass


def acq_save(force=False):
    if not force and not _acq.get("dirty"):
        return
    with _acq["lock"]:
        data = {"ts": time.time(), "next": _acq["next"],
                "last": _acq["last"], "log": _acq["log"][-ACQ_LOG:]}
        _acq["dirty"] = False
    tmp = ACQ_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False)
        os.replace(tmp, ACQ_FILE)
    except Exception as e:
        print("[aggcollect] save fail:", e, flush=True)


def acq_log(text, kind="", **kw):
    it = {"ts": time.time(), "kind": kind, "text": text}
    it.update(kw)
    with _acq["lock"]:
        _acq["log"].append(it)
        _acq["log"] = _acq["log"][-ACQ_LOG:]
        _acq["dirty"] = True


def acq_next_time(at="", sec=3600, base=None):
    """下次采集时间：填了 HH:MM 按每天这个点，否则按间隔（秒）"""
    now = time.time() if base is None else float(base)
    at = str(at or "").strip()
    if at:
        try:
            hh, mm = [int(x) for x in re.split(r"[:：]", at)[:2]]
        except Exception:
            return now + max(60, int(sec))
        lt = time.localtime(now)
        t = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday,
                         max(0, min(23, hh)), max(0, min(59, mm)), 0, 0, 0, -1))
        if t <= now + 30:
            t += 86400
        return t
    return now + max(60, int(sec))


def agg_collect_cats():
    """预采集用的分类表：跟分类栏一样按排序/显隐/屏蔽过滤，但**保留空分类**
    （空分类正是要预采集去填的，用 agg_cats_visible 会把它们排除掉，永远填不上）"""
    cats = list(_agg["cats"])
    idx = {nm: i for i, nm in enumerate(_f.get("agg_cat_order") or [])}
    if idx:
        cats.sort(key=lambda c: idx.get(c["type_name"], 9999))
    if _f.get("filter_on", True) and _f.get("cats_on", True):
        cats = [c for c in cats if not bad_cat(c["type_name"])]
    off = _f.get("agg_cat_off") or set()
    if off:
        top_off = {str(c["type_id"]) for c in cats
                   if not c.get("type_pid") and c["type_name"] in off}
        cats = [c for c in cats
                if c["type_name"] not in off and str(c.get("type_pid") or "") not in top_off]
    return cats


def acq_targets(scope=None, hot=None, maxn=None):
    """本轮预采集哪些分类：top=只大分类 / hot=大分类+前 N 个细分类 / all=全部细分类"""
    scope = str(scope or _f.get("agg_auto_scope") or "hot").lower()
    hot = int(_f.get("agg_auto_n") if hot is None else hot)
    maxn = int(_f.get("agg_auto_max") if maxn is None else maxn)
    try:
        vis = agg_collect_cats()
    except Exception:
        vis = []
    tops = [c for c in vis if not c.get("type_pid")]
    subs = [c for c in vis if c.get("type_pid")]
    subs.sort(key=lambda c: len(agg_map_on(c.get("type_id"))), reverse=True)   # 源多的先热
    if scope == "top":
        picked = tops
    elif scope == "all":
        picked = tops + subs
    else:
        picked = tops + subs[:max(0, hot)]
    return [{"tid": str(c.get("type_id")), "name": str(c.get("type_name") or ""),
             "pid": str(c.get("type_pid") or "")} for c in picked][:max(1, maxn)]


def acq_snapshot():
    c = {"on": bool(_f.get("agg_auto_on")), "sec": int(_f.get("agg_auto_sec") or 3600),
         "at": str(_f.get("agg_auto_at") or ""), "scope": str(_f.get("agg_auto_scope") or "hot"),
         "hot": int(_f.get("agg_auto_n") or 12), "max": int(_f.get("agg_auto_max") or 80),
         "budget": int(_f.get("agg_auto_budget") or 600),
         "lazy": bool(_f.get("agg_lazy", True)), "ttl": int(agg_pool_ttl())}
    now = time.time()
    with _acq["lock"]:
        snap = {"running": _acq["running"], "cur": _acq["cur"], "done": _acq["done"],
                "total": _acq["total"], "cats": _acq["cats"], "items": _acq["items"],
                "failed": _acq["failed"], "abort": _acq["abort"],
                "trigger": _acq["trigger"],
                "startedTs": _acq["started"], "finishedTs": _acq["finished"],
                "last": dict(_acq["last"] or {}), "log": list(_acq["log"] or [])[-ACQ_LOG:],
                "nextTs": float(_acq["next"] or 0)}
    if snap["running"]:
        snap["elapsed"] = int(max(0, now - (snap["startedTs"] or now)))
        snap["nextIn"] = 0
    else:
        snap["elapsed"] = 0
        snap["nextIn"] = max(0, int(snap["nextTs"] - now)) if snap["nextTs"] else 0
    return {"conf": c, **snap,
            "poolSize": len(_agg.get("pool") or {}),
            "catsTotal": len(agg_collect_cats()),
            "preview": [x["name"] for x in acq_targets()],
            "tops": [c["type_name"] for c in agg_collect_cats() if not c.get("type_pid")]}


def acq_run(trigger="manual", scope=None, hot=None, maxn=None, budget=None):
    """跑一轮预采集（线程体）。跑完把 running 置回 False，不再循环"""
    # 分类树还没建好（刚启动）就先等一下，否则会按残缺的树去采
    for _ in range(20):
        if _agg.get("cats"):
            break
        time.sleep(2)
    tgt = acq_targets(scope, hot, maxn)
    budget = int(_f.get("agg_auto_budget") if budget is None else budget)
    st = time.time()
    with _acq["lock"]:
        _acq.update({"running": True, "started": st, "finished": 0.0, "stop": False,
                     "cur": "", "done": 0, "total": len(tgt), "cats": 0, "items": 0,
                     "failed": 0, "abort": "", "trigger": trigger})
    print("[aggcollect] 开始%s预采集：%d 个分类，预算 %ds"
          % ("自动" if trigger == "auto" else "手动", len(tgt), budget), flush=True)
    ok = empty = 0
    got = 0

    def one(it):
        tid = it["tid"]
        # 强制重抓：清掉基础页缓存与「空分类」标记，让本轮结果覆盖旧数据
        with _agg["lock"]:
            _agg["pool"].pop(tid + "|", None)
            _agg["empty"].pop(tid, None)
        try:
            rows = agg_cat_pool(tid, None, force=True)
            return int(len(rows or []))
        except Exception as e:
            print("[aggcollect] %s fail: %s" % (tid, str(e)[:50]), flush=True)
            return -1

    idx = 0
    try:
        while idx < len(tgt):
            if _acq.get("stop"):
                with _acq["lock"]:
                    _acq["abort"] = "手动停止"
                break
            if time.time() - st > budget:
                with _acq["lock"]:
                    _acq["abort"] = "超出本轮时间预算"
                break
            wave = tgt[idx: idx + ACQ_WORKERS]
            idx += len(wave)
            with ThreadPoolExecutor(max_workers=max(1, len(wave))) as ex:
                for it, n in zip(wave, ex.map(one, wave)):
                    with _acq["lock"]:
                        _acq["cur"] = it["name"]
                        _acq["done"] = min(_acq["total"], _acq["done"] + 1)
                    if n > 0:
                        ok += 1
                        got += n
                    elif n == 0:
                        empty += 1
                    with _acq["lock"]:
                        _acq["cats"] = ok + empty
                        _acq["items"] = got
                        _acq["failed"] = empty
            acq_save()
    except Exception as e:
        with _acq["lock"]:
            _acq["abort"] = "异常：%s" % str(e)[:40]
        print("[aggcollect] error:", e, flush=True)
    finally:
        fin = time.time()
        dur = int(fin - st)
        with _acq["lock"]:
            _acq["running"] = False
            _acq["finished"] = fin
            _acq["cur"] = ""
            _acq["cats"] = ok + empty
            _acq["items"] = got
            _acq["failed"] = empty
            last = {"startedTs": st, "finishedTs": fin, "duration": dur,
                    "total": len(tgt), "cats": ok, "empty": empty, "items": got,
                    "abort": _acq["abort"], "trigger": trigger,
                    "scope": str(scope or _f.get("agg_auto_scope") or "hot")}
            _acq["last"] = last
            _acq["next"] = acq_next_time(_f.get("agg_auto_at"), int(_f.get("agg_auto_sec") or 3600), fin)
            _acq["dirty"] = True
        txt = "%s预采集完成：%d/%d 个分类有数据（空 %d 个），共 %d 条，用时 %ds%s" % (
            "自动" if trigger == "auto" else "手动", ok, len(tgt), empty, got, dur,
            "，提前结束：" + last["abort"] if last["abort"] else "")
        print("[aggcollect] " + txt, flush=True)
        acq_log(txt, kind="done", **{k: last[k] for k in ("duration", "items", "cats", "empty", "total")})
        acq_save(True)


def acq_start(trigger="manual", scope=None, hot=None, maxn=None, budget=None):
    with _acq["lock"]:
        if _acq["running"]:
            return {"ok": False, "msg": "上一轮采集还在跑"}
    threading.Thread(target=acq_run,
                     kwargs={"trigger": trigger, "scope": scope, "hot": hot,
                             "maxn": maxn, "budget": budget}, daemon=True).start()
    return {"ok": True}


def acq_stop():
    with _acq["lock"]:
        if not _acq["running"]:
            return {"ok": False, "msg": "当前没有正在跑的采集"}
        _acq["stop"] = True
    return {"ok": True}


def acq_loop():
    """采集调度专用线程：跟 CMS/图片预热分开，预热再慢也不会拖住定时采集"""
    time.sleep(ACQ_BOOT_DELAY)
    while True:
        try:
            agg_tick()
            acq_save()
        except Exception as e:
            print("[aggcollect] tick error:", e, flush=True)
        time.sleep(15)


def agg_tick():
    """warm_loop 每 15 秒调一次：到点起一轮，跑完就等下一次（不常驻抓取）"""
    if not _f.get("agg_auto_on") or _acq.get("running"):
        return
    if not _f.get("agg_on", True):
        return
    now = time.time()
    if now < float(_acq.get("next") or 0):
        return
    with _acq["lock"]:
        _acq["next"] = acq_next_time(_f.get("agg_auto_at"), int(_f.get("agg_auto_sec") or 3600), now)
        _acq["dirty"] = True
    acq_start(trigger="auto")


def cms_store(ck, out, ctype, cacheable=True):
    if not cacheable:
        return
    with _cms_lock:
        _cms_cache[ck] = (time.time(), out, ctype)
        total = sum(len(v[1]) for v in _cms_cache.values())
        while total > CMS_BUDGET and len(_cms_cache) > 1:
            old = min(_cms_cache, key=lambda x: _cms_cache[x][0])
            total -= len(_cms_cache.pop(old)[1])
        while len(_cms_cache) > 400 and len(_cms_cache) > 1:
            old = min(_cms_cache, key=lambda x: _cms_cache[x][0])
            _cms_cache.pop(old, None)


_cls_pid = {}                  # key -> {type_id: type_pid}（父分类判定用）
_cls_pid_lock = threading.Lock()


def cls_children(key, meta, tid):
    """返回分类 tid 的子分类 id 列表（依据 ac=class 里的 type_pid），无子分类返回 []"""
    try:
        tid = int(tid)
    except Exception:
        return []
    with _cls_pid_lock:
        m = _cls_pid.get(key)
    if m is None:
        try:
            out, _ct = cms_build(key, "ac=class", meta, True)
            d = json.loads(out.decode("utf-8", "ignore"))
            m = {str(c.get("type_id")): str(c.get("type_pid") or 0)
                 for c in (d.get("class") or []) if isinstance(c, dict) and c.get("type_id") is not None}
        except Exception:
            m = {}
        with _cls_pid_lock:
            _cls_pid[key] = m
    try:
        kids = [int(k) for k, v in m.items() if str(v) == str(tid) and int(k) != tid]
    except Exception:
        kids = []
    return sorted(kids)


def cms_build_parent(key, meta, query, tid, pg, kids, timeout=15):
    """父分类（如魔都「连续剧」「电影」）上游本身无内容、内容挂在子分类：
    并行取各子分类同页码，按更新时间合并成一页；结果照常写缓存，后台静默刷新同样走这条路"""
    pg = max(1, int(pg or 1))

    def one(kid):
        try:
            cq = re.sub(r"(^|&)t=\d+", r"\1t=%d" % kid, query)
            out, _ = cms_build(key, cq, meta, True, timeout)
            d = json.loads(out.decode("utf-8", "ignore"))
            return int(d.get("total") or 0) or len(d.get("list") or []), (d.get("list") or [])
        except Exception:
            return 0, []

    total, items = 0, []
    with ThreadPoolExecutor(max_workers=min(8, len(kids))) as ex:
        for t_n, lst in ex.map(one, kids[:16]):
            total += t_n
            items.extend(lst)
    items.sort(key=lambda v: (str(v.get("vod_time") or ""), str(v.get("vod_id") or "")), reverse=True)
    out = {"code": 1, "msg": "list", "page": pg,
           "pagecount": max(1, (total + 19) // 20), "limit": "20",
           "total": total, "list": items[:20]}
    body = json.dumps(out, ensure_ascii=False).encode()
    cms_store((key, query), body, "application/json; charset=utf-8", True)
    print("[parent] %s t=%d 合并 %d 个子分类 → %d 条 (total %d)" % (key, tid, len(kids), len(items[:20]), total), flush=True)
    return body, "application/json; charset=utf-8"


def cms_build(key, query, meta, cacheable=True, timeout=15):
    """请求上游 CMS，过滤分类/条目，改写封面地址，并写入缓存"""
    ck = (key, query)
    # 父分类（type_pid 指向它、自身无内容）→ 直接合并子分类，不去请求上游这个空分类
    tm = re.search(r"(?:^|&)t=(\d+)(?:&|$)", query)
    if tm and "ac=videolist" in query:
        _kids = cls_children(key, meta, int(tm.group(1)))
        if _kids:
            _pg = re.search(r"(?:^|&)pg=(\d+)(?:&|$)", query)
            return cms_build_parent(key, meta, query, int(tm.group(1)),
                                    int(_pg.group(1)) if _pg else 1, _kids, timeout)
    url = meta["api"] + ("?" + query if query else "")
    req = urllib.request.Request(url, headers={"User-Agent": "TVBox/1.0.0"})
    t0 = time.time()
    try:
        raw = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")
    except Exception as e:
        fo_note(key, False, str(e)[:50] or "上游请求失败")
        raise
    _stats["slow"] = _stats.get("slow", 0) + (1 if time.time() - t0 > 3 else 0)
    try:
        data = json.loads(raw)
    except Exception:
        fo_note(key, False, "上游返回非 JSON")
        out = raw.encode()
        cms_store(ck, out, "text/plain; charset=utf-8", cacheable)
        return out, "text/plain; charset=utf-8"
    fo_note(key, True)
    if isinstance(data, dict):
        if isinstance(data.get("class"), list):
            if cacheable and data["class"]:
                cls_map_learn(key, data["class"])
                data["class"] = cls_map_fill(key, data["class"])
            b = len(data["class"])
            data["class"] = [c for c in data["class"] if isinstance(c, dict)
                             and not bad_cat(c.get("type_name") or c.get("name"))]
            if cacheable:
                bump_key(ck[0] + "|" + ck[1] + "|class", "cat", b - len(data["class"]))
            else:
                bump("cat", b - len(data["class"]))
        if isinstance(data.get("list"), list):
            b = len(data["list"])
            kept = [v for v in data["list"]
                    if not isinstance(v, dict) or not bad_cat_item(v.get("type_name") or "")]
            kept2 = [v for v in kept
                     if not isinstance(v, dict) or not bad_title(v.get("vod_name") or "")]
            if cacheable:
                bump_key(ck[0] + "|" + ck[1] + "|list", "cat", b - len(kept))
                bump_key(ck[0] + "|" + ck[1] + "|list", "title", len(kept) - len(kept2))
            else:
                bump("cat", b - len(kept))
                bump("title", len(kept) - len(kept2))
            data["list"] = kept2
            for v in data["list"]:
                if isinstance(v, dict):
                    proxify_item(v, meta.get("api") or "")
    out = json.dumps(data, ensure_ascii=False).encode()
    cms_store(ck, out, "application/json; charset=utf-8", cacheable)
    return out, "application/json; charset=utf-8"


_bg = {"running": set()}
_bg_lock = threading.Lock()


def cms_refresh_async(key, query, meta, ck):
    with _bg_lock:
        if ck in _bg["running"]:
            return
        _bg["running"].add(ck)
    try:
        cms_build(key, query, meta, True)
    except Exception:
        pass
    finally:
        with _bg_lock:
            _bg["running"].discard(ck)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        enc = None
        if len(body) > 1024 and "gzip" in (self.headers.get("Accept-Encoding") or "").lower():
            body = gzip.compress(body, 6)
            enc = "gzip"
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        if enc:
            self.send_header("Content-Encoding", enc)
            self.send_header("Vary", "Accept-Encoding")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
        self._sent = len(body)      # 实际出向字节（gzip 后），供流量统计计量

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode())

    def do_GET(self):
        t0 = time.time()
        reload_config()
        _stats["requests"] += 1
        p = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(p.query)
        try:
            if p.path in ("/", "/index.html"):
                # 浏览器打开 = 后台页面；TVBox/FongMi 这类 App 的 UA 是 okhttp/dalvik，
                # 直接把配置 JSON 给它 —— 机顶盒里点播源可以只填 http://NAS:3032
                ua0 = (self.headers.get("User-Agent") or "").lower()
                if any(k in ua0 for k in ("okhttp", "dalvik", "tvbox", "fongmi", "_movie", "/movie")):
                    return self.handle_tvbox()
                return self._send(200, render_admin().encode("utf-8"), "text/html; charset=utf-8")
            if p.path == "/logo.png":
                return self.handle_logo()
            if p.path == "/img":
                return self.handle_img(qs)
            if p.path == "/api/settings":
                return self._json({"ok": True,
                                   "sourceSyncMinutes": _f.get("sync_min", 5),
                                   "backupOnSave": bool(_f.get("backup")),
                                   "backupKeep": _f.get("keep", 5),
                                   "tmdbEnabled": bool(_f.get("tmdb_on", True)),
                                   "filterEnabled": bool(_f.get("filter_on", True)),
                                   "categoryFilterEnabled": bool(_f.get("cats_on", True)),
                                   "titleFilterEnabled": bool(_f.get("titles_on", True)),
                                   "tmdbHit": _stats["thit"], "tmdbMiss": _stats["tmiss"],
                                   "sourceSyncEnabled": bool(_f.get("sync_on", True)),
                                   "imageCacheEnabled": bool(_f.get("img_on", True)),
                                   "imgEntries": img_stats()["n"],
                                   "imgMB": round(img_stats()["b"] / 1048576.0, 1),
                                   "imageCacheMaxMB": int(_f.get("img_max_mb") or 256),
                                   "imageCacheKeepDays": int(_f.get("img_keep_days") or 30),
                                   "tmdbCacheKeepDays": int(_f.get("tmdb_keep_days") or 60),
                                   "cacheMaxMB": int(_f.get("cache_max_mb") or 512),
                                   "cacheAutoCleanEnabled": bool(_f.get("cache_auto_clean", True)),
                                   "tvboxRefreshMinutes": _f.get("tvcache_min", 5),
                                   "tvboxCacheAgo": (time.time() - _luna["ts"]) if _luna["ts"] else None,
                                   "tvboxBuildSec": round(_luna["dur"], 2),
                                   "logoUpdated": logo_updated(),
                                   "onlineIdleMinutes": int(_f.get("idle_min") or 5),
                                   "ignoreDevices": sorted(_f.get("ignore_dev") or []),
                                   "lastSyncAgo": (time.time() - _sources["ts"]) if _sources["ts"] else None})
            if p.path == "/api/devices":
                return self._json({"ok": True, **devices_snapshot()})
            if p.path == "/api/cache":
                items = cache_items()
                return self._json({"ok": True, "dir": CACHE_DIR, "items": items,
                                   "total": sum(int(x["bytes"]) for x in items),
                                   "maxMB": int(_f.get("cache_max_mb") or 512),
                                   "imgMaxMB": int(_f.get("img_max_mb") or 256),
                                   "imgKeepDays": int(_f.get("img_keep_days") or 30),
                                   "tmdbKeepDays": int(_f.get("tmdb_keep_days") or 60),
                                   "autoClean": bool(_f.get("cache_auto_clean", True)),
                                   "hk": dict(CACHE_HK)})
            if p.path == "/api/search_stats":
                return self._json(searchstats_snapshot())
            if p.path == "/api/tmdb_test":
                t0 = time.time()
                if not TMDB_TOKEN:
                    return self._json({"ok": False, "ms": 0, "error": "未配置 TMDB 令牌"})
                try:
                    _tmdb_api("/search/multi", query="test", language=TMDB_LANG)
                    return self._json({"ok": True, "ms": round((time.time() - t0) * 1000),
                                       "enabled": bool(_f.get("tmdb_on", True))})
                except Exception as e:
                    return self._json({"ok": False, "ms": round((time.time() - t0) * 1000),
                                       "error": str(e)[:80]})
            if p.path == "/health":
                return self._json({"ok": True, "cats": len(_f["cats"]), "titles": len(_f["titles"]),
                                   "sources": len(sources_cached())})
            if p.path == "/api/status":
                mm = month_rec()
                return self._json({"ok": True, "uptime": time.time() - START_TS,
                                   "cats": len(_f["cats"]), "titles": len(_f["titles"]),
                                   "sources": len(sources_cached()),
                                   "sourcesEnabled": len([k for k in sources_cached() if not src_off(k)]),
                                   "blocked": _stats["blocked"], "requests": _stats["requests"],
                                   "blockedCat": _stats["bcat"], "blockedTitle": _stats["btitle"],
                                   "statsSince": _stats_p["since"], "statsMonth": stats_month_key(),
                                   "monthCat": mm["cat"], "monthTitle": mm["title"],
                                   "monthHit": mm["hit"], "monthMiss": mm["miss"],
                                   "statsMonths": sorted(_stats_p["months"]),
                                   "logoUpdated": logo_updated(),
                                   "lastSyncAgo": (time.time() - _sources["ts"]) if _sources["ts"] else None,
                                   "tvboxCacheAgo": (time.time() - _luna["ts"]) if _luna["ts"] else None,
                                   "tvboxBuildSec": round(_luna["dur"], 2),
                                   "slowCms": _stats.get("slow", 0),
                                   "cmsEntries": len(_cms_cache),
                                   "cmsMB": round(sum(len(v[1]) for v in _cms_cache.values()) / 1048576, 1),
                                   "imgEntries": img_stats()["n"],
                                   "imgMB": round(img_stats()["b"] / 1048576.0, 1),
                                   "filterOn": bool(_f.get("filter_on", True)),
                                   "adultMode": bool(ADULT_MODE),
                                   "tmdbOn": bool(_f.get("tmdb_on", True)),
                                   "catsOn": bool(_f.get("cats_on", True)),
                                   "titlesOn": bool(_f.get("titles_on", True)),
                                   "syncOn": bool(_f.get("sync_on", True)),
                                   "imgOn": bool(_f.get("img_on", True)),
                                   "tmdbHit": _stats["thit"], "tmdbMiss": _stats["tmiss"],
                                   "tmdbErr": _tmdb["err"], "tmdbN": len(_tmdb["map"]),
                                   "idleMin": int(_f.get("idle_min") or 5),
                                   "devices": devices_snapshot(),
                                   "traffic": flow_snapshot()})
            if p.path == "/api/config":
                c = read_config_file()
                return self._json({"categories": c.get("categories") or list(DEFAULT_CATS),
                                   "titles": c.get("titles") or list(DEFAULT_TITLES),
                                   "disabledSources": c.get("disabledSources") or [],
                                   "sourceOrder": c.get("sourceOrder") or [],
                                   "sourceLimit": c.get("sourceLimit", 0) or 0,
                                   "activeSource": c.get("activeSource", "") or "",
                                   "autoFailover": bool(_f.get("fo_on", True)),
                                   "failoverThreshold": fo_thresh(),
                                   "backupSources": list(_f.get("fo_list") or []),
                                   "filterEnabled": bool(_f.get("filter_on", True)),
                                   "categoryFilterEnabled": bool(_f.get("cats_on", True)),
                                   "titleFilterEnabled": bool(_f.get("titles_on", True)),
                                   "tmdbEnabled": bool(_f.get("tmdb_on", True)),
                                   "searchAllSources": bool(_f.get("search_all", True)),
                                   "searchLimit": int(_f.get("search_limit") or 0),
                                   "sourceHealthEnabled": bool(_f.get("sh_on", True)),
                                   "sourceHealthInterval": int(_f.get("sh_sec") or 600),
                                   "sourceHealthFail": int(_f.get("sh_fail") or 3),
                                   "sourceHealthRecover": int(_f.get("sh_ok") or 2),
                                   "sourceHealthTimeout": int(_f.get("sh_to") or 10)})
            if p.path == "/api/sources":
                order = {k: i for i, k in enumerate(_f["order"])}
                eff = {k: i for i, k in enumerate(ordered_source_keys("deliver"))}
                items = [{"key": k, "name": v["name"], "disabled": src_off(k),
                          "autoOff": k in _sh["auto"],
                          "manualOff": k in _f["disabled"],
                          "lunaOff": bool(v.get("disabled")),
                          "avgMs": (lambda m: int(sum(m) / len(m)) if m else 0)(
                              [int(x) for x in ((_sh["items"].get(k) or {}).get("msh") or []) if x]),
                          "lastMs": int((_sh["items"].get(k) or {}).get("ms") or 0),
                          "pos": eff.get(k, len(eff)) + 1 if k in eff else 0}
                         for k, v in sources_cached().items()]
                asort_on = bool(_f.get("asort_on")) and str(_f.get("asort_scope") or "all") in ("all", "deliver")
                if asort_on:      # 自动排序开启：列表顺序 = 实际下发顺序
                    items.sort(key=lambda x: (eff.get(x["key"], 9999), order.get(x["key"], 9999)))
                else:
                    items.sort(key=lambda x: order.get(x["key"], len(order) + 100))
                keys = [x["key"] for x in items if not x["disabled"]]
                delivered = pick_delivered(keys)
                return self._json({"ok": True, "sources": items,
                                   "total": len(items), "enabled": len(keys),
                                   "delivered": len(delivered),
                                   "searchAllSources": bool(_f.get("search_all", True)),
                                   "searchLimit": int(_f.get("search_limit") or 0),
                                   "searchPrimary": fo_primary(),
                                   "primary": fo_primary(),
                                   "configuredPrimary": fo_conf_primary(),
                                   "failoverActive": bool(fo_active()),
                                   "failoverSince": _fo.get("since") or 0,
                                   "failoverSwitches": int(_fo.get("switches") or 0),
                                   "failoverLog": list(_fo.get("log") or [])[-8:][::-1],
                                   "failoverFail": {k: dict(v) for k, v in (_fo.get("fail") or {}).items()},
                                   "autoFailover": bool(_f.get("fo_on", True)),
                                   "failoverThreshold": fo_thresh(),
                                   "backupSources": list(_f.get("fo_list") or []),
                                   "backupChain": fo_backups(),
                                   "searchExtraTotal": len(search_extras()),
                                   "searchSources": _search_breadth(),
                                   "asort": {"on": bool(_f.get("asort_on")),
                                             "mode": str(_f.get("asort_mode") or "avg"),
                                             "min": int(_f.get("asort_min") or 2),
                                             "scope": str(_f.get("asort_scope") or "all"),
                                             "failLast": bool(_f.get("asort_fail_last", True))}})
            if p.path == "/api/test":
                return self.handle_test((qs.get("q") or [""])[0])
            if p.path == "/api/tvbox":
                return self.handle_tvbox()
            if p.path in ("/agg", "/agg/", "/agg/api.php/provide/vod", "/api.php/provide/vod"):
                return self.handle_agg(qs)
            if p.path == "/api/agg":
                agg_build()
                with _agg["lock"]:
                    pool_n = len(_agg["pool"])
                return self._json({"ok": True, "enabled": bool(_f.get("agg_on", True)),
                                   "url": SELF + "/agg",
                                   "browseSources": int(_f.get("agg_browse") or 8),
                                   "searchSources": int(_f.get("agg_search") or 0),
                                   "inTvbox": bool(_f.get("agg_tvbox")),
                                   "tvSubs": bool(_f.get("agg_tvsub")),
                                   "homeCat": str(_f.get("agg_home") or ""),
                                   "pageSize": int(_f.get("agg_page") or 20),
                                   "topPages": int(_f.get("agg_top_pg") or 1),
                                   "subPages": int(_f.get("agg_sub_pg") or 1),
                                   "subSources": int(_f.get("agg_sub_src") or 12),
                                   "fillOn": bool(_f.get("agg_fill", True)),
                                   "fillMax": int(_f.get("agg_fill_max") or 20),
                                   "autoOn": bool(_f.get("agg_auto_on")),
                                   "autoSec": int(_f.get("agg_auto_sec") or 3600),
                                   "autoAt": str(_f.get("agg_auto_at") or ""),
                                   "autoScope": str(_f.get("agg_auto_scope") or "hot"),
                                   "autoHot": int(_f.get("agg_auto_n") or 12),
                                   "autoMax": int(_f.get("agg_auto_max") or 80),
                                   "autoBudget": int(_f.get("agg_auto_budget") or 600),
                                   "lazyOn": bool(_f.get("agg_lazy", True)),
                                   "cats": _agg["cats"],
                                   "catCount": len(_agg["cats"]),
                                   "topCount": len(_agg["top"]),
                                   "subCount": len(_agg["cats"]) - len(_agg["top"]),
                                   "sourceCount": len(set(k for v in _agg["map"].values() for k in v if src_on(k))),
                                   "poolCount": pool_n,
                                   "builtAgo": (time.time() - _agg["ts"]) if _agg["ts"] else None})
            if p.path == "/api/agg/cats":
                agg_build()
                off = _f.get("agg_cat_off") or set()
                idx = {nm: i for i, nm in enumerate(_f.get("agg_cat_order") or [])}
                cats = sorted(_agg["cats"], key=lambda c: idx.get(c["type_name"], 9999)) \
                    if idx else list(_agg["cats"])
                groups = []
                for c in cats:
                    if c.get("type_pid"):
                        continue
                    subs = [x for x in cats if str(x.get("type_pid") or "") == str(c["type_id"])]
                    groups.append({"name": c["type_name"], "tid": str(c["type_id"]),
                                   "on": c["type_name"] not in off,
                                   "srcN": len(agg_map_on(str(c["type_id"]))),
                                   "subs": [{"name": x["type_name"], "tid": str(x["type_id"]),
                                             "on": x["type_name"] not in off,
                                             "srcN": len(agg_map_on(str(x["type_id"])))}
                                            for x in subs if agg_map_on(str(x["type_id"]))]})
                return self._json({"ok": True, "groups": groups,
                                   "order": list(_f.get("agg_cat_order") or []),
                                   "off": sorted(off)})
            if p.path == "/api/agg/test":
                return self.handle_agg_test((qs.get("q") or [""])[0])
            if p.path == "/api/agg/collect":
                return self._json({"ok": True, **acq_snapshot()})
            if p.path in ("/api/srcstat", "/api/src/health"):
                return self._json(sh_snapshot())
            if p.path.startswith("/cms/"):
                return self.handle_cms(p)
            self._json({"error": "not found"}, 404)
        except Exception as e:
            try:
                self._json({"error": str(e)}, 502)
            except Exception:
                pass
        finally:
            self.log_access(p, time.time() - t0)

    def log_access(self, p, dur):
        ua0 = (self.headers.get("User-Agent") or "")
        # 机顶盒 App 直接拉 http://NAS:3032（" / "）也算一次数据流：配置 JSON 有时几十 KB
        cfg_fetch = p.path in ("/", "/index.html") and any(
            k in ua0.lower() for k in ("okhttp", "dalvik", "tvbox", "fongmi", "_movie", "/movie"))
        tracked = cfg_fetch or p.path == "/api/tvbox" or p.path.startswith("/cms/") or p.path == "/img"
        if not tracked:
            return
        ua = ua0[:30]
        ip = (self.client_address or ("?", 0))[0]
        # 端口隔离：只计量本实例端口进来的请求（3032 的账本只记 3032，3035 的只记 3035）
        try:
            if int((self.server.server_address or (None, LISTEN_PORT))[1]) != LISTEN_PORT:
                return
        except Exception:
            pass
        flow_note(ip, getattr(self, "_sent", 0))     # 流量：总量全记，服务流量不进设备明细
        if ip not in ignore_hosts() and not _is_docker_gw(ip):   # 网关自身 / NAS LunaTV / 网桥网关不算设备
            dev_touch(ip, ua, p.path)
        if p.path == "/img":       # 封面请求只算活跃，不进控制台日志（太吵）
            return
        print("[req] %s %s%s %.0fms ua=%s" % (ip, p.path, ("?" + p.query) if p.query else "", dur * 1000, ua), flush=True)

    def do_POST(self):
        reload_config()
        p = urllib.parse.urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n).decode("utf-8") if n else "{}"
            body = json.loads(raw or "{}")
            if p.path == "/api/config":
                save_config(body)
                return self._json({"ok": True, "cats": len(_f["cats"]), "titles": len(_f["titles"])})
            if p.path == "/api/settings":
                save_config(body)
                return self._json({"ok": True, "sourceSyncMinutes": _f["sync_min"],
                                   "backupOnSave": _f["backup"], "backupKeep": _f["keep"],
                                   "tmdbEnabled": bool(_f.get("tmdb_on", True)),
                                   "sourceSyncEnabled": bool(_f.get("sync_on", True)),
                                   "imageCacheEnabled": bool(_f.get("img_on", True)),
                                   "onlineIdleMinutes": int(_f.get("idle_min") or 5),
                                   "ignoreDevices": sorted(_f.get("ignore_dev") or []),
                                   "imageCacheMaxMB": int(_f.get("img_max_mb") or 256),
                                   "imageCacheKeepDays": int(_f.get("img_keep_days") or 30),
                                   "tmdbCacheKeepDays": int(_f.get("tmdb_keep_days") or 60),
                                   "cacheMaxMB": int(_f.get("cache_max_mb") or 512),
                                   "cacheAutoCleanEnabled": bool(_f.get("cache_auto_clean", True)),
                                   "tvboxRefreshMinutes": _f.get("tvcache_min", 5)})
            if p.path == "/api/img/clear":
                n, freed = img_clear()
                return self._json({"ok": True, "files": n, "mb": round(freed / 1048576.0, 1)})
            if p.path == "/api/cache/clear":
                tgt = body.get("target") or body.get("key") or ""
                if not tgt and isinstance(body.get("targets"), list):
                    ok_all, msgs = True, []
                    for t in body["targets"]:
                        o, m = cache_clear(t)
                        ok_all = ok_all and o
                        msgs.append(m)
                    return self._json({"ok": ok_all, "msg": "；".join(msgs),
                                       "items": cache_items()})
                ok, msg = cache_clear(tgt)
                return self._json({"ok": ok, "msg": msg, "items": cache_items()})
            if p.path == "/api/cache/clean":
                hk = cache_housekeep(force=True)
                items = cache_items()
                return self._json({"ok": True, "msg": (hk or {}).get("msg") or "",
                                   "hk": hk or {}, "items": items,
                                   "total": sum(int(x["bytes"]) for x in items)})
            if p.path == "/api/stats/reset":
                stats_reset()
                return self._json({"ok": True, "blockedCat": 0, "blockedTitle": 0,
                                   "tmdbHit": 0, "tmdbMiss": 0,
                                   "statsSince": _stats_p["since"], "statsMonth": stats_month_key()})
            if p.path == "/api/traffic/reset":
                flow_reset()
                return self._json({"ok": True, **flow_snapshot()})
            if p.path == "/api/devices/reset":
                devices_reset()
                return self._json({"ok": True, **devices_snapshot()})
            if p.path == "/api/failover/reset":
                fo_reset()
                return self._json({"ok": True, "primary": fo_primary(),
                                   "failoverActive": bool(fo_active())})
            if p.path == "/api/failover/probe":
                k = fo_primary()
                meta = sources_cached().get(k) or {}
                ok, ms, msg = False, 0, "源不存在"
                if meta:
                    t0 = time.time()
                    try:
                        d = _upstream_json(meta, "ac=class", 10)
                        ok = isinstance(d, dict) and bool(d.get("class") or d.get("list"))
                        msg = ("分类 %d 个" % len(d.get("class") or [])) if ok else "返回空分类"
                    except Exception as e:
                        msg = str(e)[:60]
                    ms = int((time.time() - t0) * 1000)
                    fo_note(k, ok, msg)
                # 当前在备源上时，顺手探一下原主源；恢复了就立刻切回去（手动检测不受冷却限制）
                back = ""
                if fo_active():
                    k2 = fo_conf_primary()
                    m2 = sources_cached().get(k2) or {}
                    if m2:
                        try:
                            d2 = _upstream_json(m2, "ac=class", 10)
                            if isinstance(d2, dict) and (d2.get("class") or d2.get("list")):
                                back = k2
                        except Exception:
                            back = ""
                    if back:
                        fo_reset()
                return self._json({"ok": True, "source": k, "alive": ok, "ms": ms,
                                   "detail": msg, "primary": fo_primary(),
                                   "switchedBack": back,
                                   "failoverActive": bool(fo_active())})
            if p.path == "/api/search_stats/reset":
                searchstats_reset()
                return self._json({"ok": True, **searchstats_snapshot()})
            if p.path == "/api/sync":
                m = sources_cached(force=True)
                return self._json({"ok": bool(m), "sources": len(m)})
            if p.path == "/api/agg":
                save_config(body)
                return self._json({"ok": True, "enabled": bool(_f.get("agg_on", True)),
                                   "browseSources": int(_f.get("agg_browse") or 8),
                                   "searchSources": int(_f.get("agg_search") or 0),
                                   "inTvbox": bool(_f.get("agg_tvbox")),
                                   "tvSubs": bool(_f.get("agg_tvsub")),
                                   "homeCat": str(_f.get("agg_home") or ""),
                                   "pageSize": int(_f.get("agg_page") or 20),
                                   "topPages": int(_f.get("agg_top_pg") or 1),
                                   "subPages": int(_f.get("agg_sub_pg") or 1),
                                   "subSources": int(_f.get("agg_sub_src") or 12)})
            if p.path == "/api/agg/rebuild":
                with _agg["lock"]:
                    _agg["pool"].clear()
                _agg["ts"] = 0
                threading.Thread(target=agg_build, kwargs={"force": True}, daemon=True).start()
                return self._json({"ok": True})
            if p.path == "/api/agg/cats":
                save_config(body)
                return self._json({"ok": True,
                                   "off": sorted(_f.get("agg_cat_off") or set()),
                                   "order": list(_f.get("agg_cat_order") or [])})
            if p.path == "/api/agg/collect":
                act = str(body.get("action") or "save")
                if act == "save":
                    up = {}
                    if "on" in body:
                        up["aggAutoCollectEnabled"] = bool(body.get("on"))
                    if "sec" in body:
                        up["aggAutoInterval"] = max(300, min(86400, int(body.get("sec") or 3600)))
                    if "at" in body:
                        up["aggAutoAt"] = str(body.get("at") or "").strip()
                    if "scope" in body:
                        up["aggAutoScope"] = str(body.get("scope") or "hot")
                    if "hot" in body:
                        up["aggAutoHotCount"] = max(1, min(80, int(body.get("hot") or 12)))
                    if "max" in body:
                        up["aggAutoMaxCats"] = max(1, min(400, int(body.get("max") or 80)))
                    if "budget" in body:
                        up["aggAutoBudget"] = max(60, min(3600, int(body.get("budget") or 600)))
                    if "lazy" in body:
                        up["aggLazyCollectEnabled"] = bool(body.get("lazy"))
                    save_config(up)
                    with _acq["lock"]:
                        _acq["next"] = acq_next_time(_f.get("agg_auto_at"), int(_f.get("agg_auto_sec") or 3600)) \
                            if _f.get("agg_auto_on") else 0
                        _acq["dirty"] = True
                    acq_save(True)
                    return self._json({"ok": True, **acq_snapshot()})
                if act == "start":
                    kw = {"scope": str(body.get("scope") or _f.get("agg_auto_scope") or "hot")}
                    if body.get("hot") is not None:
                        kw["hot"] = int(body.get("hot"))
                    if body.get("max") is not None:
                        kw["maxn"] = int(body.get("max"))
                    if body.get("budget") is not None:
                        kw["budget"] = int(body.get("budget"))
                    r = acq_start("manual", **kw)
                    return self._json({**acq_snapshot(), "ok": bool(r.get("ok")),
                                       "msg": r.get("msg") or ""})
                if act == "stop":
                    r = acq_stop()
                    return self._json({**acq_snapshot(), "ok": bool(r.get("ok")),
                                       "msg": r.get("msg") or ""})
                if act == "clear":
                    # 清零采集记录：日志、上次结果、本轮计数一起归零；不动采集计划（next）与已缓存内容
                    if _acq.get("running"):
                        return self._json({"ok": False, "msg": "正在采集，先点「停止」再清零",
                                           **acq_snapshot()})
                    with _acq["lock"]:
                        _acq["log"] = []
                        _acq["last"] = {}
                        _acq.update({"cur": "", "done": 0, "total": 0, "cats": 0,
                                     "items": 0, "failed": 0, "abort": "",
                                     "trigger": "", "started": 0.0, "finished": 0.0,
                                     "dirty": True})
                    acq_save(True)
                    return self._json({"ok": True, **acq_snapshot()})
            if p.path in ("/api/srcstat", "/api/src/health"):
                act = str(body.get("action") or "save")
                if act == "save":
                    # 只写请求里带了的字段，缺的沿用配置文件里的现有值
                    conf_up = {}
                    if "on" in body:
                        conf_up["sourceHealthEnabled"] = bool(body.get("on"))
                    if "sec" in body:
                        conf_up["sourceHealthInterval"] = max(60, min(86400, int(body.get("sec") or 600)))
                    if "fail" in body:
                        conf_up["sourceHealthFail"] = max(1, min(20, int(body.get("fail") or 3)))
                    if "ok" in body:
                        conf_up["sourceHealthRecover"] = max(1, min(10, int(body.get("ok") or 2)))
                    if "timeout" in body:
                        conf_up["sourceHealthTimeout"] = max(3, min(60, int(body.get("timeout") or 10)))
                    # 排序设置（片源列表 / 源检测页共用这套字段）
                    if "asortOn" in body:
                        conf_up["sourceAutoSortEnabled"] = bool(body.get("asortOn"))
                    if "asortMode" in body:
                        conf_up["sourceAutoSortMode"] = "recent" if str(body.get("asortMode")) == "recent" else "avg"
                    if "asortMin" in body:
                        conf_up["sourceAutoSortMinProbes"] = max(1, min(20, int(body.get("asortMin") or 2)))
                    if "asortScope" in body:
                        conf_up["sourceAutoSortScope"] = str(body.get("asortScope") or "all")
                    if "asortFailLast" in body:
                        conf_up["sourceAutoSortFailLast"] = bool(body.get("asortFailLast"))
                    if not conf_up:
                        return self._json({"ok": True, "conf": sh_conf()})
                    save_config(conf_up)
                    c = sh_conf()
                    if c["on"] and not _sh.get("next"):
                        _sh["next"] = time.time() + 5
                    return self._json({"ok": True, "conf": c,
                                       "asort": {"on": bool(_f.get("asort_on")),
                                                 "mode": str(_f.get("asort_mode") or "avg"),
                                                 "min": int(_f.get("asort_min") or 2),
                                                 "scope": str(_f.get("asort_scope") or "all"),
                                                 "failLast": bool(_f.get("asort_fail_last", True))},
                                       "nextIn": max(0, int(float(_sh.get("next") or 0) - time.time()))})
                if act == "probe":
                    if _sh.get("running"):
                        return self._json({"ok": True, "running": True, "skipped": True})
                    keys = [str(k) for k in (body.get("keys") or []) if str(k).strip()] or None
                    _sh["next"] = time.time() + sh_conf()["sec"]
                    threading.Thread(target=sh_probe_all, kwargs={"keys": keys}, daemon=True).start()
                    return self._json({"ok": True, "running": True})
                if act == "probe_one":
                    k = str(body.get("key") or "")
                    ok, ms, detail = sh_probe_retry(k)
                    r = sh_note(k, ok, ms, detail, probe=True, persist=True)
                    return self._json({"ok": True, "key": k, "alive": ok, "ms": ms,
                                       "detail": detail, "state": _src_state(k),
                                       "dropped": r.get("dropped") or "",
                                       "recovered": r.get("recovered") or ""})
                if act in ("on", "off"):
                    return self._json(sh_set_enabled(str(body.get("key") or ""), act == "on"))
                if act == "clear":
                    with _sh["lock"]:
                        _sh["auto"].clear()
                        for it in _sh["items"].values():
                            it["auto"] = 0.0
                        sh_log("全部自动停用标记已清除")
                        _sh["dirty"] = True
                    sh_save(True)
                    sh_drop_cache()
                    return self._json({"ok": True, "autoKeys": []})
                return self._json({"ok": False, "error": "未知操作"}, 400)
            if p.path == "/api/logo":
                return self.handle_logo_upload(body)
            self._json({"error": "not found"}, 404)
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 400)

    def handle_logo(self):
        if os.path.exists(LOGO_PATH):
            with open(LOGO_PATH, "rb") as fp:
                data = fp.read()
            ctype = "image/png"
            if data[:3] == b"\xff\xd8\xff":
                ctype = "image/jpeg"
            elif data[:4] == b"RIFF":
                ctype = "image/webp"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            return self.wfile.write(data)
        try:
            data = urllib.request.urlopen(LUNA + "/logo.png", timeout=10).read()
            return self._send(200, data, "image/png")
        except Exception:
            return self._json({"error": "no logo"}, 404)

    def handle_logo_upload(self, body):
        if body.get("reset"):
            try:
                if os.path.exists(LOGO_PATH):
                    os.remove(LOGO_PATH)
            except Exception as e:
                return self._json({"ok": False, "error": str(e)}, 400)
            _admin["mtime"] = 0.0
            return self._json({"ok": True, "updated": 0})
        d = body.get("data") or ""
        if "," in d:
            d = d.split(",", 1)[1]
        try:
            raw = base64.b64decode(d)
        except Exception:
            return self._json({"ok": False, "error": "base64 解析失败"}, 400)
        if len(raw) < 64:
            return self._json({"ok": False, "error": "图片过小或为空"}, 400)
        if len(raw) > 2 * 1024 * 1024:
            return self._json({"ok": False, "error": "图片超过 2MB"}, 400)
        if not (raw[:8] == b"\x89PNG\r\n\x1a\n" or raw[:3] == b"\xff\xd8\xff" or raw[:4] == b"RIFF"):
            return self._json({"ok": False, "error": "仅支持 PNG / JPG / WebP"}, 400)
        try:
            os.makedirs(os.path.dirname(LOGO_PATH), exist_ok=True)
            with open(LOGO_PATH, "wb") as fp:
                fp.write(raw)
        except Exception as e:
            return self._json({"ok": False, "error": str(e)}, 400)
        return self._json({"ok": True, "updated": logo_updated()})

    def handle_test(self, q):
        if not q:
            return self._json({"ok": False, "error": "empty query"})
        srcs = sources_cached()
        wq = urllib.parse.quote(q)

        def one(item):
            key, meta = item
            try:
                url = meta["api"] + "?ac=videolist&wd=" + wq
                req = urllib.request.Request(url, headers={"User-Agent": "TVBox/1.0.0"})
                d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode("utf-8", "ignore"))
                lst = d.get("list") or []
                kept = [v for v in lst
                        if not bad_cat_item(v.get("type_name") or "") and not bad_title(v.get("vod_name") or "")]
                return {"name": meta["name"], "raw": len(lst), "kept": len(kept)}
            except Exception:
                return {"name": meta["name"], "raw": 0, "kept": 0}

        active = [(k, v) for k, v in srcs.items() if not src_off(k)]
        with ThreadPoolExecutor(max_workers=6) as ex:
            results = list(ex.map(one, active))
        word_blocked = any(w in q.lower() for w in _f["cats"] + _f["titles"])
        self._json({"ok": True, "wordBlocked": word_blocked, "results": results})

    def handle_tvbox(self):
        base = luna_cfg()
        if base is None:
            return self._json({"error": "upstream unavailable", "detail": _luna["err"]}, 502)
        cfg = copy.deepcopy(base)
        srcmap = sources_cached()
        # 别把网关自己的聚合源（LunaTV 里那条 /agg）当普通站下发，否则 3035 会看到一个带屏蔽的 3032
        sites = [x for x in (cfg.get("sites") or [])
                 if not src_off(x.get("key") or "") and not self_api(x.get("api") or "")]
        # 下发顺序：手动顺序或「按响应时间自动排序」（自动排序开启时快的排前面）
        order = {k: i for i, k in enumerate(ordered_source_keys("deliver"))}
        sites.sort(key=lambda x: order.get(x.get("key") or "", len(order) + 100))
        # 单源/限量模式：只下发指定数量的源，机顶盒启动只加载这些
        act = _f.get("src_active") or ""
        limit = int(_f.get("src_limit") or 0)
        ovr = fo_active()      # 自动切换后的生效主源，排到第一位
        if act:
            picked = [x for x in sites if (x.get("key") or "") == (ovr or act)]
            sites = picked or sites[:1]
        elif ovr:
            first = [x for x in sites if (x.get("key") or "") == ovr]
            rest = [x for x in sites if (x.get("key") or "") != ovr]
            sites = ([first[0]] + rest) if first else sites
            if limit > 0:
                sites = sites[:limit]
        elif limit > 0:
            sites = sites[:limit]
        cfg["sites"] = sites
        # 自建聚合采集源可选下发到机顶盒：一个站点装下全部源的片库
        if _f.get("agg_tvbox") and _f.get("agg_on", True):
            if not _agg["cats"]:
                threading.Thread(target=agg_build, daemon=True).start()
            elif _agg["cats"]:
                tpl = dict(sites[0]) if sites else {}
                agg = dict(tpl)
                agg.update({"key": "__agg__", "name": "🧩 聚合源", "type": 1,
                            "api": SELF + "/agg", "searchable": 1, "quickSearch": 1,
                            "filterable": 1, "changeable": 0, "hide": 0,
                            "categories": agg_tvbox_cats(),
                            "filters": agg_filters()})
                cfg["sites"] = [agg] + cfg["sites"]
        cfg["wallpaper"] = SELF + "/logo.png?t=" + str(logo_updated() or int(time.time()))
        for s in sites:
            cats0 = s.get("categories") or []
            # 屏蔽总开关或分类屏蔽关闭时，分类原样下发（连空分类裁剪也跳过）
            trim = bool(_f.get("filter_on", True)) and bool(_f.get("cats_on", True))
            empt = cat_empty_set(s.get("key") or "") if trim else set()
            kept = [c for c in cats0 if str(c).strip() and not bad_cat(c)]
            bump_key("tvbox|" + str(s.get("key") or ""), "cat", len(cats0) - len(kept))
            s["categories"] = [c for c in kept if c not in empt]
            if s.get("key") in srcmap:
                s["api"] = SELF + "/cms/" + s["key"]
        self._send(200, json.dumps(cfg, ensure_ascii=False).encode())

    def handle_img(self, qs):
        url = (qs.get("u") or [""])[0].strip()
        name = (qs.get("n") or [""])[0].strip()
        year = (qs.get("y") or [""])[0].strip()
        ok = url.startswith("http://") or url.startswith("https://")
        if not _f.get("img_on", True):
            # 封面缓存关闭：不下载不落盘，直接 302 回源；无原图时才问一次 TMDB
            tgt = url if ok else ""
            if not tgt and name and _f.get("tmdb_on", True):
                tgt = tmdb_poster(name, year)
            if tgt:
                self.send_response(302)
                self.send_header("Location", tgt)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            return self._json({"error": "no image"}, 404)
        fp = ct = None
        if ok:
            fp, _sz = img_cached(url)
            if fp:
                ct = _img_guess_ct(url)
            else:
                fp, ct = ensure_img(url)
        if not fp and name and _f.get("tmdb_on", True):
            turl = tmdb_poster(name, year)
            if turl:
                fp, _sz = img_cached(turl)
                ct = _img_guess_ct(turl) if fp else None
                if not fp:
                    fp, ct = ensure_img(turl)
        if not fp:
            if ok:
                self.send_response(302)
                self.send_header("Location", url)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            return self._json({"error": "no image"}, 404)
        try:
            with open(fp, "rb") as f:
                data = f.read()
        except OSError:
            return self._json({"error": "read fail"}, 404)
        ct = _sniff_ct(data[:16]) or ct or "image/jpeg"
        self.send_response(200)
        self.send_header("Content-Type", ct or "image/jpeg")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=604800, immutable")
        self.end_headers()
        try:
            self.wfile.write(data)
            self._sent = len(data)
        except Exception:
            pass

    def handle_agg(self, qs):
        """自建聚合采集源：标准苹果 CMS 接口，LunaTV / TVBox 直接当普通源用"""
        if not _f.get("agg_on", True):
            return self._json(agg_empty("聚合采集源已关闭"))
        ac = (qs.get("ac") or [""])[0].strip().lower()
        wd = (qs.get("wd") or qs.get("key") or [""])[0].strip()
        ids = (qs.get("ids") or qs.get("id") or [""])[0].strip()
        try:
            pg = int((qs.get("pg") or ["1"])[0] or 1)
        except Exception:
            pg = 1
        # 详情：id 形如 <源key>~<上游id>，按前缀回到对应源取，播放地址原样透传
        if ids and SEP in ids:
            out = agg_detail_page(ids, ac or "videolist")
            return self._send(200, json.dumps(out, ensure_ascii=False).encode())
        # 搜索：全部（或指定数量）启用源并行搜，合并去重
        if wd:
            wd_hit = _f.get("filter_on", True) and any(
                w in wd.lower() for w in _f["cats"] + _f["titles"])
            search_bump(wd, blocked=wd_hit)
            if wd_hit:
                bump("title")
                print("[agg] 关键词命中屏蔽词: %s" % wd[:20], flush=True)
                return self._json(agg_empty("blocked"))
            ck = ("__agg__", query_key("wd=" + wd))
            with _search_lock:
                hit = _search_cache.get(ck)
            if hit and time.time() - hit[0] < SEARCH_TTL:
                return self._send(200, hit[1])
            out = agg_search_page("ac=videolist&wd=" + urllib.parse.quote(wd))
            body = json.dumps(out, ensure_ascii=False).encode()
            with _search_lock:
                if len(_search_cache) > 200:
                    _search_cache.clear()
                _search_cache[ck] = (time.time(), body)
            return self._send(200, body)
        # 分类表：TVBox 拉分类栏用（按「分类管理」的显隐与排序输出）
        if ac in ("list", "class"):
            agg_build()
            vis = agg_cats_visible()
            return self._json({"code": 1, "msg": "数据列表", "page": 1, "pagecount": 1,
                               "limit": "20", "total": len(vis),
                               "class": vis, "list": []})
        # 分类浏览：t=<我们的 type_id>，跨源合并后本地分页；细分类与筛选条都走这里
        agg_build()
        tid = agg_resolve_tid((qs.get("t") or qs.get("type") or qs.get("type_id") or [""])[0])
        cls_flt = (qs.get("class") or qs.get("cate") or [""])[0]
        if str(cls_flt).strip():       # TVBox 筛选条里的「类型」= 细分类（id 或名字都认）
            ctid = agg_resolve_tid(cls_flt)
            if ctid and ctid.isdigit():
                tid = ctid
        flt = {}
        for k in ("area", "year", "lang", "by"):
            v = (qs.get(k) or [""])[0].strip()
            if v:
                flt[k] = v
        out = agg_list_page(tid or None, pg, flt)
        return self._send(200, json.dumps(out, ensure_ascii=False).encode())

    def handle_agg_test(self, q):
        if not q:
            return self._json({"ok": False, "error": "empty query"})
        keys = agg_search_keys()
        srcs = sources_cached()
        wq = urllib.parse.quote(q)

        def one(k):
            meta = srcs.get(k) or {}
            try:
                d = _upstream_json(meta, "ac=videolist&wd=" + wq, 8)
                lst = d.get("list") or []
                kept = [v for v in lst if not bad_cat_item(v.get("type_name") or "")
                        and not bad_title(v.get("vod_name") or "")]
                return {"key": k, "name": meta.get("name") or k,
                        "raw": len(lst), "kept": len(kept)}
            except Exception:
                return {"key": k, "name": meta.get("name") or k, "raw": 0, "kept": 0, "error": True}

        with ThreadPoolExecutor(max_workers=min(8, max(1, len(keys)))) as ex:
            rows = list(ex.map(one, keys))
        rows.sort(key=lambda x: -x["kept"])
        return self._json({"ok": True, "sources": len(keys),
                           "hit": sum(1 for r in rows if r["kept"]),
                           "items": sum(r["kept"] for r in rows), "results": rows})

    def handle_cms(self, p):
        key = p.path[len("/cms/"):].strip("/")
        meta = sources_cached().get(key)
        if not meta:
            return self._json({"error": "unknown source key"}, 404)
        if src_off(key):
            return self._json({"code": 0, "msg": "source disabled", "class": [], "list": [], "total": 0})
        query = p.query or ""
        qs0 = urllib.parse.parse_qs(query)
        # 机顶盒搜索只带 wd / quick（没有 ac），上游会返回精简字段、连封面都不给；
        # 统一补成 ac=videolist，拿回 vod_pic 等完整字段，搜索结果才有封面
        if (qs0.get("wd") or [""])[0] and not (qs0.get("ac") or [""])[0]:
            query = (query + "&ac=videolist") if query else "ac=videolist"
            qs0 = urllib.parse.parse_qs(query, keep_blank_values=True)
        # 详情：结果可能来自多个源（id 带 <key>~ 前缀），拆开分别取
        ids0 = (qs0.get("ids") or qs0.get("id") or [""])[0]
        if ids0 and SEP in ids0:
            try:
                out = merge_detail(query, ids0)
            except Exception as e:
                print("[detail] merge fail:", e, flush=True)
                out = {"code": 0, "msg": "detail error", "list": [], "total": 0}
            return self._send(200, json.dumps(out, ensure_ascii=False).encode())
        # 搜索：主源聚合其它未下发源
        if (qs0.get("wd") or [""])[0]:
            # 屏蔽开关开启时，搜索词本身命中黑名单 → 整个搜索直接返回空。
            # 部分源是模糊匹配，搜屏蔽词会返回一堆不含屏蔽词的擦边内容，逐条过滤拦不住
            wd0 = (qs0.get("wd") or [""])[0]
            wd_hit = _f.get("filter_on", True) and any(w in wd0.lower() for w in _f["cats"] + _f["titles"])
            search_bump(wd0, blocked=wd_hit)   # 搜索统计：含缓存命中与被拦截的搜索
            if wd_hit:
                bump("title")
                print("[search] 关键词命中屏蔽词: %s" % wd0[:20], flush=True)
                return self._send(200, json.dumps(
                    {"code": 0, "msg": "blocked", "class": [], "list": [], "total": 0},
                    ensure_ascii=False).encode())
            # 缓存键必须带站点：否则 A 站聚合出来的结果会被 B 站直接拿去用
            ck_s = ("__search__", key, query_key(query))
            with _search_lock:
                hit = _search_cache.get(ck_s)
            if hit and time.time() - hit[0] < SEARCH_TTL:
                return self._send(200, hit[1])
            try:
                out = merge_search(key, query)
            except Exception as e:
                print("[search] merge fail:", e, flush=True)
                out = None
            if out is not None:
                body = json.dumps(out, ensure_ascii=False).encode()
                with _search_lock:
                    if len(_search_cache) > 200:
                        _search_cache.clear()
                    _search_cache[ck_s] = (time.time(), body)
                return self._send(200, body)
        # 除搜索（带 wd）外全部可缓存：分类列表、首页/分类列表、详情
        cacheable = "ac=" in query and "wd=" not in query
        ck = (key, query)
        ttl = CMS_TTL_CLASS if "ac=list" in query else CMS_TTL
        if cacheable:
            with _cms_lock:
                hit = _cms_cache.get(ck)
            if hit:
                age = time.time() - hit[0]
                if age < ttl:
                    with _cms_lock:
                        _cms_cache[ck] = (time.time(), hit[1], hit[2])
                    return self._send(200, hit[1], hit[2])
                if age < 3600:
                    # 已过期先返回旧数据（秒开），后台静默刷新
                    threading.Thread(target=cms_refresh_async, args=(key, query, meta, ck),
                                     daemon=True).start()
                    return self._send(200, hit[1], hit[2])
        try:
            out, ctype = cms_build(key, query, meta, cacheable)
        except Exception as e:
            # 上游挂了但有旧数据时兜底返回旧数据
            with _cms_lock:
                hit = _cms_cache.get(ck)
            if hit:
                return self._send(200, hit[1], hit[2])
            return self._json({"code": 0, "msg": "upstream error", "class": [], "list": [], "total": 0}, 200)
        self._send(200, out, ctype)


# ---------------- 本地缓存管理（手动清理各类缓存） ----------------
# 只动「可以重建的缓存」：配置、统计、设备、流量、采集计划都不在这里，
# 它们各自有专门的清零按钮，避免用户清缓存时误删配置。
CACHE_DEFS = [
    ("tmdb", "TMDB 海报查询结果", "按片名缓存的海报地址，命中过就不再请求 TMDB。清完下次补图会重新查询 TMDB（已下载到 NAS 的封面图不受影响）"),
    ("img", "封面图片缓存", "网关抓到 NAS 的封面原图。清完下次浏览会重新下载，只影响加载速度，不影响内容"),
    ("cfg", "订阅配置缓存", "从 LunaTV 拉到的 TVBox 订阅配置。清完下次请求会重新构建，构建期间机顶盒仍可用旧数据等待"),
    ("agg", "聚合分类树 / 分类页", "自建聚合源的分类树与合并后的分类页内容。清完会自动重建，重建期间新分类可能暂时为空"),
    ("cat", "分类探测 / 分类映射", "各源分类探测结果与「分类名 → type_id」映射。清完下次访问重新探测，能让源站改过分类的条目对上"),
    ("sh", "源检测历史", "各源的成功率、响应时间与探测日志。清完重新累计；自动停用状态保留，不会因此把挂掉的源放回订阅"),
    ("fo", "主备源切换记录", "备源切换状态、失败计数与切换日志。清完回到配置里的主源"),
    ("acq", "预采集记录", "上一轮预采集的结果与日志。采集计划（下次采集时间）与已缓存分类页不受影响"),
]


def _cst(p):
    """文件大小 + 最后修改时间，读不到就返回 0"""
    try:
        st = os.stat(p)
        return int(st.st_size), float(st.st_mtime)
    except Exception:
        return 0, 0.0


def _cdir(p):
    n = b = 0
    try:
        with os.scandir(p) as it:
            for e in it:
                try:
                    if e.is_file():
                        n += 1
                        b += e.stat().st_size
                except OSError:
                    pass
    except Exception:
        pass
    return n, b


def _crm(*paths):
    for p in paths:
        try:
            os.remove(p)
        except FileNotFoundError:
            pass
        except Exception as e:
            print("[cache] remove %s fail: %s" % (p, str(e)[:40]), flush=True)


def cache_items():
    """缓存清单：条目数 / 占用字节 / 最后写入时间，给「缓存管理」卡用"""
    out = []
    for key, name, desc in CACHE_DEFS:
        n, b, mt = 0, 0, 0.0
        try:
            if key == "tmdb":
                b, mt = _cst(TMDB_FILE)
                with _tmdb["lock"]:
                    n = len(_tmdb["map"])
            elif key == "img":
                n, b = _cdir(IMG_DIR)
                mt = _cst(IMG_DIR)[1]
            elif key == "cfg":
                b, mt = _cst(CACHE_FILE)
                n = len((_luna.get("cfg") or {}).get("sites") or [])
            elif key == "agg":
                b, mt = _cst(AGG_FILE)
                n = len(_agg.get("cats") or [])
            elif key == "cat":
                b1, m1 = _cst(CAT_PROBE)
                b2, m2 = _cst(CLASS_MAP)
                b, mt = b1 + b2, max(m1, m2)
                with _cat_probe_lock:
                    n = len(_cat_probe.get("data") or {})
                with _cls_lock:
                    n += len(_cls_map)
            elif key == "sh":
                b, mt = _cst(SH_FILE)
                n = len(_sh.get("items") or {})
            elif key == "fo":
                b, mt = _cst(FO_FILE)
                n = len(_fo.get("log") or [])
            elif key == "acq":
                b, mt = _cst(ACQ_FILE)
                n = len(_acq.get("log") or [])
        except Exception as e:
            print("[cache] stat %s fail: %s" % (key, str(e)[:40]), flush=True)
        out.append({"key": key, "name": name, "desc": desc,
                    "entries": int(n), "bytes": int(b), "mtime": float(mt)})
    return out


def cache_clear(key):
    """清一个缓存，返回 (ok, msg)；key = all 清全部可重建缓存"""
    key = str(key or "").strip().lower()
    if key in ("", "all"):
        msgs = []
        ok_all = True
        for k, _n, _d in CACHE_DEFS:
            try:
                ok, m = cache_clear(k)
                ok_all = ok_all and ok
                msgs.append(m)
            except Exception as e:
                msgs.append("%s 失败 %s" % (k, str(e)[:30]))
                ok_all = False
        return ok_all, "；".join(msgs)

    if key == "tmdb":
        with _tmdb["lock"]:
            n = len(_tmdb["map"])
            _tmdb["map"] = {}
            _tmdb["dirty"] = False
            _tmdb["ts"] = 0.0
        _tmdb["err"] = 0
        _tmdb["down_until"] = 0.0            # 顺手解除失败熔断，清完能立刻重试
        _crm(TMDB_FILE, TMDB_FILE + ".tmp")
        print("[cache] tmdb cleared %d" % n, flush=True)
        return True, "TMDB %d 条" % n

    if key == "img":
        n, freed = img_clear()
        return True, "封面 %d 张/%.1fMB" % (n, freed / 1048576.0)

    if key == "cfg":
        with _luna_lock:
            n = len((_luna.get("cfg") or {}).get("sites") or [])
            _luna["cfg"] = None
            _luna["ts"] = 0.0
            _luna["err"] = None
        _crm(CACHE_FILE, CACHE_FILE + ".tmp")
        try:
            refresh_async()
        except Exception:
            pass
        print("[cache] tvbox cfg cleared", flush=True)
        return True, "订阅配置 %d 站点" % n

    if key == "agg":
        with _agg["lock"]:
            n = len(_agg.get("cats") or [])
            _agg["cats"] = []
            _agg["ids"] = {}
            _agg["map"] = {}
            _agg["top"] = set()
            _agg["pool"].clear()
            _agg["empty"].clear()
            _agg["ts"] = 0.0
        _crm(AGG_FILE, AGG_FILE + ".tmp")
        try:
            threading.Thread(target=agg_build, kwargs={"force": True}, daemon=True).start()
        except Exception:
            pass
        print("[cache] agg tree cleared %d cats" % n, flush=True)
        return True, "聚合分类 %d 类（重建中）" % n

    if key == "cat":
        with _cat_probe_lock:
            n = len(_cat_probe.get("data") or {})
            _cat_probe["data"] = {}
        with _cls_lock:
            n2 = len(_cls_map)
            _cls_map.clear()
        _crm(CAT_PROBE, CAT_PROBE + ".tmp", CLASS_MAP, CLASS_MAP + ".tmp")
        try:
            sh_drop_cache()          # 分类信息变了，列表与聚合池的旧结果一起丢掉
        except Exception:
            pass
        print("[cache] cat probe/map cleared %d/%d" % (n, n2), flush=True)
        return True, "分类探测 %d 源 / 映射 %d 源" % (n, n2)

    if key == "sh":
        with _sh["lock"]:
            n = len(_sh.get("items") or {})
            for it in (_sh.get("items") or {}).values():
                if not isinstance(it, dict):
                    continue
                auto, auto_ts = it.get("auto"), it.get("auto_ts")
                it.clear()
                it.update({"ok": None, "fail": 0, "pass": 0, "ms": 0, "msh": [], "ts": 0.0,
                           "ok_ts": 0.0, "hist": [], "err": "", "probe_ts": 0.0,
                           "fails": 0, "total": 0,
                           "auto": auto or 0, "auto_ts": auto_ts or 0.0})
            _sh["log"] = []
            _sh["dirty"] = True
        sh_save(True)
        print("[cache] srchealth history cleared %d" % n, flush=True)
        return True, "检测历史 %d 源" % n

    if key == "fo":
        with _fo["lock"]:
            n = len(_fo.get("log") or [])
            _fo["log"] = []
            _fo["fail"] = {}
            _fo["switches"] = 0
            _fo["active"] = ""
            _fo["since"] = 0.0
            _fo["probe_ok"] = 0
            _fo["dirty"] = True
        fo_save(True)
        print("[cache] failover log cleared %d" % n, flush=True)
        return True, "切换记录 %d 条" % n

    if key == "acq":
        if _acq.get("running"):
            return False, "正在采集，先点「停止」再清"
        with _acq["lock"]:
            n = len(_acq.get("log") or [])
            _acq["log"] = []
            _acq["last"] = {}
            _acq.update({"cur": "", "done": 0, "total": 0, "cats": 0,
                         "items": 0, "failed": 0, "abort": "",
                         "trigger": "", "started": 0.0, "finished": 0.0,
                         "dirty": True})
        acq_save(True)
        return True, "采集记录 %d 条" % n

    return False, "未知缓存项"


# 每日巡检：按保留天数 + 容量上限自动清理，防止缓存越攒越大把 NAS 磁盘占满
CACHE_HK = {"last": 0.0, "ts": 0.0, "freed": 0, "msg": "", "runs": 0}
CACHE_HK_GAP = 24 * 3600


def cache_total():
    return sum(int(x.get("bytes") or 0) for x in cache_items())


def cache_housekeep(force=False):
    """清理过期与超量缓存；返回本次记录（未到点且非强制时返回 None）"""
    now = time.time()
    if not force and now - float(CACHE_HK.get("last") or 0) < CACHE_HK_GAP:
        return None
    CACHE_HK["last"] = now
    freed, notes = 0, []
    try:
        n, b = img_cleanup()
        if n:
            freed += b
            notes.append("封面 %d 张/%.1fMB" % (n, b / 1048576.0))
    except Exception as e:
        print("[cache] housekeep img fail:", e, flush=True)
    try:
        k = tmdb_prune()
        if k:
            notes.append("TMDB %d 条" % k)
    except Exception as e:
        print("[cache] housekeep tmdb fail:", e, flush=True)
    # 总容量上限：超了就按「最后写入时间最旧」整项清理，只动可重建缓存
    limit = max(64, min(8192, int(_f.get("cache_max_mb") or 512))) * 1048576
    try:
        items = cache_items()
        total = sum(int(x["bytes"]) for x in items)
        if total > limit:
            cand = sorted([x for x in items if int(x["bytes"]) > 0 and x["key"] != "cfg"],
                          key=lambda x: x["mtime"] or 0)
            for x in cand:
                if total <= limit * 0.9:
                    break
                ok, msg = cache_clear(x["key"])
                print("[cache] housekeep over-limit clear %s -> %s" % (x["key"], msg), flush=True)
                notes.append("超量清 " + x["name"])
                total = cache_total()
    except Exception as e:
        print("[cache] housekeep limit fail:", e, flush=True)
    CACHE_HK["ts"] = time.time()
    CACHE_HK["freed"] = int(freed)
    CACHE_HK["msg"] = "；".join(notes) or "无需清理"
    CACHE_HK["runs"] = int(CACHE_HK.get("runs") or 0) + 1
    if notes:
        print("[cache] housekeep:", CACHE_HK["msg"], flush=True)
    return dict(CACHE_HK)


def cache_loop():
    """独立线程：每天巡检一次（开关在「本地缓存」里，默认开）"""
    time.sleep(150)
    while True:
        try:
            if _f.get("cache_auto_clean", True):
                cache_housekeep()
        except Exception as e:
            print("[cache] loop error:", e, flush=True)
        time.sleep(1800)


if __name__ == "__main__":
    reload_config(force=True)
    load_cfg_disk()
    cat_probe_load()
    cls_map_load()
    tmdb_load()
    stats_load()
    devices_load()
    flow_load()
    searchstats_load()
    fo_load()
    sh_load()
    acq_load()
    agg_ids_load()
    # 载入的分类映射里可能留着上次已被停用的源，先摘掉（不重建树，重建交给下面的后台线程）
    try:
        agg_src_prune(rebuild=False)
    except Exception as e:
        print("[agg] 启动清理停用源失败: %s" % str(e)[:60], flush=True)
    img_cleanup()
    if sh_conf()["on"]:
        _sh["next"] = time.time() + SH_BOOT_DELAY     # 启动先让聚合建树，之后再全量探测
    # 重启后内存缓存是空的，别傻等上一轮存的 next：先跑一轮把常用分类热起来
    if _f.get("agg_auto_on") and (float(_acq.get("next") or 0) <= time.time() or not _agg.get("pool")):
        _acq["next"] = time.time() + ACQ_BOOT_DELAY   # 等分类树建好再开始首轮预采集
    threading.Thread(target=warm_loop, daemon=True).start()
    threading.Thread(target=acq_loop, daemon=True).start()
    threading.Thread(target=cache_loop, daemon=True).start()   # 每天按容量/保留期自动清理
    # 启动后重建一次分类树：顺带把各源父子关系灌进缓存，分类页首次访问不用现拉 ac=class
    threading.Thread(target=agg_build, kwargs={"force": True}, daemon=True).start()
    print("tvbox-guard listening on 0.0.0.0:%d" % LISTEN_PORT, flush=True)
    ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler).serve_forever()

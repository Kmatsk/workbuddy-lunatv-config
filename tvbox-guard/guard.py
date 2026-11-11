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
from concurrent.futures import ThreadPoolExecutor

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
      "cats_on": True, "titles_on": True, "filter_on": True,
      "search_all": True, "search_limit": 0,
      "sync_on": True, "img_on": True, "idle_min": 5, "ignore_dev": set(DEFAULT_IGNORE_DEVS)}
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
                                  "day": today, "dayn": 0, "dayFirst": now}
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
    for ip, r in items:
        if r.get("day") != today or _sk(ip):
            continue
        ago = now - float(r.get("ts") or 0)
        out.append({"ip": ip, "online": ago <= idle, "ago": round(ago, 1),
                    "n": int((hday.get("dev") or {}).get(ip) or r.get("dayn") or 0),
                    "month": int((hmon.get("dev") or {}).get(ip) or 0),
                    "total": int(r.get("n") or 0),
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


def read_config_file():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception:
        return {"categories": list(DEFAULT_CATS), "titles": list(DEFAULT_TITLES),
                "placeholderPattern": DEFAULT_PLACEHOLDER,
                "disabledSources": [], "sourceOrder": [],
                "sourceSyncMinutes": 5, "backupOnSave": True, "backupKeep": 5,
                "tvboxRefreshMinutes": 5, "sourceLimit": 0, "activeSource": ""}


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
    _f["tmdb_on"] = bool(c.get("tmdbEnabled", True))
    _f["cats_on"] = bool(c.get("categoryFilterEnabled", True))
    _f["titles_on"] = bool(c.get("titleFilterEnabled", True))
    _f["filter_on"] = bool(c.get("filterEnabled", True))
    _f["search_all"] = bool(c.get("searchAllSources", True))
    _f["search_limit"] = max(0, min(50, int(c.get("searchLimit") or 0)))
    _f["sync_on"] = bool(c.get("sourceSyncEnabled", True))
    _f["img_on"] = bool(c.get("imageCacheEnabled", True))
    try:
        _iv = int(c["onlineIdleMinutes"])
    except Exception:
        _iv = 5
    _f["idle_min"] = max(1, min(1440, _iv))
    ig = c.get("ignoreDevices")
    if ig is None:
        ig = DEFAULT_IGNORE_DEVS
    _f["ignore_dev"] = {str(x).strip() for x in ig if str(x).strip()}


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
        "tmdbEnabled": bool(pick("tmdbEnabled", True)),
        "categoryFilterEnabled": bool(pick("categoryFilterEnabled", True)),
        "titleFilterEnabled": bool(pick("titleFilterEnabled", True)),
        "filterEnabled": bool(pick("filterEnabled", True)),
        "searchAllSources": bool(pick("searchAllSources", True)),
        "searchLimit": max(0, min(50, int(pick("searchLimit", 0) or 0))),
        "sourceSyncEnabled": bool(pick("sourceSyncEnabled", True)),
        "imageCacheEnabled": bool(pick("imageCacheEnabled", True)),
        "onlineIdleMinutes": max(1, min(1440, int(pick("onlineIdleMinutes", 5)))),
        "ignoreDevices": list(dict.fromkeys([str(x).strip() for x in pick("ignoreDevices", DEFAULT_IGNORE_DEVS) or [] if str(x).strip()])),
    }
    # 单文件 bind mount 不支持 rename，直接截断写入
    with open(CONFIG_PATH, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
        fp.flush()
    apply_config(data)
    _f["mtime"] = os.path.getmtime(CONFIG_PATH)
    # 开关变化会影响封面改写，旧缓存一并丢弃
    try:
        with _cms_lock:
            _cms_cache.clear()
        with _search_lock:
            _search_cache.clear()
    except Exception:
        pass


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
                 for s in (cfg["Config"].get("SourceConfig") or []) if s.get("api")}
            if m:
                _sources["data"], _sources["ts"] = m, time.time()
        except Exception as e:
            print("[warn] fetch sources failed:", e, flush=True)
    return _sources["data"] or {}


def src_off(k):
    """源关闭 = 网关里手动停用，或 LunaTV 里已停用（两边任一关闭即不下发、不参与聚合）"""
    return k in _f["disabled"] or bool((sources_cached().get(k) or {}).get("disabled"))


ADMIN_PATH = os.environ.get("GUARD_ADMIN", "/app/admin.html")
LOGO_PATH = os.environ.get("GUARD_LOGO", "/app/logo/logo.png")
BACKUP_DIR = os.environ.get("GUARD_BACKUP", "/app/backups")
CACHE_DIR = os.environ.get("GUARD_CACHE", "/app/cache")
CACHE_FILE = os.path.join(CACHE_DIR, "tvbox-config.json")
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


_luna = {"cfg": None, "ts": 0.0, "dur": 0.0, "err": None, "building": False}
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


_pf = {"done": False}
_warm_full = {"done": False}
TMDB_WARM_MAX = int(os.environ.get("TMDB_WARM_MAX", "400"))


def prefetch_cms():
    """后台预热各源分类列表，机顶盒进入站点时直接命中缓存"""
    try:
        srcs = sources_cached()
        keys = [k for k in srcs if not src_off(k)]
        _order = {k: i for i, k in enumerate(_f["order"])}
        keys.sort(key=lambda k: _order.get(k, len(_order) + 100))
        if _f.get("src_active"):
            keys = [k for k in keys if k == _f["src_active"]] or keys[:1]
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
        warm_images(keys, ncat=(40 if not _warm_full["done"] else 6))
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
                before = _luna["ts"]
                fetch_luna_cfg()
                if _luna["ts"] != before:
                    prefetch_cms()
            elif not _pf["done"]:
                prefetch_cms()
            stats_save()
            devices_save()
            searchstats_save()
        except Exception as e:
            print("[warm] error:", e, flush=True)
        time.sleep(15)


# ---------------- 封面图缓存代理 ----------------
IMG_DIR = os.path.join(CACHE_DIR, "img")
IMG_TTL = 30 * 86400
IMG_BUDGET = int(float(os.environ.get("IMG_BUDGET_MB", "256")) * 1048576)
CT_BY_EXT = {".webp": "image/webp", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
             ".png": "image/png", ".gif": "image/gif", ".avif": "image/avif"}
_img_locks = {}
_img_locks_guard = threading.Lock()
_img_stats = {"ts": 0.0, "n": 0, "b": 0}


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
        if st.st_size > 512 and time.time() - st.st_mtime < IMG_TTL:
            return fp, st.st_size
    except OSError:
        pass
    return None, 0


def ensure_img(url):
    """确保本地有这张图，返回 (路径, content-type)；失败返回 (None, None)"""
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
            return dest, ct
        except Exception as e:
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
    try:
        files = []
        total = 0
        with os.scandir(IMG_DIR) as it:
            for e in it:
                try:
                    st = e.stat()
                except OSError:
                    continue
                files.append((st.st_mtime, st.st_size, e.path))
                total += st.st_size
    except FileNotFoundError:
        return
    except Exception as e:
        print("[img] cleanup scan error:", e, flush=True)
        return
    if total <= IMG_BUDGET:
        print("[img] cache %d files / %.1fMB" % (len(files), total / 1048576.0), flush=True)
        return
    files.sort()
    for mt, sz, fp in files:
        if total <= IMG_BUDGET * 0.8:
            break
        try:
            os.remove(fp)
            total -= sz
        except OSError:
            pass
    print("[img] trimmed to %.1fMB" % (total / 1048576.0), flush=True)


# ---------------- TMDB 封面兜底 ----------------
TMDB_TOKEN = os.environ.get("TMDB_TOKEN") or ""
TMDB_LANG = os.environ.get("TMDB_LANG", "zh-CN")
TMDB_FILE = os.path.join(CACHE_DIR, "tmdb.json")
TMDB_TTL = 60 * 86400
TMDB_MISS_TTL = 12 * 3600
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
    ttl = TMDB_TTL if e.get("p") else TMDB_MISS_TTL
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
            ttl = TMDB_TTL if e.get("p") else TMDB_MISS_TTL
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


def ordered_source_keys():
    """已启用源，按配置顺序"""
    srcs = sources_cached()
    keys = [k for k in srcs if not src_off(k)]
    order = {k: i for i, k in enumerate(_f["order"])}
    keys.sort(key=lambda k: order.get(k, len(order) + 100))
    return keys


def pick_delivered(keys):
    """实际下发给机顶盒的源（与 handle_tvbox 一致）"""
    act = _f.get("src_active") or ""
    lim = int(_f.get("src_limit") or 0)
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
    # 多查几个候选：某些源没有该片源（甚至挂掉）时不占用「额外源数量」的额度，
    # 由 merge_search 只并入前 N 个真正有结果的额外源
    return [base_key] + search_extras(base_key)[:7]


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


def merge_search(base_key, query):
    """主源 + 其它源并行搜索，合并成一个列表（id 带来源前缀，详情可回溯）"""
    targets = search_targets(base_key)
    srcs = sources_cached()
    if len(targets) <= 1:
        return None
    t0 = time.time()

    def one(k):
        meta = srcs.get(k)
        if not meta:
            return k, None
        try:
            return k, _upstream_json(meta, query, 10)
        except Exception as e:
            print("[search] %s fail: %s" % (k, str(e)[:60]), flush=True)
            return k, None

    with ThreadPoolExecutor(max_workers=min(6, max(2, len(targets)))) as ex:
        parts = list(ex.map(one, targets))
    items, seen, hit = [], {}, 0
    lim = int(_f.get("search_limit") or 0)     # 0 = 不限
    extra_used, used = 0, []
    for k, d in parts:
        if not isinstance(d, dict):
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
            if len(items) >= 80:      # 结果太多反而难翻，够用就行
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


def merge_detail(query, ids_raw):
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
        groups.setdefault(k, []).append(vid)
        order.append(k)

    def one(k):
        meta = srcs.get(k)
        if not meta:
            return k, None
        ids = ",".join(groups[k])
        q = "ac=" + urllib.parse.quote(ac) + "&ids=" + urllib.parse.quote(ids, safe=",")
        try:
            return k, _upstream_json(meta, q, 25)
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


def cms_build_parent(key, meta, query, tid, pg, kids):
    """父分类（如魔都「连续剧」「电影」）上游本身无内容、内容挂在子分类：
    并行取各子分类同页码，按更新时间合并成一页；结果照常写缓存，后台静默刷新同样走这条路"""
    pg = max(1, int(pg or 1))

    def one(kid):
        try:
            cq = re.sub(r"(^|&)t=\d+", r"\1t=%d" % kid, query)
            out, _ = cms_build(key, cq, meta, True)
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


def cms_build(key, query, meta, cacheable=True):
    """请求上游 CMS，过滤分类/条目，改写封面地址，并写入缓存"""
    ck = (key, query)
    # 父分类（type_pid 指向它、自身无内容）→ 直接合并子分类，不去请求上游这个空分类
    tm = re.search(r"(?:^|&)t=(\d+)(?:&|$)", query)
    if tm and "ac=videolist" in query:
        _kids = cls_children(key, meta, int(tm.group(1)))
        if _kids:
            _pg = re.search(r"(?:^|&)pg=(\d+)(?:&|$)", query)
            return cms_build_parent(key, meta, query, int(tm.group(1)),
                                    int(_pg.group(1)) if _pg else 1, _kids)
    url = meta["api"] + ("?" + query if query else "")
    req = urllib.request.Request(url, headers={"User-Agent": "TVBox/1.0.0"})
    t0 = time.time()
    raw = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    _stats["slow"] = _stats.get("slow", 0) + (1 if time.time() - t0 > 3 else 0)
    try:
        data = json.loads(raw)
    except Exception:
        out = raw.encode()
        cms_store(ck, out, "text/plain; charset=utf-8", cacheable)
        return out, "text/plain; charset=utf-8"
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
                                   "tvboxRefreshMinutes": _f.get("tvcache_min", 5),
                                   "tvboxCacheAgo": (time.time() - _luna["ts"]) if _luna["ts"] else None,
                                   "tvboxBuildSec": round(_luna["dur"], 2),
                                   "logoUpdated": logo_updated(),
                                   "onlineIdleMinutes": int(_f.get("idle_min") or 5),
                                   "ignoreDevices": sorted(_f.get("ignore_dev") or []),
                                   "lastSyncAgo": (time.time() - _sources["ts"]) if _sources["ts"] else None})
            if p.path == "/api/devices":
                return self._json({"ok": True, **devices_snapshot()})
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
                                   "tmdbOn": bool(_f.get("tmdb_on", True)),
                                   "catsOn": bool(_f.get("cats_on", True)),
                                   "titlesOn": bool(_f.get("titles_on", True)),
                                   "syncOn": bool(_f.get("sync_on", True)),
                                   "imgOn": bool(_f.get("img_on", True)),
                                   "tmdbHit": _stats["thit"], "tmdbMiss": _stats["tmiss"],
                                   "tmdbErr": _tmdb["err"], "tmdbN": len(_tmdb["map"]),
                                   "idleMin": int(_f.get("idle_min") or 5),
                                   "devices": devices_snapshot()})
            if p.path == "/api/config":
                c = read_config_file()
                return self._json({"categories": c.get("categories") or list(DEFAULT_CATS),
                                   "titles": c.get("titles") or list(DEFAULT_TITLES),
                                   "disabledSources": c.get("disabledSources") or [],
                                   "sourceOrder": c.get("sourceOrder") or [],
                                   "sourceLimit": c.get("sourceLimit", 0) or 0,
                                   "activeSource": c.get("activeSource", "") or "",
                                   "filterEnabled": bool(_f.get("filter_on", True)),
                                   "categoryFilterEnabled": bool(_f.get("cats_on", True)),
                                   "titleFilterEnabled": bool(_f.get("titles_on", True)),
                                   "tmdbEnabled": bool(_f.get("tmdb_on", True)),
                                   "searchAllSources": bool(_f.get("search_all", True)),
                                   "searchLimit": int(_f.get("search_limit") or 0)})
            if p.path == "/api/sources":
                order = {k: i for i, k in enumerate(_f["order"])}
                items = [{"key": k, "name": v["name"], "disabled": src_off(k)}
                         for k, v in sources_cached().items()]
                items.sort(key=lambda x: order.get(x["key"], len(order) + 100))
                keys = [x["key"] for x in items if not x["disabled"]]
                delivered = pick_delivered(keys)
                return self._json({"ok": True, "sources": items,
                                   "total": len(items), "enabled": len(keys),
                                   "delivered": len(delivered),
                                   "searchAllSources": bool(_f.get("search_all", True)),
                                   "searchLimit": int(_f.get("search_limit") or 0),
                                   "searchPrimary": search_primary(),
                                   "searchExtraTotal": len(search_extras()),
                                   "searchSources": _search_breadth()})
            if p.path == "/api/test":
                return self.handle_test((qs.get("q") or [""])[0])
            if p.path == "/api/tvbox":
                return self.handle_tvbox()
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
        tracked = p.path == "/api/tvbox" or p.path.startswith("/cms/") or p.path == "/img"
        if not tracked:
            return
        ua = (self.headers.get("User-Agent") or "")[:30]
        ip = (self.client_address or ("?", 0))[0]
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
                                   "tvboxRefreshMinutes": _f.get("tvcache_min", 5)})
            if p.path == "/api/img/clear":
                n, freed = img_clear()
                return self._json({"ok": True, "files": n, "mb": round(freed / 1048576.0, 1)})
            if p.path == "/api/stats/reset":
                stats_reset()
                return self._json({"ok": True, "blockedCat": 0, "blockedTitle": 0,
                                   "tmdbHit": 0, "tmdbMiss": 0,
                                   "statsSince": _stats_p["since"], "statsMonth": stats_month_key()})
            if p.path == "/api/devices/reset":
                devices_reset()
                return self._json({"ok": True, **devices_snapshot()})
            if p.path == "/api/search_stats/reset":
                searchstats_reset()
                return self._json({"ok": True, **searchstats_snapshot()})
            if p.path == "/api/sync":
                m = sources_cached(force=True)
                return self._json({"ok": bool(m), "sources": len(m)})
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
        sites = [x for x in (cfg.get("sites") or []) if not src_off(x.get("key") or "")]
        order = {k: i for i, k in enumerate(_f["order"])}
        sites.sort(key=lambda x: order.get(x.get("key") or "", len(order) + 100))
        # 单源/限量模式：只下发指定数量的源，机顶盒启动只加载这些
        act = _f.get("src_active") or ""
        limit = int(_f.get("src_limit") or 0)
        if act:
            picked = [x for x in sites if (x.get("key") or "") == act]
            sites = picked or sites[:1]
        elif limit > 0:
            sites = sites[:limit]
        cfg["sites"] = sites
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
        except Exception:
            pass

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

if __name__ == "__main__":
    reload_config(force=True)
    load_cfg_disk()
    cat_probe_load()
    cls_map_load()
    tmdb_load()
    stats_load()
    devices_load()
    searchstats_load()
    img_cleanup()
    threading.Thread(target=warm_loop, daemon=True).start()
    print("tvbox-guard listening on 0.0.0.0:%d" % LISTEN_PORT, flush=True)
    ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler).serve_forever()

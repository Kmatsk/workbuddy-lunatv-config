#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""LunaTV 配置脱敏 / 还原工具（纯 socket 连 Kvrocks，不需要 docker 权限）。

用法:
  redact.py export  --repo <仓库目录> [--env <.env 路径>] [--host H] [--port P]
  redact.py restore --repo <仓库目录> [--host H] [--port P] [--restart]

export : 从 Kvrocks 读 admin:config → 脱敏写入 config/admin-config.json，
         明文写入 secrets/secrets.json（不入库）
restore: 用 config/ + secrets/ 合并后写回 Kvrocks
"""
import argparse
import datetime
import hashlib
import json
import os
import re
import socket
import subprocess
import sys

SECRET_KEY_NAMES = {
    "password", "passwd", "pwd", "token", "apikey", "secret",
    "accesskey", "secretkey", "cookie", "authorization",
    "tavilyapikeys", "pansoutoken",
}
SECRET_PATHS = {"NetDiskConfig.username"}
PLACEHOLDER = "<SECRET>"
SKIP_PREFIX = ("cache:", "calendar:")
SKIP_KEYS = {"video:total_size"}


# ---------------- 极简 Redis 客户端 ----------------
class Redis:
    def __init__(self, host, port, timeout=8):
        self.sock = socket.create_connection((host, port), timeout)
        self.fp = self.sock.makefile("rb")

    def close(self):
        try:
            self.fp.close()
            self.sock.close()
        except Exception:
            pass

    def cmd(self, *args):
        buf = [b"*%d\r\n" % len(args)]
        for a in args:
            b = str(a).encode()
            buf.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(buf))
        return self._read()

    def _read(self):
        line = self.fp.readline()
        if not line:
            raise RuntimeError("连接被关闭")
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return rest.decode("utf-8", "replace")
        if t == b"-":
            raise RuntimeError(rest.decode("utf-8", "replace"))
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.fp.read(n + 2)[:-2]
            return data.decode("utf-8", "replace")
        if t == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self._read() for _ in range(n)]
        raise RuntimeError("无法解析响应: %r" % line)


# ---------------- 脱敏 ----------------
def norm(k):
    return re.sub(r"[^a-z0-9]", "", str(k).lower())


def is_secret_key(k):
    """键名归一化后按「后缀」匹配：TMDBApiKey→tmdpapikey 命中 apikey，maxTokens 不会误中 token"""
    n = norm(k)
    if not n:
        return False
    return any(n == s or n.endswith(s) for s in SECRET_KEY_NAMES)


def walk_redact(node, path, found):
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            p = path + "." + str(k) if path else str(k)
            if (is_secret_key(k) or p in SECRET_PATHS) and v not in (None, "", [], {}):
                found[p] = v
                out[k] = [PLACEHOLDER] if isinstance(v, list) else PLACEHOLDER
            else:
                out[k] = walk_redact(v, p, found)
        return out
    if isinstance(node, list):
        return [walk_redact(v, path, found) for v in node]
    return node


def walk_restore(node, path, secrets, used):
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            p = path + "." + str(k) if path else str(k)
            if v == PLACEHOLDER or (isinstance(v, list) and v == [PLACEHOLDER]):
                if p in secrets:
                    out[k] = secrets[p]
                    used.append(p)
                else:
                    out[k] = [] if isinstance(v, list) else ""
                    print("[warn] secrets 缺少 %s，已置空" % p, file=sys.stderr)
            else:
                out[k] = walk_restore(v, p, secrets, used)
        return out
    if isinstance(node, list):
        return [walk_restore(v, path, secrets, used) for v in node]
    return node


def parse_env(path):
    env = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    except Exception:
        pass
    return env


# ---------------- 导出 ----------------
def do_export(repo, env_file, host, port):
    r = Redis(host, port)
    try:
        keys = r.cmd("KEYS", "*") or []
    finally:
        r.close()

    wanted = [k for k in keys if not k.startswith(SKIP_PREFIX) and k not in SKIP_KEYS]
    if "admin:config" not in wanted:
        print("[error] Kvrocks 里没有 admin:config，导出中止", file=sys.stderr)
        return 2

    vals, hist, kv = {}, {}, {}
    r = Redis(host, port)
    try:
        for k in wanted:
            t = r.cmd("TYPE", k)
            if t == "string":
                if k == "admin:config" or k.startswith("sys:migration:"):
                    vals[k] = r.cmd("GET", k)
                if k.startswith("sys:migration:"):
                    kv[k] = vals[k]
            elif t == "list" and k.startswith("u:") and k.endswith(":sh"):
                hist[k] = [x for x in (r.cmd("LRANGE", k, 0, -1) or []) if x]
    finally:
        r.close()

    try:
        cfg = json.loads(vals["admin:config"])
    except Exception as e:
        print("[error] admin:config 不是合法 JSON: %s" % e, file=sys.stderr)
        return 2

    prev = {}
    sec_path = os.path.join(repo, "secrets", "secrets.json")
    if os.path.exists(sec_path):
        try:
            prev = json.loads(open(sec_path, encoding="utf-8").read())
        except Exception:
            prev = {}

    secrets = {"config": {}, "env": {}, "kv": kv}
    clean = walk_redact(cfg, "", secrets["config"])

    env = parse_env(env_file) if env_file else {}
    if not env:
        env = prev.get("env") or {}
    for k in ("USERNAME", "PASSWORD"):
        if env.get(k):
            secrets["env"][k] = env[k]

    os.makedirs(os.path.join(repo, "config"), exist_ok=True)
    os.makedirs(os.path.join(repo, "secrets"), exist_ok=True)

    cfg_path = os.path.join(repo, "config", "admin-config.json")
    body = json.dumps(clean, ensure_ascii=False, indent=2) + "\n"
    old = open(cfg_path, encoding="utf-8").read() if os.path.exists(cfg_path) else ""
    with open(cfg_path, "w", encoding="utf-8") as f:
        f.write(body)

    with open(os.path.join(repo, "config", "user-history.json"), "w", encoding="utf-8") as f:
        f.write(json.dumps(hist, ensure_ascii=False, indent=2) + "\n")

    secrets["_note"] = "敏感值明文，仅存本机，勿提交 Git（.gitignore 已排除）"
    secrets["exported"] = datetime.datetime.now().isoformat(timespec="seconds")
    with open(sec_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(secrets, ensure_ascii=False, indent=2) + "\n")
    os.chmod(sec_path, 0o600)

    meta = {
        "exported": secrets["exported"],
        "source": "Kvrocks admin:config",
        "configSha256": hashlib.sha256(body.encode()).hexdigest()[:16],
        "sources": len(clean.get("SourceConfig") or []),
        "sourceKeys": [s.get("key") for s in (clean.get("SourceConfig") or [])],
        "siteName": (clean.get("SiteConfig") or {}).get("SiteName"),
        "redactedFields": sorted(secrets["config"].keys()),
        "historyKeys": sorted(hist.keys()),
        "kvKeys": sorted(kv.keys()),
        "changed": old.strip() != body.strip(),
    }
    # 配置没变化时不重写 meta.json，避免时间戳造成无意义的提交
    meta_path = os.path.join(repo, "meta.json")
    if meta["changed"] or not os.path.exists(meta_path):
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(meta, ensure_ascii=False, indent=2) + "\n")

    print("[export] sources=%d site=%s redacted=%d 配置%s"
          % (meta["sources"], meta["siteName"], len(meta["redactedFields"]),
             "有变化" if meta["changed"] else "无变化"))
    return 0


# ---------------- 还原 ----------------
def do_restore(repo, host, port, restart=False):
    cfg_path = os.path.join(repo, "config", "admin-config.json")
    if not os.path.exists(cfg_path):
        print("[error] 缺少 config/admin-config.json", file=sys.stderr)
        return 2

    cfg = json.loads(open(cfg_path, encoding="utf-8").read())
    sec_path = os.path.join(repo, "secrets", "secrets.json")
    secrets = {}
    if os.path.exists(sec_path):
        secrets = json.loads(open(sec_path, encoding="utf-8").read())
    else:
        print("[warn] 没有 secrets/secrets.json，敏感字段会置空", file=sys.stderr)

    used = []
    merged = walk_restore(cfg, "", secrets.get("config") or {}, used)

    hist_path = os.path.join(repo, "config", "user-history.json")
    hist = json.loads(open(hist_path, encoding="utf-8").read()) if os.path.exists(hist_path) else {}

    r = Redis(host, port)
    try:
        r.cmd("SET", "admin:config", json.dumps(merged, ensure_ascii=False))
        for k, v in (secrets.get("kv") or {}).items():
            r.cmd("SET", k, v)
        for k, vals in hist.items():
            r.cmd("DEL", k)
            for v in vals:
                r.cmd("RPUSH", k, v)
    finally:
        r.close()

    env_path = os.path.join(repo, ".env")
    if not os.path.exists(env_path):
        tpl = os.path.join(repo, ".env.example")
        if os.path.exists(tpl):
            txt = open(tpl, encoding="utf-8").read()
            for k, v in (secrets.get("env") or {}).items():
                txt = re.sub(r"(?m)^%s=.*$" % re.escape(k), "%s=%s" % (k, v), txt)
            with open(env_path, "w", encoding="utf-8") as f:
                f.write(txt)
            os.chmod(env_path, 0o600)
            print("[restore] 已生成 .env（%d 个变量来自 secrets）" % len(secrets.get("env") or {}))

    print("[restore] 写入 admin:config，注入 %d 个敏感字段，历史 %d 项" % (len(used), len(hist)))

    if restart:
        try:
            subprocess.run(["docker", "restart", "lunatv-core"], check=True,
                           stdout=subprocess.DEVNULL, timeout=60)
            print("[restore] 已重启 lunatv-core")
        except Exception as e:
            print("[warn] 重启失败（%s），请手动重启 lunatv-core" % e, file=sys.stderr)
    return 0


def main():
    ap = argparse.ArgumentParser(description="LunaTV 配置备份/还原")
    ap.add_argument("mode", choices=["export", "restore"])
    ap.add_argument("--repo", required=True)
    ap.add_argument("--env", default="")
    ap.add_argument("--host", default=os.environ.get("KV_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("KV_PORT", "6666")))
    ap.add_argument("--restart", action="store_true")
    a = ap.parse_args()
    if a.mode == "export":
        return do_export(a.repo, a.env, a.host, a.port)
    return do_restore(a.repo, a.host, a.port, a.restart)


if __name__ == "__main__":
    sys.exit(main())

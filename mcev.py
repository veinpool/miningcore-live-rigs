#!/usr/bin/env python3
"""miningcore-live-rigs - realtime rig connect/disconnect events for Miningcore pools.

Miningcore's API only refreshes miners/workers on its stats interval and keeps a
worker "online" for the whole hashrate window, so a dashboard sees rigs connect minutes
late and disconnect ~10 min late. This service follows the containers' logs instead:

  [ts] [I] [pool] [CID] Authorized worker ADDR.worker   -> connection CID is rig ADDR/worker
  [ts] [I] [pool] [CID] Connection closed               -> connection CID is gone

and serves a small JSON feed your front-end can poll every few seconds:

  GET /pools/{id}/events[?since=N] -> {"last": N, "online": rigs, "events": [{id, kind, miner, worker, scheme, ts}]}
  GET /pools/all/events[?since=N]  -> every pool at once ("online": {pool: n}, "rigs": {pool: ["addr|worker"]}, events carry "pool")

A rig is online while it has at least one authorized connection. rig_offline waits
GRACE seconds, so a miner that reconnects at once (pool switch, network blip) does
not produce an off/on pair. Each log is replayed from the container start, so the
online set is exact after a restart of this service.
"""
import json, os, re, subprocess, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CONTAINERS = [c for c in os.environ.get("MCEV_CONTAINERS", "miningcore").split(",") if c]
PORT = int(os.environ.get("MCEV_PORT", "4020"))
GRACE = int(os.environ.get("MCEV_GRACE", "20"))         # s before a closed rig is announced offline
KEEP = 300          # events kept per pool

LINE = re.compile(r"^\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)(?:\.\d+)?\] \[\w\] \[([a-z0-9]+)\] \[([0-9A-Z]{10,20})\] (Authorized worker (\S+)|Connection closed)")

LOCK = threading.Lock()
SEQ = [0]
EVENTS = {}         # pool -> [event]
CONNS = {}          # (container, cid) -> (pool, miner, worker)
PENDING = {}        # (pool, miner, worker) -> (due_monotonic, ts_ms) for rig_offline


def split_worker(login):
    if "@" in login and "." not in login:            # cryptonote style worker@address
        w, a = login.split("@", 1)
        if len(w) > len(a): w, a = a, w
        return a, w[:40]
    a, _, w = login.partition(".")
    return a, (w or "default")[:40]


def rigs(pool):
    return {(m, w) for (p, m, w) in CONNS.values() if p == pool}


def push(pool, kind, miner, worker, ts):
    SEQ[0] += 1
    lst = EVENTS.setdefault(pool, [])
    lst.append({"id": SEQ[0], "kind": kind, "miner": miner, "worker": worker,
                "scheme": "SOLO" if pool.endswith("solo") else "PPLNS", "ts": ts})
    del lst[:-KEEP]


def on_auth(container, cid, pool, login, ts):
    miner, worker = split_worker(login)
    rig = (pool, miner, worker)
    with LOCK:
        was = (miner, worker) in rigs(pool)
        CONNS[(container, cid)] = rig
        if PENDING.pop(rig, None) is None and not was:
            push(pool, "rig_online", miner, worker, ts)


def on_close(container, cid, ts, now=None):
    with LOCK:
        rig = CONNS.pop((container, cid), None)
        if rig and (rig[1], rig[2]) not in rigs(rig[0]):
            PENDING[rig] = ((now if now is not None else time.monotonic()) + GRACE, ts)


def flush_pending(force=False):
    t = time.monotonic()
    with LOCK:
        for rig, (due, ts) in list(PENDING.items()):
            if force or due <= t:
                del PENDING[rig]
                push(rig[0], "rig_offline", rig[1], rig[2], ts)


def drop_container(container):
    """The container stopped: every rig it served is offline."""
    with LOCK:
        for key in [k for k in CONNS if k[0] == container]:
            pool, miner, worker = CONNS.pop(key)
            if (miner, worker) not in rigs(pool):
                PENDING[(pool, miner, worker)] = (0, int(time.time() * 1000))
    flush_pending()


def started_at(container):
    try:
        return subprocess.run(["docker", "inspect", "-f", "{{.State.StartedAt}}", container],
                              capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception:
        return ""


def follow(container):
    start, since = None, None
    while True:
        s = started_at(container)
        if not s:
            time.sleep(10); continue
        if s != start:                       # first run or the container restarted: replay from its start
            if start is not None: drop_container(container)
            start, since = s, s
        replay_until = time.time() - 5
        p = subprocess.Popen(["docker", "logs", "-f", "--since", since, container],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace", bufsize=1)
        for raw in p.stdout:
            m = LINE.match(raw)
            if not m: continue
            ts_s, pool, cid, _, login = m.groups()
            dt = datetime.strptime(ts_s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            ts = int(dt.timestamp() * 1000)
            since = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if login: on_auth(container, cid, pool, login, ts)
            else:
                # while replaying old lines, closures are settled at once (no grace wait for history)
                on_close(container, cid, ts, now=0 if dt.timestamp() < replay_until else None)
        p.wait()
        time.sleep(3)


def ticker():
    while True:
        time.sleep(1)
        flush_pending()


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send(self, obj, code=200):
        b = json.dumps(obj, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        r = [x for x in path.split("/") if x]
        if r and r[0] == "api": r = r[1:]
        if len(r) == 3 and r[0] == "pools" and r[2] == "events":
            pool = r[1]
            q = dict(x.split("=", 1) for x in qs.split("&") if "=" in x)
            with LOCK:
                lst = EVENTS.get(pool, [])
                try:
                    since = int(q["since"]) if "since" in q else None
                except ValueError:
                    since = None
                if pool == "all":           # every pool in one call (a dashboard can poll this every 5 s)
                    allev = sorted((dict(e, pool=p) for p, l in EVENTS.items() for e in l), key=lambda e: e["id"])
                    out = [e for e in allev if e["id"] > since] if since is not None else allev[-40:]
                    online = {}
                    for (p, m, w) in CONNS.values(): online.setdefault(p, set()).add((m, w))
                    return self.send({"last": SEQ[0], "online": {p: len(s) for p, s in online.items()},
                                      "rigs": {p: sorted(f"{m}|{w}" for m, w in s) for p, s in online.items()}, "events": out})
                out = [e for e in lst if e["id"] > since] if since is not None else lst[-20:]
                return self.send({"last": SEQ[0], "online": len(rigs(pool)), "events": out})
        if r == ["health"]:
            with LOCK:
                return self.send({"ok": True, "connections": len(CONNS), "pending": len(PENDING)})
        self.send({"error": "not found"}, 404)


if __name__ == "__main__":
    for c in CONTAINERS:
        threading.Thread(target=follow, args=(c,), daemon=True).start()
    threading.Thread(target=ticker, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()

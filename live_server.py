"""实时新闻收集守护进程 + 本地看板服务。

用法：
    python live_server.py [--port 8765] [--high-interval 120] [--low-interval 21600]

功能：
    1. 后台线程分层轮询信息源：
       - 高频源（Reuters/AI实验室/arXiv/Trending 等 19 源）：默认每 2 分钟（条件请求防封）
       - 低频源（人民日报 + 中国政策/学术 10 源）：默认每 6 小时
    2. 条目经"当天过滤"后写入 SQLite（data/news.db），按 URL 去重
    3. 内置 HTTP 服务：
       - /            实时看板（SSE 推送，新条目秒级上屏+NEW标记）
       - /api/items   当天条目 JSON
       - /api/stream  SSE 实时推送端点（新文章入库即推送）
       - /api/status  各源采集状态 JSON
       - /api/meta    分类结构 JSON
"""
from __future__ import annotations

import argparse
import html as html_module
import json
import logging
import re
import queue
import sys
import threading
import time
from datetime import date, datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# 北京时区：用于跨环境 day keying（GH Actions runner 默认 UTC；若用 date.today()
# 会拿到 UTC 日期，导致 Beijing 凌晨文章被归到「昨天」而漏掉）。
BEIJING_TZ = timezone(timedelta(hours=8))


def today_bj() -> str:
    """今天的日期（北京时间），作为 day key。统一所有 day 决策点。"""
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import CATEGORY_TREE, SOURCES, DIRECTORY_SOURCES, all_directory_homes  # noqa: E402
from http_utils import HttpClient  # noqa: E402
from scrapers import build_scraper  # noqa: E402
import storage  # noqa: E402

# ---------------------------------------------------------------- 分层配置

# 低频源：政府/学术站点 + 人民日报（每天最多发一两次，无需高频轮询）
LOW_FREQ_IDS = {
    "people_daily",
    "gov_policy", "miit", "cac", "ndrc", "cast",
    "ia_cas", "pku_ai", "tsinghua_ai", "baai", "caai",
}

logger = logging.getLogger("news_live")

# 静态资源目录（源 logo 等）
STATIC_DIR = Path(__file__).parent / "static"
# source_id -> 静态文件名（None 表示该源沿用文字 category 徽章）
SOURCE_LOGOS: dict[str, str | None] = {
    "people_daily": "people_daily.png",
    "reuters": "reuters.png",
}

# ---------------------------------------------------------------- 采集器


class Collector:
    """分层轮询采集器（运行在后台线程）。"""

    def __init__(self, high_interval: int, low_interval: int):
        self.high_interval = high_interval
        self.low_interval = low_interval
        self.http = HttpClient()
        self.lock = threading.Lock()
        self.status: dict[str, dict] = {
            s.id: {"name": s.name, "tier": "low" if s.id in LOW_FREQ_IDS else "high",
                   "last_run": None, "ok": None, "msg": "", "last_new": 0}
            for s in SOURCES if s.enabled
        }
        self.started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.last_high_cycle: float = 0.0
        self.last_low_cycle: float = 0.0
        self.cycle_running = False
        self._backfill_done = False  # 首轮全量回填完成后才开启 SSE 实时推送
        self._pd_last_date = ""  # 上次人民日报抓取日（YYYY-MM-DD），用于每日定时任务去重
        self._stop = threading.Event()

    # ---------- 单源抓取

    def _fetch_one(self, src) -> None:
        today_str = today_bj()
        ts = datetime.now().strftime("%H:%M:%S")
        try:
            scraper = build_scraper(src, self.http)
            items = scraper.fetch()
            # 当天过滤（与 run.py 口径一致：无 published 的保留）
            filtered = [
                it for it in items
                if it.published is None or it.published[:10] == today_str
            ]
            conn = storage.get_conn()
            try:
                new_items = storage.upsert_items(conn, today_str, src, filtered)
            finally:
                conn.close()
            new_count = len(new_items)
            with self.lock:
                st = self.status[src.id]
                st.update(last_run=ts, ok=True, last_new=new_count,
                          msg=f"{len(filtered)} 条当天 / 新增 {new_count}")
            if new_count:
                logger.info("[ NEW ] %s: +%d 条", src.name, new_count)
                # 首轮全量回填不推送，避免看板初始重复；之后才实时推送
                if self._backfill_done:
                    for it in new_items:
                        SSE_HUB.publish(self._payload(it, src, today_str))
            else:
                logger.info("[  OK ] %s: 无新增", src.name)
        except Exception as e:
            msg = str(e)[:150]
            with self.lock:
                self.status[src.id].update(last_run=ts, ok=False, msg=msg)
            logger.warning("[FAIL] %s: %s", src.name, msg)

    @staticmethod
    def _payload(it, src, day: str) -> dict:
        """把一条新增新闻整理成 SSE 推送的 JSON 结构（与 /api/items 字段一致）。"""
        return {
            "day": day,
            "source_id": src.id,
            "source_name": src.name,
            "category": src.category,
            "subcategory": src.subcategory,
            "title": it.title,
            "url": it.url,
            "published": it.published,
            "extra": it.extra or {},
            "comment": getattr(it, "comment", "") or "",
            "first_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    # ---------- 轮询循环

    def _run_tier(self, tier: str) -> None:
        srcs = [s for s in SOURCES
                if s.enabled and (s.id in LOW_FREQ_IDS) == (tier == "low")]
        logger.info("=== 开始 %s 频轮询（%d 源） ===", "低" if tier == "low" else "高", len(srcs))
        for src in srcs:
            if self._stop.is_set():
                return
            self._fetch_one(src)
        logger.info("=== %s 频轮询完成 ===", "低" if tier == "low" else "高")

    def _morning_cron(self) -> None:
        """每日定时任务：人民日报每日 06:00 上版，确保当日 06:00 之后必抓一次。"""
        now = datetime.now(BEIJING_TZ)
        today_str = today_bj()
        if self._pd_last_date == today_str:
            return  # 今天已抓过
        if now.hour < 6:
            return  # 不到 06:00，报纸可能还没上版，避免抓到昨日
        # 找到人民日报源并抓取
        for s in SOURCES:
            if s.id == "people_daily" and s.enabled:
                logger.info("[每日] %s 后定时抓取人民日报", now.strftime("%H:%M"))
                self._fetch_one(s)
                break
        self._pd_last_date = today_str

    def loop(self) -> None:
        """调度主循环：启动时先跑一轮全量，之后按间隔分层轮询。"""
        while not self._stop.is_set():
            now = time.time()
            # 每日定时：06:00+ 当日未抓则抓人民日报
            self._morning_cron()
            due_low = now - self.last_low_cycle >= self.low_interval
            due_high = now - self.last_high_cycle >= self.high_interval
            if due_low or due_high:
                self.cycle_running = True
                if due_low:
                    self.last_low_cycle = now
                    self._run_tier("low")
                if due_high:
                    self.last_high_cycle = now
                    self._run_tier("high")
                self.cycle_running = False
                self._backfill_done = True  # 首轮全量回填完成，之后开启 SSE 实时推送
            self._stop.wait(20)

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> dict:
        with self.lock:
            per_source = {k: dict(v) for k, v in self.status.items()}
        now = time.time()
        return {
            "started_at": self.started_at,
            "cycle_running": self.cycle_running,
            "high_interval": self.high_interval,
            "low_interval": self.low_interval,
            "next_high_in": max(0, int(self.high_interval - (now - self.last_high_cycle))),
            "next_low_in": max(0, int(self.low_interval - (now - self.last_low_cycle))),
            "sources": per_source,
        }


# ---------------------------------------------------------------- HTTP 服务

COLLECTOR: Collector | None = None


class SSEHub:
    """管理所有看板的长连接，新条目入库时即时推送给每个连接。"""

    def __init__(self) -> None:
        self._clients: set[queue.Queue] = set()
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue()
        with self._lock:
            self._clients.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            self._clients.discard(q)

    def publish(self, payload: dict) -> None:
        with self._lock:
            for q in list(self._clients):
                try:
                    q.put_nowait(payload)
                except Exception:
                    pass


SSE_HUB = SSEHub()


class Handler(BaseHTTPRequestHandler):
    # 关闭输出缓冲，保证 SSE 事件即时写出
    wbufsize = 0

    def log_message(self, fmt, *args):  # 静默访问日志
        pass
    def log_message(self, fmt, *args):  # 静默访问日志
        pass

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, obj) -> None:
        self._send(200, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def do_GET(self):  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/":
                self._send(200, DASHBOARD_HTML.encode("utf-8"),
                           "text/html; charset=utf-8")
            elif path in ("/simple.html", "/simple"):
                self._serve_simple()
            elif path.startswith("/static/"):
                # 静态资源：仅允许 STATIC_DIR 内白名单扩展名，防路径穿越
                rel = path[len("/static/"):]
                if "/" in rel or "\\" in rel or ".." in rel:
                    self._send(400, b"bad path", "text/plain")
                    return
                fp = (STATIC_DIR / rel).resolve()
                if not str(fp).startswith(str(STATIC_DIR.resolve())):
                    self._send(400, b"bad path", "text/plain")
                    return
                if not fp.is_file():
                    self._send(404, b"not found", "text/plain")
                    return
                ext = fp.suffix.lower()
                mime = {
                    ".png": "image/png",
                    ".jpg": "image/jpeg",
                    ".jpeg": "image/jpeg",
                    ".gif": "image/gif",
                    ".svg": "image/svg+xml",
                    ".webp": "image/webp",
                    ".ico": "image/x-icon",
                }.get(ext, "application/octet-stream")
                try:
                    self._send(200, fp.read_bytes(), mime)
                except OSError:
                    self._send(404, b"not found", "text/plain")
            elif path == "/api/items":
                today_str = today_bj()
                conn = storage.get_conn()
                try:
                    items = storage.get_items(conn, today_str)
                finally:
                    conn.close()
                self._send_json({
                    "day": today_str,
                    "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "total": len(items),
                    "items": items,
                })
            elif path == "/api/stream":
                self._handle_sse()
            elif path == "/api/status":
                self._send_json(COLLECTOR.snapshot() if COLLECTOR else {})
            elif path == "/api/meta":
                self._send_json({"category_tree": CATEGORY_TREE})
            else:
                self._send(404, b"not found", "text/plain")
        except Exception as e:  # 防止单个请求崩掉服务
            try:
                self._send(500, str(e).encode("utf-8"), "text/plain")
            except Exception:
                pass

    def do_POST(self):  # noqa: N802
        """接收「我的点评」写入请求（/api/comment）。"""
        path = urlparse(self.path).path
        if path != "/api/comment":
            self._send(404, b"not found", "text/plain")
            return
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
            raw = self.rfile.read(length) if length else b"{}"
            data = json.loads(raw.decode("utf-8") or "{}")
            source_id = str(data.get("source_id", "")).strip()
            url = str(data.get("url", "")).strip()
            comment = str(data.get("comment", ""))[:500]
            if not source_id or not url:
                self._send_json({"ok": False, "error": "source_id 与 url 必填"})
                return
            # day 使用服务端北京时间，避免客户端篡改归属日期
            day = today_bj()
            conn = storage.get_conn()
            try:
                n = storage.update_comment(conn, day, source_id, url, comment)
            finally:
                conn.close()
            self._send_json({"ok": True, "updated": n})
        except Exception as e:  # 防止单个请求崩掉服务
            try:
                self._send_json({"ok": False, "error": str(e)[:200]})
            except Exception:
                pass

    def _handle_sse(self) -> None:
        """Server-Sent Events：保持连接，新条目入库即推送（默认 HTTP/1.0 流式）。"""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        q = SSE_HUB.subscribe()
        try:
            while True:
                try:
                    payload = q.get(timeout=15)
                except queue.Empty:
                    # 心跳保活，并探测客户端是否断开
                    try:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        break
                    continue
                try:
                    data = json.dumps(payload, ensure_ascii=False)
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    break
        finally:
            SSE_HUB.unsubscribe(q)

    # ---------- 简易版（内存缓存 30 秒，避免频繁拉库）

    _SIMPLE_CACHE: dict = {"ts": 0.0, "body": b""}

    def _serve_simple(self) -> None:
        """Serve 简易版 HTML（人民日报要闻 / Reuters 配图版 / 科技 6 品牌）。"""
        now = time.time()
        cached = self._SIMPLE_CACHE
        if now - cached["ts"] < 30 and cached["body"]:
            self._send(200, cached["body"], "text/html; charset=utf-8")
            return
        today_str = today_bj()
        conn = storage.get_conn()
        try:
            items = storage.get_items(conn, today_str)
        finally:
            conn.close()
        html = build_simple_html(items, today_str).encode("utf-8")
        cached["ts"] = now
        cached["body"] = html
        self._send(200, html, "text/html; charset=utf-8")


# ---------------------------------------------------------------- 看板页面

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>新闻实时看板</title>
<style>
:root {
  --bg: #f6f7f9; --card: #ffffff; --text: #1f2328; --text-muted: #737a85;
  --border: #e4e7ec; --accent: #2563eb; --accent-soft: #eff4ff;
  --new-bg: #fff7e6; --new-border: #f0b849; --green: #16a34a;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: "Segoe UI", "Microsoft YaHei", sans-serif; background: var(--bg); color: var(--text); }
header { position: sticky; top: 0; z-index: 10; background: var(--card); border-bottom: 1px solid var(--border); padding: 12px 20px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
header h1 { font-size: 17px; font-weight: 700; }
.live-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); display: inline-block; margin-right: 6px; animation: pulse 2s infinite; }
@keyframes pulse { 0%,100% { opacity: 1 } 50% { opacity: .3 } }
.meta-line { color: var(--text-muted); font-size: 12.5px; }
.tabs { display: flex; gap: 8px; flex-wrap: wrap; margin-left: auto; }
.tab { border: 1px solid var(--border); background: var(--card); border-radius: 16px; padding: 4px 14px; font-size: 13px; cursor: pointer; color: var(--text); }
.tab.active { background: var(--accent); border-color: var(--accent); color: #fff; }
.tab .cnt { opacity: .75; font-size: 11.5px; margin-left: 3px; }
main { max-width: 100%; margin: 0; padding: 14px 20px 60px; }
/* ====== 列表项（2026-08-26 第三轮调整 / 2026-08-28 放宽 / 2026-08-29 撑满 main）：top-row + 可选 summary =====
   top：row1（meta + thumb 可选）+ title（同行右侧，flex:1）+ cmt-action
   row2：summary（可选，单行截断）
   row3：点评块
   视觉：去掉外边框（border），仅底部 1px 分隔线；卡片宽度 = main 宽 - 8px，与 attr-filter 卡片等宽（用户 2026-08-29 反馈「做成和上面一样长」）
*/
.item {
  display: flex; flex-direction: column; gap: 4px;
  background: #fff;
  border: 0;
  border-bottom: 1px solid #f1f5f9;
  border-radius: 0;
  padding: 9px 12px 10px;
  margin: 0 0 2px;               /* 左对齐撑满 main，与上方 attr-filter 同宽 */
  /* 2026-08-29 用户反馈「做成和上面一样长」—— 新闻列表宽度跟 attr-filter 一致，撑满 main 内容区 */
  /* attr-filter 在 main 内 100% 撑满；item 也 100% + 左对齐，任意视宽下左右边缘完全对齐 */
  width: 100%;
  max-width: none;               /* 不限宽，确保各视宽都与上方 attr-filter 等宽 */
  transition: background .12s ease;
}
.item:hover { background: #fafbfd; }

/* ── top 行：row1（meta）+ title + cmt-action 横排同一行 ── */
.item .top {
  display: flex; align-items: center; gap: 10px;
  min-width: 0;
}

/* ── 第 1 段：meta（元数据条 + 可选缩略图） ── */
.item .row1 {
  display: flex; align-items: center; gap: 7px;
  font-size: 12px; line-height: 1.4; color: var(--text-muted);
  flex-shrink: 1; min-width: 0;
  overflow: hidden;
}
.item .row1 > * { white-space: nowrap; flex-shrink: 0; }

/* 绝对日期 + 时间：拆成两个独立字段（编号、日期、时间、媒体…） */
.item .date, .item .time {
  font-variant-numeric: tabular-nums;
  color: #94a3b8; font-size: 11.5px;
  white-space: nowrap;
}
.item .date { color: #64748b; font-weight: 600; }

/* 编号：胶囊（顶部 10 条橙红渐变、11-30 琥珀色、其余浅灰） */
.item .num {
  display: inline-flex; align-items: center; justify-content: center;
  min-width: 26px; height: 20px; padding: 0 7px;
  border-radius: 999px;
  font-size: 11.5px; font-weight: 700;
  background: #f1f5f9; color: #64748b;
  font-variant-numeric: tabular-nums;
}
.item .num.fresh { background: linear-gradient(135deg, #ef4444, #f97316); color: #fff; box-shadow: 0 1px 3px rgba(239,68,68,.30); }
.item .num.warm { background: #fef3c7; color: #b45309; }

/* 倒计时 + 日期：紧凑色阶 */
.item .time-block { display: inline-flex; align-items: center; gap: 5px; font-variant-numeric: tabular-nums; }
.item .rel { font-weight: 600; color: #475569; }
.item .rel.hot { color: #dc2626; }       /* < 1h 红 */
.item .rel.warm { color: #d97706; }      /* < 6h 橙 */
.item .rel-dot { color: #cbd5e1; font-size: 9px; }
.item .pub { color: #94a3b8; font-size: 11.5px; }

/* 来源 pill：浅底圆角 */
.item .src-inline {
  display: inline-flex; align-items: center; gap: 4px;
  font-size: 12.5px; font-weight: 600; color: #1e293b;
  padding: 1px 9px 1px 6px;
  background: #f6f8fc; border: 1px solid #e7ecf5;
  border-radius: 999px;
}

/* section / subcategory / upvotes 通用徽章（保留旧 .badge 兼容性，但加彩色背景仅给 sec） */
.badge { display: inline-block; font-weight: 500; font-size: 12.5px; padding: 0; }
.badge.sec {
  display: inline-block;
  padding: 1px 9px; border-radius: 999px;
  font-size: 11px; font-weight: 700; letter-spacing: .2px;
  background: #ede9fe; color: #6d28d9;
}
.badge.sec.world    { background: #dbeafe; color: #1d4ed8; }
.badge.sec.business { background: #dcfce7; color: #15803d; }
.badge.sec.busi     { background: #dcfce7; color: #15803d; }
.badge.sec.ai       { background: #fce7f3; color: #be185d; }
.badge.sec.tech     { background: #ffedd5; color: #c2410c; }
.badge.sec.policy   { background: #e0e7ff; color: #4338ca; }
.badge.sec.cn       { background: #fee2e2; color: #b91c1c; }
.badge.red { color: var(--text); }
.badge.intl { color: var(--text); }
.badge.ups { color: var(--text); }
.badge.stars { color: #d97706; font-weight: 600; }

.src-logo { height: 20px; width: auto; max-width: 96px; vertical-align: -4px; margin-right: 0; }
/* 各源 logo 容器微调 */
.src-logo.pd { height: 22px; vertical-align: -6px; padding: 1px 6px; background: #fff; border: 1px solid #f0e2e2; border-radius: 4px; box-sizing: content-box; }
.src-logo.mit { height: 20px; padding: 1px 6px; background: #000; border-radius: 4px; box-sizing: content-box; }
.src-logo.reuters { height: 20px; padding: 1px 6px; background: #fff; border: 1px solid #f5e0d8; border-radius: 4px; box-sizing: content-box; }

/* ── title：占满 top 剩余空间，单行截断 ──
   用户要求：标题与元数据条显示在同一行 */
.item .title {
  color: var(--text); text-decoration: none;
  font-size: 14.5px; font-weight: 600; line-height: 1.5;
  display: block;
  flex: 1 1 auto; min-width: 0;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  max-width: 100%;
}
.item .title:hover { color: var(--accent); text-decoration: underline; }
.item .title.visited { color: #9ca3af; }
.item .title.visited:hover { color: var(--accent); text-decoration: underline; }

/* 缩略图（row1 末尾，可点击新窗口放大原图）：Reuters 等源 extra.image */
.item .thumb-wrap {
  display: inline-block; flex-shrink: 0;
  line-height: 0;
  border-radius: 5px;
  overflow: hidden;
  border: 1px solid #e3e8f2;
  background: #f6f8fc;
  transition: border-color .15s ease;
}
.item .thumb-wrap:hover { border-color: #ff7d39; }
.item .thumb-wrap img {
  display: block;
  width: 64px; height: 40px;
  object-fit: cover;
  vertical-align: middle;
  transition: transform .18s ease;
}
.item .thumb-wrap:hover img { transform: scale(1.06); }

/* ── row2：简介（可选，单行截断） ── */
.item .row2 { min-width: 0; }
/* 简介：单行截断 */
.item .summary {
  font-size: 12.5px; line-height: 1.55; color: #64748b;
  font-weight: 400;
  display: block;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  max-width: 100%;
}
/* ===== 属性过滤器：Boss 直聘风格行式筛选器 =====
   布局：每个属性维度占一行，左侧属性名 + 右侧 pill 列表
   视觉：未激活 = 白底浅灰边；激活 = 橙色背景 (#ff7d39) + 白字
*/
.attr-filter { background: linear-gradient(180deg, #ffffff 0%, #f7f9fc 100%); border: 1px solid #e7ecf5; border-radius: 12px; padding: 14px 18px 14px; margin-bottom: 14px; box-shadow: 0 1px 3px rgba(30,41,59,.05); position: relative; padding-top: 38px; }
.attr-filter .view-switcher { position: absolute; top: 10px; left: 18px; right: 18px; display: flex; align-items: center; justify-content: space-between; gap: 8px; pointer-events: none; }
.attr-filter .view-switcher .switcher-label { font-size: 11.5px; color: #94a3b8; font-weight: 600; letter-spacing: .4px; pointer-events: auto; }
.attr-filter .view-switcher .switcher-tabs { display: inline-flex; background: #f1f5f9; border: 1px solid #e3e8f2; border-radius: 9px; padding: 3px; gap: 2px; pointer-events: auto; }
.attr-filter .view-switcher a { display: inline-block; padding: 5px 14px; border-radius: 6px; font-size: 12.5px; color: #475569; text-decoration: none; font-weight: 600; transition: all .14s ease; white-space: nowrap; line-height: 1.3; }
.attr-filter .view-switcher a:hover:not(.active) { color: #ff7d39; background: #fff8f3; }
.attr-filter .view-switcher a.active { background: linear-gradient(135deg, #ff7d39 0%, #ff9558 100%); color: #fff; box-shadow: 0 2px 5px rgba(255,125,57,.30); }
.attr-rows { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 32px; align-items: start; }
/* 2026-08-29：两栏布局——每个 group（全球/科技）一个 .attr-col，水平并排；左=全球（含 人民日报/Reuters sub-row），右=科技（含 AI/航天/综合 sub-row） */
.attr-col { display: flex; flex-direction: column; gap: 8px; min-width: 0; }
.attr-row { display: flex; align-items: flex-start; gap: 16px; min-width: 0; }
/* 2026-08-28：过滤行默认折叠（HTML hidden 属性）—— 须显式排除 [hidden]，否则 .attr-row 的 flex 覆盖浏览器默认的 display:none */
/* 用 !important 保证浏览器默认 [hidden] 行为生效，确保折叠行真正隐藏 */
.attr-row[hidden] { display: none !important; }
.attr-row-label { flex: 0 0 76px; padding-top: 5px; font-size: 13px; font-weight: 700; color: #1e293b; text-align: right; letter-spacing: .2px; }
.attr-row-pills { flex: 1; display: flex; flex-wrap: wrap; gap: 6px 6px; }
.attr-pill {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 4px 12px; border-radius: 999px;
  border: 1px solid #e3e8f2; background: #fff;
  color: #334155; font-size: 12.5px; font-weight: 500;
  cursor: pointer; user-select: none; line-height: 1.4;
  transition: all .14s ease; white-space: nowrap;
}
.attr-pill:hover { border-color: #ff7d39; color: #ff7d39; }
.attr-pill .plabel { display: inline-block; }
.attr-pill .pcnt {
  display: inline-block; min-width: 18px; text-align: center;
  font-size: 10.5px; font-weight: 700; color: #94a3b8;
  background: transparent; border-radius: 8px; padding: 0 4px;
  font-variant-numeric: tabular-nums; line-height: 1.4;
}
.attr-pill.zero { opacity: .38; pointer-events: none; }
.attr-pill.zero .pcnt { display: none; }
/* 0 条 → 直达源站的链接 pill：恢复点击响应 + 去掉下划线 */
.attr-pill-link.zero { pointer-events: auto; text-decoration: none; }
.attr-pill-link.zero:hover { color: var(--accent); border-color: var(--accent); opacity: .9; }
/* 激活态：橙色背景 + 白字（参考 Boss 直聘） */
.attr-pill.active {
  background: linear-gradient(135deg, #ff7d39 0%, #ff9558 100%);
  border-color: transparent; color: #fff; box-shadow: 0 2px 6px rgba(255,125,57,.30);
}
.attr-pill.active .pcnt { color: #fff; opacity: .92; background: rgba(255,255,255,.18); }
/* 全部按钮（每行第一个 pill）样式稍弱以区分 */
.attr-pill.all { font-weight: 700; }
/* 叶子行（具体单源 pill）样式稍弱以区分聚合行 */
.attr-row.sub .attr-row-label { color: #64748b; font-weight: 600; font-size: 12.5px; }
/* 2026-08-28：属性过滤器三级嵌套 + 人民日报动态板块行样式 */
/* .level-1 = 二级行（人民日报板块/World-Business/航天/SpaceX/AI 等），缩进比顶级再深 */
.attr-row.sub.level-1 { padding-left: 14px; border-left: 2px solid #eef2f7; margin-left: 0; }
/* .level-2 = 三级行（公司 12 品牌 / HF Daily Papers），再深缩进 + 字号略小 */
.attr-row.sub.level-2 { padding-left: 24px; border-left: 1.5px solid #f0f4f9; }
.attr-row.sub.level-2 .attr-row-label { font-size: 12px; font-weight: 500; color: #475569; }
.attr-row.sub.level-2 .attr-pill { font-size: 12px; padding: 3px 10px; }
/* 人民日报动态板块行：红色左边线 + 板块文字色 → 强化"中国官媒"语义 */
.attr-row.pd-block { padding-left: 14px; border-left: 2px solid #c0392b; }
.attr-row.pd-block .attr-row-label { color: #c0392b; }
.attr-row.pd-block .attr-row-pills .attr-pill { font-size: 12px; padding: 3px 10px; }
/* 2026-08-29：顶级「全部 / 全球 / 科技」行——空 label 隐藏，与下方两栏网格分隔 */
.attr-row.top-cat { padding-bottom: 10px; margin-bottom: 6px; border-bottom: 1px dashed #eef2f7; }
.attr-row.top-cat .attr-row-label { display: none; }
.attr-row.top-cat .attr-row-pills { padding-left: 0; }
.attr-row.top-cat .attr-pill { font-size: 13.5px; padding: 5px 14px; }
/* Reuters World/Business 行：蓝色左边线突出"国际" */
.attr-row.sub.level-1:not(.pd-block) { border-left-color: #e7ecf5; }
/* 0 条源 → 直达源站链接（与 pill 同一形状，灰化） */
.attr-pill-link { text-decoration: none; color: inherit; }
.empty { text-align: center; color: var(--text-muted); padding: 48px 0 60px; font-size: 14px; }
.empty-hint { margin-top: 18px; font-size: 12.5px; color: var(--text-muted); }
.src-links { display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; margin-top: 12px; max-width: 640px; margin-left: auto; margin-right: auto; }
.src-link { display: inline-flex; align-items: center; gap: 4px; border: 1px solid #d8e1f1; background: #fff; border-radius: 999px; padding: 4px 14px; font-size: 12.5px; color: #2d3748; text-decoration: none; font-weight: 500; transition: all .15s ease; box-shadow: 0 1px 1px rgba(60,90,150,.04); }
.src-link:hover { border-color: var(--accent); color: var(--accent); transform: translateY(-1px); box-shadow: 0 2px 5px rgba(60,90,150,.10); }
.src-link .arrow { font-size: 11px; opacity: .55; }
footer { text-align: center; color: var(--text-muted); font-size: 12px; padding: 20px; }
#statusbar { font-size: 12px; color: var(--text-muted); }
/* ===== 「我的点评」块（运营者人工撰写，区别于抓取内容）===== */
.item .row3 { margin-top: 3px; }
.comment-box { font-size: 12.5px; }
.comment-view {
  display: flex; align-items: flex-start; gap: 6px;
  background: #fffbeb; border-left: 3px solid #f0b849;
  border-radius: 0 6px 6px 0; padding: 6px 10px;
}
.cmt-label { font-weight: 700; color: #b45309; flex-shrink: 0; }
.cmt-text { color: #78350f; flex: 1; line-height: 1.5; word-break: break-word; }
.cmt-btn {
  border: 1px solid #e7ecf5; background: #fff; color: #64748b;
  font-size: 12px; border-radius: 6px; padding: 2px 10px; cursor: pointer;
  white-space: nowrap; flex-shrink: 0;
}
.cmt-btn:hover { border-color: #ff7d39; color: #ff7d39; }
.cmt-action { flex-shrink: 0; margin-left: 6px; }
.cmt-readonly { font-size: 12px; color: #94a3b8; font-weight: 500; }
.cmt-readonly-hint { font-size: 12.5px; line-height: 1.5; color: #64748b; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 6px 10px; margin-top: 4px; }
.cmt-readonly-hint code { background: #eef2f7; padding: 1px 5px; border-radius: 4px; font-size: 12px; }
.cmt-input {
  width: 100%; border: 1px solid #f0b849; border-radius: 6px;
  padding: 6px 8px; font: inherit; font-size: 12.5px; line-height: 1.5;
  resize: vertical; box-sizing: border-box;
}
.cmt-actions { margin-top: 4px; display: flex; gap: 6px; }
.cmt-save {
  background: #ff7d39; color: #fff; border: none; border-radius: 6px;
  padding: 3px 14px; cursor: pointer; font-size: 12px; font-weight: 600;
}
.cmt-cancel {
  background: #fff; border: 1px solid #e7ecf5; color: #64748b;
  border-radius: 6px; padding: 3px 14px; cursor: pointer; font-size: 12px;
}
</style>
</head>
<body>
<header>
  <h1><span class="live-dot"></span>新闻实时看板</h1>
  <div class="meta-line" id="statusbar">加载中…</div>
  <nav class="tabs" id="tabs"></nav>
</header>
<main id="list"><div class="empty">数据加载中…</div></main>
<footer>本地实时收集 · SSE 实时推送，新文章即时上屏</footer>
<script>
window.__errs = [];
window.addEventListener("error", (e) => {
  const msg = (e.message || (e.error && e.error.message) || "未知错误") + " @ " + (e.filename || "?") + ":" + (e.lineno || "?");
  window.__errs.push(msg);
  const sb = document.getElementById("statusbar");
  if (sb) sb.textContent = "JS 错误：" + msg;
});
window.addEventListener("unhandledrejection", (e) => {
  const msg = "未捕获 Promise：" + ((e.reason && e.reason.message) || e.reason || "未知");
  window.__errs.push(msg);
  const sb = document.getElementById("statusbar");
  if (sb) sb.textContent = msg;
});
// 各源官网入口（服务端注入），空状态时提供直达链接
const SOURCE_HOMES = __SOURCE_HOMES__;
const CAT_LABELS = { international: "国际", china: "中国", tech_global: "科技", ai: "科技" };
const SUB_LABELS = { labs: "AI实验室", papers: "学术论文", opensource: "开源生态", eval: "模型评测", policy: "官方政策", academia: "学术机构" };
const SOURCE_LOGOS = { people_daily: "people_daily", reuters: "reuters", mit_tech_review: "mit_tech_review" };
// 来源显示名覆盖：保证 DB 里旧条目（source_name="Reuters"）也能立刻显示为 "Reuters 路透社"
const SOURCE_DISPLAY = { people_daily: "人民日报", reuters: "Reuters 路透社", mit_tech_review: "MIT Technology Review" };
// 把 tech_global 和 ai 合并为同一个 tab "科技"
function tabOf(c) { return c === "tech_global" || c === "ai" ? "tech" : c; }
let currentCat = "tech";
let cachedItems = [];
let day = "";

// 把 published 转换成"距今多久"的相对时间（如"3 分钟前"），仅用于非人民日报条目
function relTime(published) {
  if (!published) return "";
  const m = /^([0-9]{4})-([0-9]{2})-([0-9]{2})[ T]([0-9]{2}):([0-9]{2})/.exec(published);
  if (!m) return "";
  const ts = new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5]).getTime();
  const diffMin = Math.max(0, Math.floor((Date.now() - ts) / 60000));
  if (diffMin < 1) return "刚刚";
  if (diffMin < 60) return diffMin + " 分钟前";
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return diffHr + " 小时前";
  return Math.floor(diffHr / 24) + " 天前";
}

// 兜底：人民日报历史数据若仍是 "YYYY-MM-DD 00:00"，渲染时改成 "YYYY-MM-DD 06:00"
function fmtPub(it) {
  if (!it.published) return "";
  if (it.source_id === "people_daily" && it.published.slice(-5) === "00:00") {
    return it.published.slice(0, 10) + " 06:00";
  }
  return it.published;
}

// 从 section 文本提取版号数字（人民日报 "01版：要闻" → 1）
function _secNum(s) {
  const m = /^[0-9]{2}版/.exec(s || "");
  return m ? parseInt(m[0], 10) : 99;
}

// ===== 属性过滤器（树状 + 板块过滤；2026-08-28 大重构）=====
// 重大变更：
//  1) pill 模型升级为 (sources[], section|null) 二元组：
//     - sources 限定来源 id 集合（核心过滤维度）
//     - section 是 extra.section 精确匹配（用于人民日报"01版：要闻" / Reuters"World"/"Business"）
//  2) 三个一级分类 → 两个：「全球」「科技」
//  3) 全球下分「人民日报」「Reuters 路透社」，
//     - 人民日报固定按钮 = 整个源；动态 sub-row 由当天 items 实时扫描板块（排除广告/副刊）
//     - Reuters 固定按钮 = 整个源；固定 sub-row = World / Business
//  4) 科技下三级嵌套：分支(AI/航天/综合) → sub_branch(公司/论文/SpaceX/MIT) → leaves(12 品牌/HF)
//  5) 「公司」= 12 个 AI 品牌 pill（含抖音/腾讯 DirectoryEntry 兜底）
//  6) pill 选中态通过 canonical key = sources 排序 + "§" + section 精确匹配；
//     activeSources/activeSection 取代单一 activeSourceIds；再次点击已激活 pill 取消
const FILTER_GROUPS = [
  // ===== 全球 =====
  {
    key: "global", label: "全球",
    sources: ["reuters", "people_daily"],
    branches: [
      // 人民日报：根=整个源；动态 sub-row 按当天板块自动列出（剔除 广告 / 副刊）
      {
        label: "人民日报", sources: ["people_daily"],
        dynamic_sections: { source_id: "people_daily", exclude: /广告|副刊/ },
      },
      // 路透社：根=整个源；固定 sub-row = World / Business（来自 extra.section）
      {
        label: "Reuters 路透社", sources: ["reuters"],
        fixed_subsections: [
          { label: "World", section: "World" },
          { label: "Business", section: "Business" },
        ],
      },
    ],
  },
  // ===== 科技（3 级嵌套）=====
  {
    key: "tech", label: "科技",
    sources: ["hf_papers","openai_news","openai_research","anthropic","deepmind","google_research","meta_ai","ms_ai","nvidia_ai","mit_tech_review","github_trending","hf_blog","spacex_news","deepseek","zhipu","qwen","moonshot","dir_doubao","dir_tencent"],
    branches: [
      // ---- AI：sub_branch = 公司（含 12 AI 品牌）/ 论文（HF Daily Papers）----
      {
        label: "AI",
        sources: ["openai_news","openai_research","anthropic","deepmind","google_research","meta_ai","ms_ai","nvidia_ai","hf_papers","deepseek","zhipu","qwen","moonshot","dir_doubao","dir_tencent"],
        sub_branches: [
          {
            label: "公司", sources: ["openai_news","openai_research","anthropic","deepmind","google_research","meta_ai","ms_ai","nvidia_ai","deepseek","zhipu","qwen","moonshot","dir_doubao","dir_tencent"],
            leaves: [
              { label: "OpenAI", sources: ["openai_news","openai_research"] },
              { label: "Anthropic", sources: ["anthropic"] },
              { label: "Google DeepMind", sources: ["deepmind","google_research"] },
              { label: "Meta AI", sources: ["meta_ai"] },
              { label: "Microsoft AI", sources: ["ms_ai"] },
              { label: "NVIDIA", sources: ["nvidia_ai"] },
              { label: "DeepSeek", sources: ["deepseek"] },
              { label: "智谱 GLM", sources: ["zhipu"] },
              { label: "通义千问", sources: ["qwen"] },
              { label: "月之暗面", sources: ["moonshot"] },
              { label: "抖音", sources: ["dir_doubao"] },
              { label: "腾讯", sources: ["dir_tencent"] },
            ],
          },
          {
            label: "论文", sources: ["hf_papers"],
            leaves: [
              { label: "HF Daily Papers", sources: ["hf_papers"] },
            ],
          },
        ],
      },
      // ---- 航天：sub_branch = SpaceX（无 leaves，单一 pill）----
      {
        label: "航天", sources: ["spacex_news"],
        sub_branches: [
          { label: "SpaceX", sources: ["spacex_news"], leaves: [] },
        ],
      },
      // ---- 综合：sub_branch = MIT Tech Review（无 leaves）----
      {
        label: "综合", sources: ["mit_tech_review"],
        sub_branches: [
          { label: "MIT Tech Review", sources: ["mit_tech_review"], leaves: [] },
        ],
      },
    ],
  },
];
// 真实爬虫源 id（2026-08-25：cctv 下架；2026-08-28 无变动）
const REAL_SOURCE_IDS = ["reuters", "people_daily", "mit_tech_review", "openai_news",
  "openai_research", "anthropic", "deepmind", "google_research", "meta_ai", "ms_ai",
  "nvidia_ai", "hf_blog", "hf_papers", "github_trending", "spacex_news",
  "deepseek", "zhipu", "qwen", "moonshot",
  "ia_cas", "pku_ai", "tsinghua_ai",
  "baai", "caai"];

// 当前 sub-filter 选中的 pill id（null = 不过滤，显示全部）
// 每个 pill 拥有全局唯一 id（来源位置路径，如 'tech.AI.公司.OpenAI'）；选中/取消基于该 id
// 这样多个 (sources,section) 完全相同的 pill（论文 vs HF Daily Papers vs 全部）只会点亮用户真正点的那个
let activeKey = null;
let activeSources = null;
let activeSection = null;

// 把 (sources, section) 转成稳定的 canonical key（仅用于未指定 pill id 的 fallback 比较）
const pillKey = (sources, section) => sources.slice().sort().join("|") + "\u00a7" + (section == null ? "" : section);

// 计算 items 命中 pill 数量（O(N) 一次扫描，传 matcher 闭包复用）
const makeMatcher = (items) => (sources, section) => {
  let n = 0;
  for (const it of items) {
    if (!sources.includes(it.source_id)) continue;
    if (section != null) {
      const sec = (it.extra && it.extra.section) || "";
      if (sec !== section) continue;
    }
    n++;
  }
  return n;
};

// 默认不过滤（activeSources = null）→ 看板加载即展示全部新闻
activeSources = null;
activeSection = null;
activeKey = null;

// pill 选中判定：基于 pill id 精确匹配（每个 pill 一对一）
// 避免多个 (sources,section) 完全相同的 pill 同时点亮（如「论文」「HF Daily Papers」「全部」三者行为一致 → 只点亮用户真正点的那个）
const isActive = (id) => activeKey != null && activeKey === id;

// HTML data-attribute 转义（section 可能含 " ' < 等）
const escapeAttr = (s) => String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");

// 渲染单个 pill；matcher 是 makeMatcher 返回的计数闭包
// pillId: 全局唯一（路径式，如 'tech.AI.公司.OpenAI'），用于点击/激活态匹配
// 0 条 → 直达源站外链（pill 仍占位可点击；空数据 fallback 不渲染筛选项，避免视觉噪音）
function renderPill(label, sources, section, matcher, opts) {
  opts = opts || {};
  const cnt = matcher(sources, section == null ? null : section);
  const pillId = opts.pillId || pillKey(sources, section == null ? null : section);
  const isZero = (cnt === 0 && !opts.alwaysShow);
  if (isZero) {
    const h = SOURCE_HOMES[sources[0]];
    if (h && h.url) {
      const cls = "attr-pill attr-pill-link zero";
      const tip = sources.length === 1
        ? '今日暂无收录，点击直达 ' + escapeHtml(h.name)
        : '今日该方向暂无收录（已选 ' + sources.length + ' 个源），点击直达 ' + escapeHtml(h.name);
      return '<a class="' + cls + '" href="' + h.url + '" target="_blank" rel="noopener" ' +
        'data-pill-id="' + escapeAttr(pillId) + '" ' +
        'data-sources="' + sources.join(",") + '" data-section="' + escapeAttr(section == null ? "" : section) + '" ' +
        'title="' + tip + '">' +
        '<span class="plabel">' + escapeHtml(label) + '</span>' +
      '</a>';
    }
  }
  const cls = "attr-pill"
    + (opts.isAll ? " all" : "")
    + (isActive(pillId) ? " active" : "")
    + (isZero ? " zero" : "");
  const cntStr = '<span class="pcnt">' + cnt + '</span>';
  return '<button class="' + cls + '" ' +
    'data-pill-id="' + escapeAttr(pillId) + '" ' +
    'data-sources="' + sources.join(",") + '" ' +
    'data-section="' + escapeAttr(section == null ? "" : section) + '">' +
    '<span class="plabel">' + escapeHtml(label) + '</span>' +
    cntStr +
  '</button>';
}

// 渲染一行：属性名 + pills；level 控制缩进（0=顶级 1=二级 2=三级）
// collapsible=true 时给 row 加 hidden + data-collapse-prefix（点击对应 pill 才展开）
// collapsePrefix 支持字符串或数组（数组用 "," 拼成多值——支持多个 pillId 触发显示/隐藏）
function renderRow(label, pills, opts) {
  opts = opts || {};
  const level = opts.level || 0;
  const isSub = level > 0;
  const extraCls = opts.extraCls || "";
  // level 2 加更深缩进（科技公司品牌的 12 颗 pill 行）
  const depthCls = level >= 2 ? " deep" : "";
  let prefixAttr = "";
  if (opts.collapsePrefix) {
    const arr = Array.isArray(opts.collapsePrefix) ? opts.collapsePrefix : [opts.collapsePrefix];
    if (arr.length) prefixAttr = ' data-collapse-prefix="' + escapeAttr(arr.join(",")) + '"';
  }
  const hiddenAttr = opts.collapsible ? " hidden" : "";
  return '<div class="attr-row' + (isSub ? ' sub' : '') + ' level-' + level + depthCls + ' ' + extraCls + '"' + prefixAttr + hiddenAttr + '>' +
    '<div class="attr-row-label">' + escapeHtml(label) + '</div>' +
    '<div class="attr-row-pills">' + pills.join("") + '</div>' +
  '</div>';
}

// 工具：从 items 扫描 people_daily 当天所有有效板块（剔除 广告 / 副刊），按版号排序
function scanPDSections(items) {
  const sec = /广告|副刊/;
  // 用对象记录板块 → 计数（section unique）
  const seen = Object.create(null);
  for (const it of items) {
    if (it.source_id !== "people_daily") continue;
    const e = it.extra || {};
    const s = (e.section || "").trim();
    if (!s || sec.test(s)) continue;
    seen[s] = (seen[s] || 0) + 1;
  }
  // 按版号升序（01 在顶，20 在底）；用 [0-9] 字符类避免 Python non-raw 字符串的 \d 转义陷阱
  const arr = Object.entries(seen);
  arr.sort((a, b) => {
    const na = parseInt((a[0].match(/[0-9]+/) || ["999"])[0], 10);
    const nb = parseInt((b[0].match(/[0-9]+/) || ["999"])[0], 10);
    return na - nb;
  });
  return arr; // [[section, count], ...]
}

// ===== 渲染过滤器（含 3 级嵌套 + 动态 PD 板块行）=====
// 2026-08-28：sub-row 默认折叠（hidden）；用户点击上方 branch pill 时才展开（toggle 显隐）
// 2026-08-29：两栏布局——每个 group（全球/科技）一个 .attr-col，两列 grid 并排
// 2026-08-29 用户再反馈：合并简化——顶级「全部 / 全球 / 科技」一行；col 内 L0/L1 不再重复"全部"子级（避免视觉噪音）
function renderAttrFilter(items, groups) {
  const matcher = makeMatcher(items);

  // ===== 顶级 L0：单独一行，承载「全部 / 全球 / 科技」3 颗 pill =====
  // 「全部」= 清空筛选（pillId="all"，sources=REAL_SOURCE_IDS 仅用于显示总数）
  // 「全球/科技」= 该群组全源（pillId=group.key）
  const topPills = [];
  topPills.push(renderPill("全部", REAL_SOURCE_IDS.slice(), null, matcher, { pillId: "all" }));
  for (const g of groups) {
    topPills.push(renderPill(g.label, g.sources, null, matcher, { pillId: g.key }));
  }
  const topRowHtml = '<div class="attr-row top-cat level-0">' +
    '<div class="attr-row-label"></div>' +
    '<div class="attr-row-pills">' + topPills.join("") + '</div>' +
  '</div>';

  const colsHtml = groups.map(g => {
    const colRows = [];
    // ===== 行 L0：根（"全球"/"科技"）—— 仅含各 branch pill，不再重复"全部"子级（已上提至顶级）=====
    const rootPills = g.branches.map(br =>
      renderPill(br.label, br.sources, null, matcher, { pillId: g.key + "." + br.label }));
    colRows.push(renderRow(g.label, rootPills, { level: 0 }));

    // ===== 每个 branch 行 =====
    g.branches.forEach(br => {
      const brPath = g.key + "." + br.label;
      // 人民日报分支：动态板块 sub-row（每次 items 更新时由 updateAttrFilterCounts 局部重建）
      // 默认折叠；点击分支 pill「人民日报」时展开（collapsePrefix=['global.人民日报']）
      if (br.dynamic_sections) {
        const secArr = scanPDSections(items);
        if (secArr.length) {
          const pills = secArr.map(([s]) =>
            renderPill(s, [br.dynamic_sections.source_id], s, matcher, { pillId: brPath + "." + s }));
          colRows.push(renderRow(br.label, pills, { level: 1, extraCls: "pd-block", collapsible: true, collapsePrefix: [brPath] }));
        }
        return;
      }
      // Reuters 分支：World / Business sub-row（板块固定，无需增量更新）
      // 默认折叠；点击「Reuters 路透社」pill 时展开
      if (br.fixed_subsections) {
        const pills = br.fixed_subsections.map(sb =>
          renderPill(sb.label, br.sources, sb.section, matcher, { pillId: brPath + "." + sb.label }));
        if (pills.length) colRows.push(renderRow(br.label, pills, { level: 1, collapsible: true, collapsePrefix: [brPath] }));
        return;
      }
      // 科技分支（sub_branches 模式）：每个 sub_branch 一行
      if (br.sub_branches) {
        // 行 L1：branch 自身（"AI"）：各 sub_branch pill（"全部" 子级已上提至顶级，不再重复）
        const subPills = br.sub_branches.map(sb =>
          renderPill(sb.label, sb.sources, null, matcher, { pillId: brPath + "." + sb.label }));
        if (subPills.length) {
          colRows.push(renderRow(br.label, subPills, { level: 1 }));
        }
        // 行 L2：每个 sub_branch 的 leaves（12 AI 品牌、HF Daily Papers）
        // 默认折叠；点 sub_branch pill（如「公司」「论文」）时展开；同时点父 pill「AI」也展开其下所有 sub_branch row
        // 「全部」子级保留：作为 sub_branch 内部的快捷入口（"公司"行→[全部, OpenAI, ...]，"论文"行→[全部, HF Daily Papers]）
        br.sub_branches.forEach(sb => {
          if (sb.leaves && sb.leaves.length) {
            const sbPath = brPath + "." + sb.label;
            const leafPills = sb.leaves.map(lf =>
              renderPill(lf.label, lf.sources, null, matcher, { pillId: sbPath + "." + lf.label }));
            leafPills.unshift(renderPill("全部", sb.sources, null, matcher, { pillId: sbPath + ".全部", isAll: true }));
            // collapsePrefix 双值：父 branch path + 自己 sub_branch path（点 AI 或 公司/论文 都展开这行）
            colRows.push(renderRow(sb.label, leafPills, { level: 2, collapsible: true, collapsePrefix: [brPath, sbPath] }));
          }
          // 没有 leaves 的分支（航天/SpaceX、综合/MIT）只占 L1 一行，不渲染额外 sub-row
        });
      }
    });
    return '<div class="attr-col" data-col-key="' + g.key + '">' + colRows.join("") + '</div>';
  }).join("");

  return '<div class="attr-filter" id="attrfilter">__VIEW_SWITCHER__' + topRowHtml + '<div class="attr-rows">' +
    colsHtml + '</div></div>';
}

// ===== 视图切换器（点击 → 跳 simple.html 或回到 /）=====
// 全量版/精简版部署在同级目录：./simple.html 和 ./ 都适用
function bindViewSwitcher() {
  const root = document.querySelector('.attr-filter .view-switcher');
  if (!root) return;
  const full = root.querySelector('a[data-mode="full"]');
  const simple = root.querySelector('a[data-mode="simple"]');
  if (full) full.href = './';
  if (simple) simple.href = 'simple.html';
}

// ===== 增量更新：刷 attr-filter 内每个 pill 的数字徽章 + 灰化 + 重建动态 PD sub-row =====
// 不重建固定 DOM；动态人民日报板块单独从 #attrfilter .pd-block .attr-row-pills 容器里更新
function updateAttrFilterCounts(items) {
  const matcher = makeMatcher(items);
  const af = document.getElementById("attrfilter");
  if (!af) return;
  // 1) 固定 pill：刷新 count + zero 灰化（基于 pill id 匹配 active，避免误点亮行为相同的其他 pill）
  af.querySelectorAll(".attr-pill").forEach(el => {
    const sources = (el.dataset.sources || "").split(",").filter(Boolean);
    const section = el.dataset.section || null;
    if (!sources.length) return;
    const n = matcher(sources, section);
    const cntEl = el.querySelector(".pcnt");
    if (cntEl) cntEl.textContent = n;
    if (n === 0) el.classList.add("zero");
    else el.classList.remove("zero");
  });
  // 2) 动态 PD sub-row 重建（人民日报板块可能当天新增/移除；保留 pill id 语义）：
  //    扫描得到的板块 pill id 用稳定路径 'global.人民日报.<sec>'，与初次构建一致
  const pdContainer = af.querySelector(".attr-row.pd-block .attr-row-pills");
  if (pdContainer) {
    const secArr = scanPDSections(items);
    if (secArr.length) {
      pdContainer.innerHTML = secArr.map(([s]) =>
        renderPill(s, ["people_daily"], s, matcher, { pillId: "global.人民日报." + s })).join("");
      // 重建后保留已激活态（用户已选中某板块的情况）
      syncAttrFilterActive();
    } else {
      pdContainer.innerHTML = "";
    }
  }
}

// ===== 点击 pill 后增量更新 active 态（不重建 DOM；基于 pill id 精确匹配）=====
function syncAttrFilterActive() {
  const af = document.getElementById("attrfilter");
  if (!af) return;
  af.querySelectorAll(".attr-pill").forEach(el => {
    const id = el.dataset.pillId || "";
    if (isActive(id)) el.classList.add("active");
    else el.classList.remove("active");
  });
}

// 切换指定 prefix 的所有 collapse-row 显隐状态（默认折叠 → 点 pill 展开 / 再点收起）
// 一个 row 的 data-collapse-prefix 可能是 "a,b,c" 多值，匹配其中任一即触发 toggle
function toggleCollapseRows(pillId) {
  const af = document.getElementById("attrfilter");
  if (!af || !pillId) return;
  af.querySelectorAll(".attr-row[data-collapse-prefix]").forEach(el => {
    const cp = (el.dataset.collapsePrefix || "").split(",").map(s => s.trim()).filter(Boolean);
    if (cp.indexOf(pillId) >= 0) el.hidden = !el.hidden;
  });
}

// ===== 绑定 pill 点击：toggle 选中 / 取消；切完 active 态不重建 DOM =====
function bindAttrFilter() {
  const root = document.getElementById("attrfilter");
  if (!root) return;
  root.addEventListener("click", (e) => {
    const b = e.target.closest("button[data-pill-id]");
    if (!b) return;
    const pillId = b.dataset.pillId || "";
    const sources = (b.dataset.sources || "").split(",").filter(Boolean);
    if (!sources.length || !pillId) return;
    const section = b.dataset.section || null;
    // 顶级「全部」(pillId="all")：永远是清空筛选（activeSources=null），不应用任何 sources 过滤
    if (pillId === "all") {
      activeKey = "all";
      activeSources = null;
      activeSection = null;
    } else if (activeKey === pillId) {
      activeKey = null;
      activeSources = null;
      activeSection = null;
    } else {
      activeKey = pillId;
      activeSources = sources;
      activeSection = section;
    }
    // 0) toggle 当前 pill 对应的 collapse-row 显隐（用户 2026-08-28 反馈：默认不展开分支，点 pill 才展开）
    toggleCollapseRows(pillId);
    // 1) 增量同步 pill active 态（不重建 attr-filter DOM）
    syncAttrFilterActive();
    // 2) 重建 items 区（过滤变了，列表要刷新）
    renderItemsArea(
      (activeSources ? cachedItems.filter(i => {
        if (!activeSources.includes(i.source_id)) return false;
        if (activeSection != null) {
          const sec = (i.extra && i.extra.section) || "";
          if (sec !== activeSection) return false;
        }
        return true;
      }) : cachedItems.slice()),
      activeSources ? { sources: activeSources, section: activeSection } : null
    );
  });
}
// 把 12934 → '12.9k' / 1234 → '1.2k' / 48950 → '49k' / 999 → '999'
function fmtStars(n) {
  if (typeof n !== "number" || isNaN(n)) return "";
  if (n < 1000) return String(n);
  if (n < 10000) return (n / 1000).toFixed(1).replace(/[.]0$/, "") + "k";
  return Math.floor(n / 1000) + "k";
}

// SequenceMatcher 比例相似度（Python difflib 行为一致；用于简介与标题去重）
function smRatio(a, b) {
  if (!a || !b) return 0;
  if (a === b) return 1;
  const la = a.length, lb = b.length;
  if (la < 2 || lb < 2) return 0;
  // 双指针 + 公共子串匹配（与 difflib.SequenceMatcher.ratio 近似：2*M/(la+lb)）
  // 简化：用 LongestCommonSubsequence 不现实；改用 LCS 滑动窗口近似
  // 这里用 Python-like 实现：贪心扫描找最大公共子块
  const n = la + lb;
  const dp = new Array(n + 1).fill(0);
  for (let i = 0; i < la; i++) {
    const cur = dp.slice();
    for (let j = 0; j < lb; j++) {
      if (a[i] === b[j]) dp[j + 1] = cur[j] + 1;
      else dp[j + 1] = Math.max(dp[j], dp[j + 1]);
    }
  }
  const m = dp[lb];
  return (2 * m) / n;
}

// 去 HTML 标签 + 实体 + 多余空白（简介预览用）
function stripHtml(s) {
  if (!s) return "";
  return String(s)
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

// 解析「N 分钟前」「N 小时前」「N 天前」→ 分钟数（用于色阶）；不认识返回 -1
function relToMin(rel) {
  if (!rel) return -1;
  const m1 = /^([0-9]+) 分钟前$/.exec(rel);
  if (m1) return parseInt(m1[1], 10);
  const m2 = /^([0-9]+) 小时前$/.exec(rel);
  if (m2) return parseInt(m2[1], 10) * 60;
  const m3 = /^([0-9]+) 天前$/.exec(rel);
  if (m3) return parseInt(m3[1], 10) * 1440;
  if (rel === "刚刚") return 0;
  return -1;
}

// element 顺序：行 1 = [num] [time:YYYY-MM-DD HH:MM] [src] [sec] [sub] [stars] [thumb 可选]
//                行 2 = [title] [cmt-action]
//                行 3 = summary
//                行 4 = comment-box
function renderItemRow(i, num, dispName) {
  // 兜底：部分源（GitHub Trending / HF Models）的 published 为空，用 first_seen 代替以保证时间始终显示
  const pub = i.published || (i.first_seen ? i.first_seen.replace("T", " ") : "");
  // 日期 / 时间 拆成两个独立字段（用户要求：编号、日期、时间、媒体…）
  const dateStr = pub ? pub.slice(0, 10) : "";
  const timeStr = pub ? pub.slice(11, 16) : "";

  // 编号色阶：≤10 fresh（橙红）、11-30 warm（琥珀）、其余 浅灰
  let numCls = "num";
  if (num <= 10) numCls += " fresh";
  else if (num <= 30) numCls += " warm";

  // 板块 class 映射（根据 section 名动态选色）
  let secCls = "badge sec";
  let secLabel = "";
  if (i.extra && i.extra.section) {
    secLabel = i.extra.section;
    const s = secLabel.toLowerCase();
    if (s.includes("world")) secCls += " world";
    else if (s.includes("business") || s.includes("busi") || s === "biz") secCls += " business";
    else if (s.includes("ai") || s.includes("智能")) secCls += " ai";
    else if (s.includes("tech") || s.includes("科技")) secCls += " tech";
    else if (s.includes("policy") || s.includes("政策") || s.includes("要闻")) secCls += " policy";
    else if (s.includes("中国") || s.includes("cn")) secCls += " cn";
  }
  const secCell = secLabel
    ? '<span class="' + secCls + '">' + escapeHtml(secLabel) + '</span>'
    : "";

  // 来源 pill
  const srcCell = dispName
    ? '<span class="src-inline">' + escapeHtml(dispName) + '</span>'
    : "";

  // subcategory 徽章
  const subCell = i.subcategory
    ? '<span class="badge">' + escapeHtml(SUB_LABELS[i.subcategory] || i.subcategory) + '</span>'
    : "";

  // GitHub Trending：渲染 ★ 总数 + 今日数
  let starsCell = "";
  if (i.source_id === "github_trending" && i.extra) {
    const total = i.extra.stars;
    const today = i.extra.trend;
    const totalFmt = (typeof total === "number")
      ? '<span class="badge stars">★ ' + escapeHtml(fmtStars(total)) + '</span>'
      : "";
    let todayFmt = "";
    if (today) {
      const mm = today.match(/^([0-9,]+)[ ]+stars?[ ]+(today|this week|this month)/i);
      if (mm) {
        todayFmt = '<span class="badge stars">+ ' + escapeHtml(fmtStars(parseInt(mm[1].replace(/,/g, "")))) + ' today</span>';
      }
    }
    starsCell = totalFmt + todayFmt;
  }

  // 绝对日期 / 时间（两个独立字段）
  const dateCell = dateStr
    ? '<span class="date">' + escapeHtml(dateStr) + '</span>'
    : "";
  const timeCell = timeStr
    ? '<span class="time">' + escapeHtml(timeStr) + '</span>'
    : "";

  // 缩略图（row1 末尾，可点击放大）：来源有 extra.image 时显示
  // 2026-08-29 用户反馈「路透社图片不要放」—— Reuters 全量版统一不带 thumb（与简版去图保持一致）
  // onerror 隐藏失败图；点击在新窗口打开原图 = "单独放大"
  let thumbCell = "";
  const imageUrl = (i.source_id === "reuters") ? "" : ((i.extra && (i.extra.image || i.extra.thumb)) || "");
  if (imageUrl) {
    // 拼接 onerror 属性时不能用裸 'none'（JS 单引号字面量会提前闭合）。
    // 用 '\\u0027' 让 Python 字面值保留 \u0027，JS 引擎再翻译成 '。
    thumbCell =
      '<a class="thumb-wrap" href="' + escapeHtml(imageUrl) +
      '" target="_blank" rel="noopener" title="点击放大原图">' +
      '<img class="thumb" src="' + escapeHtml(imageUrl) +
      '" loading="lazy" alt="" onerror="this.parentNode.style.display=\\u0027none\\u0027">' +
      '</a>';
  }

  // 已访问标记
  const vis = visitedSet.has(i.url) ? " visited" : "";

  // 简介：来自 RSS description 写入的 extra.summary；GitHub Trending 用 extra.desc 兜底
  // 渲染时再做一次与标题的去重（feed_scraper 写入时已去过，这里兜底覆盖 GitHub 等"desc ≠ summary" 源）
  // 判定规则：完全相等 / 短边包含于长边（短≥20）/ SequenceMatcher 比例 ≥ 0.72（前三档）
  let summaryCell = "";
  const rawSummary = (i.extra && (i.extra.summary || i.extra.desc)) || "";
  if (rawSummary) {
    const cleaned = stripHtml(rawSummary);
    const titleNorm = String(i.title || "").replace(/\s+/g, " ").trim();
    const cleanedNorm = cleaned.replace(/\s+/g, " ").trim();
    let isDup = false;
    if (!cleanedNorm) {
      isDup = true;
    } else if (cleanedNorm === titleNorm) {
      isDup = true;
    } else {
      const [s, l] = cleanedNorm.length <= titleNorm.length ? [cleanedNorm, titleNorm] : [titleNorm, cleanedNorm];
      if (s.length >= 20 && l.indexOf(s) >= 0) {
        isDup = true;
      } else {
        // SequenceMatcher 比例相似度（>= 0.72 视为重复）
        const ratio = smRatio(cleanedNorm, titleNorm);
        if (ratio >= 0.72) isDup = true;
      }
    }
    if (!isDup) {
      const truncated = cleanedNorm.length > 140 ? cleanedNorm.slice(0, 140) + "…" : cleanedNorm;
      summaryCell = '<div class="summary" title="' + escapeHtml(cleanedNorm) + '">' + escapeHtml(truncated) + '</div>';
    }
  }

  return '<div class="item" data-url="' + escapeHtml(i.url) + '">' +
    '<div class="top">' +
    '<div class="row1">' +
    '<span class="' + numCls + '">' + num + '</span>' +
    dateCell +
    timeCell +
    srcCell +
    secCell +
    subCell +
    starsCell +
    thumbCell +
    '</div>' +
    '<a class="title' + vis + '" href="' + i.url + '" target="_blank" rel="noopener">' + escapeHtml(i.title) + '</a>' +
    '<span class="cmt-action">' + renderCommentAction(i) + '</span>' +
    '</div>' +
    '<div class="row2">' + summaryCell + '</div>' +
    '<div class="row3"><div class="comment-box">' + renderCommentBlock(i) + '</div></div>' +
    '</div>';
}

// 全局排序函数：按 published 倒序，人民日报同日用版号作 tiebreaker
// 提到顶层以便 render() 与增量路径共享
function sortByTime(arr) {
  return arr.sort((a, b) => {
    const ap = (a.published || "").replace(" ", "T");
    const bp = (b.published || "").replace(" ", "T");
    if (ap !== bp) return ap < bp ? 1 : -1;
    if (a.source_id === "people_daily" && b.source_id === "people_daily") {
      return _secNum(a.extra && a.extra.section) - _secNum(b.extra && b.extra.section);
    }
    return 0;
  });
}

// （原"源站导航"区已下线：2026-08-22 移除 src-nav 块，0 条源改为直接链接到源站）

// 空状态：列出当前 filter 覆盖源的官网入口
function renderEmptyState(activeSelection) {
  // 兼容老接口（数组）与新接口（{sources,section}）
  let ids;
  if (Array.isArray(activeSelection)) {
    ids = activeSelection;
  } else if (activeSelection && Array.isArray(activeSelection.sources)) {
    ids = activeSelection.sources;
  } else {
    ids = Object.keys(SOURCE_HOMES);
  }
  const links = ids
    .filter(id => SOURCE_HOMES[id])
    .map(id => '<a class="src-link" href="' + SOURCE_HOMES[id].url +
      '" target="_blank" rel="noopener">' + escapeHtml(SOURCE_HOMES[id].name) +
      ' <span class="arrow">↗</span></a>')
    .join("");
  return '<div class="empty">该过滤条件下暂无收录' +
    (links ? '<div class="empty-hint">可直接访问源站查看：</div><div class="src-links">' + links + '</div>' : '') +
    '</div>';
}

// ===== 渲染 items 列表区（独立容器，可独立重建）=====
function renderItemsArea(visibleItems, activeSelection) {
  const container = document.getElementById("items");
  if (!container) return;
  if (!visibleItems.length) {
    container.innerHTML = renderEmptyState(activeSelection);
    return;
  }
  const sorted = visibleItems.slice();
  sortByTime(sorted);
  const total = sorted.length;
  // 编号规则：人民日报按版号正序（顶层 1 → 底部 total；与新华社播报节奏一致），
  //          其余源保持倒序（最新顶到底，total→1）
  container.innerHTML = sorted.map((i, idx) => renderItemRow(
    i,
    (i.source_id === "people_daily") ? (idx + 1) : (total - idx),
    SOURCE_DISPLAY[i.source_id] || i.source_name
  )).join("");
}

function render(nowStr) {
  const list = document.getElementById("list");
  for (const i of cachedItems) itemByUrl[i.url] = i;
  let visibleItems = cachedItems.slice();
  // 过滤逻辑：精确匹配 (sources, section)
  if (activeSources) {
    const srcSet = new Set(activeSources);
    visibleItems = visibleItems.filter(i => {
      if (!srcSet.has(i.source_id)) return false;
      if (activeSection != null) {
        const sec = (i.extra && i.extra.section) || "";
        if (sec !== activeSection) return false;
      }
      return true;
    });
  }

  // ===== attr-filter：首次完整构建，后续只更新数字 =====
  if (!document.getElementById("attrfilter")) {
    // 首次：把 attr-filter + nav + items 三个分区一次性挂到 #list
    list.innerHTML =
      renderAttrFilter(cachedItems.slice(), FILTER_GROUPS) +
      '<div id="items"></div>';
    bindAttrFilter();
    bindViewSwitcher();
  } else {
    // 增量：attr-filter 的所有节点结构都保留，只刷 .pcnt 数字 + .zero 灰化 + 重建动态 PD 子行
    updateAttrFilterCounts(cachedItems);
  }

  // items 区独立管理
  renderItemsArea(visibleItems, activeSources ? { sources: activeSources, section: activeSection } : null);
}

const IS_STATIC = !!window.__STATIC__;

// ===== 「我的点评」功能（运营者人工撰写，存 DB，区别于抓取内容）=====
// url -> item 映射，供点评编辑时取 source_id 与回填
let itemByUrl = {};
// 静态构建（GitHub Pages）无后端，标记只读：禁用写/编辑，点击给提示
const READONLY = !!window.__STATIC__;

// 找到对应 .item 元素（避免 CSS 属性选择器对特殊字符 URL 的转义问题）
function findItemEl(url) {
  for (const el of document.querySelectorAll(".item")) {
    if (el.dataset.url === url) return el;
  }
  return null;
}

// 标题行右侧动作按钮：无评论=写点评，有评论=编辑（只读静态版显示「只读」标签）
function renderCommentAction(it) {
  if (READONLY) return '<span class="cmt-readonly">只读</span>';
  const c = (it.comment || "").trim();
  if (c) {
    return '<button class="cmt-btn" data-act="edit" data-url="' + escapeHtml(it.url) + '">编辑</button>';
  }
  return '<button class="cmt-btn cmt-add" data-act="add" data-url="' + escapeHtml(it.url) + '">➕ 写点评</button>';
}

// 渲染单条点评展示块（有则显示，无则留空；编辑框将注入 .comment-box）
function renderCommentBlock(it) {
  const c = (it.comment || "").trim();
  if (!c) return "";
  const editBtn = READONLY ? "" :
    '<button class="cmt-btn" data-act="edit" data-url="' + escapeHtml(it.url) + '">编辑</button>';
  return '<div class="comment-view">' +
    '<span class="cmt-label">点评</span>' +
    '<span class="cmt-text">' + escapeHtml(c) + '</span>' +
    editBtn +
    '</div>';
}

// 就地展开编辑框
function openCommentEditor(url) {
  if (READONLY) {
    const el = findItemEl(url);
    if (!el) return;
    const box = el.querySelector(".comment-box");
    if (!box) return;
    box.innerHTML = '<div class="cmt-readonly-hint">当前为<strong>只读静态站点</strong>，无法保存点评。请本地运行 <code>python live_server.py</code> 后访问 <b>http://127.0.0.1:8765</b>（带后端）写入。</div>';
    return;
  }
  const it = itemByUrl[url];
  if (!it) return;
  const el = findItemEl(url);
  if (!el) return;
  const box = el.querySelector(".comment-box");
  if (!box) return;
  const cur = it.comment || "";
  box.innerHTML =
    '<textarea class="cmt-input" rows="2" placeholder="写下你的点评（摘要 + 链接 + 你的判断，别搬运正文）…">' +
    escapeHtml(cur) + '</textarea>' +
    '<div class="cmt-actions">' +
    '<button class="cmt-save" data-act="save" data-url="' + escapeHtml(url) + '">保存</button>' +
    '<button class="cmt-cancel" data-act="cancel" data-url="' + escapeHtml(url) + '">取消</button>' +
    '</div>';
  const ta = box.querySelector("textarea");
  if (ta) ta.focus();
}

// 保存点评到服务端
async function saveComment(url) {
  const it = itemByUrl[url];
  const el = findItemEl(url);
  if (!it || !el) return;
  const box = el.querySelector(".comment-box");
  const ta = box.querySelector("textarea");
  const text = ta ? ta.value : "";
  try {
    const res = await fetch("/api/comment", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_id: it.source_id, url: url, comment: text }),
    });
    let data = {};
    try { data = await res.json(); } catch (e) { data = {}; }
    if (!res.ok || !data.ok) {
      let msg = data.error || "";
      if (!msg) {
        if (res.status === 404) msg = "当前站点没有后端，无法保存。请在本地运行 live_server.py 后访问 http://127.0.0.1:8765 写入。";
        else msg = "保存失败（HTTP " + res.status + "）";
      }
      throw new Error(msg);
    }
    it.comment = text;
    box.innerHTML = renderCommentBlock(it);
    const action = el.querySelector(".cmt-action");
    if (action) action.innerHTML = renderCommentAction(it);
  } catch (e) {
    alert("点评保存失败：" + (e.message || e));
  }
}

// 「我的点评」按钮事件委托（写 / 编辑 / 保存 / 取消）
document.getElementById("list").addEventListener("click", (e) => {
  const btn = e.target.closest("[data-act]");
  if (!btn) return;
  const act = btn.dataset.act, url = btn.dataset.url;
  if (act === "add" || act === "edit") openCommentEditor(url);
  else if (act === "save") saveComment(url);
  else if (act === "cancel") {
    const it = itemByUrl[url];
    const el = findItemEl(url);
    if (el && it) {
      const box = el.querySelector(".comment-box");
      if (box) box.innerHTML = renderCommentBlock(it);
      const action = el.querySelector(".cmt-action");
      if (action) action.innerHTML = renderCommentAction(it);
    }
  }
});

function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function renderTabs() {
  // 单一分类"科技"，不需要 tab 切换：清空 nav 即可
  document.getElementById("tabs").innerHTML = "";
}

let lastNow = "";
function refreshView() { renderTabs(); render(lastNow); }

let seenUrls = new Set();
let sseConnected = false;
let lastStatus = { sources: {} };

// 访问过的链接：localStorage 持久化 + 立即给点击的 <a> 加 visited class（不等下次 render）
let visitedSet = new Set();
try { visitedSet = new Set(JSON.parse(localStorage.getItem("news_visited") || "[]")); } catch (e) {}
function markVisited(url) {
  if (!url) return;
  visitedSet.add(url);
  try { localStorage.setItem("news_visited", JSON.stringify([...visitedSet].slice(-800))); } catch (e) {}
}
document.getElementById("list").addEventListener("click", (e) => {
  const a = e.target.closest("a");
  if (a && a.href) {
    markVisited(a.href);
    a.classList.add("visited");  // 立刻生效
  }
});

function applyStatus() {
  if (IS_STATIC) {
    const sb = document.getElementById("statusbar");
    if (sb) sb.textContent =
      "● 静态生成 · " + day + " 今日 " + cachedItems.length + " 条" +
      (lastNow ? " 更新于 " + lastNow.slice(11, 19) : "");
    return;
  }
  const st = lastStatus;
  const okCnt = Object.values(st.sources || {}).filter(s => s.ok).length;
  const totalCnt = Object.keys(st.sources || {}).length;
  const sseTxt = sseConnected ? "● 实时推送已连接" : "○ 推送未连接（轮询兜底）";
  document.getElementById("statusbar").textContent =
    sseTxt + " " + day + " 今日 " + cachedItems.length + " 条 源 " + okCnt + "/" + totalCnt +
    " 正常 下次高频 " + Math.ceil((st.next_high_in || 0) / 60) + " 分钟后" +
    (lastNow ? " 更新于 " + lastNow.slice(11, 19) : "");
}

async function loadInitial() {
  if (IS_STATIC) {
    cachedItems = window.__ITEMS__ || [];
    day = window.__DAY__ || "";
    lastNow = window.__NOW__ || "";
    lastStatus = { sources: {} };
    for (const i of cachedItems) seenUrls.add(i.url);
    applyStatus(); refreshView();
    return;
  }
  try {
    const [itemsRes, statusRes] = await Promise.all([fetch("/api/items"), fetch("/api/status")]);
    const data = await itemsRes.json();
    const st = await statusRes.json();
    cachedItems = data.items; day = data.day; lastNow = data.now; lastStatus = st;
    for (const i of cachedItems) seenUrls.add(i.url);
    applyStatus(); refreshView();
  } catch (e) {
    document.getElementById("statusbar").textContent = "加载失败：" + e.message;
    console.error("loadInitial:", e);
  }
}

// 把所有顶层未捕获错误暴露在状态栏（方便排查"加载不出来"）
window.addEventListener("error", (e) => {
  const sb = document.getElementById("statusbar");
  if (sb) sb.textContent = "JS 错误：" + (e.message || e.error || "未知");
});

if (!IS_STATIC) {
  // 每 30 秒轻量刷新状态栏（不重新拉取列表，保持顺序）
  setInterval(async () => {
    try { lastStatus = await (await fetch("/api/status")).json(); applyStatus(); } catch (e) {}
  }, 30000);

  // 每 60 秒刷新一次列表（让相对时间"X 分钟前"保持最新）
  setInterval(() => { if (cachedItems.length) refreshView(); }, 60000);

  // SSE 实时推送：新文章入库即上屏
  const es = new EventSource("/api/stream");
  es.onopen = () => { sseConnected = true; applyStatus(); };
  es.onerror = () => { sseConnected = false; applyStatus(); };
  es.onmessage = (e) => {
    try {
      const item = JSON.parse(e.data);
      if (seenUrls.has(item.url)) return;
      seenUrls.add(item.url);
      cachedItems.unshift(item);
      itemByUrl[item.url] = item;
      lastNow = item.first_seen;
      applyStatus(); refreshView();
    } catch (err) {}
  };
}

// 行式布局无绝对定位，窗口 resize 自动重排，不需要重布局代码

loadInitial();
</script>
</body>
</html>
"""

def _view_switcher_html(mode: str, *, full_href: str = "#", simple_href: str = "#") -> str:
    """生成 .view-switcher HTML；mode='full' 全量版高亮，mode='simple' 精简版高亮。

    full_href / simple_href 在构建静态页或 live server 路由时按"目标页 URL"直接注入，
    避免简单版本（无 JS）点击不跳转。
    """
    import html as _html
    full_cls = "active" if mode == "full" else ""
    simple_cls = "active" if mode == "simple" else ""
    full_href_e = _html.escape(full_href, quote=True)
    simple_href_e = _html.escape(simple_href, quote=True)
    return (
        '<div class="view-switcher" id="viewswitcher">'
        '<span class="switcher-label">查看模式</span>'
        '<span class="switcher-tabs">'
        f'<a data-mode="full" class="{full_cls}" href="{full_href_e}">全量版</a>'
        f'<a data-mode="simple" class="{simple_cls}" href="{simple_href_e}">精简版</a>'
        '</span>'
        '</div>'
    )


# 注入各源官网入口：{source_id: {name, url}}，供前端空状态直达链接使用
# 合并：抓取源 SOURCES + 目录式条目 DIRECTORY_SOURCES（无爬虫，仅展示入口）
_dash_homes = {s.id: {"name": s.name, "url": s.url} for s in SOURCES if s.url}
_dash_homes.update(all_directory_homes())
DASHBOARD_HTML = DASHBOARD_HTML.replace(
    "__SOURCE_HOMES__",
    json.dumps(_dash_homes, ensure_ascii=False),
)
# 实时版 + 静态版共用同一份 DASHBOARD_HTML 模板；默认全量版高亮
# 静态导出 docs/index.html 时再次覆写 href=simple.html（指向同级文件）
DASHBOARD_HTML = DASHBOARD_HTML.replace(
    "__VIEW_SWITCHER__",
    _view_switcher_html("full", full_href="#", simple_href="simple.html"),
)


# ---------------------------------------------------------------- 静态导出（GitHub Pages 等）

def _uncomment_filter_groups(html: str) -> str:
    """静态版展示全部分类：把 FILTER_GROUPS 里被注释掉的「科技」组恢复出来。"""
    pat = re.compile(r"(  // ===== 科技（暂时隐藏[^\n]*\n)(.*?)(\n  // \},)", re.S)
    m = pat.search(html)
    if not m:
        return html
    body = m.group(2)
    uncommented = "\n".join(
        (re.sub(r"^  // ?", "", line) if line.startswith("  //") else line)
        for line in body.split("\n")
    )
    return html[: m.start()] + m.group(1) + uncommented + "\n  }," + html[m.end():]


def build_static_html(items: list[dict], day_str: str) -> str:
    """生成自包含静态看板 HTML：内嵌当天条目数据，禁用 SSE/轮询。"""
    now_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    # DASHBOARD_HTML 为只读常量（字符串不可变，replace 返回新串，不污染实时服务）
    html = DASHBOARD_HTML
    embed = (
        '<script>window.__STATIC__=true;'
        "window.__ITEMS__=" + json.dumps(items, ensure_ascii=False) + ";"
        "window.__DAY__=" + json.dumps(day_str) + ";"
        "window.__NOW__=" + json.dumps(now_str) + ";</script>"
    )
    html = html.replace("<body>", "<body>\n" + embed, 1)
    # 静态托管面向公网，展示全部分类（科技/国际亦可用）
    html = _uncomment_filter_groups(html)
    return html


def build_static(output_path: str) -> int:
    """收集当天全源 → 读 SQLite → 生成静态 index.html。"""
    setup_logging()
    logger.info("静态导出模式：收集当天全源并生成 %s", output_path)
    collector = Collector(high_interval=10**9, low_interval=10**9)
    collector._run_tier("low")
    collector._run_tier("high")
    today_str = today_bj()
    conn = storage.get_conn()
    try:
        items = storage.get_items(conn, today_str)
    finally:
        conn.close()
    html = build_static_html(items, today_str)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("静态看板已生成: %s (%d 条)", output_path, len(items))
    print(f"\n✓ 静态看板已生成: {output_path}（{len(items)} 条新闻）\n")
    return 0


# ---------------------------------------------------------------- 简易版（3 块精要，独立 docs/simple.html）

# 简易版 7 大品牌源 ID 集合（ChatGPT/Gemini/Anthropic/智谱/DeepSeek/千问/Moonshot/HF）
SIMPLE_TECH_IDS = {"hf_papers", "openai_news", "openai_research", "deepmind", "anthropic",
                   "zhipu", "deepseek", "qwen", "moonshot"}

# 科技段分组展示：每个品牌一个子标题；源 id → 品牌归属（与 FILTER_GROUPS 科技组一致）
SIMPLE_TECH_GROUPS = [
    ("ChatGPT / OpenAI", ["openai_news", "openai_research"]),
    ("Gemini / Google", ["deepmind", "google_research"]),
    ("Anthropic", ["anthropic"]),
    ("智谱 GLM", ["zhipu"]),
    ("DeepSeek", ["deepseek"]),
    ("通义千问", ["qwen"]),
    ("月之暗面 Moonshot", ["moonshot"]),
    ("Hugging Face Papers", ["hf_papers"]),
]

# 简易版模板（静态 HTML，无 JS、无 SSE；数据在构建时直接渲染为 HTML）
SIMPLE_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>新闻简易版 · __DAY__</title>
<style>
body { font: 14px/1.6 -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
       max-width: 800px; margin: 24px auto; padding: 0 16px 80px; color: #1e293b; }
header { padding: 12px 0 18px; border-bottom: 1px solid #f1f5f9; margin-bottom: 24px; }
h1 { font-size: 22px; margin: 0 0 4px; }
.meta { color: #64748b; font-size: 12.5px; }
.meta a { color: #64748b; }
section { margin: 28px 0; }
h2 { font-size: 15.5px; margin: 0 0 12px; padding-bottom: 6px; border-bottom: 2px solid #f1f5f9; }
h2 .count { color: #94a3b8; font-weight: 400; margin-left: 8px; font-size: 13px; }
h2 .tag { display: inline-block; font-size: 11px; color: #64748b; background: #f1f5f9;
          border-radius: 4px; padding: 1px 6px; margin-left: 6px; font-weight: 500; }
ol { padding-left: 22px; margin: 0; }
li { margin: 10px 0; line-height: 1.5; }
li .time { color: #94a3b8; font-size: 11.5px; margin-left: 8px; white-space: nowrap; }
li .src { color: #64748b; font-size: 11.5px; margin-left: 6px;
          background: #f8fafc; border-radius: 3px; padding: 1px 5px; }
li .sec { color: #94a3b8; font-size: 11px; margin-left: 6px; }
li .thumb { max-width: 56px; max-height: 36px; margin-left: 8px; vertical-align: middle;
            border-radius: 4px; border: 1px solid #f1f5f9; }
li a { color: #1e293b; text-decoration: none; font-weight: 500; }
li a:hover { color: #ff7d39; }
.empty-msg { color: #94a3b8; font-size: 13px; padding: 14px 0 6px; }
/* 科技段按品牌分组：每个品牌一个小标题 + 该品牌文章列表 */
.brand-block { margin: 18px 0 4px; }
.brand-block h3 { font-size: 14px; margin: 0 0 8px; padding: 7px 12px; color: #1e293b;
                  background: linear-gradient(180deg, #ffffff, #f7f9fc); border: 1px solid #e7ecf5;
                  border-left: 3px solid #ff7d39; border-radius: 8px; display: flex; align-items: center; gap: 8px; }
.brand-block h3 .bcnt { font-size: 11px; font-weight: 700; color: #fff; background: #ff7d39;
                        border-radius: 999px; padding: 1px 8px; min-width: 20px; text-align: center; }
.brand-block ol { padding-left: 22px; margin: 0; }
.brand-block li { margin: 9px 0; }
/* 人民日报要闻：按版面号分组（01版：要闻 在上，07版：要闻 在下） */
.pd-block { margin: 16px 0 4px; }
.pd-block h4 { font-size: 13.5px; margin: 0 0 8px; padding: 6px 12px; color: #1e293b;
               background: linear-gradient(180deg, #ffffff, #fbfcfd); border: 1px solid #f0e2e2;
               border-left: 3px solid #c0392b; border-radius: 8px; display: flex; align-items: center; gap: 8px; }
.pd-block h4 .bcnt { font-size: 11px; font-weight: 700; color: #fff; background: #c0392b;
                     border-radius: 999px; padding: 1px 8px; min-width: 20px; text-align: center; }
.pd-block ol { padding-left: 22px; margin: 0; }
.pd-block li { margin: 9px 0; }
/* 简易版 view-switcher：与主看板一致风格（橙色胶囊，左label右tabs）
   全量版 view-switcher 在 .attr-filter 顶部绝对定位（label-left tabs-right）；
   简易版无 attr-filter 容器，靠自身 style 模拟等同布局 */
.view-switcher { display: flex; align-items: center; justify-content: space-between;
                gap: 10px; pointer-events: auto;
                margin-top: 12px; padding: 6px 10px;
                background: #fff; border: 1px solid #e7ecf5; border-radius: 10px; }
.view-switcher .switcher-label { font-size: 11.5px; color: #94a3b8; font-weight: 600;
                                  letter-spacing: .4px; }
.view-switcher .switcher-tabs { display: inline-flex; background: #f1f5f9;
                                 border: 1px solid #e3e8f2; border-radius: 9px;
                                 padding: 3px; gap: 2px; }
.view-switcher a { display: inline-block; padding: 5px 14px; border-radius: 6px;
                    font-size: 12.5px; color: #475569; text-decoration: none; font-weight: 600;
                    transition: all .14s ease; white-space: nowrap; line-height: 1.3; }
.view-switcher a:hover:not(.active) { color: #ff7d39; background: #fff8f3; }
.view-switcher a.active { background: linear-gradient(135deg, #ff7d39 0%, #ff9558 100%);
                           color: #fff; box-shadow: 0 2px 5px rgba(255,125,57,.30); }
footer { color: #94a3b8; font-size: 11.5px; text-align: center;
         margin-top: 56px; padding-top: 16px; border-top: 1px solid #f1f5f9; }
footer a { color: #94a3b8; }
/* 我的点评：精简版差异化标记，橙色左条 + 暖底，区别于官方标题 */
.my-note { margin: 5px 0 2px; padding: 5px 10px; background: #fff8f3;
          border-left: 3px solid #ff7d39; border-radius: 0 6px 6px 0;
          font-size: 12.5px; line-height: 1.5; color: #8a4b1f; }
.my-note .note-label { font-weight: 700; color: #ff7d39; margin-right: 5px; }
</style>
</head>
<body>
<header>
  <h1>📰 新闻简易版</h1>
  <div class="meta">__DAY__ · 静态快照 · 每小时更新 · <a href="./">主看板</a></div>
  __VIEW_SWITCHER__
</header>

<section>
  <h2>人民日报 · 要闻 <span class="tag">top headlines</span><span class="count">__N1__ 条</span></h2>
  __SEC1__
</section>

<section>
  <h2>Reuters 路透社 <span class="tag">world / business</span><span class="count">__N2__ 条</span></h2>
  __SEC2__
</section>

<section>
  <h2>科技 · AI 品牌 <span class="tag">ChatGPT / Gemini / Anthropic / 智谱 / DeepSeek / 千问 / 月之暗面 / HF</span><span class="count">__N3__ 条</span></h2>
  __SEC3__
</section>

<footer>
  简易版 · GitHub Actions 每小时构建 · <a href="./">返回主看板</a>
</footer>
</body>
</html>
"""


def _escape(s) -> str:
    """统一 escape；容忍 None/非字符串。"""
    if s is None:
        return ""
    return html_module.escape(str(s))


def _render_simple_section(items: list[dict], *, with_thumb: bool = False, show_sec: bool = True) -> str:
    """渲染简易版单个分区为 <ol><li>...</li></ol>；空集合返回提示。
    2026-08-28 调整：li 内顺序 = thumb? + time + src + sec + <a>title</a> + note，
    与全量版「时间 媒体 板块 标题」保持一致（标题由最左移到最后）。"""
    if not items:
        return '<div class="empty-msg">今日暂无收录</div>'
    lis: list[str] = []
    for it in items:
        title = _escape(it.get("title", "")).strip()
        if not title:
            continue
        url = _escape(it.get("url", "#"))
        pub = (it.get("published") or "").replace("T", " ")
        time_str = _escape(pub[-16:]) if pub else ""
        src = _escape(it.get("source_name") or it.get("source_id") or "")
        extra = it.get("extra") or {}
        sec_label = extra.get("section") or ""
        sec_html = f'<span class="sec">{_escape(sec_label)}</span>' if (show_sec and sec_label) else ""
        thumb_html = ""
        if with_thumb and extra.get("image"):
            thumb_html = f'<img class="thumb" src="{_escape(extra["image"])}" loading="lazy" alt="">'
        comment = (it.get("comment") or "").strip()
        note_html = ""
        if comment:
            note = _escape(comment).replace("\n", "<br>")
            note_html = f'<div class="my-note"><span class="note-label">我的点评</span>{note}</div>'
        lis.append(
            '<li>'
            f'{thumb_html}'
            f'<span class="time">{time_str}</span>'
            f'<span class="src">{src}</span>'
            f'{sec_html}'
            f'<a href="{url}" target="_blank" rel="noopener">{title}</a>'
            f'{note_html}'
            '</li>'
        )
    return f'<ol>{"".join(lis)}</ol>'


def _render_simple_tech(items: list[dict]) -> str:
    """科技段按品牌分组渲染：每个品牌一个子标题 (h3) + 该品牌文章列表。
    仅展示有内容的品牌，避免空组噪音；品牌顺序由 SIMPLE_TECH_GROUPS 决定。
    """
    if not items:
        return '<div class="empty-msg">今日暂无收录</div>'
    blocks: list[str] = []
    for label, ids in SIMPLE_TECH_GROUPS:
        subset = [it for it in items if it.get("source_id", "") in ids]
        if not subset:
            continue
        subset.sort(key=lambda x: (x.get("published") or ""), reverse=True)
        blocks.append(
            '<div class="brand-block">'
            f'<h3>{_escape(label)}<span class="bcnt">{len(subset)}</span></h3>'
            + _render_simple_section(subset)
            + '</div>'
        )
    return "".join(blocks) if blocks else '<div class="empty-msg">今日暂无收录</div>'


def _sec_num(sec_name: str) -> int:
    """从 '01版：要闻' 提取版号 1；解析失败返回 999（沉到最底）。"""
    m = re.match(r"(\d+)\s*版", sec_name or "")
    return int(m.group(1)) if m else 999


def _render_simple_people(items: list[dict]) -> str:
    """人民日报要闻：按版面号（01→07）从上到下分组，每组一个小标题 + 该版文章列表。
    组内按发布时间倒序；版面按版号升序（01 在上、07 在下），满足"从上到下排列"诉求。
    """
    if not items:
        return '<div class="empty-msg">今日暂无收录</div>'
    groups: dict[int, list[dict]] = {}
    for it in items:
        sec = (it.get("extra") or {}).get("section") or ""
        groups.setdefault(_sec_num(sec), []).append(it)
    blocks: list[str] = []
    for num in sorted(groups.keys()):
        subset = groups[num]
        subset.sort(key=lambda x: (x.get("published") or ""), reverse=True)
        sec_label = (subset[0].get("extra") or {}).get("section") or f"{num:02d}版：要闻"
        blocks.append(
            '<div class="pd-block">'
            f'<h4>{_escape(sec_label)}<span class="bcnt">{len(subset)}</span></h4>'
            + _render_simple_section(subset, show_sec=False)
            + '</div>'
        )
    return "".join(blocks) if blocks else '<div class="empty-msg">今日暂无收录</div>'


def _filter_simple(items: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """按 3 块精要切分条目：
    - 人民日报 仅 要闻版面（section 以 "要闻" 结尾）
    - Reuters 仅带 extra.image 的（2026-08-29 用户要求：精简版只收集带图片的新闻，渲染仍不带图）
    - 科技 仅 SIMPLE_TECH_IDS 中的 6 大品牌
    """
    sec1: list[dict] = []  # 人民日报 要闻
    sec2: list[dict] = []  # Reuters 仅带图（extra.image 存在性过滤）
    sec3: list[dict] = []  # 科技 6 品牌
    for it in items:
        sid = it.get("source_id", "")
        extra = it.get("extra") or {}
        if sid == "people_daily":
            sec_name = extra.get("section") or ""
            # 版面名形如 "01版：要闻" / "02版：要闻" —— 过滤出所有"要闻"版面
            if sec_name.endswith("要闻") or sec_name == "要闻":
                sec1.append(it)
        elif sid == "reuters":
            # 2026-08-29：精简版只收带图片的（extra.image 存在）；scraper 不变（全量仍收所有 World/Business）
            if extra.get("image"):
                sec2.append(it)
        elif sid in SIMPLE_TECH_IDS:
            sec3.append(it)
    # 按发布时间倒序
    for lst in (sec1, sec2, sec3):
        lst.sort(key=lambda x: (x.get("published") or ""), reverse=True)
    return sec1, sec2, sec3


def build_simple_html(items: list[dict], day_str: str) -> str:
    """生成简易版 HTML：3 块精要（人民日报要闻 / Reuters 配图版 / 科技 6 品牌）。
    2026-08-28：Reuters section 不再带图片（with_thumb=False，简版更紧凑）。
    2026-08-29：Reuters 仅收带 extra.image 的（仅显示配图新闻，渲染仍不带图）。"""
    sec1, sec2, sec3 = _filter_simple(items)
    return (
        SIMPLE_HTML
        .replace("__DAY__", _escape(day_str))
        .replace("__N1__", str(len(sec1)))
        .replace("__N2__", str(len(sec2)))
        .replace("__N3__", str(len(sec3)))
        .replace("__SEC1__", _render_simple_people(sec1))
        .replace("__SEC2__", _render_simple_section(sec2, with_thumb=False))
        .replace("__SEC3__", _render_simple_tech(sec3))
        .replace("__VIEW_SWITCHER__",
                 _view_switcher_html("simple", full_href="./", simple_href="#"))
    )


def build_simple(output_path: str) -> int:
    """收集当天全源 → 读 SQLite → 生成简易版 simple.html。"""
    setup_logging()
    logger.info("简易版导出：收集当天全源并生成 %s", output_path)
    collector = Collector(high_interval=10**9, low_interval=10**9)
    collector._run_tier("low")
    collector._run_tier("high")
    today_str = today_bj()
    conn = storage.get_conn()
    try:
        items = storage.get_items(conn, today_str)
    finally:
        conn.close()
    sec1, sec2, sec3 = _filter_simple(items)
    html = build_simple_html(items, today_str)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("简易版已生成: %s（人民日报要闻=%d / Reuters配图版=%d / 科技6品牌=%d）",
                output_path, len(sec1), len(sec2), len(sec3))
    print(f"\n✓ 简易版已生成: {output_path}")
    print(f"  - 人民日报·要闻：{len(sec1)} 条")
    print(f"  - Reuters·配图版（仅收带 extra.image 的，渲染仍不带图）：{len(sec2)} 条")
    print(f"  - 科技·6 大品牌：{len(sec3)} 条\n")
    return 0


# ---------------------------------------------------------------- 主入口


def setup_logging() -> None:
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.FileHandler(log_dir / f"live_{today_bj()}.log",
                                encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> int:
    global COLLECTOR
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--high-interval", type=int, default=120,
                    help="高频源轮询间隔秒数（默认 120 = 2 分钟）")
    ap.add_argument("--low-interval", type=int, default=21600,
                    help="低频源轮询间隔秒数（默认 21600 = 6 小时）")
    ap.add_argument("--build-static", metavar="OUTPUT", default=None,
                    help="生成静态看板 HTML 到 OUTPUT（如 docs/index.html）并退出，"
                         "用于 GitHub Pages 等静态托管")
    ap.add_argument("--build-simple", metavar="OUTPUT", default=None,
                    help="生成简易版 HTML 到 OUTPUT（如 docs/simple.html）并退出，"
                         "3 块精要：人民日报要闻 / Reuters有图 / 科技6大品牌")
    args = ap.parse_args()

    if args.build_static:
        return build_static(args.build_static)
    if args.build_simple:
        return build_simple(args.build_simple)

    setup_logging()
    logger.info("实时收集启动：高频每 %d 分钟 / 低频每 %.1f 小时 / 端口 %d",
                args.high_interval // 60, args.low_interval / 3600, args.port)

    COLLECTOR = Collector(args.high_interval, args.low_interval)
    t = threading.Thread(target=COLLECTOR.loop, daemon=True, name="collector")
    t.start()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    logger.info("看板地址: http://127.0.0.1:%d/", args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("收到中断，退出。")
        COLLECTOR.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

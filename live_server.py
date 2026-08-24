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
        """Serve 简易版 HTML（人民日报要闻 / Reuters 有图 / 科技 6 品牌）。"""
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
.item { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 8px 14px; margin-bottom: 6px; }
/* 单行布局：flex 让标题列自动占满剩余宽度，所有元素颜色统一为 var(--text)（用户要求"颜色保持一致"） */
.item .row1 { display: flex; align-items: center; gap: 10px; font-size: 13.5px; line-height: 1.5; color: var(--text); flex-wrap: nowrap; }
.item .row1 > * { white-space: nowrap; flex-shrink: 0; }
.item .row1 .num { color: var(--text); font-variant-numeric: tabular-nums; font-weight: 500; min-width: 26px; text-align: right; }
.item .row1 .pub { color: var(--text); font-variant-numeric: tabular-nums; }
.item .row1 .rel { color: var(--text); font-weight: 500; font-variant-numeric: tabular-nums; }
.item .row1 .title { color: var(--text); text-decoration: none; font-weight: 500; font-size: 13.5px; flex: 1 1 0; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.item .row1 .title:hover { color: var(--accent); text-decoration: underline; }
.item .row1 .title.visited { color: #9ca3af; }
.item .row1 .title.visited:hover { color: var(--accent); text-decoration: underline; }
/* 全部 tab 扁平列表里的来源小标签：与其他元素同色（去掉灰底，避免颜色不一致） */
.src-inline { font-size: 13.5px; color: var(--text); font-weight: 500; }
/* 国际/中国 tab 的来源分组头：logo + 来源名 + 条数 三者同字号同字重同色 */
.src-group { margin-bottom: 16px; }
.src-head { display: flex; align-items: center; gap: 8px; margin: 4px 0 8px; padding-left: 2px; }
.src-head .src-name { font-size: 14px; font-weight: 700; color: var(--text); }
.src-head .src-cnt { font-size: 14px; font-weight: 700; color: var(--text); }
.src-head .src-pill { font-size: 14px; font-weight: 700; color: var(--text); background: transparent; }
/* section / subcategory / upvotes 徽章：去掉彩色背景，文字与标题同色同字号（用户要求"字体和颜色保持一致"） */
.badge { color: var(--text); font-weight: 500; font-size: 13.5px; padding: 0; background: transparent; border-radius: 0; }
.badge.sec { color: var(--text); }
.badge.red { color: var(--text); }
.badge.intl { color: var(--text); }
.badge.ups { color: var(--text); }
.src-logo { height: 22px; width: auto; max-width: 110px; vertical-align: -5px; margin-right: 2px; }
/* 人民日报 logo 自带红框，在白卡片上太刺眼 → 缩小 + 加白色 padding + 轻边框 */
.src-logo.pd { height: 24px; vertical-align: -7px; padding: 2px 6px; background: #fff; border: 1px solid #f0e2e2; border-radius: 4px; box-sizing: content-box; }
/* MIT 等深色 logo 在白底上可读性差 → 保留原色（自带背景） */
.src-logo.mit { height: 22px; padding: 2px 6px; background: #000; border-radius: 4px; box-sizing: content-box; }
/* Reuters 红底白字 logo */
.src-logo.reuters { height: 22px; padding: 2px 6px; background: #fff; border: 1px solid #f5e0d8; border-radius: 4px; box-sizing: content-box; }
.rel { font-variant-numeric: tabular-nums; }
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
.attr-rows { display: flex; flex-direction: column; gap: 12px; }
.attr-row { display: flex; align-items: flex-start; gap: 16px; }
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
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/.exec(published);
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

// ===== 属性过滤器（树状分支：单一顶层"科技"，下挂国际/中国/AI 三个一级分支）=====
// 顶层 3 个一级分类：中国 / 国际 / 科技
// 国际 = 路透社；科技 = 全部 AI 实验室 + HF Daily Papers + 开源 + 政策/学术
// ===== 属性过滤器（2026-08-22 用户全量收集版）=====
const FILTER_GROUPS = [
  // ===== 中国 =====
  {
    key: "cn", label: "中国",
    root: { label: "中国", match: ["people_daily","gov_policy","miit","cac","ndrc","cast","ia_cas","pku_ai","tsinghua_ai","baai","caai","cctv"] },
    branches: [
      { label: "人民日报", match: ["people_daily"] },
      { label: "中央电视台", match: ["cctv"],
        leaves: [
          { label: "CCTV 新闻联播", match: ["cctv"] },
        ]},
      { label: "政府机构", match: ["gov_policy","miit","cac","ndrc","cast"],
        leaves: [
          { label: "国务院政策文件库", match: ["gov_policy"] },
          { label: "工信部", match: ["miit"] },
          { label: "国家网信办", match: ["cac"] },
          { label: "发改委", match: ["ndrc"] },
          { label: "中国科协", match: ["cast"] },
        ]},
      { label: "学术机构", match: ["ia_cas","pku_ai","tsinghua_ai","baai","caai"],
        leaves: [
          { label: "中科院自动化所", match: ["ia_cas"] },
          { label: "北大 AI", match: ["pku_ai"] },
          { label: "清华 AI", match: ["tsinghua_ai"] },
          { label: "智源研究院", match: ["baai"] },
          { label: "中国 AI 学会", match: ["caai"] },
        ]},
    ],
  },
  // ===== 国际（路透社）=====
  {
    key: "intl", label: "国际",
    root: { label: "国际", match: ["reuters"] },
    branches: [
      { label: "路透社", match: ["reuters"] },
    ],
  },
  // ===== 科技（2026-08-22：arXiv + 模型榜单去除，论文改为 HF Daily Papers）=====
  {
    key: "tech", label: "科技",
    root: { label: "科技", match: ["hf_papers","openai_news","openai_research","anthropic","deepmind","google_research","meta_ai","ms_ai","nvidia_ai","mit_tech_review","github_trending","hf_blog"] },
    branches: [
      { label: "论文 · HF Daily Papers", match: ["hf_papers"] },
      { label: "AI 研究", match: ["openai_news","openai_research","anthropic","deepmind","google_research","meta_ai","ms_ai","nvidia_ai","mit_tech_review","github_trending","hf_blog"],
        leaves: [
          { label: "OpenAI", match: ["openai_news","openai_research"] },
          { label: "Anthropic", match: ["anthropic"] },
          { label: "Google DeepMind", match: ["deepmind","google_research"] },
          { label: "Meta AI", match: ["meta_ai"] },
          { label: "Microsoft AI", match: ["ms_ai"] },
          { label: "NVIDIA AI", match: ["nvidia_ai"] },
          { label: "MIT Tech Review", match: ["mit_tech_review"] },
          { label: "GitHub Trending", match: ["github_trending"] },
          { label: "HF Blog", match: ["hf_blog"] },
        ]},
    ],
  },
];
// 真实爬虫源 id
const REAL_SOURCE_IDS = ["reuters", "people_daily", "mit_tech_review", "openai_news",
  "openai_research", "anthropic", "deepmind", "google_research", "meta_ai", "ms_ai",
  "nvidia_ai", "hf_blog", "hf_papers", "github_trending",
  "gov_policy", "miit", "cac", "ndrc", "cast", "ia_cas", "pku_ai", "tsinghua_ai",
  "baai", "caai", "cctv"];

// 当前 sub-filter 选中的 source_id 集合（null = 不过滤，显示全部）
// 用稳定字符串比较："a,b" === "a,b" 视为同一个选择，便于 toggle
let activeKey = null;            // 与 activeSourceIds 同步的字符串形式（点击历史用）
let activeSourceIds = null;      // 同上但为数组

// 把数组转成排序后的稳定字符串（用于 toggle 比较，避免 ["a","b"] vs ["b","a"] 误判）
const toKey = (ids) => ids.slice().sort().join(",");

// 默认不过滤（activeSourceIds = null）→ 看板加载即展示全部新闻（中国 + 国际 + 科技）。
// 点击任意分组的"全部" pill 缩小到该分类；再次点击已激活 pill 取消过滤回到全部。
activeSourceIds = null;
activeKey = null;

// pill 的"激活"判定：精确匹配 activeSourceIds（避免父 pill 被高亮的视觉混乱）。
// 行式布局下，每个 pill 都是"选中精确等于自己"才高亮，避免选中叶子时整行变色。
const isActive = (ids) => {
  if (!activeSourceIds) return false;
  return toKey(ids) === toKey(activeSourceIds);
};

// 工具：把一个 match 数组转成 HTML data-ids 字符串
const idsAttr = (ids) => ids.join(",");

// 工具：渲染单个 pill
// 单源 0 条 → 直接外链到源站（避免点了筛选却空列表的尴尬）；
// 其余情况 → 渲染为按钮（筛选行为）
function renderPill(label, ids, opts) {
  opts = opts || {};
  const c = opts.count;
  const isZero = (c === 0 && !opts.alwaysShow);
  const isSingleSrc = (ids.length === 1);
  // 单源 0 条：直接外链到源站
  if (isZero && isSingleSrc) {
    const h = SOURCE_HOMES[ids[0]];
    if (h && h.url) {
      const cls = "attr-pill attr-pill-link zero";
      return '<a class="' + cls + '" href="' + h.url + '" target="_blank" rel="noopener" ' +
        'data-ids="' + idsAttr(ids) + '" title="今日暂无收录，点击直达 ' + escapeHtml(h.name) + '">' +
        '<span class="plabel">' + escapeHtml(label) + '</span>' +
      '</a>';
    }
  }
  const cls = "attr-pill"
    + (opts.isAll ? " all" : "")
    + (isActive(ids) ? " active" : "")
    + (isZero ? " zero" : "");
  const cntStr = (c === undefined) ? "" : '<span class="pcnt">' + c + '</span>';
  return '<button class="' + cls + '" data-ids="' + idsAttr(ids) + '">' +
    '<span class="plabel">' + escapeHtml(label) + '</span>' +
    cntStr +
  '</button>';
}

// 工具：渲染一行（属性名 + pills）
function renderRow(label, pills, isSub) {
  return '<div class="attr-row' + (isSub ? ' sub' : '') + '">' +
    '<div class="attr-row-label">' + escapeHtml(label) + '</div>' +
    '<div class="attr-row-pills">' + pills.join("") + '</div>' +
  '</div>';
}

// ===== 行式属性过滤器（Boss 直聘风格 · 2026-08-22 重构）=====
// 层级：根(cn root) → 3 个 L1 mid(人民日报/政府机构/学术机构) → 每个 L1 的 leaves
// 渲染：每个节点一行；每行 = 属性名 + "全部" pill + N 个子节点 pill
// 点击：精确匹配选中（避免父 pill 联动高亮带来的视觉混乱）；再次点击 → 取消
function renderAttrFilter(items, groups) {
  const cnt = {};
  for (const it of items) cnt[it.source_id] = (cnt[it.source_id] || 0) + 1;
  const sum = (ids) => ids.reduce((s, id) => s + (cnt[id] || 0), 0);

  const rowsHtml = [];

  // 遍历每个 group（当前只有 cn 一个组）
  groups.forEach(g => {
    const allIds = g.root.match.slice();
    const allC = sum(allIds);
    // ===== 行 0：根（"中国"）=====
    // 子节点 = 各 branch.label + branch.match
    const l1Pills = g.branches.map(br => renderPill(br.label, br.match, { count: sum(br.match) }));
    // "全部" pill = 该 group 的全 match
    l1Pills.unshift(renderPill("全部", allIds, { count: allC, isAll: true }));
    rowsHtml.push(renderRow(g.label, l1Pills, false));

    // ===== 行 1..N：每个 L1 branch（人民日报/政府机构/学术机构）=====
    g.branches.forEach(br => {
      const leaves = br.leaves || [];
      // 如果 L1 本身就是叶子（无子级），跳过该独立行（已经在 L0 行里显示了）
      if (leaves.length === 0) return;
      const allBranchIds = br.match.slice();
      const allBranchC = sum(allBranchIds);
      const subPills = leaves.map(lv => renderPill(lv.label, lv.match, { count: sum(lv.match) }));
      subPills.unshift(renderPill("全部", allBranchIds, { count: allBranchC, isAll: true }));
      rowsHtml.push(renderRow(br.label, subPills, true));
    });
  });

  return '<div class="attr-filter" id="attrfilter">__VIEW_SWITCHER__<div class="attr-rows">' +
    rowsHtml.join("") + '</div></div>';
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

// ===== 增量更新：仅刷 attr-filter 内每个 pill 的数字徽章 + active 态，不重建 DOM =====
// SSE 推送 / 60s 刷新都走这里，避免 DOM 重建导致的闪烁
function updateAttrFilterCounts(items) {
  const cnt = {};
  for (const it of items) cnt[it.source_id] = (cnt[it.source_id] || 0) + 1;
  const sum = (ids) => ids.reduce((s, id) => s + (cnt[id] || 0), 0);
  const af = document.getElementById("attrfilter");
  if (!af) return;
  af.querySelectorAll(".attr-pill").forEach(el => {
    const ids = (el.dataset.ids || "").split(",").filter(Boolean);
    if (!ids.length) return;
    const n = sum(ids);
    const cntEl = el.querySelector(".pcnt");
    if (cntEl) cntEl.textContent = n;
    // 0 条灰化（与首次构建口径一致）
    if (n === 0) el.classList.add("zero");
    else el.classList.remove("zero");
  });
}

// ===== 点击 pill 后增量更新 active 态（不重建 DOM）=====
function syncAttrFilterActive() {
  const af = document.getElementById("attrfilter");
  if (!af) return;
  af.querySelectorAll(".attr-pill").forEach(el => {
    const ids = (el.dataset.ids || "").split(",").filter(Boolean);
    if (isActive(ids)) el.classList.add("active");
    else el.classList.remove("active");
  });
}

// ===== 绑定 pill 点击：toggle 选中 / 取消；切完 active 态不重建 DOM =====
function bindAttrFilter() {
  const root = document.getElementById("attrfilter");
  if (!root) return;
  root.addEventListener("click", (e) => {
    const b = e.target.closest("button[data-ids]");
    if (!b) return;
    const d = b.dataset.ids;
    if (!d) return;
    const newIds = d.split(",").filter(Boolean);
    if (!newIds.length) return;
    const newKey = toKey(newIds);
    // toggle：再次点击当前激活的 pill → 取消选择
    if (newKey === activeKey) {
      activeKey = null;
      activeSourceIds = null;
    } else {
      activeKey = newKey;
      activeSourceIds = newIds;
    }
    // 1) 增量同步 pill active 态（不重建 attr-filter DOM）
    syncAttrFilterActive();
    // 2) 重建 items 区（看板过滤变了，列表要刷新）
    renderItemsArea(
      activeSourceIds ? cachedItems.filter(i => new Set(activeSourceIds).has(i.source_id)) : cachedItems.slice(),
      activeSourceIds
    );
  });
}
// 元素顺序严格按用户示例：编号 → 相对时间 → 日期 → 板块 → [来源/分类/点赞 徽章] → 标题
function renderItemRow(i, num, dispName) {
  // 兜底：部分源（GitHub Trending / HF Models）的 published 为空，用 first_seen 代替以保证时间始终显示
  const pub = i.published || (i.first_seen ? i.first_seen.replace("T", " ").slice(0, 16) : "");
  const rel = (i.source_id === "people_daily") ? "" : relTime(pub);
  const relCell = rel ? '<span class="rel">' + escapeHtml(rel) + "</span>" : "";
  const pubCell = pub ? '<span class="pub">' + escapeHtml(fmtPub({...i, published: pub})) + "</span>" : "";
  const sec = (i.extra && i.extra.section) ? '<span class="badge sec">' + escapeHtml(i.extra.section) + "</span>" : "";
  const srcLabel = dispName
    ? '<span class="src-inline">' + escapeHtml(dispName) + "</span>"
    : "";
  const sub = i.subcategory ? '<span class="badge">' + escapeHtml(SUB_LABELS[i.subcategory] || i.subcategory) + "</span>" : "";
  const ups = (i.extra && i.extra.upvotes) ? '<span class="badge ups">▲ ' + i.extra.upvotes + "</span>" : "";
  const vis = visitedSet.has(i.url) ? " visited" : "";
  // data-url 用来在增量更新时定位/移除条目
  return '<div class="item" data-url="' + escapeHtml(i.url) + '"><div class="row1">' +
    '<span class="num">' + num + "</span>" +
    relCell + pubCell + sec + srcLabel + sub + ups +
    '<a class="title' + vis + '" href="' + i.url + '" target="_blank" rel="noopener">' + escapeHtml(i.title) + "</a>" +
    "</div></div>";
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
function renderEmptyState(visibleIds) {
  const ids = visibleIds || Object.keys(SOURCE_HOMES);
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
function renderItemsArea(visibleItems, filterIdsForEmpty) {
  const container = document.getElementById("items");
  if (!container) return;
  if (!visibleItems.length) {
    container.innerHTML = renderEmptyState(filterIdsForEmpty);
    return;
  }
  const sorted = visibleItems.slice();
  sortByTime(sorted);
  const total = sorted.length;
  container.innerHTML = sorted.map((i, idx) => renderItemRow(
    i, total - idx, SOURCE_DISPLAY[i.source_id] || i.source_name
  )).join("");
}

function render(nowStr) {
  const list = document.getElementById("list");
  let visibleItems = cachedItems.slice();
  if (activeSourceIds) {
    const set = new Set(activeSourceIds);
    visibleItems = visibleItems.filter(i => set.has(i.source_id));
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
    // 增量：attr-filter 的所有节点结构都保留，只刷 .pcnt 数字 + .zero 灰化
    updateAttrFilterCounts(cachedItems);
  }

  // items 区独立管理
  renderItemsArea(visibleItems, activeSourceIds);
}

const IS_STATIC = !!window.__STATIC__;

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

# 简易版 6 大品牌源 ID 集合（ChatGPT/Gemini/Anthropic/智谱/DeepSeek/千问）
SIMPLE_TECH_IDS = {"hf_papers", "openai_news", "openai_research", "deepmind", "anthropic",
                   "zhipu", "deepseek", "qwen"}

# 科技段分组展示：每个品牌一个子标题；源 id → 品牌归属（与 FILTER_GROUPS 科技组一致）
SIMPLE_TECH_GROUPS = [
    ("ChatGPT / OpenAI", ["openai_news", "openai_research"]),
    ("Gemini / Google", ["deepmind", "google_research"]),
    ("Anthropic", ["anthropic"]),
    ("智谱 GLM", ["zhipu"]),
    ("DeepSeek", ["deepseek"]),
    ("通义千问", ["qwen"]),
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
/* 简易版 view-switcher：与主看板一致风格（橙色胶囊） */
.view-switcher { display: flex; align-items: center; gap: 10px; margin-top: 12px; }
.view-switcher .switcher-label { font-size: 11.5px; color: #94a3b8; font-weight: 600; letter-spacing: .4px; }
.view-switcher .switcher-tabs { display: inline-flex; background: #f1f5f9; border: 1px solid #e3e8f2; border-radius: 9px; padding: 3px; gap: 2px; }
.view-switcher a { display: inline-block; padding: 5px 14px; border-radius: 6px; font-size: 12.5px; color: #475569; text-decoration: none; font-weight: 600; transition: all .14s ease; white-space: nowrap; line-height: 1.3; }
.view-switcher a:hover:not(.active) { color: #ff7d39; background: #fff8f3; }
.view-switcher a.active { background: linear-gradient(135deg, #ff7d39 0%, #ff9558 100%); color: #fff; box-shadow: 0 2px 5px rgba(255,125,57,.30); }
footer { color: #94a3b8; font-size: 11.5px; text-align: center;
         margin-top: 56px; padding-top: 16px; border-top: 1px solid #f1f5f9; }
footer a { color: #94a3b8; }
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
  <h2>Reuters · 有图片 <span class="tag">world / business</span><span class="count">__N2__ 条</span></h2>
  __SEC2__
</section>

<section>
  <h2>科技 · AI 品牌 <span class="tag">ChatGPT / Gemini / Anthropic / 智谱 / DeepSeek / 千问 / HF</span><span class="count">__N3__ 条</span></h2>
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


def _render_simple_section(items: list[dict], *, with_thumb: bool = False) -> str:
    """渲染简易版单个分区为 <ol><li>...</li></ol>；空集合返回提示。"""
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
        sec_html = f'<span class="sec">{_escape(sec_label)}</span>' if sec_label else ""
        thumb_html = ""
        if with_thumb and extra.get("image"):
            thumb_html = f'<img class="thumb" src="{_escape(extra["image"])}" loading="lazy" alt="">'
        lis.append(
            '<li>'
            f'<a href="{url}" target="_blank" rel="noopener">{title}</a>'
            f'{thumb_html}'
            f'<span class="time">{time_str}</span>'
            f'<span class="src">{src}</span>'
            f'{sec_html}'
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


def _filter_simple(items: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """按 3 块精要切分条目：
    - 人民日报 仅 要闻版面（section 以 "要闻" 结尾）
    - Reuters 仅带图片（extra.image 存在）
    - 科技 仅 SIMPLE_TECH_IDS 中的 6 大品牌
    """
    sec1: list[dict] = []  # 人民日报 要闻
    sec2: list[dict] = []  # Reuters 有图
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
            if extra.get("image"):
                sec2.append(it)
        elif sid in SIMPLE_TECH_IDS:
            sec3.append(it)
    # 按发布时间倒序
    for lst in (sec1, sec2, sec3):
        lst.sort(key=lambda x: (x.get("published") or ""), reverse=True)
    return sec1, sec2, sec3


def build_simple_html(items: list[dict], day_str: str) -> str:
    """生成简易版 HTML：3 块精要（人民日报要闻 / Reuters有图 / 科技6品牌）。"""
    sec1, sec2, sec3 = _filter_simple(items)
    return (
        SIMPLE_HTML
        .replace("__DAY__", _escape(day_str))
        .replace("__N1__", str(len(sec1)))
        .replace("__N2__", str(len(sec2)))
        .replace("__N3__", str(len(sec3)))
        .replace("__SEC1__", _render_simple_section(sec1))
        .replace("__SEC2__", _render_simple_section(sec2, with_thumb=True))
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
    logger.info("简易版已生成: %s（人民日报要闻=%d / Reuters有图=%d / 科技6品牌=%d）",
                output_path, len(sec1), len(sec2), len(sec3))
    print(f"\n✓ 简易版已生成: {output_path}")
    print(f"  - 人民日报·要闻：{len(sec1)} 条")
    print(f"  - Reuters·有图片：{len(sec2)} 条")
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

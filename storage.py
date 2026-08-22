"""SQLite 存储层：实时收集的新闻条目落库与查询。

表结构 items：
    day         TEXT  收集日（北京时间 YYYY-MM-DD）
    source_id   TEXT  源 ID（config.SOURCES 中的 id）
    source_name TEXT  源显示名
    category    TEXT  顶级分类
    subcategory TEXT  子分类（可空）
    title       TEXT
    url         TEXT
    published   TEXT  发布时间（YYYY-MM-DD HH:MM，可空）
    extra_json  TEXT  extra 字典的 JSON
    first_seen  TEXT  首次入库时间（YYYY-MM-DD HH:MM:SS）
    UNIQUE(day, source_id, url) 用于去重
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from scrapers.base import NewsItem, SourceConfig

DB_PATH = Path(__file__).resolve().parent / "data" / "news.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    day         TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    source_name TEXT NOT NULL,
    category    TEXT NOT NULL,
    subcategory TEXT,
    title       TEXT NOT NULL,
    url         TEXT NOT NULL,
    published   TEXT,
    extra_json  TEXT DEFAULT '{}',
    first_seen  TEXT NOT NULL,
    UNIQUE(day, source_id, url)
);
CREATE INDEX IF NOT EXISTS idx_items_day ON items(day);
CREATE INDEX IF NOT EXISTS idx_items_day_seen ON items(day, first_seen DESC);
"""


def get_conn(db_path: Path | None = None) -> sqlite3.Connection:
    """打开数据库连接（自动建表，WAL 模式支持读写并发）。"""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    return conn


def upsert_items(
    conn: sqlite3.Connection,
    day: str,
    source: SourceConfig,
    items: list[NewsItem],
) -> list[NewsItem]:
    """批量写入条目，按 (day, source_id, url) 去重。返回本次新增的 NewsItem 列表。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_items: list[NewsItem] = []
    with conn:
        for it in items:
            cur = conn.execute(
                """INSERT OR IGNORE INTO items
                   (day, source_id, source_name, category, subcategory,
                    title, url, published, extra_json, first_seen)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    day,
                    source.id,
                    source.name,
                    source.category,
                    source.subcategory,
                    it.title,
                    it.url,
                    it.published,
                    json.dumps(it.extra or {}, ensure_ascii=False),
                    now,
                ),
            )
            if cur.rowcount:
                new_items.append(it)
    return new_items


def replace_source_items(
    conn: sqlite3.Connection,
    day: str,
    source: SourceConfig,
    items: list[NewsItem],
) -> int:
    """整体替换某源当天条目（用于榜单/Trending 这类快照型源）。返回条数。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        # 保留原 first_seen：先取已有 url -> first_seen 映射
        old = {
            row["url"]: row["first_seen"]
            for row in conn.execute(
                "SELECT url, first_seen FROM items WHERE day=? AND source_id=?",
                (day, source.id),
            )
        }
        conn.execute(
            "DELETE FROM items WHERE day=? AND source_id=?", (day, source.id)
        )
        for it in items:
            conn.execute(
                """INSERT OR IGNORE INTO items
                   (day, source_id, source_name, category, subcategory,
                    title, url, published, extra_json, first_seen)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    day,
                    source.id,
                    source.name,
                    source.category,
                    source.subcategory,
                    it.title,
                    it.url,
                    it.published,
                    json.dumps(it.extra or {}, ensure_ascii=False),
                    old.get(it.url, now),
                ),
            )
    return len(items)


def get_items(conn: sqlite3.Connection, day: str) -> list[dict]:
    """取某天全部条目（按入库时间倒序）。"""
    rows = conn.execute(
        """SELECT day, source_id, source_name, category, subcategory,
                  title, url, published, extra_json, first_seen
           FROM items WHERE day=? ORDER BY first_seen DESC, id DESC""",
        (day,),
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["extra"] = json.loads(d.pop("extra_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            d["extra"] = {}
        result.append(d)
    return result


def count_items(conn: sqlite3.Connection, day: str) -> int:
    row = conn.execute("SELECT COUNT(*) AS c FROM items WHERE day=?", (day,)).fetchone()
    return row["c"] if row else 0

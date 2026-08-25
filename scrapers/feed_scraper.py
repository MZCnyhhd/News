"""RSS / Atom feed 抓取器（feedparser 统一处理，覆盖 12 个 feed 源）。"""
from __future__ import annotations

import time
from datetime import datetime

import feedparser

from scrapers.base import BaseScraper, NewsItem


def _clean_summary(text: str, max_len: int = 280) -> str:
    """清理 RSS description 中的 HTML 标签、实体、多余空白，截断到 max_len。

    用作 extra.summary 在看板上的简介预览（行 2 副行）。
    """
    if not text:
        return ""
    import html as html_lib
    import re as re_lib

    cleaned = re_lib.sub(r"<[^>]+>", " ", text)
    cleaned = html_lib.unescape(cleaned)
    cleaned = re_lib.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1] + "…"
    return cleaned


def _normalize_date(entry) -> str | None:
    """从 feedparser 条目提取并规范化日期为北京时间 YYYY-MM-DD HH:MM。

    feedparser 解析的时间通常是 UTC，转换为北京时间 (+8) 后格式化。
    """
    from datetime import timezone, timedelta

    BEIJING = timezone(timedelta(hours=8))

    for field_name in ("published_parsed", "updated_parsed"):
        st = entry.get(field_name)
        if st:
            try:
                # feedparser 解析的 struct_time 视为 UTC
                dt = datetime(*st[:6], tzinfo=timezone.utc)
                local_dt = dt.astimezone(BEIJING)
                return local_dt.strftime("%Y-%m-%d %H:%M")
            except (TypeError, ValueError):
                pass
    # 回退：尝试原始字符串 + dateutil
    for field_name in ("published", "updated", "created"):
        raw = entry.get(field_name)
        if raw:
            try:
                from dateutil import parser as date_parser

                dt = date_parser.parse(raw)
                if dt.tzinfo is None:
                    # 无时区信息视为 UTC
                    dt = dt.replace(tzinfo=timezone.utc)
                local_dt = dt.astimezone(BEIJING)
                return local_dt.strftime("%Y-%m-%d %H:%M")
            except (ImportError, ValueError, OverflowError):
                pass
    return None


def _clean_title(title: str) -> str:
    """清理标题中的 HTML 实体与多余空白。"""
    if not title:
        return ""
    # feedparser 通常已处理 HTML，但有时残留实体
    title = title.replace("\n", " ").replace("\r", " ")
    import html

    title = html.unescape(title)
    return " ".join(title.split())


class FeedScraper(BaseScraper):
    """RSS / Atom feed 抓取器。"""

    def fetch(self) -> list[NewsItem]:
        url = self.source.feed_url
        if not url:
            raise ValueError(f"feed 源 {self.source.id} 缺少 feed_url")

        # 用 HttpClient 抓字节，再交给 feedparser 解析（可控 UA/超时）
        raw = self.http.get_bytes(url)
        # feedparser 可接受 bytes，并自动检测编码
        parsed = feedparser.parse(raw)

        if parsed.bozo and not parsed.entries:
            # 解析出错且无条目
            exc = getattr(parsed, "bozo_exception", None)
            raise RuntimeError(
                f"feed 解析失败: {exc or '未知错误'}"
            )

        items: list[NewsItem] = []
        for entry in parsed.entries[: self.limit]:
            link = entry.get("link", "")
            title = _clean_title(entry.get("title", ""))
            if not title or not link:
                continue
            # 提取 description 作 summary（行 2 副行简介预览）
            summary = _clean_summary(entry.get("summary") or entry.get("description") or "")
            extra: dict = {}
            if summary:
                extra["summary"] = summary
            items.append(
                NewsItem(
                    title=title,
                    url=link,
                    published=_normalize_date(entry),
                    source=self.source.name,
                    extra=extra or None,
                )
            )
        return items

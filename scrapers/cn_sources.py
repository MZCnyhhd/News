"""中国官方政策与学术机构信息源抓取器。

覆盖 9 个源（工信部走 FeedScraper + Google News RSS 代理，不在此模块）：
- gov_policy    国务院政策文件库（JSON API）
- cac           国家网信办（HTML 列表页）
- ndrc          国家发改委（HTML 列表页）
- cast          中国科协（HTML 列表页 + 文章页补日期）
- ia_cas        中科院自动化所（HTML 列表页）
- pku_ai        北大 AI 研究院（HTML 列表页）
- tsinghua_ai   清华 AI 学院（HTML 列表页，学院头条）
- baai          北京智源研究院（JSON API）
- caai          中国人工智能学会（HTML 列表页）

日期约定：中文站点日期本身即北京时间，直接存 YYYY-MM-DD（或含时分）。
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, NewsItem

BEIJING_TZ = timezone(timedelta(hours=8))


def _ms_to_beijing(ms) -> str | None:
    """毫秒时间戳 -> 北京时间 YYYY-MM-DD HH:MM。"""
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=BEIJING_TZ)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError, OSError):
        return None


def _iso_to_beijing(iso_str: str) -> str | None:
    """ISO8601（可能带 Z）-> 北京时间 YYYY-MM-DD HH:MM。"""
    try:
        s = iso_str.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
    except (ValueError, AttributeError):
        return None


class CnSourceScraper(BaseScraper):
    """中国政策/学术源统一抓取器，按 source.parser 分发。"""

    def fetch(self) -> list[NewsItem]:
        parser = self.source.parser
        method = getattr(self, f"_fetch_{parser}", None)
        if method is None:
            raise ValueError(f"未知中国源 parser: {parser}")
        items = method()
        # 统一补 source 名
        for it in items:
            it.source = self.source.name
        return self._truncate(items)

    # ------------------------------------------------------------------
    # 顶尖学术机构
    # ------------------------------------------------------------------

    def _fetch_ia_cas(self) -> list[NewsItem]:
        """中科院自动化所：头条新闻 + 今日要闻列表页。"""
        pages = [
            ("http://www.ia.ac.cn/xwzx/ttxw/", "头条新闻"),
            ("http://www.ia.ac.cn/xwzx/jryw/", "今日要闻"),
        ]
        items: list[NewsItem] = []
        seen: set[str] = set()
        for page_url, tag in pages:
            try:
                html = self.http.get_text(page_url)
            except Exception:
                continue
            soup = BeautifulSoup(html, "lxml")
            for a in soup.select("li a.db"):
                href = (a.get("href") or "").strip()
                title = (a.get("title") or "").strip()
                if not title:
                    t_el = a.select_one(".title")
                    title = t_el.get_text(strip=True) if t_el else a.get_text(strip=True)
                if not href or not title or len(title) < 6:
                    continue
                full = urljoin(page_url, href)
                if full in seen:
                    continue
                date_el = a.select_one(".date-s")
                published = None
                if date_el:
                    m = re.search(r"(\d{4}-\d{2}-\d{2})", date_el.get_text(strip=True))
                    if m:
                        published = m.group(1)
                seen.add(full)
                items.append(NewsItem(title=title, url=full, published=published,
                                      extra={"section": tag}))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_pku_ai(self) -> list[NewsItem]:
        """北大 AI 研究院：新闻列表页。"""
        page_url = "http://www.ai.pku.edu.cn/xwgg1/xwxx.htm"
        html = self.http.get_text(page_url)
        soup = BeautifulSoup(html, "lxml")
        items: list[NewsItem] = []
        seen: set[str] = set()
        for card in soup.select("div.lists"):
            a = card.select_one("div.listtext_tit a") or card.select_one("a[href]")
            if not a:
                continue
            href = (a.get("href") or "").strip()
            title = (a.get("title") or a.get_text(strip=True)).strip()
            if not href or not title:
                continue
            full = urljoin(page_url, href)
            if full in seen:
                continue
            published = None
            conts = card.select("div.listtext_cont")
            for c in reversed(conts):
                m = re.search(r"(\d{4}-\d{2}-\d{2})", c.get_text(strip=True))
                if m:
                    published = m.group(1)
                    break
            seen.add(full)
            items.append(NewsItem(title=title, url=full, published=published))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_tsinghua_ai(self) -> list[NewsItem]:
        """清华 AI 学院：学院头条列表页（div.time h3=MM-DD h6=YYYY）。"""
        candidates = [
            "https://collegeai.tsinghua.edu.cn/index/xytt.htm",
            "https://collegeai.tsinghua.edu.cn/xwtz/xwdt.htm",
        ]
        items: list[NewsItem] = []
        seen: set[str] = set()
        for page_url in candidates:
            try:
                html = self.http.get_text(page_url)
            except Exception:
                continue
            soup = BeautifulSoup(html, "lxml")
            for li in soup.find_all("li"):
                a = li.find("a", href=True)
                if not a:
                    continue
                href = a["href"].strip()
                # 只要站内文章链接，跳过微信外链
                if "info/" not in href:
                    continue
                title = ""
                h4 = li.select_one("h4")
                if h4:
                    title = h4.get_text(strip=True)
                if not title:
                    title = (a.get("title") or a.get_text(strip=True)).strip()
                if not title or len(title) < 6:
                    continue
                full = urljoin(page_url, href)
                if full in seen:
                    continue
                published = None
                time_el = li.select_one("div.time")
                if time_el:
                    h3 = time_el.find("h3")
                    h6 = time_el.find("h6")
                    if h3 and h6:
                        md = h3.get_text(strip=True)
                        y = h6.get_text(strip=True)
                        m = re.match(r"(\d{2})-(\d{2})", md)
                        if m and re.match(r"\d{4}", y):
                            published = f"{y}-{m.group(1)}-{m.group(2)}"
                if not published:
                    m = re.search(r"(\d{4}-\d{2}-\d{2})", li.get_text(" ", strip=True))
                    if m:
                        published = m.group(1)
                seen.add(full)
                items.append(NewsItem(title=title, url=full, published=published))
            if items:
                break  # 首个可用入口即够
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_baai(self) -> list[NewsItem]:
        """北京智源研究院：官方 JSON API。"""
        api = "https://www.baai.ac.cn/api/news?page=1"
        raw = self.http.get_text(api)
        data = json.loads(raw)
        entries = data.get("items") or data.get("data") or []
        items: list[NewsItem] = []
        seen: set[str] = set()
        for e in entries:
            title = (e.get("title") or "").strip()
            url = (e.get("source_url") or e.get("url") or "").strip()
            if not title or not url or url in seen:
                continue
            published = _iso_to_beijing(e.get("published_at") or "")
            extra = {}
            if e.get("category"):
                extra["section"] = str(e["category"]).strip()
            seen.add(url)
            items.append(NewsItem(title=title, url=url, published=published, extra=extra))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_caai(self) -> list[NewsItem]:
        """中国人工智能学会：学会新闻 + 新闻动态列表页。"""
        pages = [
            ("http://www.caai.cn/site/term/13.html", "学会新闻"),
            ("http://www.caai.cn/site/term/11.html", "新闻动态"),
        ]
        items: list[NewsItem] = []
        seen: set[str] = set()
        for page_url, tag in pages:
            try:
                html = self.http.get_text(page_url)
            except Exception:
                continue
            soup = BeautifulSoup(html, "lxml")
            for li in soup.select("ul.news-list-page li"):
                a = li.find("a", href=True)
                if not a:
                    continue
                href = a["href"].strip()
                h4 = li.select_one("div.new-title h4") or li.find("h4")
                title = h4.get_text(strip=True) if h4 else a.get_text(strip=True)
                if not href or not title or len(title) < 6:
                    continue
                full = urljoin("http://www.caai.cn", href)
                if full in seen:
                    continue
                published = None
                date_el = li.select_one("span.date") or li.find("span", class_="date")
                if date_el:
                    m = re.search(r"(\d{4}-\d{2}-\d{2})", date_el.get_text(strip=True))
                    if m:
                        published = m.group(1)
                if not published:
                    m = re.search(r"(\d{4}-\d{2}-\d{2})", li.get_text(" ", strip=True))
                    if m:
                        published = m.group(1)
                seen.add(full)
                items.append(NewsItem(title=title, url=full, published=published,
                                      extra={"section": tag}))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

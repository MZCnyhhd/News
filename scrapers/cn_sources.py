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
    # 中央电视台（央视新闻门户，含《新闻联播》当日头条）
    # 说明：CCTV 新闻联播栏目页（tv.cctv.com/lm/xwlb）为 JS 渲染、官方 API
    # 返回「拒绝访问/无效服务」、搜索 403、无 sitemap/RSS，无法稳定抓取单集
    # 文字稿。故退而抓取央视新闻门户 news.cctv.com 首页头条（含新闻联播内容），
    # 日期取自文章 URL 中的 /YYYY/MM/DD/ 路径。
    # ------------------------------------------------------------------

    def _fetch_cctv(self) -> list[NewsItem]:
        """央视新闻门户首页头条（标题 + 链接 + 发布日期，日期取 URL 路径）。"""
        page_url = "https://news.cctv.com/"
        html = self.http.get_text(page_url)
        soup = BeautifulSoup(html, "lxml")
        items: list[NewsItem] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            title = (a.get("title") or a.get_text(strip=True)).strip()
            if not href or not title or len(title) < 8:
                continue
            # 只保留带明确发布日期路径的文章（/YYYY/MM/DD/...shtml）
            m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", href)
            if not m:
                continue
            full = href if href.startswith("http") else urljoin(page_url, href)
            if full in seen:
                continue
            seen.add(full)
            published = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            items.append(NewsItem(title=title, url=full, published=published))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    # ------------------------------------------------------------------
    # 官方政策
    # ------------------------------------------------------------------

    def _fetch_gov_policy(self) -> list[NewsItem]:
        """国务院政策文件库 JSON API（搜索"人工智能"，按发布时间倒序）。"""
        api = (
            "https://sousuo.www.gov.cn/search-gov/data"
            "?t=zhengcelibrary&q=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD"
            "&timetype=timeqb&mintime=&maxtime=&sort=pubtime&sortType=1"
            "&searchfield=title&puborg=&pcodeYear=&pcodeNum=&filetype="
            "&p=1&n=20&inpro=&bmfl=&dup=&orpro="
        )
        raw = self.http.get_text(api)
        data = json.loads(raw)

        items: list[NewsItem] = []
        cat_map = (data.get("searchVO") or {}).get("catMap") or {}
        entries = []
        for cat in ("gongwen", "bumenfile", "otherfile", "gongbao"):
            block = cat_map.get(cat) or {}
            entries.extend(block.get("listVO") or [])
        # 顶级 listVO 兜底
        entries.extend(data.get("searchVO", {}).get("listVO") or [])

        seen: set[str] = set()
        for e in entries:
            title = re.sub(r"<[^>]+>", "", e.get("title") or "").strip()
            url = (e.get("url") or "").strip()
            if not title or not url or url in seen:
                continue
            seen.add(url)
            published = _ms_to_beijing(e.get("pubtime"))
            extra = {}
            if e.get("puborg"):
                extra["section"] = str(e["puborg"]).strip()
            items.append(NewsItem(title=title, url=url, published=published, extra=extra))
        # 按时间倒序
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_miit(self) -> list[NewsItem]:
        """工信部：新闻发布会列表页（主站多数栏目为 JS 壳，此页服务端渲染）。

        条目所在 li 文本末尾自带 YYYY-MM-DD 日期。
        """
        page_url = "https://www.miit.gov.cn/xwfb/xwfbh/index.html"
        html = self.http.get_text(page_url)
        soup = BeautifulSoup(html, "lxml")
        items: list[NewsItem] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "art_" not in href:
                continue
            title = (a.get("title") or a.get_text(strip=True)).strip()
            if not title or len(title) < 8:
                continue
            full = urljoin(page_url, href)
            if full in seen:
                continue
            li = a.find_parent("li") or a.parent
            published = None
            if li is not None:
                m = re.search(r"(\d{4}-\d{2}-\d{2})", li.get_text(" ", strip=True))
                if m:
                    published = m.group(1)
            seen.add(full)
            items.append(NewsItem(title=title, url=full, published=published,
                                  extra={"section": "新闻发布"}))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_cac(self) -> list[NewsItem]:
        """国家网信办：网信要闻 + 网信发布 列表页。"""
        pages = [
            ("http://www.cac.gov.cn/yaowen/wxyw/A093602index_1.htm", "网信要闻"),
            ("http://www.cac.gov.cn/wxzw/wxfb/A093702index_1.htm", "网信发布"),
        ]
        items: list[NewsItem] = []
        seen: set[str] = set()
        for page_url, tag in pages:
            try:
                html = self.http.get_text(page_url)
            except Exception:
                continue
            soup = BeautifulSoup(html, "lxml")
            for a in soup.select('a[href*="c_"]'):
                href = (a.get("href") or "").strip()
                title = a.get_text(strip=True)
                if not href or not title or len(title) < 8:
                    continue
                # //www.cac.gov.cn/2026-07/22/c_xxx.htm 或相对路径
                if href.startswith("//"):
                    full = "https:" + href
                else:
                    full = urljoin(page_url, href)
                if full in seen:
                    continue
                m = re.search(r"(\d{4}-\d{2})/(\d{2})/c_", full)
                published = f"{m.group(1)}-{m.group(2)}" if m else None
                seen.add(full)
                items.append(NewsItem(title=title, url=full, published=published,
                                      extra={"section": tag}))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_ndrc(self) -> list[NewsItem]:
        """国家发改委：新闻发布列表页。"""
        page_url = "https://www.ndrc.gov.cn/xwdt/index.html"
        html = self.http.get_text(page_url)
        soup = BeautifulSoup(html, "lxml")
        items: list[NewsItem] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="t202"]'):
            href = (a.get("href") or "").strip()
            title = (a.get("title") or a.get_text(strip=True)).strip()
            if not href or not title or len(title) < 8:
                continue
            full = urljoin(page_url, href)
            if full in seen:
                continue
            m = re.search(r"t(\d{4})(\d{2})(\d{2})_", full)
            published = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None
            seen.add(full)
            items.append(NewsItem(title=title, url=full, published=published))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

    def _fetch_cast(self) -> list[NewsItem]:
        """中国科协：新闻聚合页（列表无日期，进文章页补齐，最多补 limit 条）。"""
        page_url = "https://www.cast.org.cn/xw/index.html"
        html = self.http.get_text(page_url)
        soup = BeautifulSoup(html, "lxml")

        raw: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.select("div.fyywli li a") or soup.select('a[href*="/art/"]'):
            href = (a.get("href") or "").strip()
            title = (a.get("title") or a.get_text(strip=True)).strip()
            if not href or not title or len(title) < 8:
                continue
            full = urljoin("https://www.cast.org.cn/", href)
            if full in seen or "/art/" not in full:
                continue
            seen.add(full)
            raw.append((title, full))
            if len(raw) >= self.limit:
                break

        items: list[NewsItem] = []
        for title, full in raw:
            published = None
            # 尝试从 URL 提取年份目录后进文章页取精确日期
            try:
                art_html = self.http.get_text(full)
                m = re.search(r"(\d{4}-\d{2}-\d{2})", art_html)
                if m:
                    published = m.group(1)
            except Exception:
                pass
            items.append(NewsItem(title=title, url=full, published=published))
        items.sort(key=lambda x: x.published or "", reverse=True)
        return items

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

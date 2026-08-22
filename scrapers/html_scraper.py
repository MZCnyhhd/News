"""HTML 抓取器与各站解析器（Anthropic News / OpenAI Research / Hugging Face Papers）。"""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, NewsItem


def _parse_flexible_date(text: str) -> str | None:
    """用 dateutil 解析多种日期格式，返回 YYYY-MM-DD。"""
    text = text.strip()
    if not text:
        return None
    try:
        from dateutil import parser as date_parser

        return date_parser.parse(text).strftime("%Y-%m-%d")
    except (ImportError, ValueError, OverflowError):
        return None


def parse_anthropic(soup: BeautifulSoup, source) -> list[NewsItem]:
    """解析 Anthropic News 页面卡片。

    卡片链接指向 /news/{slug} 或 /features/{slug}，含日期文本。
    """
    items: list[NewsItem] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not (href.startswith("/news/") or href.startswith("/features/")):
            continue
        # 排除索引页自身
        slug = href.rstrip("/").rsplit("/", 1)[-1]
        if not slug:
            continue
        full_url = urljoin("https://www.anthropic.com", href)
        if full_url in seen:
            continue

        # 标题：卡片内通常有标题元素
        title = ""
        for tag_name in ("h2", "h3", "h4"):
            t = a.find(tag_name)
            if t:
                title = t.get_text(strip=True)
                break
        if not title:
            title = a.get_text(strip=True)
        if not title:
            continue

        # 日期：卡片内可能含日期文本（如 "Jul 24, 2026"）
        published = None
        card_text = a.get_text(" ", strip=True)
        # 匹配常见日期格式
        date_match = re.search(
            r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b)",
            card_text,
        )
        if date_match:
            published = _parse_flexible_date(date_match.group(1))

        seen.add(full_url)
        items.append(
            NewsItem(
                title=title,
                url=full_url,
                published=published,
                source=source.name,
            )
        )
        if len(items) >= source.limit:
            break
    return items


def parse_openai_research(soup: BeautifulSoup, source) -> list[NewsItem]:
    """解析 OpenAI Research 页面卡片。

    研究卡片链接指向 /research/{slug} 或 /index/{slug}，含日期。
    """
    items: list[NewsItem] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not (href.startswith("/research/") or href.startswith("/index/")):
            continue
        slug = href.rstrip("/").rsplit("/", 1)[-1]
        if not slug:
            continue
        full_url = urljoin("https://openai.com", href)
        if full_url in seen:
            continue

        title = ""
        for tag_name in ("h2", "h3", "h4"):
            t = a.find(tag_name)
            if t:
                title = t.get_text(strip=True)
                break
        if not title:
            title = a.get_text(strip=True)
        if not title:
            continue

        published = None
        card_text = a.get_text(" ", strip=True)
        date_match = re.search(
            r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b)",
            card_text,
        )
        if date_match:
            published = _parse_flexible_date(date_match.group(1))

        seen.add(full_url)
        items.append(
            NewsItem(
                title=title,
                url=full_url,
                published=published,
                source=source.name,
            )
        )
        if len(items) >= source.limit:
            break
    return items


def parse_hf_papers(soup: BeautifulSoup, source) -> list[NewsItem]:
    """解析 Hugging Face Papers (daily / trending) 页面卡片。

    HF Papers 页面是 Svelte SPA，正文在 <div class="SVELTE_HYDRATER"
    data-target="DailyPapers" data-props="..."> 里以 JSON 形式注入。
    直接读 <a> 拿不到真实标题（只有 paper id），所以优先从 JSON 提取。
    为了通过"只爬当天"的过滤，用 page 自身的 dateString 作 published（标 06:00）。
    """
    import html as html_mod
    import json

    items: list[NewsItem] = []
    seen: set[str] = set()

    # 1) 优先：从 SVELTE_HYDRATER 的 JSON 读
    daily_papers = []
    page_date = None  # 例如 "2026-07-30"
    for div in soup.find_all("div", attrs={"data-target": "DailyPapers"}):
        raw = div.get("data-props", "")
        if not raw:
            continue
        try:
            decoded = html_mod.unescape(raw)
            data = json.loads(decoded)
            daily_papers = data.get("dailyPapers", []) or []
            page_date = data.get("dateString") or page_date
            if daily_papers:
                break
        except Exception:
            continue

    # 用 page 自身的日期作 published（通过"只爬当天"过滤，并标 06:00 兜底）
    if page_date:
        page_published = f"{page_date} 06:00"
    else:
        page_published = None

    for entry in daily_papers:
        paper = entry.get("paper") or {}
        pid = paper.get("id", "").strip()
        title = (paper.get("title") or "").strip()
        if not pid or not title:
            continue
        url = f"https://huggingface.co/papers/{pid}"
        if url in seen:
            continue
        extra = {}
        # upvotes 在 paper 字段下面（如 CLBench-V: 73）
        for src in (entry, paper):
            try:
                ups = src.get("upvotes")
                if isinstance(ups, (int, float)) and ups > 0:
                    extra["upvotes"] = int(ups)
                    break
            except Exception:
                pass
        seen.add(url)
        items.append(
            NewsItem(
                title=title,
                url=url,
                published=page_published,
                source=source.name,
                extra=extra,
            )
        )
        if len(items) >= source.limit:
            return items

    # 2) 回退：直接扫 <a>
    if not items:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            m = re.match(r"^/papers/(\d+)$", href)
            if not m:
                continue
            pid = m.group(1)
            full_url = f"https://huggingface.co/papers/{pid}"
            if full_url in seen:
                continue
            title = ""
            for tag in ("h3", "h2", "h1"):
                t = a.find(tag)
                if t:
                    title = t.get_text(strip=True)
                    break
            if not title:
                title = pid
            seen.add(full_url)
            items.append(
                NewsItem(
                    title=title, url=full_url,
                    published=page_published, source=source.name,
                )
            )
            if len(items) >= source.limit:
                break
    return items


def parse_meta_ai(soup: BeautifulSoup, source) -> list[NewsItem]:
    """解析 Meta AI Blog 页面卡片。

    文章链接形如 https://ai.meta.com/blog/{slug}/，含日期文本。
    """
    items: list[NewsItem] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # 文章链接形如 /blog/{slug} 或 https://ai.meta.com/blog/{slug}
        if href.startswith("https://ai.meta.com/blog/"):
            path = href[len("https://ai.meta.com/blog/"):]
        elif href.startswith("/blog/"):
            path = href[len("/blog/"):]
        else:
            continue
        slug = path.rstrip("/")
        # 排除空 slug、category、tag 等非文章链接
        if not slug or "/" in slug or slug in ("category", "tag", "page"):
            continue
        full_url = urljoin("https://ai.meta.com", href if href.startswith("/") else "/" + path)
        if full_url in seen:
            continue

        # 标题：卡片内 h2/h3/h1
        title = ""
        for tag_name in ("h2", "h3", "h1"):
            t = a.find(tag_name)
            if t:
                title = t.get_text(strip=True)
                break
        if not title:
            # 回退：用链接文本（去除 "Read" 前缀）
            title = a.get_text(strip=True)
            title = re.sub(r"^Read\s+", "", title)
        if not title or len(title) < 5:
            continue

        # 日期：卡片文本中匹配 "Jul 21, 2026" 或 "July 21, 2026"
        published = None
        card_text = a.get_text(" ", strip=True)
        date_match = re.search(
            r"(\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
            r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
            r"\s+\d{1,2},?\s+\d{4}\b)",
            card_text,
        )
        if date_match:
            published = _parse_flexible_date(date_match.group(1))

        seen.add(full_url)
        items.append(
            NewsItem(
                title=title,
                url=full_url,
                published=published,
                source=source.name,
            )
        )
        if len(items) >= source.limit:
            break
    return items


def parse_ms_ai(soup: BeautifulSoup, source) -> list[NewsItem]:
    """解析 Microsoft AI Blog 页面卡片。

    文章链接指向 blogs.microsoft.com/blog/YYYY/MM/DD/{slug}/，
    标题在 "Read the story titled {title}" 或 h2 中。
    """
    items: list[NewsItem] = []
    seen: set[str] = set()

    # 匹配带日期的博客文章 URL
    date_url_pattern = re.compile(
        r"(?:https?://blogs\.microsoft\.com)?/blog/(\d{4})/(\d{1,2})/(\d{1,2})/[^/]+/?$"
    )

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        m = date_url_pattern.match(href)
        if not m:
            continue
        yy, mm, dd = m.group(1), m.group(2), m.group(3)
        full_url = urljoin("https://blogs.microsoft.com", href)
        if full_url in seen:
            continue

        # 标题：从 h2 提取，或从 "Read the story titled {title}" 提取
        title = ""
        h2 = a.find("h2") or a.find("h3")
        if h2:
            title = h2.get_text(strip=True)
        if not title:
            text = a.get_text(" ", strip=True)
            t_match = re.search(r"Read the story titled\s+(.+?)(?:\s*-|\s*$)", text)
            if t_match:
                title = t_match.group(1).strip()
        if not title:
            title = a.get_text(strip=True)
            title = re.sub(r"^Read the story titled\s+", "", title)
        if not title or len(title) < 5:
            continue

        published = f"{yy}-{int(mm):02d}-{int(dd):02d}"

        seen.add(full_url)
        items.append(
            NewsItem(
                title=title,
                url=full_url,
                published=published,
                source=source.name,
            )
        )
        if len(items) >= source.limit:
            break
    return items


# 解析器注册表
HTML_PARSERS = {
    "anthropic": parse_anthropic,
    "openai_research": parse_openai_research,
    "hf_papers": parse_hf_papers,
    "meta_ai": parse_meta_ai,
    "ms_ai": parse_ms_ai,
}


class HtmlScraper(BaseScraper):
    """HTML 抓取器，根据 source.parser 分发到具体解析器。"""

    def fetch(self) -> list[NewsItem]:
        from datetime import datetime
        url = self.source.feed_url or self.source.url
        parser_name = self.source.parser
        # HF Daily Papers：URL 必须带当天日期参数，且所有条目 published 标当天
        # （HF daily 按 PT 时区公布，dateString 通常是本地"昨天"，不改正被"只爬当天"过滤）
        force_today = False
        if parser_name == "hf_papers":
            url = url + "?date=" + datetime.now().strftime("%Y-%m-%d")
            force_today = True
        if not parser_name or parser_name not in HTML_PARSERS:
            raise ValueError(f"未知 HTML 解析器: {parser_name}")

        html = self.http.get_text(url)
        soup = BeautifulSoup(html, "lxml")
        items = HTML_PARSERS[parser_name](soup, self.source)
        if force_today:
            today_str = datetime.now().strftime("%Y-%m-%d")
            for it in items:
                it.published = f"{today_str} 06:00"
        return self._truncate(items)

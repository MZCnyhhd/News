"""Reuters Sitemap 抓取器。

通过 Reuters 官方 news sitemap 抓取最新文章，仅保留 World 和 Business 板块。

Sitemap 结构：
  - 索引: https://www.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml
    含多个子 sitemap 链接
  - 子 sitemap: https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml
    每个含约 50 条 <url>，含 <news:title>、<news:publication_date>、<loc>（含板块路径）
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, NewsItem


class ReutersSitemapScraper(BaseScraper):
    """通过 news sitemap 抓取 Reuters World 和 Business 板块最新文章。"""

    INDEX_URL = "https://www.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml"
    # 只保留这两个板块
    ALLOWED_SECTIONS = ("/world/", "/business/")

    def fetch(self) -> list[NewsItem]:
        # 1. 抓取索引，获取子 sitemap URL 列表
        index_xml = self.http.get_text(self.INDEX_URL)
        sub_sitemaps = self._extract_sub_sitemaps(index_xml)
        if not sub_sitemaps:
            raise RuntimeError("Reuters sitemap 索引未找到子 sitemap")

        # 2. 依次抓取子 sitemap。Reuters 把最近新闻分页到多个子 sitemap
        # （?from=0/100/200/...），每页约 50 条，按发布时间倒序（offset=0 最新）。
        # 必须遍历多个子 sitemap 才能覆盖当天 00:00 起的全部 World/Business 文章。
        from datetime import datetime, timezone, timedelta
        BEIJING = timezone(timedelta(hours=8))
        today_bj = datetime.now(BEIJING).strftime("%Y-%m-%d")
        # 当天 00:00 Beijing time（用于跨日边界比较）
        today_start_bj = today_bj + " 00:00"

        all_items: list[NewsItem] = []
        seen_urls: set[str] = set()

        for sm_url in sub_sitemaps:
            try:
                sm_xml = self.http.get_text(sm_url)
            except Exception:
                continue
            page_max_pub = None  # 当前 sub-sitemap 中所有 published 的最大值
            page_had_today = False
            for it in self._extract_items(sm_xml):
                pub = it.published or ""
                # 记录页面内最新发布时间（用于早停判断）
                if not page_max_pub or pub > page_max_pub:
                    page_max_pub = pub
                # 仅保留当天（北京时间）的文章
                if not pub.startswith(today_bj):
                    continue
                page_had_today = True
                if it.url in seen_urls:
                    continue
                seen_urls.add(it.url)
                all_items.append(it)

            # 早停条件：当前 sub-sitemap 中「最晚一篇」已 < 当天 00:00 Beijing，
            # 后续子 sitemap 只会更旧，无需继续（每个 sub-sitemap 内本身也是按新→旧排列）
            if page_max_pub and page_max_pub < today_start_bj:
                break
            # 已收集足够多今天文章，但继续到下一子 sitemap 一次以防漏边缘
            if len(all_items) >= self.limit and not page_had_today:
                break

        # 3. 筛选 world/business，按发布日期倒序
        items = [it for it in all_items if self._is_allowed(it.url)]
        items.sort(key=lambda x: x.published or "", reverse=True)

        if not items:
            raise RuntimeError(f"Reuters sitemap 未找到 {today_bj} 当天 World/Business 板块文章")

        return items[: self.limit]

    def _is_allowed(self, url: str) -> bool:
        """检查 URL 是否属于允许的板块。"""
        path = urlparse(url).path
        return any(sec in path for sec in self.ALLOWED_SECTIONS)

    def _extract_sub_sitemaps(self, xml: str) -> list[str]:
        """从 sitemap 索引提取子 sitemap URL。"""
        soup = BeautifulSoup(xml, "lxml-xml")
        urls = []
        for sm in soup.find_all("sitemap"):
            loc = sm.find("loc")
            if loc and loc.text:
                urls.append(loc.text.strip())
        return urls

    def _extract_items(self, xml: str) -> list[NewsItem]:
        """从子 sitemap 提取文章条目（标题、URL、发布日期）。"""
        soup = BeautifulSoup(xml, "lxml-xml")
        items: list[NewsItem] = []

        for url_tag in soup.find_all("url"):
            loc_tag = url_tag.find("loc")
            if not loc_tag or not loc_tag.text:
                continue
            loc = loc_tag.text.strip()

            # 标题在 <news:title> 中
            title = ""
            news_title = url_tag.find("news:title")
            if news_title and news_title.text:
                title = news_title.text.strip()

            # 发布日期在 <news:publication_date> 中（UTC ISO 格式）
            published = None
            news_date = url_tag.find("news:publication_date")
            if news_date and news_date.text:
                # 2026-07-26T21:49:23.452Z -> 北京时间 2026-07-27 05:49
                iso = news_date.text.strip()
                try:
                    from datetime import datetime, timezone, timedelta

                    BEIJING = timezone(timedelta(hours=8))
                    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                    published = dt.astimezone(BEIJING).strftime("%Y-%m-%d %H:%M")
                except (ValueError, TypeError):
                    # 回退：直接拼接
                    published = iso[:10] + " " + iso[11:16]

            if not title or not loc:
                continue

            # 从 URL 路径提取板块标签（World / Business）
            extra: dict = {}
            path = urlparse(loc).path
            m = re.match(r"^/(world|business)/", path)
            if m:
                extra["section"] = m.group(1).capitalize()

            # 尝试从 news sitemap 的 <image:image><image:loc> 提取主图
            # 部分文章 sitemap 不带图片，提取失败则不写入 extra.image
            img_tag = url_tag.find("image:image")
            if img_tag:
                img_loc = img_tag.find("image:loc")
                if img_loc and img_loc.text:
                    extra["image"] = img_loc.text.strip()

            items.append(
                NewsItem(
                    title=title,
                    url=loc,
                    published=published,
                    source=self.source.name,
                    extra=extra,
                )
            )
        return items

"""人民日报电子版抓取器（适配新版 PC 端结构）。

新版 URL 结构：
  - 版面索引页: http://paper.people.com.cn/rmrb/pc/layout/index.html
    含 #list 元素，版面链接为相对路径: YYYYMM/DD/node_NN.html
  - 版面页: http://paper.people.com.cn/rmrb/pc/layout/YYYYMM/DD/node_NN.html
    文章链接为相对路径: ../../../content/YYYYMM/DD/content_XXXXXXXX.html
  - 文章页: http://paper.people.com.cn/rmrb/pc/content/YYYYMM/DD/content_XXXXXXXX.html

索引页显示的总是最新一期报纸（当日未发布则显示昨日）。
每篇文章附带版面名标签（如 "01版：要闻"）。
"""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, NewsItem


class PeopleDailyScraper(BaseScraper):
    """抓取人民日报最新一期各版面文章，并为每篇文章标注所属版面。"""

    INDEX_URL = "http://paper.people.com.cn/rmrb/pc/layout/index.html"
    LAYOUT_BASE = "http://paper.people.com.cn/rmrb/pc/layout/"

    def fetch(self) -> list[NewsItem]:
        # 1. 抓取版面索引页
        index_html = self.http.get_text(self.INDEX_URL)
        index_soup = BeautifulSoup(index_html, "lxml")

        # 2. 从 #list 提取版面链接和版面名映射
        section_paths, section_map, published_date = self._extract_sections(index_soup)
        if not section_paths:
            raise RuntimeError("人民日报索引页未找到版面链接")

        # 3. 遍历各版面，提取文章（带版面名标签；副刊与广告一并收录）
        items: list[NewsItem] = []
        for rel_path in section_paths:
            sec_label = section_map.get(rel_path, "")
            try:
                sec_url = urljoin(self.LAYOUT_BASE, rel_path)
                sec_html = self.http.get_text(sec_url)
                sec_soup = BeautifulSoup(sec_html, "lxml")
                section_name = sec_label
                articles = self._extract_articles(sec_soup, sec_url, section_name)
                items.extend(articles)
                if len(items) >= self.limit:
                    break
            except Exception:
                continue

        # 去重并设置 published（人民日报每日 06:00 上版，用 06:00 作为发布时间占位）
        published_str = f"{published_date} 06:00" if published_date else None
        seen: set[str] = set()
        unique: list[NewsItem] = []
        for it in items:
            if it.url not in seen:
                seen.add(it.url)
                it.published = published_str
                it.source = self.source.name
                unique.append(it)

        if not unique:
            raise RuntimeError(f"人民日报 {published_date} 未提取到任何文章")

        return unique[: self.limit]

    def _extract_sections(self, soup: BeautifulSoup):
        """从索引页 #list 提取版面链接和版面名映射。

        Returns:
            (版面相对路径列表, {相对路径: 版面名}, 报纸日期 YYYY-MM-DD)
            版面名格式: "01版：要闻"
        """
        list_el = soup.find(id="list")
        section_map: dict[str, str] = {}
        links: list[str] = []

        if list_el:
            for a in list_el.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                # text 形如 "第01版 要闻" -> 转换为 "01版：要闻"
                m = re.match(r"第?(\d+)版\s*(\S+)", text)
                if m:
                    section_map[href] = f"{int(m.group(1)):02d}版：{m.group(2)}"
                else:
                    section_map[href] = text
                links.append(href)
        else:
            # 回退：查找所有形如 YYYYMM/DD/node_NN.html 的链接
            for a in soup.find_all("a", href=True):
                if re.match(r"\d{6}/\d{2}/node_\d+\.html", a["href"]):
                    href = a["href"]
                    section_map[href] = ""
                    links.append(href)

        # 去重保持顺序
        seen: set[str] = set()
        unique_links: list[str] = []
        for l in links:
            if l not in seen:
                seen.add(l)
                unique_links.append(l)

        # 从路径推断日期: YYYYMM/DD
        published_date = None
        if unique_links:
            m = re.match(r"(\d{4})(\d{2})/(\d{2})/", unique_links[0])
            if m:
                published_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        return unique_links, section_map, published_date

    def _extract_articles(
        self, soup: BeautifulSoup, base_url: str, section_name: str = ""
    ) -> list[NewsItem]:
        """从版面页提取文章标题与链接，并附带版面名标签。

        用户偏好（2026-08-25）：跳过广告和副刊版面（不要广告、不要副刊/文学副刊）。
        """
        items: list[NewsItem] = []
        # 用户偏好：广告 / 副刊 类版面整体跳过
        if any(kw in section_name for kw in ("广告", "副刊")):
            return items
        # 文章链接形如 content_XXXXXXXX.html
        pattern = re.compile(r"content_\d+\.html")

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not pattern.search(href):
                continue
            title = a.get_text(strip=True)
            if not title or len(title) < 2:
                continue
            # 防御性过滤：版面页里"本版责编：xxx"等编辑署名信息不应作为文章标题
            if "责编" in title:
                continue
            url = urljoin(base_url, href)
            extra = {"section": section_name} if section_name else {}
            items.append(NewsItem(title=title, url=url, source=self.source.name, extra=extra))
        # 人民日报版面页右侧文章列表的视觉顺序与 DOM 顺序相反
        # （DOM 从底部往顶排列），按视觉顺序倒序输出
        items.reverse()
        return items
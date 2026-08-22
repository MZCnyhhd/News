"""GitHub Trending 抓取器。"""
from __future__ import annotations

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, NewsItem


class GitHubTrendingScraper(BaseScraper):
    """抓取 GitHub Trending 仓库列表。"""

    TRENDING_URL = "https://github.com/trending"

    def fetch(self) -> list[NewsItem]:
        html = self.http.get_text(self.TRENDING_URL)
        soup = BeautifulSoup(html, "lxml")
        items: list[NewsItem] = []

        for article in soup.select("article.Box-row"):
            # 仓库链接在 h2 > a
            h2 = article.find("h2")
            if not h2:
                continue
            a = h2.find("a")
            if not a or not a.get("href"):
                continue
            href = a["href"].strip()
            if href.startswith("/"):
                href = href.lstrip("/")
            repo_path = href  # owner/repo
            if not repo_path:
                continue
            url = urljoin("https://github.com/", repo_path)

            # 描述（可选）
            desc_tag = article.find("p")
            desc = desc_tag.get_text(strip=True) if desc_tag else ""

            # 今日 stars（可选）
            stars_today = ""
            for span in article.select("span.d-inline-block"):
                text = span.get_text(strip=True)
                if "stars today" in text or "stars this week" in text or "stars this month" in text:
                    stars_today = text
                    break

            extra = {"desc": desc} if desc else {}
            if stars_today:
                extra["trend"] = stars_today

            items.append(
                NewsItem(
                    title=repo_path,
                    url=url,
                    published=None,  # trending 无发布时间
                    source=self.source.name,
                    extra=extra,
                )
            )
            if len(items) >= self.limit:
                break

        return items

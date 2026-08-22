"""Hugging Face Models trending 抓取器。"""
from __future__ import annotations

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, NewsItem


class HfModelsScraper(BaseScraper):
    """抓取 Hugging Face Models trending 列表。"""

    MODELS_URL = "https://huggingface.co/models?sort=trending"

    def fetch(self) -> list[NewsItem]:
        html = self.http.get_text(self.MODELS_URL)
        soup = BeautifulSoup(html, "lxml")
        items: list[NewsItem] = []
        seen_urls: set[str] = set()

        # HF 模型卡片是 <a> 标签，href 形如 /<org>/<model>
        # SSR 中模型链接通常在 group 类或直接是 /org/model 的链接
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # 模型链接形如 /org/model-name（不含 /models、/datasets 等前缀）
            if href.startswith("/models") or href.startswith("/datasets") or href.startswith("/spaces"):
                continue
            if href.startswith("/"):
                pass  # 相对路径，继续
            else:
                continue  # 绝对路径或外部链接，跳过
            # 排除导航类链接
            if href in ("/", "/login", "/signup", "/models", "/datasets", "/spaces"):
                continue
            # 模型路径通常有两段：/org/model
            parts = href.strip("/").split("/")
            if len(parts) < 2:
                continue
            # 排除含查询参数或锚点的
            if "?" in href or "#" in href:
                continue

            full_url = urljoin("https://huggingface.co", href)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # 显示名取最后一段
            display_name = parts[-1]
            # 从链接文本提取更多信息
            link_text = a.get_text(strip=True)
            if link_text and len(link_text) > len(display_name):
                display_name = parts[0] + "/" + parts[-1] if len(parts) >= 2 else display_name

            items.append(
                NewsItem(
                    title=parts[0] + "/" + parts[-1],
                    url=full_url,
                    published=None,
                    source=self.source.name,
                )
            )
            if len(items) >= self.limit:
                break

        return items

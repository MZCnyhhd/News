"""基础数据结构与抓取器抽象基类。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class NewsItem:
    """单条新闻条目。"""

    title: str
    url: str
    published: Optional[str] = None  # 规范化后的展示字符串，如 "2026-07-23"
    source: str = ""  # 源显示名
    extra: dict = field(default_factory=dict)  # 可选：作者/分数/stars 等补充信息
    comment: str = ""  # 「我的点评」：运营者人工撰写，默认空（抓取阶段不填）


@dataclass
class SourceConfig:
    """单个信息源的配置。"""

    id: str  # 唯一 ID，如 "openai_news"
    name: str  # 显示名，如 "OpenAI News"
    category: str  # 顶级分类：international / china / tech_global / ai
    subcategory: Optional[str]  # AI 子分类：labs / papers / opensource / eval
    url: str  # 源主页（展示/可点击）
    scraper_type: str  # feed / html / people_daily / github / hf_models / leaderboard
    feed_url: Optional[str] = None  # feed/atom/api 入口
    parser: Optional[str] = None  # HTML 解析器标识（html 类型必填）
    limit: int = 15
    enabled: bool = True


class BaseScraper(ABC):
    """抓取器抽象基类。"""

    def __init__(self, source: SourceConfig, http, limit: Optional[int] = None):
        self.source = source
        self.http = http
        # 修复 bug：默认值 None（不再硬编码 15），否则 source.limit 永远被忽略
        self.limit = source.limit if limit is None else limit

    @abstractmethod
    def fetch(self) -> list[NewsItem]:
        """抓取条目；失败时抛异常，由主流程捕获（不影响其他源）。"""
        ...

    def _truncate(self, items: list[NewsItem]) -> list[NewsItem]:
        """按 limit 截断条目数量。"""
        return items[: self.limit]

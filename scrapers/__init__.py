"""抓取器工厂：根据 source.scraper_type 构建对应抓取器实例。"""
from __future__ import annotations

from scrapers.base import BaseScraper, SourceConfig
from scrapers.cn_sources import CnSourceScraper
from scrapers.feed_scraper import FeedScraper
from scrapers.github_trending import GitHubTrendingScraper
from scrapers.hf_models import HfModelsScraper
from scrapers.html_scraper import HtmlScraper
from scrapers.leaderboards import LeaderboardScraper
from scrapers.people_daily import PeopleDailyScraper
from scrapers.reuters_sitemap import ReutersSitemapScraper


def build_scraper(source: SourceConfig, http) -> BaseScraper:
    """根据 source.scraper_type 返回对应的抓取器实例。"""
    t = source.scraper_type
    if t == "feed":
        return FeedScraper(source, http)
    if t == "html":
        return HtmlScraper(source, http)
    if t == "people_daily":
        return PeopleDailyScraper(source, http)
    if t == "github":
        return GitHubTrendingScraper(source, http)
    if t == "hf_models":
        return HfModelsScraper(source, http)
    if t == "leaderboard":
        return LeaderboardScraper(source, http)
    if t == "reuters_sitemap":
        return ReutersSitemapScraper(source, http)
    if t == "cn_source":
        return CnSourceScraper(source, http)
    raise ValueError(f"未知 scraper_type: {t}")


__all__ = [
    "build_scraper",
    "BaseScraper",
    "SourceConfig",
    "FeedScraper",
    "HtmlScraper",
    "PeopleDailyScraper",
    "GitHubTrendingScraper",
    "HfModelsScraper",
    "LeaderboardScraper",
    "ReutersSitemapScraper",
    "CnSourceScraper",
]

"""模型评测榜单抓取器（Chatbot Arena + Artificial Analysis）。

主路径：从榜单页 SSR 响应中提取内嵌的模型数据。
回退路径：使用社区维护的每日 JSON 快照（GitHub raw，稳定可靠）。
"""
from __future__ import annotations

import json
import re
from datetime import datetime

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, NewsItem


# 社区维护的每日快照（稳定回退源）
SNAPSHOTS = {
    "chatbot_arena": "https://raw.githubusercontent.com/oolong-tea-2026/arena-ai-leaderboards/main/data/latest.json",
    "artificial_analysis": "https://raw.githubusercontent.com/oolong-tea-2026/artificial-analysis-leaderboards/main/data/latest.json",
}

# 榜单主页
LEADERBOARD_PAGES = {
    "chatbot_arena": "https://lmarena.ai/leaderboard",
    "artificial_analysis": "https://artificialanalysis.ai/leaderboards/models",
}


class LeaderboardScraper(BaseScraper):
    """抓取模型评测榜单 Top N。"""

    def fetch(self) -> list[NewsItem]:
        parser = self.source.parser
        if parser not in SNAPSHOTS:
            raise ValueError(f"未知榜单 parser: {parser}")

        today = datetime.now().strftime("%Y-%m-%d %H:%M")
        models: list[dict] = []

        # 1. 尝试主路径：从榜单页提取
        try:
            models = self._extract_from_page(parser)
        except Exception:
            models = []  # 主路径失败，走回退

        # 2. 回退：社区快照
        if not models:
            try:
                models = self._fetch_snapshot(parser)
            except Exception:
                models = []

        # 3. 最终回退：生成榜单页面链接条目（保证报告有内容）
        if not models:
            return self._fallback_link(parser, today)

        items: list[NewsItem] = []
        for i, m in enumerate(models[: self.limit], start=1):
            name = m.get("name") or m.get("model") or m.get("model_name") or "未知"
            score = m.get("score") or m.get("arena_score") or m.get("elo") or m.get("quality_score")
            vendor = m.get("vendor") or m.get("organization") or m.get("org") or ""
            url = m.get("url") or m.get("link") or self.source.url

            title_parts = [f"#{i}", name]
            if score is not None:
                title_parts.append(f"— {score}")
            title = " ".join(title_parts)

            extra = {"vendor": vendor} if vendor else {}
            if m.get("votes") or m.get("num_votes"):
                extra["votes"] = m.get("votes") or m.get("num_votes")

            items.append(
                NewsItem(
                    title=title,
                    url=url,
                    published=today,  # 榜单无单条日期，用快照日
                    source=self.source.name,
                    extra=extra,
                )
            )
        return items

    def _fallback_link(self, parser: str, today: str) -> list[NewsItem]:
        """最终回退：生成榜单页面链接条目。"""
        label = "Chatbot Arena" if parser == "chatbot_arena" else "Artificial Analysis"
        return [
            NewsItem(
                title=f"查看 {label} 实时榜单（点击访问）",
                url=LEADERBOARD_PAGES[parser],
                published=today,
                source=self.source.name,
                extra={"note": "实时榜单需访问页面查看"},
            )
        ]

    def _extract_from_page(self, parser: str) -> list[dict]:
        """从榜单页 SSR 响应中提取模型数据。"""
        page_url = LEADERBOARD_PAGES[parser]
        html = self.http.get_text(page_url)
        soup = BeautifulSoup(html, "lxml")

        # 策略1：Next.js __NEXT_DATA__
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                data = json.loads(next_data.string)
                models = _find_models_in_obj(data, parser)
                if models:
                    return models
            except (json.JSONDecodeError, ValueError):
                pass

        # 策略2：查找所有 <script> 中的 JSON 数组/对象，匹配含模型字段的
        for script in soup.find_all("script"):
            text = script.string or ""
            if not text:
                continue
            # 尝试解析为 JSON
            try:
                data = json.loads(text)
                models = _find_models_in_obj(data, parser)
                if models:
                    return models
            except (json.JSONDecodeError, ValueError):
                continue

        # 策略3：正则匹配 JSON 片段中含 "name"/"model" 和 score 字段的对象
        patterns = [
            r'\{[^{}]*"(?:name|model|model_name)"\s*:\s*"[^"]+"[^{}]*"(?:score|arena_score|elo|quality_score)"\s*:\s*[\d.]+[^{}]*\}',
            r'\{[^{}]*"(?:score|arena_score|elo|quality_score)"\s*:\s*[\d.]+[^{}]*"(?:name|model|model_name)"\s*:\s*"[^"]+"[^{}]*\}',
        ]
        for pat in patterns:
            matches = re.findall(pat, html)
            if len(matches) >= 3:  # 至少匹配 3 个才算有效
                models = []
                for m_str in matches:
                    try:
                        models.append(json.loads(m_str))
                    except json.JSONDecodeError:
                        continue
                if models:
                    return models

        return []

    def _fetch_snapshot(self, parser: str) -> list[dict]:
        """从社区快照 JSON 提取模型数据。"""
        url = SNAPSHOTS[parser]
        text = self.http.get_text(url)
        data = json.loads(text)

        # 快照结构：chatbot_arena 快照可能含 text.json；artificial_analysis 含 llms.json
        # 也可能是直接的模型数组或带 models/leaderboard 字段的对象
        candidates = []
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict):
            # 尝试常见字段名
            for key in ("models", "leaderboard", "data", "llms", "text", "results", "items"):
                if key in data and isinstance(data[key], list):
                    candidates = data[key]
                    break
            # chatbot_arena 快照特殊结构：可能含 text.json 作为嵌套字符串
            if not candidates:
                for key in ("text", "text_json", "raw"):
                    if key in data and isinstance(data[key], str):
                        try:
                            nested = json.loads(data[key])
                            if isinstance(nested, list):
                                candidates = nested
                            elif isinstance(nested, dict):
                                for k2 in ("models", "leaderboard", "data", "llms"):
                                    if k2 in nested and isinstance(nested[k2], list):
                                        candidates = nested[k2]
                                        break
                        except json.JSONDecodeError:
                            pass
                    if candidates:
                        break

        # 过滤掉非字典或无名字段的条目
        valid = [
            m
            for m in candidates
            if isinstance(m, dict)
            and (m.get("name") or m.get("model") or m.get("model_name"))
        ]
        # 按 score 降序排序（如果有）
        def _score(m):
            for k in ("score", "arena_score", "elo", "quality_score"):
                if k in m and isinstance(m[k], (int, float)):
                    return float(m[k])
            return 0.0

        valid.sort(key=_score, reverse=True)
        return valid


def _find_models_in_obj(obj, parser: str) -> list[dict]:
    """递归在 JSON 对象中查找模型列表（含 name/model + score 字段的字典数组）。"""
    if isinstance(obj, list):
        # 检查是否是模型数组
        if obj and all(isinstance(x, dict) for x in obj[:3]):
            has_name = any(
                x.get("name") or x.get("model") or x.get("model_name")
                for x in obj[:10]
            )
            has_score = any(
                any(k in x for k in ("score", "arena_score", "elo", "quality_score"))
                for x in obj[:10]
            )
            if has_name and has_score:
                return obj
        # 否则递归
        for item in obj:
            result = _find_models_in_obj(item, parser)
            if result:
                return result
    elif isinstance(obj, dict):
        for value in obj.values():
            result = _find_models_in_obj(value, parser)
            if result:
                return result
    return []

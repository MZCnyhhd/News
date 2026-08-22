"""新闻日报收集器主入口。

用法：
    python run.py             # 直接抓取所有源并生成日报
    python run.py --from-db   # 从实时收集库（data/news.db）读当天数据生成日报，不抓取

功能：
    1. 遍历 config.SOURCES 中所有信息源
    2. 每个源独立抓取（单源失败不影响整体）
    3. 生成 HTML 报告到 outputs/YYYY-MM-DD.html
    4. 写运行日志到 logs/collector_YYYY-MM-DD.log
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

# 确保项目根目录在 sys.path 中（支持从任意位置运行）
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import SOURCES, CATEGORY_TREE  # noqa: E402
from http_utils import HttpClient  # noqa: E402
from renderer import render_report  # noqa: E402
from scrapers import build_scraper  # noqa: E402


def setup_logging(log_dir: Path) -> logging.Logger:
    """配置日志，同时输出到文件和控制台。"""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"collector_{date.today().isoformat()}.log"

    logger = logging.getLogger("news_daily")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def main_from_db() -> int:
    """从实时收集库读当天条目生成日报（不重复抓取）。"""
    from scrapers.base import NewsItem
    import storage

    logger = setup_logging(PROJECT_ROOT / "logs")
    today_str = date.today().isoformat()
    logger.info("从数据库生成日报 · %s", today_str)

    conn = storage.get_conn()
    try:
        rows = storage.get_items(conn, today_str)
    finally:
        conn.close()

    if not rows:
        print(f"✗ 数据库中没有 {today_str} 的数据（实时收集服务是否在运行？）")
        return 1

    by_section: dict[tuple, list] = defaultdict(list)
    sources_seen: set[str] = set()
    for r in rows:
        item = NewsItem(
            title=r["title"], url=r["url"], published=r["published"],
            source=r["source_name"], extra=r.get("extra") or {},
        )
        by_section[(r["category"], r["subcategory"])].append(item)
        sources_seen.add(r["source_id"])

    # 每个分区按发布时间倒序（无时间的排后面）
    for key in by_section:
        by_section[key].sort(key=lambda it: it.published or "", reverse=True)

    html = render_report(
        report_date=date.today(),
        by_section=by_section,
        errors=[],
        total_sources=len(SOURCES),
        success_count=len(sources_seen),
    )
    output_dir = PROJECT_ROOT / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{today_str}.html"
    output_file.write_text(html, encoding="utf-8")

    total_items = sum(len(v) for v in by_section.values())
    logger.info("完成: 共 %d 条（来自 %d 个源的库存数据）", total_items, len(sources_seen))
    print(f"\n✓ 日报已从数据库生成: {output_file}")
    print(f"  共 {total_items} 条新闻 | 数据覆盖 {len(sources_seen)} 个源\n")
    return 0


def main() -> int:
    logger = setup_logging(PROJECT_ROOT / "logs")
    logger.info("=" * 60)
    logger.info("新闻日报收集开始 · %s", date.today().isoformat())
    logger.info("共 %d 个信息源", len(SOURCES))
    logger.info("=" * 60)

    http = HttpClient()
    by_section: dict[tuple, list] = defaultdict(list)
    errors: list[tuple[str, str]] = []
    success_count = 0

    for src in SOURCES:
        if not src.enabled:
            logger.info("[SKIP] %s (已禁用)", src.name)
            continue
        try:
            logger.info("[FETCH] %s ...", src.name)
            scraper = build_scraper(src, http)
            items = scraper.fetch()
            # 当日过滤：只保留发布日期为今天（北京时间）的条目
            # 无 published 的源（如 GitHub Trending）默认保留
            today_str = date.today().isoformat()
            filtered = []
            for it in items:
                if it.published is None:
                    # 无发布时间（如 trending），默认保留
                    filtered.append(it)
                elif it.published[:10] == today_str:
                    # 发布时间在北京时间的今天
                    filtered.append(it)
            if len(filtered) != len(items):
                logger.info("[FILTER] %s: %d -> %d 条（当日过滤）",
                            src.name, len(items), len(filtered))
            items = filtered
            by_section[(src.category, src.subcategory)].extend(items)
            success_count += 1
            logger.info("[  OK ] %s: %d 条", src.name, len(items))
        except Exception as e:
            err_msg = str(e)
            if len(err_msg) > 200:
                err_msg = err_msg[:200] + "..."
            errors.append((src.name, err_msg))
            logger.error("[FAIL] %s: %s", src.name, err_msg)

    # 生成 HTML 报告
    logger.info("-" * 60)
    logger.info("开始生成 HTML 报告...")

    html = render_report(
        report_date=date.today(),
        by_section=by_section,
        errors=errors,
        total_sources=len(SOURCES),
        success_count=success_count,
    )

    output_dir = PROJECT_ROOT / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{date.today().isoformat()}.html"
    output_file.write_text(html, encoding="utf-8")

    total_items = sum(len(v) for v in by_section.values())
    logger.info("=" * 60)
    logger.info("完成: 共 %d 条新闻, %d/%d 源成功, %d 源失败",
                total_items, success_count, len(SOURCES), len(errors))
    logger.info("报告: %s", output_file)
    logger.info("=" * 60)

    print()
    print(f"✓ 新闻日报已生成: {output_file}")
    print(f"  共 {total_items} 条新闻 | {success_count}/{len(SOURCES)} 源成功 | {len(errors)} 源失败")
    if errors:
        print(f"  失败源: {', '.join(name for name, _ in errors)}")
    print()

    return 0 if success_count > 0 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-db", action="store_true",
                    help="从实时收集库读当天数据生成日报（不抓取）")
    args = ap.parse_args()
    sys.exit(main_from_db() if args.from_db else main())

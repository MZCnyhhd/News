"""HTML 报告渲染器（Jinja2 内联模板，light 主题）。"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from jinja2 import Environment, select_autoescape

from config import CATEGORY_TREE


TEMPLATE_STR = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>新闻日报 · {{ report_date }}</title>
<style>
  :root {
    --bg: #f5f7fa;
    --card-bg: #ffffff;
    --primary: #2563eb;
    --primary-light: #eff6ff;
    --text: #1f2937;
    --text-muted: #6b7280;
    --text-light: #9ca3af;
    --border: #e5e7eb;
    --shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
    --shadow-hover: 0 4px 6px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
    --badge-bg: #eef2ff;
    --badge-text: #4f46e5;
    --error-bg: #fef2f2;
    --error-text: #991b1b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
                 "Hiragino Sans GB", "Microsoft YaHei", "Helvetica Neue", Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
  }
  .container { max-width: 1100px; margin: 0 auto; padding: 24px 20px 60px; }

  /* 顶部头部 */
  header {
    background: var(--card-bg);
    border-radius: 12px;
    padding: 28px 32px;
    margin-bottom: 24px;
    box-shadow: var(--shadow);
    border-left: 5px solid var(--primary);
  }
  header h1 {
    font-size: 28px;
    font-weight: 700;
    color: var(--text);
    margin-bottom: 12px;
  }
  header h1 .date { color: var(--primary); }
  .meta {
    display: flex;
    flex-wrap: wrap;
    gap: 18px;
    font-size: 14px;
    color: var(--text-muted);
  }
  .meta span { display: inline-flex; align-items: center; }
  .meta strong { color: var(--text); font-weight: 600; margin-right: 4px; }

  /* 分类区块 */
  .category {
    background: var(--card-bg);
    border-radius: 12px;
    padding: 24px 28px;
    margin-bottom: 20px;
    box-shadow: var(--shadow);
  }
  .category-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
    padding-bottom: 12px;
    border-bottom: 2px solid var(--primary-light);
  }
  .category-header h2 {
    font-size: 20px;
    font-weight: 700;
    color: var(--text);
  }
  .category-header .count {
    font-size: 13px;
    color: var(--text-muted);
    background: var(--primary-light);
    padding: 2px 10px;
    border-radius: 12px;
  }

  /* 子分类 */
  .subcategory {
    margin-top: 18px;
  }
  .subcategory:first-child { margin-top: 0; }
  .subcategory-title {
    font-size: 15px;
    font-weight: 600;
    color: var(--primary);
    margin-bottom: 10px;
    padding-left: 10px;
    border-left: 3px solid var(--primary);
  }

  /* 条目列表 */
  .items { list-style: none; }
  .item {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid var(--border);
  }
  .item:last-child { border-bottom: none; }
  .item-num {
    flex-shrink: 0;
    width: 24px;
    height: 24px;
    border-radius: 50%;
    background: var(--badge-bg);
    color: var(--badge-text);
    font-size: 12px;
    font-weight: 600;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-top: 2px;
  }
  .item-body { flex: 1; min-width: 0; }
  .item-title {
    font-size: 15px;
    font-weight: 500;
    line-height: 1.5;
  }
  .item-title a {
    color: var(--text);
    text-decoration: none;
    transition: color 0.15s;
  }
  .item-title a:hover { color: var(--primary); }
  .item-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-top: 4px;
    font-size: 12px;
    color: var(--text-light);
  }
  .item-meta .source {
    color: var(--badge-text);
    background: var(--badge-bg);
    padding: 1px 7px;
    border-radius: 4px;
    font-weight: 500;
  }
  .item-meta .extra { color: var(--text-muted); font-style: italic; }
  .item-meta .section-badge {
    color: #059669;
    background: #ecfdf5;
    padding: 1px 7px;
    border-radius: 4px;
    font-weight: 600;
    border: 1px solid #a7f3d0;
  }

  /* 空状态 */
  .empty {
    text-align: center;
    padding: 20px;
    color: var(--text-light);
    font-size: 14px;
    font-style: italic;
  }

  /* 错误折叠区 */
  details.errors {
    margin-top: 28px;
    background: var(--error-bg);
    border-radius: 10px;
    padding: 14px 20px;
    border: 1px solid #fecaca;
  }
  details.errors summary {
    cursor: pointer;
    font-size: 14px;
    font-weight: 600;
    color: var(--error-text);
    user-select: none;
  }
  details.errors ul {
    margin-top: 10px;
    padding-left: 20px;
    font-size: 13px;
    color: var(--error-text);
  }
  details.errors li { margin-bottom: 4px; }

  /* 页脚 */
  footer {
    text-align: center;
    margin-top: 32px;
    font-size: 12px;
    color: var(--text-light);
  }

  /* 响应式 */
  @media (max-width: 640px) {
    .container { padding: 16px 12px 40px; }
    header { padding: 20px; }
    header h1 { font-size: 22px; }
    .category { padding: 18px 16px; }
    .item { gap: 8px; }
  }
</style>
</head>
<body>
<div class="container">

  <header>
    <h1>新闻日报 · <span class="date">{{ report_date }}</span></h1>
    <div class="meta">
      <span><strong>共</strong> {{ total_items }} 条新闻</span>
      <span><strong>来源</strong> {{ success_count }}/{{ total_sources }} 个源成功</span>
      <span><strong>生成于</strong> {{ generated_at }}</span>
    </div>
  </header>

  {% for cat_key, cat_name, subs in category_tree %}
  {% set cat_items = sections_by_category.get(cat_key, []) %}
  <section class="category">
    <div class="category-header">
      <h2>{{ cat_name }}</h2>
      <span class="count">{{ cat_total_counts.get(cat_key, 0) }} 条</span>
    </div>

    {% if subs is none %}
      {# 无子分类，直接列出条目 #}
      {% if cat_items %}
      <ul class="items">
        {% for item in cat_items %}
        <li class="item">
          <span class="item-num">{{ loop.index }}</span>
          <div class="item-body">
            <div class="item-title"><a href="{{ item.url }}" target="_blank" rel="noopener">{{ item.title }}</a></div>
            <div class="item-meta">
              <span class="source">{{ item.source }}</span>
              {% if item.extra and item.extra.section %}<span class="section-badge">{{ item.extra.section }}</span>{% endif %}
              {% if item.published %}<span>{{ item.published }}</span>{% endif %}
              {% if item.extra %}<span class="extra">{{ item.extra | format_extra }}</span>{% endif %}
            </div>
          </div>
        </li>
        {% endfor %}
      </ul>
      {% else %}
      <div class="empty">暂无数据</div>
      {% endif %}

    {% else %}
      {# 有子分类，按子分类分组 #}
      {% for sub_key, sub_name in subs %}
      {% set sub_items = sections_by_subcategory.get((cat_key, sub_key), []) %}
      <div class="subcategory">
        <div class="subcategory-title">{{ sub_name }} · {{ sub_items | length }} 条</div>
        {% if sub_items %}
        <ul class="items">
          {% for item in sub_items %}
          <li class="item">
            <span class="item-num">{{ loop.index }}</span>
            <div class="item-body">
              <div class="item-title"><a href="{{ item.url }}" target="_blank" rel="noopener">{{ item.title }}</a></div>
              <div class="item-meta">
                <span class="source">{{ item.source }}</span>
                {% if item.published %}<span>{{ item.published }}</span>{% endif %}
                {% if item.extra %}<span class="extra">{{ item.extra | format_extra }}</span>{% endif %}
              </div>
            </div>
          </li>
          {% endfor %}
        </ul>
        {% else %}
        <div class="empty">暂无数据</div>
        {% endif %}
      </div>
      {% endfor %}
    {% endif %}
  </section>
  {% endfor %}

  {% if errors %}
  <details class="errors">
    <summary>采集异常（{{ errors | length }} 个源失败）</summary>
    <ul>
      {% for name, err in errors %}
      <li><strong>{{ name }}</strong>：{{ err }}</li>
      {% endfor %}
    </ul>
  </details>
  {% endif %}

  <footer>
    数据来自各源官网与公开页面，版权归原作者所有 · 新闻日报收集器自动生成
  </footer>

</div>
</body>
</html>
"""


def _format_extra(extra: dict) -> str:
    """将 extra dict 格式化为展示字符串。"""
    if not extra:
        return ""
    parts = []
    if "desc" in extra and extra["desc"]:
        parts.append(str(extra["desc"])[:80])
    if "trend" in extra and extra["trend"]:
        parts.append(extra["trend"])
    if "stars" in extra and extra["stars"]:
        parts.append(f"★ {extra['stars']}")
    if "upvotes" in extra and extra["upvotes"]:
        parts.append(f"▲ {extra['upvotes']}")
    if "vendor" in extra and extra["vendor"]:
        parts.append(extra["vendor"])
    if "votes" in extra and extra["votes"]:
        parts.append(f"votes {extra['votes']}")
    return " · ".join(parts)


def render_report(
    report_date: date,
    by_section: dict,
    errors: list,
    total_sources: int,
    success_count: int,
) -> str:
    """渲染 HTML 报告。

    Args:
        report_date: 报告日期
        by_section: key=(category, subcategory), value=list[NewsItem]
        errors: [(source_name, error_msg), ...]
        total_sources: 总源数
        success_count: 成功源数
    """
    # 按顶级分类聚合（无子分类的分类）
    sections_by_category: dict[str, list] = {}
    # 按 (category, subcategory) 聚合（有子分类的分类）
    sections_by_subcategory: dict[tuple, list] = {}

    for (cat, sub), items in by_section.items():
        if sub is None:
            sections_by_category.setdefault(cat, []).extend(items)
        else:
            sections_by_subcategory.setdefault((cat, sub), []).extend(items)

    # 计算每个顶级分类的总条数（含子分类）
    cat_total_counts: dict[str, int] = {}
    for (cat, _sub), items in by_section.items():
        cat_total_counts[cat] = cat_total_counts.get(cat, 0) + len(items)

    total_items = sum(len(v) for v in by_section.values())

    env = Environment(autoescape=select_autoescape(["html", "xml"]))
    env.filters["format_extra"] = _format_extra
    template = env.from_string(TEMPLATE_STR)

    html = template.render(
        report_date=report_date.isoformat(),
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        total_items=total_items,
        total_sources=total_sources,
        success_count=success_count,
        category_tree=CATEGORY_TREE,
        sections_by_category=sections_by_category,
        sections_by_subcategory=sections_by_subcategory,
        cat_total_counts=cat_total_counts,
        errors=errors,
    )
    return html

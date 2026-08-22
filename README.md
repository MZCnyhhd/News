# 新闻日报收集器

自动抓取 20 个信息源，生成每日 HTML 新闻日报。

## 信息源（4 大分类，20 个源）

### 国际新闻
- **Reuters**（World + Business 板块，通过 sitemap 抓取）

### 中国新闻
- **人民日报**（电子版，遍历当日各版面）

### 科技 · 全球
- **MIT Technology Review**（RSS）

### 科技 · AI

**AI 实验室动态（9 个源）**
| 源 | 抓取方式 |
|----|---------|
| OpenAI News | RSS |
| OpenAI Research | RSS（与 News 共享 feed） |
| Anthropic News | HTML 解析 |
| Google DeepMind | RSS |
| Google Research | RSS |
| Meta AI | Google News RSS 代理（直连被反爬） |
| Microsoft AI Blog | HTML 解析 |
| NVIDIA AI Blog | RSS |
| Hugging Face Blog | RSS |

**学术论文（4 个源）**
| 源 | 抓取方式 |
|----|---------|
| arXiv cs.LG | Atom API |
| arXiv cs.CL | Atom API |
| arXiv cs.CV | Atom API |
| Hugging Face Papers | HTML 解析（替代已失效的 Papers with Code） |

**开源生态（2 个源）**
| 源 | 抓取方式 |
|----|---------|
| GitHub Trending | HTML 解析 |
| Hugging Face Models | HTML 解析 |

**模型评测（2 个源）**
| 源 | 抓取方式 |
|----|---------|
| Chatbot Arena | SSR 内嵌 JSON 提取，回退为榜单链接 |
| Artificial Analysis | SSR 内嵌 JSON 提取，回退为榜单链接 |

## 快速开始

### 1. 安装依赖

```bash
# 使用托管版 Python 3.13 创建 venv
C:/Users/MZCny/.workbuddy/binaries/python/versions/3.13.12/python.exe -m venv C:/Users/MZCny/.workbuddy/binaries/python/envs/default

# 安装依赖
C:/Users/MZCny/.workbuddy/binaries/python/envs/default/Scripts/python.exe -m pip install --no-cache-dir -r requirements.txt
```

### 2. 运行收集器

```bash
cd E:/ProjectAgenticWorkspace/WorkBuddy/新闻日报
C:/Users/MZCny/.workbuddy/binaries/python/envs/default/Scripts/python.exe run.py
```

### 3. 查看报告

报告生成在 `outputs/YYYY-MM-DD.html`，用浏览器打开即可。

## 输出

- **HTML 报告**：`outputs/YYYY-MM-DD.html`
- **运行日志**：`logs/collector_YYYY-MM-DD.log`

## 项目结构

```
新闻日报/
├── run.py                  # 主入口
├── config.py               # 20 源配置 + 分类树
├── http_utils.py           # HTTP 客户端（浏览器 UA、重试、延时）
├── renderer.py             # HTML 报告渲染器（Jinja2）
├── requirements.txt
├── scrapers/
│   ├── __init__.py         # 抓取器工厂
│   ├── base.py             # 数据结构 + 抽象基类
│   ├── feed_scraper.py     # RSS/Atom 抓取（12 源）
│   ├── html_scraper.py     # HTML 解析（Anthropic/Microsoft AI/HF Papers）
│   ├── reuters_sitemap.py  # Reuters sitemap 抓取（World+Business）
│   ├── people_daily.py     # 人民日报电子版
│   ├── github_trending.py  # GitHub Trending
│   ├── hf_models.py        # Hugging Face Models
│   └── leaderboards.py     # 模型评测榜单
├── outputs/                # 生成的 HTML 报告
└── logs/                   # 运行日志
```

## 设计特点

- **错误隔离**：单个源失败不影响整体报告生成，失败源在报告底部"采集异常"区列出
- **礼貌爬取**：请求间随机延时 0.4-1.2s，使用浏览器 UA
- **自动回退**：人民日报当日未发布则取昨日；榜单提取失败则回退为链接条目
- **light 主题**：白底卡片设计，响应式布局，适配移动端

## 配置每日定时自动化

手动验证通过后，可配置为 WorkBuddy automation 每日定时运行：

- **时间**：每日 09:00（兼顾欧美昨夜新闻与人民日报当日见报）
- **命令**：`C:/Users/MZCny/.workbuddy/binaries/python/envs/default/Scripts/python.exe E:/ProjectAgenticWorkspace/WorkBuddy/新闻日报/run.py`
- **工作目录**：`E:/ProjectAgenticWorkspace/WorkBuddy/新闻日报`

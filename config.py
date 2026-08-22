"""30 个信息源配置与分类展示结构。"""
from __future__ import annotations

from dataclasses import dataclass
from scrapers.base import SourceConfig


@dataclass(frozen=True)
class DirectoryEntry:
    """仅用于「目录式」展示的源：没有爬虫，仅供属性过滤器/空状态链接使用。

    设计原因：用户希望在过滤器树里出现更多入口（AI 芯片、云平台、各家 AI
    实验室 GitHub、机器人公司等），但这些站点没有稳定的新闻流，不值得为每
    个都写爬虫。点这些 pill 时会走「暂无收录」空状态，下方直接给出官方
    链接入口。
    """
    id: str
    name: str
    url: str

# 20 个信息源（按抓取策略表配置；URL 均已实测核实）
SOURCES: list[SourceConfig] = [
    # === 国际新闻 ===
    SourceConfig(
        id="reuters",
        name="Reuters 路透社",
        category="international",
        subcategory=None,
        url="https://www.reuters.com/",
        scraper_type="reuters_sitemap",
        limit=300,  # 当天 World/Business 全部（多 sub-sitemap 分页聚合）
    ),
    # === 中国新闻 ===
    SourceConfig(
        id="people_daily",
        name="人民日报",
        category="china",
        subcategory=None,
        url="https://paper.people.com.cn/rmrb/",
        scraper_type="people_daily",
        limit=300,  # 20 版 x 每版 5-6 篇 ~ 100+，留足富余
    ),
    # === 中国新闻 · 央视 ===
    SourceConfig(
        id="cctv",
        name="CCTV 新闻联播",
        category="china",
        subcategory=None,
        url="https://news.cctv.com/",
        scraper_type="cn_source",
        parser="cctv",
        limit=40,
    ),
    # === 科技 · 全球 ===
    SourceConfig(
        id="mit_tech_review",
        name="MIT Technology Review",
        category="tech_global",
        subcategory=None,
        url="https://www.technologyreview.com/",
        scraper_type="feed",
        feed_url="https://www.technologyreview.com/feed/",
        limit=15,
    ),
    # === 科技 · AI > AI 实验室动态 ===
    SourceConfig(
        id="openai_news",
        name="OpenAI News",
        category="ai",
        subcategory="labs",
        url="https://openai.com/news/",
        scraper_type="feed",
        feed_url="https://openai.com/news/rss.xml",
        limit=15,
    ),
    SourceConfig(
        id="openai_research",
        name="OpenAI Research",
        category="ai",
        subcategory="labs",
        url="https://openai.com/research",
        scraper_type="feed",
        feed_url="https://openai.com/news/rss.xml",
        limit=15,
    ),
    SourceConfig(
        id="anthropic",
        name="Anthropic News",
        category="ai",
        subcategory="labs",
        url="https://www.anthropic.com/news",
        scraper_type="html",
        parser="anthropic",
        limit=15,
    ),
    # 中国 AI 实验室（Google News RSS 代理；与 meta_ai 同模式）
    SourceConfig(
        id="zhipu",
        name="智谱 AI",
        category="ai",
        subcategory="labs",
        url="https://www.zhipuai.cn/",
        scraper_type="feed",
        feed_url="https://news.google.com/rss/search?q=%22%E6%99%BA%E8%B0%B1%22+OR+%22Zhipu%22&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        limit=10,
    ),
    SourceConfig(
        id="deepseek",
        name="DeepSeek",
        category="ai",
        subcategory="labs",
        url="https://www.deepseek.com/",
        scraper_type="feed",
        feed_url="https://news.google.com/rss/search?q=%22DeepSeek%22&hl=en-US&gl=US&ceid=US:en",
        limit=10,
    ),
    SourceConfig(
        id="qwen",
        name="通义千问 Qwen",
        category="ai",
        subcategory="labs",
        url="https://tongyi.aliyun.com/qianwen",
        scraper_type="feed",
        feed_url="https://news.google.com/rss/search?q=%22Qwen%22+OR+%22%E5%8D%83%E9%97%AE%22&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        limit=10,
    ),
    SourceConfig(
        id="deepmind",
        name="Google DeepMind",
        category="ai",
        subcategory="labs",
        url="https://deepmind.google/discover/blog/",
        scraper_type="feed",
        feed_url="https://deepmind.google/blog/rss.xml",
        limit=15,
    ),
    SourceConfig(
        id="google_research",
        name="Google Research",
        category="ai",
        subcategory="labs",
        url="https://research.google/blog/",
        scraper_type="feed",
        feed_url="https://research.google/blog/rss/",
        limit=15,
    ),
    SourceConfig(
        id="meta_ai",
        name="Meta AI",
        category="ai",
        subcategory="labs",
        url="https://ai.meta.com/blog/",
        scraper_type="feed",
        feed_url="https://news.google.com/rss/search?q=site:ai.meta.com/blog&ceid=US:en&hl=en-US&gl=US",
        limit=15,
    ),
    SourceConfig(
        id="ms_ai",
        name="Microsoft AI Blog",
        category="ai",
        subcategory="labs",
        url="https://blogs.microsoft.com/ai/",
        scraper_type="html",
        parser="ms_ai",
        limit=15,
    ),
    SourceConfig(
        id="nvidia_ai",
        name="NVIDIA AI Blog",
        category="ai",
        subcategory="labs",
        url="https://blogs.nvidia.com/blog/category/deep-learning/",
        scraper_type="feed",
        feed_url="https://blogs.nvidia.com/blog/category/deep-learning/feed/",
        limit=15,
    ),
    SourceConfig(
        id="hf_blog",
        name="Hugging Face Blog",
        category="ai",
        subcategory="labs",
        url="https://huggingface.co/blog",
        scraper_type="feed",
        feed_url="https://huggingface.co/blog/feed.xml",
        limit=15,
    ),
    # === 科技 · AI > 学术论文（Hugging Face Daily Papers，替代 arXiv） ===
    SourceConfig(
        id="hf_papers",
        name="Hugging Face Papers · Daily",
        category="ai",
        subcategory="papers",
        # URL 由 scraper 内部拼接 ?date=YYYY-MM-DD（当天），固定占位为 /papers
        url="https://huggingface.co/papers",
        scraper_type="html",
        parser="hf_papers",
        limit=40,
    ),
    # === 科技 · AI > 开源生态 ===
    SourceConfig(
        id="github_trending",
        name="GitHub Trending",
        category="ai",
        subcategory="opensource",
        url="https://github.com/trending",
        scraper_type="github",
        limit=20,
    ),
    # === 科技 · AI > 官方政策（中国） ===
    SourceConfig(
        id="gov_policy",
        name="国务院政策文件库",
        category="ai",
        subcategory="policy",
        url="https://sousuo.www.gov.cn/zcwjk/policyDocumentLibrary?t=zhengcelibrary&q=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD",
        scraper_type="cn_source",
        parser="gov_policy",
        limit=15,
    ),
    SourceConfig(
        id="miit",
        name="工业和信息化部",
        category="ai",
        subcategory="policy",
        url="https://www.miit.gov.cn",
        scraper_type="cn_source",
        parser="miit",
        limit=15,
    ),
    SourceConfig(
        id="cac",
        name="国家网信办",
        category="ai",
        subcategory="policy",
        url="http://www.cac.gov.cn",
        scraper_type="cn_source",
        parser="cac",
        limit=15,
    ),
    SourceConfig(
        id="ndrc",
        name="国家发展改革委",
        category="ai",
        subcategory="policy",
        url="https://www.ndrc.gov.cn",
        scraper_type="cn_source",
        parser="ndrc",
        limit=15,
    ),
    SourceConfig(
        id="cast",
        name="中国科协",
        category="ai",
        subcategory="policy",
        url="https://www.cast.org.cn",
        scraper_type="cn_source",
        parser="cast",
        limit=10,
    ),
    # === 科技 · AI > 顶尖学术机构（中国） ===
    SourceConfig(
        id="ia_cas",
        name="中科院自动化所",
        category="ai",
        subcategory="academia",
        url="http://www.ia.ac.cn",
        scraper_type="cn_source",
        parser="ia_cas",
        limit=15,
    ),
    SourceConfig(
        id="pku_ai",
        name="北大AI研究院",
        category="ai",
        subcategory="academia",
        url="http://www.ai.pku.edu.cn",
        scraper_type="cn_source",
        parser="pku_ai",
        limit=15,
    ),
    SourceConfig(
        id="tsinghua_ai",
        name="清华AI学院",
        category="ai",
        subcategory="academia",
        url="https://collegeai.tsinghua.edu.cn",
        scraper_type="cn_source",
        parser="tsinghua_ai",
        limit=15,
    ),
    SourceConfig(
        id="baai",
        name="北京智源研究院",
        category="ai",
        subcategory="academia",
        url="https://www.baai.ac.cn",
        scraper_type="cn_source",
        parser="baai",
        limit=15,
    ),
    SourceConfig(
        id="caai",
        name="中国人工智能学会",
        category="ai",
        subcategory="academia",
        url="http://www.caai.cn",
        scraper_type="cn_source",
        parser="caai",
        limit=15,
    ),
]


# 分类展示结构（顺序即报告分区顺序）
# (category_key, 显示名, [(sub_key, 子显示名), ...] 或 None)
CATEGORY_TREE = [
    ("international", "国际新闻", None),
    ("china", "中国新闻", None),
    ("tech_global", "科技 · 全球", None),
    (
        "ai",
        "科技 · AI",
        [
            ("labs", "AI 实验室动态"),
            ("papers", "学术论文"),
            ("opensource", "开源生态"),
            ("eval", "模型评测"),
            ("policy", "官方政策"),
            ("academia", "顶尖学术机构"),
        ],
    ),
]


# ===== 目录式条目（无爬虫，仅供属性过滤器/空状态链接展示） =====
# 数据来源：用户提供的「AI 全景信息架构」链接清单
# 这些 ID 不会出现在 SOURCES 抓取调度里，但会注入前端 SOURCE_HOMES
DIRECTORY_SOURCES: list[DirectoryEntry] = [
    # 基础设备 · AI 芯片 / GPU
    DirectoryEntry("dir_nvidia_dev", "NVIDIA Developer", "https://developer.nvidia.com/"),
    DirectoryEntry("dir_amd", "AMD AI", "https://www.amd.com/en/products/accelerators"),
    DirectoryEntry("dir_intel", "Intel AI", "https://www.intel.com/content/www/us/en/artificial-intelligence/overview.html"),
    # 基础设备 · AI 云计算
    DirectoryEntry("dir_aws", "AWS AI", "https://aws.amazon.com/ai/"),
    DirectoryEntry("dir_gcp", "Google Cloud AI", "https://cloud.google.com/ai"),
    DirectoryEntry("dir_azure", "Microsoft Azure AI", "https://azure.microsoft.com/en-us/products/ai-services"),
    # 基础设备 · AI 基础软件
    DirectoryEntry("dir_pytorch", "PyTorch", "https://pytorch.org/"),
    DirectoryEntry("dir_tf", "TensorFlow", "https://www.tensorflow.org/"),
    # 大模型 · DeepSeek / Qwen（其它已有 SOURCES 抓取）
    DirectoryEntry("dir_deepseek", "DeepSeek GitHub", "https://github.com/deepseek-ai"),
    DirectoryEntry("dir_qwen", "Qwen GitHub", "https://github.com/QwenLM"),
    # 智能体 · 框架
    DirectoryEntry("dir_langchain", "LangChain", "https://github.com/langchain-ai/langchain"),
    DirectoryEntry("dir_llamaindex", "LlamaIndex", "https://github.com/run-llama/llama_index"),
    DirectoryEntry("dir_autogen", "AutoGen", "https://github.com/microsoft/autogen"),
    # 智能体 · 研究（arXiv cs.AI）
    DirectoryEntry("dir_arxiv_ai", "arXiv cs.AI", "https://arxiv.org/list/cs.AI/recent"),
    # 智能体 · 生态
    DirectoryEntry("dir_openai_agents", "OpenAI Agents", "https://platform.openai.com/docs/agents"),
    DirectoryEntry("dir_hf_agents", "Hugging Face Agents", "https://huggingface.co/agents"),
    # 具身智能 · 世界模型
    DirectoryEntry("dir_dm_robotics", "DeepMind Robotics", "https://deepmind.google/research/robotics/"),
    DirectoryEntry("dir_cosmos", "NVIDIA Cosmos", "https://www.nvidia.com/en-us/ai/cosmos/"),
    DirectoryEntry("dir_worldmodels", "World Models Research", "https://worldmodels.github.io/"),
    # 具身智能 · 机器人
    DirectoryEntry("dir_figure", "Figure AI", "https://www.figure.ai/"),
    DirectoryEntry("dir_bostondyn", "Boston Dynamics", "https://bostondynamics.com/"),
    DirectoryEntry("dir_tesla_ai", "Tesla Robotics", "https://www.tesla.com/AI"),
    DirectoryEntry("dir_unitree", "Unitree Robotics", "https://www.unitree.com/"),
    DirectoryEntry("dir_ieee_ras", "IEEE Robotics", "https://www.ieee-ras.org/"),
    # 具身智能 · 智能制造
    DirectoryEntry("dir_omniverse", "NVIDIA Omniverse", "https://www.nvidia.com/en-us/omniverse/"),
    DirectoryEntry("dir_siemens", "Siemens Digital Industries", "https://www.siemens.com/global/en/company/about/businesses/digital-industries.html"),
    DirectoryEntry("dir_abb", "ABB Robotics", "https://new.abb.com/products/robotics"),
]


def all_directory_homes() -> dict[str, dict]:
    """返回 {source_id: {name, url}}，供注入前端 SOURCE_HOMES 使用。"""
    return {d.id: {"name": d.name, "url": d.url} for d in DIRECTORY_SOURCES}

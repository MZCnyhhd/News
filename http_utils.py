"""统一 HTTP 客户端：UA、超时、礼貌延时、重试。"""
from __future__ import annotations

import random
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class HttpClient:
    """带重试与礼貌延时的 HTTP 客户端。"""

    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    def __init__(self, timeout: int = 20):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
            }
        )
        # 配置重试：网络错误/5xx 重试 2 次
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        # 条件请求缓存：命中 304 时直接返回上次正文，避免重复下载（防封/省流量）
        self._etag: dict[str, str] = {}
        self._last_modified: dict[str, str] = {}
        self._cached_body: dict[str, str] = {}

    def get_text(self, url: str, **kwargs) -> str:
        """GET 请求并返回文本（自动处理编码）。支持条件请求，命中 304 复用缓存。

        解码策略（按优先级）：
          1. 响应头 Content-Type 里明确声明的 charset（且可信：utf-8/gbk/gb18030/big5…）
          2. HTML 内部 <meta charset="..."> 声明
          3. chardet（apparent_encoding）探测，但过滤掉不可能用于中文/英文站的编码
             （如 MacCyrillic/cp1251/koi8-r 对纯中文页面显然是错的）
          4. 兜底 utf-8（errors=replace 容错）

        修复历史：人民日报响应头 Content-Type 不带 charset，requests 默认 ISO-8859-1，
        触发 apparent_encoding 兜底，结果 chardet 把 GBK 字节错判为 MacCyrillic，
        中文标题被解码成西里尔字母乱码（"вАЬ..."）。
        """
        headers = {}
        if url in self._etag:
            headers["If-None-Match"] = self._etag[url]
        if url in self._last_modified:
            headers["If-Modified-Since"] = self._last_modified[url]

        resp = self.session.get(
            url, timeout=self.timeout, headers=headers or None, **kwargs
        )
        # 记录验证头，供下次条件请求使用
        if "ETag" in resp.headers:
            self._etag[url] = resp.headers["ETag"]
        if "Last-Modified" in resp.headers:
            self._last_modified[url] = resp.headers["Last-Modified"]

        # 服务端内容未变：直接复用上次正文
        if resp.status_code == 304 and url in self._cached_body:
            self._polite_delay()
            return self._cached_body[url]

        resp.raise_for_status()
        text = self._decode_response_body(resp)
        self._cached_body[url] = text
        self._polite_delay()
        return text

    @staticmethod
    def _decode_response_body(resp) -> str:
        """根据响应头/HTML meta/chardet 综合判断编码，解码为字符串。"""
        import re

        # 1) 响应头明确声明的 charset（且可信）
        declared = (resp.encoding or "").lower()
        trusted_header_enc = {
            "utf-8", "utf8",
            "gbk", "gb2312", "gb18030",
            "big5",
            "shift_jis", "euc-jp", "euc-kr",
            "windows-1252", "cp1252", "iso-8859-1",
        }
        if declared and declared not in ("iso-2022-jp",) and declared in trusted_header_enc:
            # 响应头 charset 明确且可信：直接用
            # 但 iso-8859-1 是 requests 的兜底默认值（响应头无 charset 时也会得到它），不能信
            if declared != "iso-8859-1":
                try:
                    return resp.content.decode(declared, errors="replace")
                except (LookupError, UnicodeDecodeError):
                    pass

        # 2) HTML 内部 <meta charset="..."> 声明
        head = resp.content[:4096]
        m = re.search(rb'<meta[^>]+charset\s*=\s*["\']?([\w-]+)', head, re.I)
        if m:
            meta_enc = m.group(1).decode("ascii", errors="ignore").strip().lower()
            # gbk/gb2312 都用 gb18030 解（兼容更广）
            if meta_enc in ("gbk", "gb2312"):
                meta_enc = "gb18030"
            if meta_enc in ("utf8",):
                meta_enc = "utf-8"
            if meta_enc in ("utf-8", "gb18030", "big5", "shift_jis", "euc-jp", "euc-kr"):
                try:
                    return resp.content.decode(meta_enc, errors="replace")
                except (LookupError, UnicodeDecodeError):
                    pass

        # 3) chardet 探测（apparent_encoding），但过滤掉明显的错误编码
        apparent = (resp.apparent_encoding or "").lower()
        # 过滤：MacCyrillic/cp1251/koi8-r 对中文/英文站点显然是错的
        # （chardet 偶尔会把 GBK 字节误判为 MacCyrillic）
        bad_apparent = {"maccyrillic", "cp1251", "koi8-r", "koi8u", "iso-ir-111"}
        if apparent and apparent not in bad_apparent:
            try:
                return resp.content.decode(apparent, errors="replace")
            except (LookupError, UnicodeDecodeError):
                pass

        # 4) 对中文站（域名 .cn / html lang 含 zh）默认尝试 gb18030
        host = (resp.url or "").lower()
        html_head = head[:512].lower()
        if ".cn" in host or b'lang="zh' in html_head or b"lang='zh" in html_head:
            try:
                return resp.content.decode("gb18030", errors="replace")
            except (LookupError, UnicodeDecodeError):
                pass

        # 5) 兜底 utf-8
        return resp.content.decode("utf-8", errors="replace")

    def get_bytes(self, url: str, **kwargs) -> bytes:
        """GET 请求并返回原始字节。"""
        resp = self.session.get(url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        data = resp.content
        self._polite_delay()
        return data

    def _polite_delay(self):
        """请求间随机延时，遵守爬取礼仪。"""
        time.sleep(random.uniform(0.4, 1.2))

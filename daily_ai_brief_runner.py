#!/usr/bin/env python3
"""
Daily brief runner:
 - Fetches AI signal headlines from public sources.
 - Writes one markdown report and updates historical state.
 - Keeps last 14 historical records.
 - Merges today's report into any same-topic report in the previous 7 days.
"""

from __future__ import annotations

import argparse
import os
import json
import socket
from collections import Counter
import calendar
import platform
import re
import smtplib
import subprocess
import shutil
import ssl
import urllib.error
import urllib.request
from urllib.parse import quote, unquote, urljoin
from datetime import datetime, timedelta, timezone
from html import escape, unescape
from pathlib import Path
from typing import Callable, Dict, List
from email.message import EmailMessage


def _read_env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name, "")
        if value is not None:
            value = value.strip()
            if value:
                return value
    return default.strip()


def _resolve_sendmail_path() -> str | None:
    configured = _read_env_first("DAILY_AI_BRIEF_SENDMAIL_PATH", "SENDMAIL_PATH", "MAIL_PROGRAM")
    if configured:
        return configured
    if found := shutil.which("sendmail"):
        return found
    for candidate in ("/usr/sbin/sendmail", "/usr/bin/sendmail", "/sbin/sendmail"):
        if Path(candidate).exists():
            return candidate
    return None


def _read_int_env(*names: str, default: int = 0) -> int:
    for name in names:
        value = os.getenv(name, "").strip()
        if not value:
            continue
        try:
            return int(value)
        except ValueError:
            continue
    return default


STATE_FILE = Path(__file__).with_name("daily_ai_brief_state.json")
REPORT_OUTPUT_DIR = Path(__file__).with_name("report")
MAX_RECORDS = 14
RELATED_WINDOW_DAYS = 7
DAILY_AI_BRIEF_CATCHUP_DAYS = max(1, _read_int_env("DAILY_AI_BRIEF_CATCHUP_DAYS", "BACKFILL_DAYS", default=3))
SOURCE_URLS = {
    "openai_rss": "https://openai.com/feed/",
    "openai_news_rss": "https://openai.com/news/rss.xml",
    "openai_blog_rss": "https://openai.com/blog/rss.xml",
    "anthropic_managed_agents": "https://www.anthropic.com/engineering/managed-agents",
    "anthropic_rss": "https://www.anthropic.com/rss.xml",
    "agui_repo": "https://api.github.com/repos/ag-ui-protocol/ag-ui",
    "infoq_rss": "https://www.infoq.com/rss/",
    "google_ai_blog": "https://blog.google/technology/ai/rss/",
    "deepmind_blog": "https://deepmind.google/blog/feed/",
    "huggingface_blog": "https://huggingface.co/blog/feed.xml",
    "microsoft_ai_blog": "https://blogs.microsoft.com/ai/feed/",
    "github_trend_weekly": "https://git-trending-rank.github.io/post/trending-weekly-2026%E5%B9%B4%E7%AC%AC21%E5%91%A8/",
    "github_trend_json_daily": "https://raw.githubusercontent.com/isboyjc/github-trending-api/main/data/daily/all.json",
    "github_trend_json_weekly": "https://raw.githubusercontent.com/isboyjc/github-trending-api/main/data/weekly/all.json",
    "claude_blog": "https://claude.com/blog",
    "openai_research": "https://openai.com/zh-Hant-HK/research/index/",
}
DEFAULT_PRIORITY_CHANNEL_BROWSER_SNAPSHOT = str(Path(__file__).with_name("daily_ai_brief_browser_snapshot.json"))
PRIORITY_CHANNEL_BROWSER_SNAPSHOT_PATH = os.getenv("PRIORITY_CHANNEL_BROWSER_SNAPSHOT_PATH", DEFAULT_PRIORITY_CHANNEL_BROWSER_SNAPSHOT).strip()
PRIORITY_CHANNELS = [
    {"key": "claude_blog", "name": "Claude Blog", "source": "Claude 官方博文", "max_items": 6},
    {"key": "openai_research", "name": "OpenAI Research", "source": "OpenAI Research", "max_items": 6},
]
PRIORITY_CHANNEL_LOOKBACK_DAYS = 7
PRIORITY_CHANNEL_MAX_SUMMARY_CHARS = 220
OPENAI_SEARCH_API_URL = os.getenv("OPENAI_SEARCH_API_URL", "https://api.openai.com/v1/responses")
OPENAI_SEARCH_MODEL = os.getenv("OPENAI_SEARCH_MODEL", "gpt-4.1-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TAVILY_CLI = os.getenv("TAVILY_CLI", "tvly").strip() or "tvly"
AI_SEARCH_MAX_ITEMS_PER_PROVIDER = 4
AI_SEARCH_SKIP_TAVILY = _read_env_first(
    "DAILY_AI_BRIEF_SKIP_TAVILY_SEARCH",
    "DAILY_AI_BRIEF_DISABLE_TAVILY",
    default="0",
).lower() in ("1", "true", "yes", "on")
AI_SEARCH_QUERIES = [
    "OpenAI 研究 動向",
    "Claude Managed Agents",

    "AG-UI protocol",
    "agentic workflow orchestration",
    "AI 开放协议 与 MCP",
    "OpenAI 最新 AGI 研究",
]
FALLBACK_SEARCH_URL = "https://duckduckgo.com/html/?q="
WECHAT_TOP20_GZH_URL = "https://aigcrank.cn/top/202412gzh"
TWITTER_RSS_INSTANCES = [
    "https://rsshub.app",
    "https://rsshub.rssforever.com",
    "https://nitter.privacydev.net",
    "https://nitter.unixfox.eu",
    "https://nitter.poast.org",
    "https://nitter.net",
]
TWITTER_TECH_ACCOUNTS = [
    {"name": "Microsoft", "handle": "Microsoft"},
    {"name": "Apple", "handle": "Apple"},
    {"name": "Google", "handle": "Google"},
    {"name": "Amazon", "handle": "amazon"},
    {"name": "Meta", "handle": "Meta"},
    {"name": "NVIDIA", "handle": "NVIDIA"},
]
BROKER_REPORT_TEMPLATES = [
    "https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml?orgname={org}&industry=&symbol=&t1=all&title=",
    "https://stock.finance.sina.com.cn/stock/go.php/vReport_Show/kind/search/index.phtml?orgname={org}&industry=&symbol=&t1=all&title=",
]
BROKER_REPORT_SOURCES = [
    {"name": "中信证券", "type": "sina_report"},
    {"name": "中信建投证券", "type": "sina_report"},
    {"name": "华泰证券", "type": "sina_report"},
    {"name": "中金公司", "type": "sina_report"},
    {"name": "国泰君安", "type": "sina_report"},
    {"name": "广发证券", "type": "sina_report"},
    {"name": "海通证券", "type": "sina_report"},
    {"name": "招商证券", "type": "sina_report"},
    {"name": "兴业证券", "type": "sina_report"},
    {"name": "光大证券", "type": "sina_report"},
]
BROKER_AI_KEYWORDS = ["ai", "人工智能", "大模型", "生成式", "agent", "agentic", "llm", "算力", "智能"]
FRONTIER_CHANNELS = [
    {"name": "Google AI Blog", "source": "Google AI", "url": SOURCE_URLS["google_ai_blog"]},
    {"name": "DeepMind Blog", "source": "DeepMind", "url": SOURCE_URLS["deepmind_blog"]},
    {"name": "Hugging Face Blog", "source": "Hugging Face", "url": SOURCE_URLS["huggingface_blog"]},
    {"name": "Microsoft AI Blog", "source": "Microsoft AI", "url": SOURCE_URLS["microsoft_ai_blog"]},
]
MODEL_DECISION_KEYWORDS = [
    "llm",
    "large language model",
    "agent",
    "agentic",
    "multi agent",
    "tool use",
    "rag",
    "retrieval",
    "orchestration",
    "function calling",
    "mcp",
    "reasoning",
    "sora",
    "vision",
    "model",
    "release",
    "update",
]


def github_repo_link(repo: str) -> str:
    if not repo:
        return ""
    repo = repo.strip()
    if repo.startswith("http://") or repo.startswith("https://"):
        return repo
    return f"https://github.com/{repo}"


def append_source_link(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for item in items:
        repo = str(item.get("repo", "")).strip()
        item = dict(item)
        if repo and not item.get("link"):
            item["link"] = github_repo_link(repo)
        out.append(item)
    return out

EMAIL_RECIPIENT = _read_env_first("DAILY_AI_BRIEF_EMAIL_TO", "EMAIL_TO_ADDRESSES", "EMAIL_TO")
EMAIL_SMTP_HOST = _read_env_first("DAILY_AI_BRIEF_SMTP_HOST", "SMTP_HOST", "EMAIL_SMTP_HOST", default="smtp.gmail.com")
EMAIL_SMTP_PORT = int(_read_env_first("DAILY_AI_BRIEF_SMTP_PORT", "SMTP_PORT", "EMAIL_SMTP_PORT", default="587"))
EMAIL_SMTP_SSL = _read_env_first("DAILY_AI_BRIEF_SMTP_SSL", default="0").lower() in ("1", "true", "yes", "on")
EMAIL_SMTP_STARTTLS = _read_env_first("DAILY_AI_BRIEF_SMTP_STARTTLS", default="1").lower() in ("1", "true", "yes", "on")
EMAIL_SMTP_TIMEOUT = int(_read_env_first("DAILY_AI_BRIEF_SMTP_TIMEOUT", "SMTP_TIMEOUT", default="20"))
EMAIL_SMTP_HOSTS = [
    h.strip()
    for h in _read_env_first("DAILY_AI_BRIEF_SMTP_HOSTS", "SMTP_HOSTS", "EMAIL_SMTP_HOSTS").split(",")
    if h.strip()
]
if EMAIL_SMTP_HOST not in ("", None):
    if EMAIL_SMTP_HOST not in EMAIL_SMTP_HOSTS:
        EMAIL_SMTP_HOSTS.insert(0, EMAIL_SMTP_HOST)
EMAIL_SMTP_USER = _read_env_first(
    "DAILY_AI_BRIEF_SMTP_USER",
    "EMAIL_SMTP_USER",
    "SMTP_USER",
    "SMTP_USERNAME",
)
EMAIL_SMTP_PASSWORD = re.sub(
    r"\s+",
    "",
    _read_env_first(
        "DAILY_AI_BRIEF_SMTP_PASSWORD",
        "EMAIL_SMTP_PASSWORD",
        "EMAIL_APP_PASSWORD",
        "SMTP_PASSWORD",
        "GMAIL_APP_PASSWORD",
    ),
)
EMAIL_FROM = _read_env_first("DAILY_AI_BRIEF_FROM", "SMTP_FROM", EMAIL_SMTP_USER, EMAIL_RECIPIENT)
EMAIL_ENABLED = os.getenv("DAILY_AI_BRIEF_SEND_EMAIL", "1").lower() in ("1", "true", "yes", "on")
EMAIL_SENDMAIL_PATH = _resolve_sendmail_path()
EMAIL_SMTP_CA_FILE = os.getenv("DAILY_AI_BRIEF_SMTP_CA_FILE", "").strip()
if not EMAIL_SMTP_CA_FILE:
    EMAIL_SMTP_CA_FILE = os.getenv("SSL_CERT_FILE", "")
EMAIL_TRANSPORT = _read_env_first(
    "DAILY_AI_BRIEF_EMAIL_TRANSPORT",
    "DAILY_AI_BRIEF_TRANSPORT",
    "EMAIL_TRANSPORT",
    default="auto",
).lower()
EMAIL_FORCE_TERMINAL = _read_env_first(
    "DAILY_AI_BRIEF_FORCE_TERMINAL_EMAIL",
    "EMAIL_FORCE_TERMINAL",
    "FORCE_TERMINAL_EMAIL",
    default="",
).lower() in ("1", "true", "yes", "on")
EMAIL_TERMINAL_OUTBOX_DIR = Path(__file__).with_name("daily_ai_terminal_unsent")
EMAIL_RESEND_API_KEY = _read_env_first("DAILY_AI_BRIEF_RESEND_API_KEY", "RESEND_API_KEY")
EMAIL_RESEND_FROM = _read_env_first("DAILY_AI_BRIEF_RESEND_FROM", "RESEND_FROM")
EMAIL_SENDGRID_API_KEY = _read_env_first("DAILY_AI_BRIEF_SENDGRID_API_KEY", "SENDGRID_API_KEY")
EMAIL_SENDGRID_FROM = _read_env_first("DAILY_AI_BRIEF_SENDGRID_FROM", "SENDGRID_FROM")
EMAIL_API_PROVIDER = _read_env_first("DAILY_AI_BRIEF_EMAIL_API_PROVIDER", "EMAIL_API_PROVIDER", default="").lower()
EMAIL_OUTBOX_DIR = Path(__file__).with_name("daily_ai_unsent")
EMAIL_SMTP_CHECK_TIMEOUT = float(_read_env_first("DAILY_AI_BRIEF_SMTP_CHECK_TIMEOUT", default="2.5"))
EMAIL_QUEUE_ON_FAIL = _read_env_first("DAILY_AI_BRIEF_QUEUE_ON_FAIL", "EMAIL_QUEUE_ON_FAIL", default="1").lower() in ("1", "true", "yes", "on")

SKILL_CATEGORIES = {
    "ai_engineering": {
        "title": "AI工程技术提效",
        "description": "偏向工作流/编排/编程提效相关的工程化能力。",
        "keywords": ["agent", "mcp", "workflow", "orchestration", "rag", "vscode", "copilot", "assistant", "tooling"],
        "fallback": [
            "langchain-ai/langchain",
            "langchain-ai/langgraph",
            "microsoft/autogen",
            "microsoft/semantic-kernel",
            "microsoft/agent-framework",
            "crewAIInc/crewAI",
            "ag2ai/ag2",
            "OpenPipe/OpenPipe",
            "pydantic/pydantic-ai",
            "open-webui/open-webui",
        ],
    },
    "ppt_skill": {
        "title": "PPT技巧",
        "description": "偏向展示产出、演讲材料、知识传播的高效模板和生成工具。",
        "keywords": ["slide", "slides", "ppt", "powerpoint", "presentation", "deck", "markdown-to-pdf"],
        "fallback": [
            "marp-team/marp",
            "marp-team/marpit",
            "microsoft/markitdown",
            "slidevjs/slidev",
            "revealjs/reveal",
        ],
    },
    "earning": {
        "title": "赚钱相关",
        "description": "偏向变现、效率变现链路或商业化场景落地。",
        "keywords": ["automation", "workflow", "marketing", "commerce", "affiliate", "saas", "finance", "revenue", "monetize", "payment"],
        "fallback": [
            "langfuse/langfuse",
            "arize-ai/phoenix",
            "llamaindex/llama-index",
            "deepset-ai/haystack",
            "OpenPipe/OpenPipe",
        ],
    },
}

FOCUSED_FIELDS = {
    "au_ui": {
        "title": "AU- UI（AG-UI 等人机交互层）",
        "keywords": ["ag-ui", "agent ui", "agent-ui", "ui protocol", "tool call ui", "human loop", "handoff"],
    },
    "ai_map": {"title": "AI地图", "keywords": ["map", "knowledge map", "graph", "knowledge graph", "topology", "agent map"]},
    "ai_search": {"title": "AI搜索", "keywords": ["search", "retrieval", "rerank", "vector", "recall", "index"]},
    "ai_image": {"title": "AI生图", "keywords": ["text-to-image", "image generation", "diffusion", "sdxl", "flux", "midjourney", "image model"]},
}


def fetch_url(url: str, timeout: int = 12) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        return f"__ERROR__:{exc}"


def fetch_first_success(urls: List[str], timeout: int = 12) -> str:
    last_error = ""
    for u in urls:
        text = fetch_url(u, timeout=timeout)
        if not text.startswith("__ERROR__"):
            return text
        last_error = text
    return last_error or "__ERROR__:无可用 URL"


def strip_html(html_text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", html_text or "")
    no_tags = unescape(no_tags)
    return re.sub(r"\s+", " ", no_tags).strip()


def _post_json(url: str, payload: Dict[str, object], headers: Dict[str, str] | None = None) -> str:
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=req_headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        return f"__ERROR__:{exc}"


def _extract_nested_text_blocks(obj: object, out: List[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and k in {"text", "title", "query", "snippet", "description"}:
                out.append(v)
            elif isinstance(v, (dict, list)):
                _extract_nested_text_blocks(v, out)
            elif isinstance(v, str) and k in {"url", "link"}:
                if v:
                    out.append(v)
    elif isinstance(v := obj, list):
        for item in v:
            _extract_nested_text_blocks(item, out)


def _build_ai_search_items(
    source: str,
    query: str,
    payload_title: str,
    link: str,
    raw_text: str,
) -> Dict[str, str]:
    return {
        "title": payload_title or f"{query}（{source}）",
        "source": f"{source}（AI检索）",
        "link": link or "",
        "time": "",
        "raw": raw_text[:260],
    }


def _collect_openai_search(query: str) -> List[Dict[str, object]]:
    if not OPENAI_API_KEY:
        return []
    payload = {
        "model": OPENAI_SEARCH_MODEL,
        "input": f"请只返回近期与AI研究相关的2-3条高可信新闻标题与简短结论：{query}",
        "tools": [{"type": "web_search_preview"}],
        "max_output_tokens": 1000,
        "store": False,
    }
    text = _post_json(
        OPENAI_SEARCH_API_URL,
        payload=payload,
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
        },
    )
    if text.startswith("__ERROR__"):
        return []
    try:
        data = json.loads(text)
    except Exception:
        return []

    out = []
    blocks: List[str] = []
    _extract_nested_text_blocks(data, blocks)
    if not blocks:
        return []

    snippet = " ".join([x for x in blocks if x and isinstance(x, str)][:14]).strip()
    if not snippet:
        return []
    out.append(
        _build_ai_search_items(
            source="OpenAI Search",
            query=query,
            payload_title=f"AI Search：{query}",
            link="",
            raw_text=snippet,
        )
    )
    return out


def _parse_time(text: str) -> datetime | None:
    if not text:
        return None
    cleaned = (text or "").strip().replace("Z", "+00:00")
    for fmt in (
        "%Y年%m月%d日",
        "%Y年%-m月%-d日",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%b %d, %Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
    ):
        try:
            dt = datetime.strptime(cleaned, fmt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(cleaned).astimezone(timezone.utc)
    except Exception:
        return None
    cn_match = re.match(r"^(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日$", cleaned)
    if cn_match:
        y, m, d = cn_match.groups()
        try:
            return datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
        except Exception:
            return None
    cn_match2 = re.match(r"^(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日$", cleaned)
    if cn_match2:
        y, m, d = cn_match2.groups()
        try:
            return datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _load_json_file(path: str) -> object:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _collect_from_priority_snapshot(source_key: str, now: datetime) -> List[Dict[str, str]]:
    payload = _load_json_file(PRIORITY_CHANNEL_BROWSER_SNAPSHOT_PATH)
    if not isinstance(payload, dict):
        return []
    raw_items = payload.get(source_key, [])
    if not isinstance(raw_items, list):
        return []
    out: List[Dict[str, str]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        raw_time = str(item.get("time", "")).strip()
        dt = _parse_time(raw_time)
        if not dt:
            # 无法确定发布时间时，避免把不确定日期的内容误认定为近期内容
            continue
        if dt < now - timedelta(days=PRIORITY_CHANNEL_LOOKBACK_DAYS):
            continue
        raw_title = str(item.get("title", "")).strip()
        raw_summary = str(item.get("summary", "")).strip()
        link = str(item.get("link", "")).strip()
        if not raw_title or not link:
            continue
        zh_title = _translate_snippet(raw_title, source=str(item.get("source", "")))
        zh_summary = _translate_snippet(raw_summary, source=str(item.get("source", "")))
        out.append(
            {
                "title": zh_title or raw_title,
                "title_zh": zh_title or raw_title,
                "title_original": raw_title,
                "summary": (zh_summary or raw_summary)[:PRIORITY_CHANNEL_MAX_SUMMARY_CHARS],
                "summary_original": raw_summary,
                "architecture_points": _extract_architecture_highlights(f"{raw_title}。{raw_summary}"),
                "architecture_nodes": _derive_architecture_nodes(raw_title, raw_summary),
                "architecture_sections": _extract_architecture_sections(raw_title + "。" + raw_summary),
                "architecture_images": [],
                "link": link,
                "source": str(item.get("source", "")) or source_key,
                "time": raw_time or dt.isoformat(),
                "translate": "已由浏览器快照补充并尝试翻译" if (zh_title or zh_summary) else "已由浏览器快照补充",
            }
        )
    return out


def _extract_published_time(html_text: str) -> str:
    if not html_text:
        return ""
    patterns = [
        r'<time[^>]*datetime=["\']([^"\']+)["\']',
        r'property=["\']article:published_time["\'][^>]*content=["\']([^"\']+)["\']',
        r'name=["\']publish_date["\'][^>]*content=["\']([^"\']+)["\']',
        r'name=["\']date["\'][^>]*content=["\']([^"\']+)["\']',
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'"dateModified"\s*:\s*"([^"]+)"',
        r'"dateCreated"\s*:\s*"([^"]+)"',
        r'content=["\']([^"\']+)\s*T\d{2}:\d{2}:\d{2}[^"\']*["\'][^>]*itemprop=["\']datePublished["\']',
    ]
    for p in patterns:
        m = re.search(p, html_text, flags=re.I | re.S)
        if m:
            return m.group(1).strip()
    return ""


def _extract_main_content_html(html_text: str) -> str:
    if not html_text:
        return ""
    candidates: List[tuple[int, str]] = []
    patterns = [
        r"<article[^>]*>(.*?)</article>",
        r"<main[^>]*>(.*?)</main>",
        r"<section[^>]*>(.*?)</section>",
        r"<div[^>]*class=[\"']([^\"']*(?:article|post|content|entry|markdown-body|docs-content|blog-post|post-content)[^\"']*)[\"'][^>]*>(.*?)</div>",
    ]
    for p in patterns:
        for m in re.finditer(p, html_text, flags=re.I | re.S):
            if p.startswith(r"<div"):
                body = m.group(2) if len(m.groups()) > 1 else m.group(1)
            else:
                body = m.group(1)
            if not body:
                continue
            body_text = strip_html(body)
            if body_text:
                candidates.append((len(body_text), body))
    if not candidates:
        return html_text
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _extract_article_text(html_text: str, max_chars: int = 900) -> str:
    if not html_text:
        return ""
    blocks: List[str] = []
    for tag in ("h1", "h2", "h3", "h4", "p", "li", "blockquote"):
        for m in re.finditer(rf"<{tag}[^>]*>(.*?)</{tag}>", html_text, flags=re.I | re.S):
            t = strip_html(m.group(1))
            if t and len(t) >= 10:
                blocks.append(t)
            if len(blocks) >= 20:
                break
        if len(blocks) >= 20:
            break
    if not blocks:
        return strip_html(html_text)[:max_chars]
    return re.sub(r"\s+", " ", " ".join(blocks))[:max_chars]


def _extract_architecture_sections(html_text: str, max_items: int = 4) -> List[str]:
    if not html_text:
        return []
    hints = (
        "architecture",
        "架构",
        "workflow",
        "工作流",
        "pipeline",
        "orchestrat",
        "编排",
        "组件",
        "系统",
        "路由",
        "memory",
        "检索",
        "agent",
    )
    out: List[str] = []
    for m in re.finditer(r"<h[2-4][^>]*>(.*?)</h[2-4]>", html_text, flags=re.I | re.S):
        title = strip_html(m.group(1)).strip()
        if not title:
            continue
        low = title.lower()
        if any(k in low for k in hints):
            if title not in out:
                out.append(title)
            if len(out) >= max_items:
                break
    return out


def _extract_abstract(html_text: str) -> str:
    if not html_text:
        return ""
    meta_patterns = [
        r'<meta[^>]+name=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]+property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]+itemprop=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
    ]
    for p in meta_patterns:
        m = re.search(p, html_text, flags=re.I | re.S)
        if m and m.group(1).strip():
            return strip_html(m.group(1).strip())
    for p in [
        r'<p[^>]*>(.*?)</p>',
        r'<div[^>]+class=["\'][^"\']*prose[^"\']*["\'][^>]*>(.*?)</div>',
        r'<article[^>]*>(.*?)</article>',
    ]:
        m = re.search(p, html_text, flags=re.I | re.S)
        if m:
            t = strip_html(m.group(1))
            if t:
                return t
    return ""


def _extract_first_image(html_text: str) -> str:
    if not html_text:
        return ""
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]*content=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>',
        r'<meta[^>]+property=["\']image["\'][^>]*content=["\']([^"\']+)["\']',
    ]
    for p in patterns:
        m = re.search(p, html_text, flags=re.I | re.S)
        if m:
            url = m.group(1).strip()
            if url:
                return url
    return ""


def _to_abs_url(base_url: str, target: str) -> str:
    if not target:
        return ""
    try:
        return urljoin(base_url, target)
    except Exception:
        return target


def _extract_diagram_images(html_text: str, base_url: str = "") -> List[str]:
    if not html_text:
        return []
    image_candidates: List[str] = []
    arch_candidates: List[str] = []
    source = base_url or ""
    text_sample = strip_html(html_text).lower()
    diag_keywords = ("diagram", "architecture", "arch", "系统图", "架构图", "workflow", "pipeline", "topology", "graph", "flow", "overview")

    for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>', html_text, flags=re.I | re.S):
        src = strip_html(m.group(1))
        if not src:
            continue
        img_url = _to_abs_url(source, src)
        if not img_url:
            continue
        alt_m = re.search(r'alt=["\']([^"\']*)["\']', m.group(0), flags=re.I | re.S)
        alt = strip_html(alt_m.group(1)).strip().lower() if alt_m else ""
        if any(k in (alt + img_url.lower()) for k in diag_keywords):
            if img_url not in arch_candidates:
                arch_candidates.append(img_url)
        else:
            image_candidates.append(img_url)

    for m in re.finditer(r'<picture>.*?<source[^>]+srcset=["\']([^"\']+)["\'][^>]*>.*?</picture>', html_text, flags=re.I | re.S):
        srcset = m.group(1).strip()
        if not srcset:
            continue
        for piece in srcset.split(","):
            src = piece.strip().split()[0]
            if not src:
                continue
            img_url = _to_abs_url(source, src)
            if img_url and img_url not in image_candidates and img_url not in arch_candidates:
                image_candidates.append(img_url)

    for m in re.finditer(r'<meta[^>]+property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']', html_text, flags=re.I | re.S):
        candidate = _to_abs_url(source, strip_html(m.group(1)).strip())
        if candidate and candidate not in image_candidates and candidate not in arch_candidates:
            image_candidates.append(candidate)
    for m in re.finditer(r'<meta[^>]+name=["\']twitter:image["\'][^>]*content=["\']([^"\']+)["\']', html_text, flags=re.I | re.S):
        candidate = _to_abs_url(source, strip_html(m.group(1)).strip())
        if candidate and candidate not in image_candidates and candidate not in arch_candidates:
            image_candidates.append(candidate)

    if ("架构" in text_sample or "architecture" in text_sample or "diagram" in text_sample) and image_candidates:
        candidate = image_candidates[0]
        if candidate not in arch_candidates:
            arch_candidates.insert(0, candidate)

    uniq: List[str] = []
    for item in arch_candidates[:3] + image_candidates:
        if item not in uniq and item:
            uniq.append(item)
    return uniq[:3]


def _extract_architecture_highlights(html_text: str) -> List[str]:
    if not html_text:
        return []
    full_text = strip_html(html_text)
    if not full_text:
        return []
    arch_keywords = [
        "architecture",
        "架构",
        "component",
        "组件",
        "pipeline",
        "pipeline",
        "workflow",
        "工作流",
        "orchestrat",
        "编排",
        "agent",
        "智能体",
        "router",
        "路由",
        "planner",
        "planning",
        "memory",
        "记忆",
        "检索",
        "retriev",
        "api",
        "网关",
        "graph",
        "图",
        "系统",
        "服务",
        "模型",
    ]
    sents = re.split(r"[。；;!?！？\\n\\r]+", full_text)
    out: List[str] = []
    for s in sents:
        txt = re.sub(r"\s+", " ", s).strip()
        if not txt or len(txt) < 8:
            continue
        low = txt.lower()
        if any(k.lower() in low for k in arch_keywords):
            if txt not in out:
                out.append(txt)
            if len(out) >= 6:
                break
    return out[:6]


def _derive_architecture_nodes(*texts: str) -> List[str]:
    merged = " ".join([str(t or "") for t in texts]).lower()
    nodes: List[str] = []
    for keyset, label in [
        (("agent", "智能体", "agentic"), "Agent 层"),
        (("planner", "计划", "planning", "planner"), "Planner"),
        (("tool", "工具", "tooling", "function"), "Tools"),
        (("orchestr", "编排"), "Orchestrator"),
        (("workflow", "工作流"), "Workflow Engine"),
        (("memory", "记忆", "memory"), "Memory Store"),
        (("retriev", "检索", "rag"), "Retriever"),
        (("mcp",), "MCP Gateway"),
        (("router", "路由"), "Router"),
        (("api", "接口", "gateway"), "API Gateway"),
        (("sdk", "sdk"), "SDK"),
        (("monitor", "观测", "tracing"), "Monitor"),
        (("knowledge", "知识", "knowledgebase", "知识库"), "Knowledge Base"),
        (("vector", "向量"), "Vector DB"),
        (("policy", "安全", "guardrails"), "Policy Guard"),
    ]:
        if any(k in merged for k in keyset) and label not in nodes:
            nodes.append(label)
    if not nodes:
        nodes = ["Input", "Core Service", "Output"]
    return nodes[:9]


def _translate_snippet(text: str, source: str = "") -> str:
    if not text or not OPENAI_API_KEY:
        return ""
    payload = {
        "model": OPENAI_SEARCH_MODEL,
        "input": f"请将以下内容翻译为中文：\n\n{source}\n{text}".strip(),
        "max_output_tokens": 1200,
        "store": False,
    }
    text_out = _post_json(
        OPENAI_SEARCH_API_URL,
        payload=payload,
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
        },
    )
    if text_out.startswith("__ERROR__"):
        return ""
    try:
        payload_obj = json.loads(text_out)
    except Exception:
        return ""
    blocks: List[str] = []
    _extract_nested_text_blocks(payload_obj, blocks)
    translated = " ".join([x for x in blocks if isinstance(x, str)]).strip()
    return translated


def _collect_priority_channel_articles(
    source_name: str,
    source_url: str,
    max_items: int,
    is_candidate: Callable[[str, str], bool],
) -> List[Dict[str, str]]:
    now = datetime.now().astimezone()
    html = fetch_first_success([source_url])
    if html.startswith("__ERROR__"):
        # 使用浏览器逐页补充快照（防 DNS/网络失败导致空结果）
        snapshot_key = "claude_blog" if "claude.com" in source_url else "openai_research"
        fallback = _collect_from_priority_snapshot(snapshot_key, now=now)
        return fallback[:max_items]
    candidates = []
    for m in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, flags=re.I | re.S):
        href = strip_html(m.group(1)).strip()
        title = strip_html(unescape(m.group(2))).strip()
        if not href or not title or len(title) < 4:
            continue
        target = urljoin(source_url, href)
        if not is_candidate(target, title):
            continue
        if target in candidates:
            continue
        candidates.append(target)
        if len(candidates) >= max_items * 2:
            break

    out = []
    fallback = []
    for link in candidates[: max_items * 2]:
        item_html = fetch_first_success([link])
        if item_html.startswith("__ERROR__"):
            continue
        item_body = _extract_main_content_html(item_html)
        published = _extract_published_time(item_html)
        dt = _parse_time(published)
        if not dt:
            fallback.append((link, item_html))
            continue
        if dt < now - timedelta(days=PRIORITY_CHANNEL_LOOKBACK_DAYS):
            continue
        title_match = re.search(r"<title[^>]*>(.*?)</title>", item_html, flags=re.I | re.S)
        raw_title = strip_html(title_match.group(1)) if title_match else "Untitled"
        raw_summary = _extract_article_text(item_body) or _extract_abstract(item_html)
        raw_image = _extract_first_image(item_html)
        arch_nodes = _derive_architecture_nodes(raw_title, raw_summary, item_body, item_html)
        arch_points = _extract_architecture_highlights(item_body or item_html)
        arch_images = _extract_diagram_images(item_html, link)
        arch_sections = _extract_architecture_sections(item_body)
        zh_title = _translate_snippet(raw_title)
        zh_summary = _translate_snippet(raw_summary)
        out.append(
            {
                "title": zh_title or raw_title,
                "title_zh": zh_title or raw_title,
                "title_original": raw_title,
                "summary": (zh_summary or raw_summary or "").strip()[:PRIORITY_CHANNEL_MAX_SUMMARY_CHARS],
                "summary_original": raw_summary,
                "image": raw_image,
                "architecture_points": arch_points,
                "architecture_nodes": arch_nodes,
                "architecture_images": arch_images,
                "architecture_sections": arch_sections,
                "link": link,
                "source": source_name,
                "time": (dt.isoformat() if dt else published),
                "translate": "已翻译" if (zh_title or zh_summary) else "未启用翻译",
            }
        )
        if len(out) >= max_items:
            break
    if not out and fallback:
        for link, item_html in fallback[:max_items]:
            title_match = re.search(r"<title[^>]*>(.*?)</title>", item_html, flags=re.I | re.S)
            raw_title = strip_html(title_match.group(1)) if title_match else "Untitled"
            fallback_body = _extract_main_content_html(item_html)
            raw_summary = _extract_article_text(fallback_body) or _extract_abstract(item_html)
            raw_image = _extract_first_image(item_html)
            arch_nodes = _derive_architecture_nodes(raw_title, raw_summary, fallback_body, item_html)
            arch_points = _extract_architecture_highlights(fallback_body or item_html)
            arch_images = _extract_diagram_images(item_html, link)
            arch_sections = _extract_architecture_sections(fallback_body)
            zh_title = _translate_snippet(raw_title)
            zh_summary = _translate_snippet(raw_summary)
            out.append(
                {
                    "title": zh_title or raw_title,
                    "title_zh": zh_title or raw_title,
                    "title_original": raw_title,
                    "summary": (zh_summary or raw_summary or "").strip()[:PRIORITY_CHANNEL_MAX_SUMMARY_CHARS],
                    "summary_original": raw_summary,
                    "image": raw_image,
                    "architecture_points": arch_points,
                    "architecture_nodes": arch_nodes,
                    "architecture_images": arch_images,
                    "architecture_sections": arch_sections,
                    "link": link,
                    "source": source_name,
                    "time": "发布日期解析失败（未确认是否为近7天）",
                    "translate": "已翻译" if (zh_title or zh_summary) else "未启用翻译",
                }
            )
            if len(out) >= max_items:
                break
    if not out:
        snapshot_key = "claude_blog" if "claude.com" in source_url else "openai_research"
        fallback = _collect_from_priority_snapshot(snapshot_key, now=now)
        if fallback:
            return fallback[:max_items]
    return out


def parse_claude_blog_recent_week() -> List[Dict[str, str]]:
    conf = next((x for x in PRIORITY_CHANNELS if x["key"] == "claude_blog"), None)
    if not conf:
        return []
    return _collect_priority_channel_articles(
        source_name=conf["source"],
        source_url=SOURCE_URLS["claude_blog"],
        max_items=conf.get("max_items", 6),
        is_candidate=lambda href, title: "/blog/" in href.lower() and href.lower().startswith("https://claude.com/"),
    )


def parse_openai_research_recent_week() -> List[Dict[str, str]]:
    conf = next((x for x in PRIORITY_CHANNELS if x["key"] == "openai_research"), None)
    if not conf:
        return []
    return _collect_priority_channel_articles(
        source_name=conf["source"],
        source_url=SOURCE_URLS["openai_research"],
        max_items=conf.get("max_items", 6),
        is_candidate=lambda href, title: href.startswith("https://openai.com/") and (
            ("/index/" in href.lower() and "/blog/" not in href.lower())
            or "/research/" in href.lower()
            or "/zh-hant-hk/research/" in href.lower()
        ),
    )


def _run_tavily_cli_search(query: str) -> Dict[str, object] | None:
    cmd = [
        TAVILY_CLI,
        "search",
        query,
        "--topic",
        "news",
        "--max-results",
        str(AI_SEARCH_MAX_ITEMS_PER_PROVIDER),
        "--include-answer",
        "basic",
        "--json",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=35,
            check=False,
        )
    except Exception as exc:
        return {
            "error": f"tvly 命令调用失败：{exc}",
            "results": [],
        }
    if proc.returncode != 0:
        return {
            "error": (proc.stderr or proc.stdout or "").strip() or "tvly 返回非0码",
            "results": [],
        }
    try:
        payload = json.loads(proc.stdout or "{}")
    except Exception as exc:
        return {
            "error": f"tvly 返回解析失败：{exc}",
            "results": [],
        }
    if isinstance(payload, dict) and "results" not in payload and "answer" in payload:
        return {"results": [payload]}
    if isinstance(payload, list):
        return {"results": payload}
    if isinstance(payload, dict):
        return payload
    return {"results": []}


def _collect_ddg_search(query: str) -> List[Dict[str, object]]:
    text = fetch_url(f"{FALLBACK_SEARCH_URL}{quote(query)}")
    if text.startswith("__ERROR__"):
        return []
    out: List[Dict[str, object]] = []
    for idx, m in enumerate(
        re.finditer(r'<a[^>]+class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', text, re.I | re.S),
        start=1,
    ):
        raw_link = m.group(1)
        raw_title = strip_html(unescape(m.group(2)))
        if not raw_title:
            continue
        link = raw_link
        if raw_link.startswith("/l/?uddg="):
            m2 = re.search(r"uddg=([^&]+)", raw_link)
            if m2:
                link = unquote(m2.group(1))
        out.append(
            _build_ai_search_items(
                source="DuckDuckGo",
                query=query,
                payload_title=raw_title,
                link=link,
                raw_text=raw_title,
            )
        )
        if idx >= AI_SEARCH_MAX_ITEMS_PER_PROVIDER:
            break
    return out


def _collect_tavily_search(query: str) -> List[Dict[str, object]]:
    if AI_SEARCH_SKIP_TAVILY:
        return []
    payload = _run_tavily_cli_search(query)
    if not payload or payload.get("error"):
        return []
    raw_results = []
    if isinstance(payload, dict):
        raw_results = payload.get("results", [])
        if payload.get("results") is None and payload.get("data"):
            raw_results = payload.get("data")
    if not isinstance(raw_results, list):
        return []

    out: List[Dict[str, object]] = []
    for item in raw_results[:AI_SEARCH_MAX_ITEMS_PER_PROVIDER]:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or "").strip()
        link = (item.get("url") or item.get("link") or "").strip()
        snippet = f"{item.get('content', '')} {item.get('snippet', '')}".strip()
        raw_text = snippet or f"来源：{item.get('raw_content', '')}"
        if not title and raw_text:
            title = f"{query}（Tavily 搜索）"
        if not title:
            continue
        out.append(
            _build_ai_search_items(
                source="Tavily Search",
                query=query,
                payload_title=title,
                link=link,
                raw_text=raw_text,
            )
        )
    return out[:AI_SEARCH_MAX_ITEMS_PER_PROVIDER]


def collect_ai_search_updates() -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    seen: set[str] = set()

    for q in AI_SEARCH_QUERIES:
        items = _collect_openai_search(q)
        for item in items:
            key = f"{item.get('title')}|{item.get('source')}"
            if key in seen:
                continue
            seen.add(key)
            out.append(item)

    for q in AI_SEARCH_QUERIES:
        items = _collect_tavily_search(q)
        for item in items:
            key = f"{item.get('title')}|{item.get('source')}"
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
            if len(out) >= AI_SEARCH_MAX_ITEMS_PER_PROVIDER * 2:
                return out[:AI_SEARCH_MAX_ITEMS_PER_PROVIDER * 2]

    if not out:
        for q in AI_SEARCH_QUERIES:
            items = _collect_ddg_search(q)
            for item in items:
                key = f"{item.get('title')}|{item.get('source')}"
                if key in seen:
                    continue
                seen.add(key)
                out.append(item)
    return out[:AI_SEARCH_MAX_ITEMS_PER_PROVIDER * 2]


def parse_openai_rss() -> List[Dict[str, str]]:
    return parse_rss_feed_signals(
        url=[
            SOURCE_URLS["openai_rss"],
            SOURCE_URLS["openai_news_rss"],
            SOURCE_URLS["openai_blog_rss"],
        ],
        source_name="OpenAI 官方",
        keyword_filter=["agent", "codex", "assistant", "responses", "api", "tool", "llm", "agentic", "gpt", "o1", "model"],
    )


def parse_anthropic_rss() -> List[Dict[str, str]]:
    return parse_rss_feed_signals(
        url=[
            SOURCE_URLS["anthropic_rss"],
            SOURCE_URLS["anthropic_managed_agents"],
        ],
        source_name="Anthropic 官方",
        keyword_filter=["agent", "agentic", "claude", "tool", "orchestration", "llm", "automation", "news", "model"],
    )


def parse_anthropic_pages() -> List[Dict[str, str]]:
    page = fetch_first_success([SOURCE_URLS["anthropic_managed_agents"]])
    if page.startswith("__ERROR__"):
        return []
    title = re.search(r"<title>(.*?)</title>", page, re.S)
    time = re.search(r'"datePublished"\s*:\s*"([^"]+)"', page) or re.search(
        r'property="article:published_time"\s*content="([^"]+)"', page
    )
    return [
        {
            "title": "Anthropic Managed Agents：会话与执行分离的任务治理路线",
            "source": "Anthropic 官方",
            "link": SOURCE_URLS["anthropic_managed_agents"],
            "time": (time.group(1) if time else ""),
            "raw": strip_html(title.group(1) if title else "Anthropic 发布"),
        }
    ]


def parse_infoq_feed() -> List[Dict[str, str]]:
    return parse_rss_feed_signals(
        url=SOURCE_URLS["infoq_rss"],
        source_name="INFOQ",
        keyword_filter=["agent", "agentic", "copilot", "ai", "openai", "anthropic", "orchestration", "llm", "模型"],
    )


def parse_rss_feed_signals(url: str | List[str], source_name: str, keyword_filter: List[str]) -> List[Dict[str, str]]:
    return parse_rss_feed_signals_custom(url=url, source_name=source_name, keyword_filter=keyword_filter, max_items=8)


def parse_rss_feed_signals_custom(
    url: str | List[str],
    source_name: str,
    keyword_filter: List[str] | None = None,
    max_items: int = 8,
) -> List[Dict[str, str]]:
    sources = [url] if isinstance(url, str) else list(url)
    for feed_url in sources:
        text = fetch_url(feed_url)
        if text.startswith("__ERROR__"):
            continue
        items: List[Dict[str, str]] = []
        blocks = []
        blocks.extend(re.finditer(r"<item>(.*?)</item>", text, re.S | re.I))
        blocks.extend(re.finditer(r"<entry>(.*?)</entry>", text, re.S | re.I))
        if not blocks:
            continue

        keywords = [k.lower() for k in (keyword_filter or [])]
        for block_match in blocks:
            block = block_match.group(1)
            title = re.search(
                r"<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>|<title\s+type=\"text\">(.*?)</title>",
                block,
                re.S | re.I,
            )
            link = re.search(r"<link[^>]*>(.*?)</link>|<link[^>]*href=[\"']([^\"']+)[\"']", block, re.S | re.I)
            pub = re.search(r"<pubDate>(.*?)</pubDate>|<updated>(.*?)</updated>|<published>(.*?)</published>", block, re.S | re.I)
            t = strip_html((title.group(1) or title.group(2) or title.group(3)) if title else "")
            if not t:
                continue
            low = t.lower()
            if keywords and not any(k in low for k in keywords):
                continue
            raw = t[:220]
            raw = re.sub(r"\s+", " ", raw).strip()
            link_value = ""
            if link:
                link_value = (link.group(1) or link.group(2) or "").strip()
            if not link_value:
                link_value = feed_url
            pub_value = ""
            if pub:
                pub_value = strip_html((pub.group(1) or pub.group(2) or pub.group(3) or "")).strip()
            items.append(
                {
                    "title": t,
                    "source": source_name,
                    "link": link_value,
                    "time": pub_value,
                    "raw": raw,
                }
            )
            if len(items) >= max_items:
                break
        if items:
            return items
    return []


def collect_top_wechat_gzh() -> List[Dict[str, str]]:
    text = fetch_first_success(
        [
            WECHAT_TOP20_GZH_URL,
            "https://www.aigcrank.cn/top/202412gzh",
        ]
    )
    if text.startswith("__ERROR__"):
        return []
    rows = []
    pattern_table = re.compile(r"^\s*([1-9]\d?|20)\s+([^\s|]{2,80})\s+([A-Za-z0-9_\-]{2,60})\s+(\d[\d\.]*)", re.M)
    for m in pattern_table.finditer(text):
        rank = m.group(1).strip()
        name = strip_html(m.group(2))
        wx_id = strip_html(m.group(3))
        score = m.group(4)
        if name and wx_id:
            rows.append(
                {
                    "title": f"{rank}）{name}",
                    "source": "AIGCRank 微信 AI 公告号榜",
                    "link": WECHAT_TOP20_GZH_URL,
                    "raw": f"微信号：{wx_id}｜热度分：{score}",
                }
            )
        if len(rows) >= 20:
            break
    if not rows:
        pattern = re.compile(
            r"^\s*([1-9]\d?|20)\s*\|\s*([^|]{1,80})\s*\|\s*([^|]{1,60})\s*\|\s*([^|\n]{1,20})",
            re.S | re.M,
        )
        for m in pattern.finditer(text):
            rank = m.group(1).strip()
            name = strip_html(m.group(2))
            wx_id = strip_html(m.group(3))
            extra = strip_html(m.group(4))
            if name:
                rows.append(
                    {
                        "title": f"{rank}）{name}",
                        "source": "AIGCRank 微信 AI 公告号榜",
                        "link": WECHAT_TOP20_GZH_URL,
                        "raw": f"微信号：{wx_id}｜{extra}",
                    }
                )
            if len(rows) >= 20:
                break
    if not rows:
        # fallback for layout change in source page
        names = re.findall(r"微信号[:：]?\s*([A-Za-z0-9_\\-]+)", text)
        for idx, wx in enumerate(names[:20], start=1):
            if wx in {""}:
                continue
            rows.append(
                {
                    "title": f"{idx}）{wx}",
                    "source": "AIGCRank 微信 AI 公告号榜",
                    "link": WECHAT_TOP20_GZH_URL,
                    "raw": "解析来源：页面文本",
                }
            )
    return rows


def _collect_twitter_rss(handle: str) -> List[Dict[str, str]]:
    source = f"Twitter/{handle}"
    for instance in TWITTER_RSS_INSTANCES:
        if "nitter" in instance:
            url = f"{instance}/{handle.lower()}/rss"
        else:
            url = f"{instance}/twitter/user/{handle}"
        items = parse_rss_feed_signals_custom(
            url=url,
            source_name=source,
            keyword_filter=["ai", "agent", "llm", "gpt", "model", "automation", "assistant"],
            max_items=8,
        )
        if items:
            return items
    return []


def collect_twitter_ai_updates() -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for company in TWITTER_TECH_ACCOUNTS:
        handle = company["handle"]
        posts = _collect_twitter_rss(handle)
        for post in posts[:4]:
            item = dict(post)
            item["source"] = f"Twitter/{company['name']}（{handle}）"
            out.append(item)
    return out


def collect_broker_ai_reports() -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for broker in BROKER_REPORT_SOURCES:
        name = broker["name"]
        org = quote(name)
        for template in BROKER_REPORT_TEMPLATES:
            url = template.format(org=org)
            text = fetch_url(url)
            if text.startswith("__ERROR__"):
                continue
            seen = set()
            for link, title in re.findall(
                r"<a[^>]*href=[\"']([^\"']*vReport_Show[^\"']*)[\"'][^>]*>([^<]{3,140})</a>",
                text,
                flags=re.S | re.I,
            ):
                if not title:
                    continue
                t = strip_html(title)
                if not any(k.lower() in t.lower() for k in BROKER_AI_KEYWORDS):
                    continue
                if t in seen:
                    continue
                seen.add(t)
                out.append(
                    {
                        "title": t,
                        "source": f"{name} 研报",
                        "link": (link if link.startswith("http") else f"https://stock.finance.sina.com.cn{link}"),
                        "time": "",
                        "raw": "来自券商研究报告检索（sina）",
                        "broker": name,
                    }
                )
                if len(out) >= 12:
                    break
            if out and len(out) >= 12:
                break
        if len(out) >= 12:
            break
    return out


def collect_frontier_signals() -> List[Dict[str, str]]:
    raw: List[Dict[str, str]] = []
    for ch in FRONTIER_CHANNELS:
        raw.extend(
            parse_rss_feed_signals(
                url=ch["url"],
                source_name=ch["source"],
                keyword_filter=MODEL_DECISION_KEYWORDS,
            )
        )
    ranked: List[Dict[str, str]] = []
    for item in raw:
        text = f"{item.get('title', '')} {item.get('raw', '')}".lower()
        score = 0
        for kw in MODEL_DECISION_KEYWORDS:
            if kw in text:
                score += 1
        if score <= 0:
            continue
        r = dict(item)
        r["score"] = score
        ranked.append(r)
    ranked = sorted(ranked, key=lambda x: (x.get("score", 0), x.get("time", "")), reverse=True)
    dedup = []
    seen = set()
    for item in ranked:
        k = f"{item.get('title', '')}::{item.get('link', '')}"
        if k in seen:
            continue
        seen.add(k)
        dedup.append(item)
    return dedup[:12]


def parse_agui_and_gh_stars() -> Dict[str, str]:
    result = {"repo": "ag-ui-protocol/ag-ui", "link": "https://github.com/ag-ui-protocol/ag-ui", "status": "ok", "stars": ""}
    api_text = fetch_url(SOURCE_URLS["agui_repo"])
    if not api_text.startswith("__ERROR__"):
        star = re.search(r'"stargazers_count"\s*:\s*(\d+)', api_text)
        if star:
            result["stars"] = star.group(1)
        desc = re.search(r'"description"\s*:\s*"([^"]{0,220})"', api_text)
        if desc:
            result["description"] = unescape(desc.group(1))
        return result
    result["status"] = "unreachable"
    return result


def parse_github_trending_weekly() -> List[Dict[str, str]]:
    for source_url in (SOURCE_URLS["github_trend_json_weekly"], SOURCE_URLS["github_trend_json_daily"]):
        text = fetch_url(source_url)
        if text.startswith("__ERROR__"):
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        items = payload.get("repositories") if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            continue
        projects = []
        for it in items[:200]:
            repo = (it.get("owner") + "/" + it.get("repo")) if isinstance(it, dict) else ""
            total = str(it.get("stars", "0")) if isinstance(it, dict) else "0"
            inc = str(it.get("trend", "0")) if isinstance(it, dict) else "0"
            total_i = int(str(total).replace(",", "").strip() or 0)
            inc_i = int(str(inc).replace(",", "").strip() or 0)
            if repo and total_i >= 10000 and inc_i >= 2000:
                projects.append(
                    {
                        "repo": repo,
                        "link": github_repo_link(repo),
                        "source": "GitHub Trending API",
                        "currentStars": total_i,
                        "delta7d": inc_i,
                        "highlights": "近7天增量满足阈值，持续观察变化。",
                    }
                )
            if len(projects) >= 12:
                break
        if projects:
            return projects

    text = fetch_url(SOURCE_URLS["github_trend_weekly"])
    if text.startswith("__ERROR__"):
        return []
    projects = []
    for item in re.finditer(
        r'github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+).*?(\d{1,3}(?:,\d{3})*)\s*stars?\s*.*?(\d{1,3}(?:,\d{3})*)\s*increase',
        text,
        re.S | re.I,
    ):
        repo = item.group(1)
        total = item.group(2).replace(",", "")
        inc = item.group(3).replace(",", "")
        if repo and total.isdigit() and inc.isdigit():
            total_i = int(total)
            inc_i = int(inc)
            if total_i >= 10000 and inc_i >= 2000:
                projects.append(
                    {
                        "repo": repo,
                        "link": github_repo_link(repo),
                        "source": "GitHub Trending",
                        "currentStars": total_i,
                        "delta7d": inc_i,
                        "highlights": "近7天增量满足阈值，持续观察变化。",
                    }
                )
        if len(projects) >= 12:
            break
    if projects:
        return projects
    return []


def _project_tokens(repo: str, extra: str = "") -> List[str]:
    base = repo.lower().replace("-", " ")
    repo_tokens = re.findall(r"[a-zA-Z0-9]+", base)
    extra_tokens = re.findall(r"[a-zA-Z0-9]+", (extra or "").lower())
    return list({*repo_tokens, *extra_tokens})


def _match_tokens(tokens: List[str], keywords: List[str]) -> bool:
    token_set = {t.lower() for t in tokens}
    return any(k in token_set for k in keywords)


def _score_project_for_keywords(project: Dict[str, object], keywords: List[str]) -> int:
    repo = str(project.get("repo", "")).lower()
    repo_tokens = set(_project_tokens(repo))
    score = 0
    for key in keywords:
        if key in repo_tokens:
            score += 2
    text = (
        f"{repo} "
        f"{str(project.get('highlights', '')).lower()} "
        f"{str(project.get('raw', '')).lower()} "
        f"{str(project.get('description', '')).lower()} "
        f"{str(project.get('source', '')).lower()}"
    )
    if any(k in repo for k in keywords):
        score += 1
    for key in keywords:
        if key in text:
            score += 1
    return score


def fetch_repo_meta(repo: str) -> Dict[str, object]:
    repo_clean = (repo or "").strip()
    if not repo_clean:
        return {}
    api_url = f"https://api.github.com/repos/{repo_clean}"
    api_text = fetch_url(api_url)
    if api_text.startswith("__ERROR__"):
        return {
            "repo": repo_clean,
            "source": "GitHub（API 不可达）",
            "link": github_repo_link(repo_clean),
            "currentStars": 0,
            "delta7d": 0,
            "highlights": "当前 API 无法抓取，先回填仓库名用于兜底展示。",
        }
    try:
        data = json.loads(api_text)
    except Exception:
        return {
            "repo": repo_clean,
            "source": "GitHub（结构解析失败）",
            "link": github_repo_link(repo_clean),
            "currentStars": 0,
            "delta7d": 0,
            "highlights": "结构解析失败，先回填仓库名用于兜底展示。",
        }
    return {
        "repo": repo_clean,
        "source": "GitHub 热门项目（兜底）",
        "link": data.get("html_url", github_repo_link(repo_clean)),
        "currentStars": data.get("stargazers_count", 0) or 0,
        "delta7d": 0,
        "highlights": data.get("description", "热门 AI 开源项目，供趋势观察与对比。") or "热门 AI 开源项目，供趋势观察与对比。",
    }


def select_top_skills(trend_projects: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    result: Dict[str, Dict[str, object]] = {}
    fallback_pool: List[Dict[str, object]] = []
    fallback_by_repo: Dict[str, Dict[str, object]] = {}
    if trend_projects:
        sorted_projects = sorted(
            trend_projects,
            key=lambda p: (int(p.get("currentStars", 0)), int(p.get("delta7d", 0))),
            reverse=True,
        )
    else:
        fallback_names: List[str] = []
        for cfg in SKILL_CATEGORIES.values():
            for repo in cfg.get("fallback", []):
                if repo not in fallback_names:
                    fallback_names.append(repo)
        for name in fallback_names:
            item = fetch_repo_meta(name)
            if item:
                fallback_pool.append(item)
        fallback_by_repo = {str(item.get("repo", "")).lower(): item for item in fallback_pool}
        sorted_projects = sorted(fallback_pool, key=lambda p: int(p.get("currentStars", 0)), reverse=True)

    for key, cfg in SKILL_CATEGORIES.items():
        keywords = cfg["keywords"]
        matched = []
        selected_repos: set[str] = set()
        for p in sorted_projects:
            score = _score_project_for_keywords(p, [k.lower() for k in keywords])
            if score > 0:
                repo_key = str(p.get("repo", "")).lower()
                if repo_key in selected_repos:
                    continue
                p["why"] = p.get("why") or f"匹配关键词：{','.join(keywords)}"
                p["highlights"] = p.get("highlights", "与目标场景相关，持续观察。")
                matched.append(p)
                selected_repos.add(repo_key)
            if len(matched) >= 3:
                break

        if len(matched) < 3:
            for repo in cfg.get("fallback", []):
                if len(matched) >= 3:
                    break
                repo_key = str(repo).strip().lower()
                if repo_key in selected_repos:
                    continue
                fb = fallback_by_repo.get(repo_key)
                if not fb:
                    fb = fetch_repo_meta(repo)
                    if fb:
                        fallback_by_repo[repo_key] = fb
                        fallback_pool.append(fb)
                if fb:
                    item = dict(fb)
                    item["why"] = f"兜底展示：{repo}"
                    item["highlights"] = item.get("highlights", "高流量项目，作为 SKILL 兜底参考。")
                    matched.append(item)
                    selected_repos.add(repo_key)

        status = "未抓到可直接匹配的 GitHub 趋势样本，已使用高知名度项目兜底"
        if trend_projects:
            status = "命中样本不足" if len(matched) < 3 else "已匹配"
            if len(matched) >= 3:
                status = "已匹配"
        elif matched:
            status = "使用高知名度仓库兜底"

        result[key] = {
            "title": cfg["title"],
            "description": cfg["description"],
            "status": status,
            "items": matched[:3],
        }
    return result


def summarize_focus_domains(signals: List[Dict[str, str]]) -> List[Dict[str, object]]:
    pool: List[Dict[str, str]] = []
    for sig in signals:
        pool.append(
            {
                "title": sig.get("title", ""),
                "source": sig.get("source", ""),
                "link": sig.get("link", ""),
                "time": sig.get("time", ""),
                "snippet": sig.get("raw", ""),
            }
        )

    out = []
    for key, conf in FOCUSED_FIELDS.items():
        picked = []
        keys = [k.lower() for k in conf["keywords"]]
        for item in pool:
            text = f"{item['title']} {item['source']} {item['snippet']}".lower()
            if any(k in text for k in keys):
                picked.append(item)
        if not picked:
            picked = [
                {
                    "title": "暂无明确新增信号",
                    "source": "本地汇总",
                    "link": "",
                    "time": "",
                    "snippet": "建议补齐对应渠道（AG-UI、向量检索、图谱、生成式模型）周度跟踪。",
                }
            ]
        out.append(
            {
                "field": conf["title"],
                "items": picked[:4],
            }
        )
    return out


def fallback_github_projects() -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for cfg in SKILL_CATEGORIES.values():
        for repo in cfg.get("fallback", []):
            meta = fetch_repo_meta(repo)
            if meta:
                out.append(meta)
    return out


def _run_applescript(script_body: str) -> str:
    if platform.system() != "Darwin":
        return "__UNSUPPORTED__"
    try:
        completed = subprocess.run(
            ["osascript", "-e", script_body],
            check=False,
            capture_output=True,
            text=True,
            timeout=12,
        )
        if completed.returncode != 0:
            return f"__ERROR__:{completed.stderr.strip()}"
        return completed.stdout.strip()
    except Exception as exc:
        return f"__ERROR__:{exc}"


def summarize_hot_topics(report: Dict) -> List[Dict[str, object]]:
    texts = []
    for item in report.get("aiHighlights", []):
        texts.append(item.get("title", ""))
        texts.append(item.get("raw", ""))
    for item in report.get("trendProjects", []):
        texts.append(str(item.get("repo", "")))
        texts.append(str(item.get("highlights", "")))
    for item in report.get("skillTop", {}).values():
        for p in item.get("items", []):
            texts.append(str(p.get("repo", "")))
            texts.append(str(p.get("highlights", "")))
            texts.append(str(p.get("why", "")))
    for item in report.get("wechatTop20", []):
        texts.append(str(item.get("title", "")))
        texts.append(str(item.get("raw", "")))
    for item in report.get("twitterUpdates", []):
        texts.append(str(item.get("title", "")))
    for item in report.get("brokerReports", []):
        texts.append(str(item.get("title", "")))
    counter = Counter()
    stop = {
        "the",
        "of",
        "and",
        "to",
        "in",
        "for",
        "with",
        "on",
        "a",
        "an",
        "is",
        "are",
        "this",
        "that",
        "from",
        "about",
        "its",
        "it",
        "more",
        "as",
        "into",
        "at",
        "by",
        "or",
        "we",
        "you",
        "they",
        "have",
        "has",
        "using",
        "via",
        "how",
        "what",
        "who",
        "will",
        "today",
        "2026",
        "AI",
        "ai",
    }
    for s in texts:
        for w in re.findall(r"[a-zA-Z0-9\-\+]+", str(s).lower()):
            if len(w) > 2 and w not in stop:
                counter[w] += 1
    topics = []
    for term, count in counter.most_common(8):
        topics.append({"term": term, "count": count})
    return topics


def build_diff_summary(today: Dict, merged_with: Dict | None) -> str:
    if not merged_with:
        return "本日新增数据：独立日报，未命中7天窗口内同主题报告。"
    before = extract_topics(merged_with)
    after = extract_topics(today)
    delta = sorted(after - before)
    if delta:
        return "本日与前7天报告有关联，新增差异：" + "、".join(delta[:12])
    return "本日与前7天报告高度相关，新增信号集中在既有主题延续推进。"


def collect_calendar() -> Dict[str, object]:
    return {"status": "disabled", "items": [], "note": "已按配置禁用：本次不采集 Calendar。"}

    # legacy path removed: AppleScript collection disabled by user preference.
    script = (
        'tell application "Calendar"\n'
        "set theDate to (current date)\n"
        'set tomorrow to theDate + 24 * hours\n'
        "set startTime to theDate\n"
        "set endTime to tomorrow + 24 * hours\n"
        "set out to \"\"\n"
        "repeat with aCalendar in calendars\n"
        "set evs to (every event of aCalendar whose start date ≥ startTime and start date ≤ endTime)\n"
        "repeat with e in evs\n"
        "set out to out & (summary of e as string) & \"|\" & (start date of e as string) & \"\\n\"\n"
        "end repeat\n"
        "end repeat\n"
        "if out = \"\" then return \"none\"\n"
        "return out\n"
        "end tell"
    )
    text = _run_applescript(script)
    if text.startswith("__ERROR__") or text.startswith("__UNSUPPORTED__") or text == "none":
        status = "unreachable" if text != "none" else "empty"
        return {"status": status, "items": [], "note": "当前环境未能读取 Calendar 数据。"}
    items = []
    for line in text.splitlines():
        if "|" in line:
            title, when = line.split("|", 1)
            items.append({"title": title.strip(), "time": when.strip()})
    return {"status": "ok", "items": items[:10], "note": ""}


def collect_mail() -> Dict[str, object]:
    return {"status": "disabled", "items": [], "note": "已按配置禁用：本次不采集 Apple Mail。"}

    # legacy path removed: AppleScript collection disabled by user preference.
    script = (
        'tell application "Mail"\n'
        "set out to \"\"\n"
        "set inboxMsgs to (messages of inbox whose read status is false)\n"
        "repeat with m in inboxMsgs\n"
        "set subj to (subject of m)\n"
        "set senderName to extract name from sender of m\n"
        "set out to out & subj & \"|\" & senderName & \"\\n\"\n"
        "if (count of words of out) > 200 then exit repeat\n"
        "end repeat\n"
        "if out = \"\" then return \"none\"\n"
        "return out\n"
        "end tell"
    )
    text = _run_applescript(script)
    if text.startswith("__ERROR__") or text.startswith("__UNSUPPORTED__"):
        return {"status": "unreachable", "items": [], "note": "当前环境未能读取 Mail 数据。"}
    if text == "none":
        return {"status": "empty", "items": [], "note": "无未读邮件。"}
    items = []
    for line in text.splitlines():
        if "|" in line:
            subj, sender = line.split("|", 1)
            items.append({"subject": subj.strip(), "from": sender.strip()})
    return {"status": "ok", "items": items[:10], "note": ""}


def collect_notes() -> Dict[str, object]:
    script = (
        'tell application "Notes"\n'
        "set out to \"\"\n"
        'set theNotes to (every note of default account)\n'
        "set topNotes to first 10 of (reverse of theNotes)\n"
        'repeat with n in topNotes\n'
        "set nName to name of n\n"
        "set out to out & nName & \"|\" & (body of n as string) & \"\\n\"\n"
        "end repeat\n"
        "if out = \"\" then return \"none\"\n"
        "return out\n"
        "end tell"
    )
    text = _run_applescript(script)
    if text.startswith("__ERROR__") or text.startswith("__UNSUPPORTED__"):
        return {"status": "unreachable", "items": [], "note": "当前环境未能读取 Notes 数据。"}
    if text == "none":
        return {"status": "empty", "items": [], "note": "无可读取备忘录。"}
    items = []
    for line in text.splitlines():
        if "|" in line:
            title, _body = line.split("|", 1)
            items.append({"title": title.strip()[:80]})
    return {"status": "ok", "items": items, "note": ""}


def _parse_smtp_host(host_entry: str) -> tuple[str, int]:
    if ":" in host_entry:
        host, port = host_entry.rsplit(":", 1)
        host = host.strip()
        try:
            return host, int(port)
        except ValueError:
            return host, EMAIL_SMTP_PORT
    return host_entry.strip(), EMAIL_SMTP_PORT


def _build_email_message(
    report_markdown: str,
    report_html: str,
    title: str,
    output_file: str,
    recipients: List[str],
) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = EMAIL_FROM or EMAIL_SMTP_USER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = title
    msg["Date"] = datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0800")
    body_text = (report_markdown or "AI 研究日报").strip()[:12000]
    msg.set_content(body_text)
    msg.add_alternative(report_html, subtype="html")
    return msg


def _split_recipients(raw: str) -> List[str]:
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def _check_smtp_reachability(host: str, port: int) -> tuple[bool, str]:
    try:
        info = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        if not info:
            return False, "DNS 解析成功但未返回地址"
    except Exception as exc:
        return False, f"DNS/解析失败: {exc}"
    try:
        with socket.create_connection((host, port), timeout=min(EMAIL_SMTP_CHECK_TIMEOUT, max(1.0, EMAIL_SMTP_TIMEOUT))):
            return True, ""
    except Exception as exc:
        return False, f"TCP 连接失败: {exc}"


def _send_via_resend(msg: EmailMessage, recipients: List[str]) -> Dict[str, object]:
    if not EMAIL_RESEND_API_KEY:
        return {"status": "skip", "reason": "未配置 DAILY_AI_BRIEF_RESEND_API_KEY"}
    send_from = EMAIL_RESEND_FROM or (EMAIL_FROM or EMAIL_SMTP_USER or recipients[0])
    plain_body = msg.get_body(preferencelist=("plain",))
    html_body = msg.get_body(preferencelist=("html",))
    text = plain_body.get_content() if plain_body else msg.get_content()
    html = html_body.get_content() if html_body else ""
    payload = {
        "from": send_from,
        "to": recipients,
        "subject": msg["Subject"] or "",
        "text": text or "",
        "html": html or "",
    }
    request = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {EMAIL_RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as resp:
            body_text = resp.read().decode("utf-8", errors="ignore")
            status_code = getattr(resp, "status", 0)
            if status_code in (200, 201, 202):
                return {"status": "sent", "method": "resend", "status_code": status_code, "response": body_text}
            return {"status": "error", "reason": f"resend HTTP {status_code}: {body_text or '无返回'}"}
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}


def _send_via_sendgrid(msg: EmailMessage, recipients: List[str]) -> Dict[str, object]:
    if not EMAIL_SENDGRID_API_KEY:
        return {"status": "skip", "reason": "未配置 DAILY_AI_BRIEF_SENDGRID_API_KEY"}
    send_from = EMAIL_SENDGRID_FROM or (EMAIL_FROM or EMAIL_SMTP_USER or recipients[0])
    plain_body = msg.get_body(preferencelist=("plain",))
    html_body = msg.get_body(preferencelist=("html",))
    text_content = plain_body.get_content() if plain_body else (msg.get_content() or "")
    html_content = html_body.get_content() if html_body else ""
    payload = {
        "personalizations": [
            {
                "to": [{"email": r} for r in recipients],
                "subject": msg["Subject"] or "",
            }
        ],
        "from": {"email": send_from},
        "content": (
            [{"type": "text/plain", "value": text_content}, {"type": "text/html", "value": html_content}]
            if html_content
            else [{"type": "text/plain", "value": text_content}]
        ),
    }
    request = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {EMAIL_SENDGRID_API_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            status_code = getattr(resp, "status", 0)
            if 200 <= status_code < 300:
                return {"status": "sent", "method": "sendgrid", "status_code": status_code, "response": body}
            return {"status": "error", "reason": f"sendgrid HTTP {status_code}: {body or '无返回'}"}
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}


def _queue_unsent_email(msg: EmailMessage, reason: str) -> None:
    if not EMAIL_QUEUE_ON_FAIL:
        return
    try:
        EMAIL_OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
        safe_ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        outbox_path = EMAIL_OUTBOX_DIR / f"daily_ai_brief_{safe_ts}.json"
        with outbox_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "created_at": datetime.now().isoformat(),
                    "reason": reason,
                    "to": msg["To"] or "",
                    "subject": msg["Subject"] or "",
                    "from": msg["From"] or "",
                    "raw": msg.as_string(),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception:
        return


def _queue_terminal_email(msg: EmailMessage, reason: str) -> Path | None:
    try:
        EMAIL_TERMINAL_OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
        safe_ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        outbox_path = EMAIL_TERMINAL_OUTBOX_DIR / f"daily_ai_terminal_{safe_ts}.json"
        with outbox_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "created_at": datetime.now().isoformat(),
                    "reason": reason,
                    "to": msg["To"] or "",
                    "subject": msg["Subject"] or "",
                    "from": msg["From"] or "",
                    "raw": msg.as_string(),
                    "method": "terminal",
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        return outbox_path
    except Exception:
        return None



def _build_tls_context() -> ssl.SSLContext:
    candidates = [EMAIL_SMTP_CA_FILE, "/etc/ssl/cert.pem", ssl.get_default_verify_paths().cafile]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            try:
                return ssl.create_default_context(cafile=str(candidate))
            except Exception:
                continue
    return ssl.create_default_context()


def _send_via_smtp(
    host: str,
    port: int,
    msg: EmailMessage,
) -> Dict[str, object]:
    try:
        context = _build_tls_context()
        if EMAIL_SMTP_SSL:
            with smtplib.SMTP_SSL(host, port, timeout=EMAIL_SMTP_TIMEOUT, context=context) as smtp:
                smtp.login(EMAIL_SMTP_USER, EMAIL_SMTP_PASSWORD)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=EMAIL_SMTP_TIMEOUT) as smtp:
                if EMAIL_SMTP_STARTTLS:
                    smtp.starttls(context=context)
                smtp.login(EMAIL_SMTP_USER, EMAIL_SMTP_PASSWORD)
                smtp.send_message(msg)
        return {"status": "sent", "smtp_host": host, "smtp_port": port}
    except Exception as exc:
        return {"status": "error", "smtp_host": host, "smtp_port": port, "reason": str(exc)}


def _send_via_sendmail(msg: EmailMessage) -> Dict[str, object]:
    if not EMAIL_SENDMAIL_PATH:
        return {"status": "skip", "reason": "未配置 DAILY_AI_BRIEF_SENDMAIL_PATH"}
    sendmail = Path(EMAIL_SENDMAIL_PATH)
    if not sendmail.exists():
        return {"status": "error", "reason": f"sendmail 路径不存在：{sendmail}"}
    try:
        proc = subprocess.run(
            [str(sendmail), "-t"],
            input=msg.as_bytes(),
            capture_output=True,
            text=False,
            timeout=20,
        )
        if proc.returncode == 0:
            return {"status": "sent", "method": "sendmail", "path": str(sendmail)}
        return {"status": "error", "reason": proc.stderr.decode(errors="ignore") if proc.stderr else "sendmail 返回非0"}
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}


def _send_via_http_api(msg: EmailMessage, recipients: List[str]) -> Dict[str, object]:
    provider = EMAIL_API_PROVIDER if EMAIL_API_PROVIDER else None
    if provider:
        if provider == "resend":
            return _send_via_resend(msg, recipients)
        if provider == "sendgrid":
            return _send_via_sendgrid(msg, recipients)
        return {"status": "error", "reason": f"不支持的 EMAIL API 提供商: {provider}"}

    if EMAIL_RESEND_API_KEY:
        return _send_via_resend(msg, recipients)
    if EMAIL_SENDGRID_API_KEY:
        return _send_via_sendgrid(msg, recipients)
    return {"status": "skip", "reason": "未配置 RESEND_API_KEY 或 SENDGRID_API_KEY"}


def email_transport_check() -> Dict[str, object]:
    checks = {"smtp": [], "sendmail": {}, "api": {}}
    for host in EMAIL_SMTP_HOSTS if EMAIL_SMTP_HOSTS else [EMAIL_SMTP_HOST]:
        host, port = _parse_smtp_host(host)
        reachable, reason = _check_smtp_reachability(host, port)
        checks["smtp"].append(
            {
                "host": host,
                "port": port,
                "reachable": reachable,
                "reason": reason,
            }
        )
    if EMAIL_SENDMAIL_PATH:
        sendmail = Path(EMAIL_SENDMAIL_PATH)
        checks["sendmail"] = {
            "configured": True,
            "path": str(sendmail),
            "exists": sendmail.exists(),
        }
    else:
        checks["sendmail"] = {"configured": False}
    checks["api"] = {
        "provider": EMAIL_API_PROVIDER or "auto",
        "resend_configured": bool(EMAIL_RESEND_API_KEY),
        "sendgrid_configured": bool(EMAIL_SENDGRID_API_KEY),
        "terminal_queue_dir": str(EMAIL_TERMINAL_OUTBOX_DIR),
        "force_terminal_email": EMAIL_FORCE_TERMINAL,
    }
    checks["transport"] = EMAIL_TRANSPORT
    return checks


def _email_config_snapshot() -> Dict[str, object]:
    return {
        "enabled": EMAIL_ENABLED,
        "recipients": EMAIL_RECIPIENT or "",
        "transport": EMAIL_TRANSPORT,
        "sendmail_path": EMAIL_SENDMAIL_PATH,
        "smtp_hosts": EMAIL_SMTP_HOSTS if EMAIL_SMTP_HOSTS else [EMAIL_SMTP_HOST],
        "smtp_ssl": EMAIL_SMTP_SSL,
        "smtp_starttls": EMAIL_SMTP_STARTTLS,
        "smtp_port": EMAIL_SMTP_PORT,
        "smtp_check_timeout": EMAIL_SMTP_CHECK_TIMEOUT,
        "terminal_outbox_dir": str(EMAIL_TERMINAL_OUTBOX_DIR),
        "force_terminal_email": EMAIL_FORCE_TERMINAL,
        "smtp_user_explicit": bool(_read_env_first("DAILY_AI_BRIEF_SMTP_USER", "EMAIL_SMTP_USER", "SMTP_USER", "SMTP_USERNAME")),
        "smtp_user_set": bool(EMAIL_SMTP_USER),
        "smtp_password_set": bool(EMAIL_SMTP_PASSWORD),
        "http_api_provider": EMAIL_API_PROVIDER or "auto",
        "resend_configured": bool(EMAIL_RESEND_API_KEY),
        "sendgrid_configured": bool(EMAIL_SENDGRID_API_KEY),
        "queue_on_fail": EMAIL_QUEUE_ON_FAIL,
        "from": EMAIL_FROM or EMAIL_SMTP_USER or "",
    }


def send_report_email(report_markdown: str, report_html: str, title: str, output_file: str) -> Dict[str, object]:
    if not EMAIL_ENABLED:
        return {"status": "skip", "reason": "DAILY_AI_BRIEF_SEND_EMAIL 未开启", "email_config": _email_config_snapshot()}
    if not EMAIL_RECIPIENT:
        return {"status": "skip", "reason": "未设置收件人（DAILY_AI_BRIEF_EMAIL_TO / EMAIL_TO_ADDRESSES）", "email_config": _email_config_snapshot()}
    recipients = _split_recipients(EMAIL_RECIPIENT)
    if not recipients:
        return {"status": "skip", "reason": "未设置有效收件人（DAILY_AI_BRIEF_EMAIL_TO / EMAIL_TO_ADDRESSES）", "email_config": _email_config_snapshot()}
    msg = _build_email_message(
        report_markdown=report_markdown,
        report_html=report_html,
        title=title,
        output_file=output_file,
        recipients=recipients,
    )

    attempted = []
    last_error = ""
    used_smtp = False
    smtp_tried = False
    sendmail_tried = False
    api_tried = False
    transport = EMAIL_TRANSPORT
    terminal_mode = (
        transport == "terminal"
        or EMAIL_FORCE_TERMINAL
        or (
            transport in {"auto", "smtp", "sendmail", "api", "resend", "sendgrid"}
            and os.getenv("CODEX_SANDBOX_NETWORK_DISABLED", "").strip().lower() in ("1", "true", "yes", "on")
        )
    )

    if terminal_mode:
        terminal_sender = Path(__file__).with_name("daily_ai_terminal_sender.py")
        queued = _queue_terminal_email(msg, "已切到终端发送模式：请在终端执行发送命令")
        if not queued:
            return {
                "status": "error",
                "reason": "终端发送模式队列写入失败",
                "attempted": attempted,
                "email_config": _email_config_snapshot(),
            }
        return {
            "status": "queued",
            "queue_path": str(queued),
            "recipients": recipients,
            "from": msg["From"],
            "attempted": attempted,
            "transport": "terminal",
            "email_config": _email_config_snapshot(),
            "terminal_command": f"python3 {terminal_sender} --file {queued}",
        }

    if transport in {"auto", "smtp"} and EMAIL_SMTP_USER and EMAIL_SMTP_PASSWORD:
        used_smtp = True
        smtp_tried = True
        hosts = EMAIL_SMTP_HOSTS if EMAIL_SMTP_HOSTS else [EMAIL_SMTP_HOST]
        for h in hosts:
            host, port = _parse_smtp_host(h)
            attempted.append(f"{host}:{port}")
            reachable, reach_reason = _check_smtp_reachability(host, port)
            if not reachable:
                last_error = f"{host}:{port} 不可达 ({reach_reason})"
                continue
            smtp_result = _send_via_smtp(host, port, msg)
            if smtp_result.get("status") == "sent":
                smtp_result["recipients"] = recipients
                smtp_result["from"] = msg["From"]
                smtp_result["attempted_hosts"] = attempted
                smtp_result["email_config"] = _email_config_snapshot()
                smtp_result["transport"] = "smtp"
                return smtp_result
            last_error = str(smtp_result.get("reason", ""))
    elif transport in {"auto", "smtp"}:
        last_error = "SMTP 用户名/密码缺失"

    if transport in {"auto", "sendmail"} and EMAIL_SENDMAIL_PATH:
        sendmail_tried = True
        sendmail_result = _send_via_sendmail(msg)
        if sendmail_result.get("status") == "sent":
            sendmail_result["recipients"] = recipients
            sendmail_result["from"] = msg["From"]
            sendmail_result["attempted_smtp_hosts"] = attempted
            sendmail_result["email_config"] = _email_config_snapshot()
            sendmail_result["transport"] = "sendmail"
            return sendmail_result
        if transport == "sendmail":
            _queue_unsent_email(msg, sendmail_result.get("reason", ""))
            return {
                "status": "error",
                "reason": f"sendmail 发送失败：{sendmail_result.get('reason', '')}",
                "attempted": attempted,
                "sendmail_error": sendmail_result.get("reason", ""),
                "email_config": _email_config_snapshot(),
            }
        last_error = f"{last_error}; sendmail 失败：{sendmail_result.get('reason', '')}".strip("; ")

    if transport in {"auto", "api", "resend", "sendgrid"}:
        api_tried = True
        api_result = _send_via_http_api(msg, recipients)
        if api_result.get("status") == "sent":
            api_result["recipients"] = recipients
            api_result["from"] = msg["From"]
            api_result["attempted_smtp_hosts"] = attempted
            api_result["email_config"] = _email_config_snapshot()
            api_result["transport"] = f"api:{api_result.get('method', 'unknown')}"
            return api_result
        if used_smtp or transport in {"api", "resend", "sendgrid"}:
            final_reason = f"HTTP API 发送失败: {api_result.get('reason', '')}"
            _queue_unsent_email(msg, final_reason)
            return {
                "status": "error",
                "reason": final_reason,
                "attempted": attempted,
                "api_error": api_result.get("reason", ""),
                "email_config": _email_config_snapshot(),
            }
        last_error = f"{last_error}; HTTP API 未配置/失败: {api_result.get('reason', '')}".strip("; ")

    if transport == "smtp":
        if used_smtp:
            return {
                "status": "error",
                "reason": f"SMTP 发送失败: {'; '.join(attempted)}; 最后错误：{last_error}",
                "attempted": attempted,
                "email_config": _email_config_snapshot(),
            }
        return {
            "status": "skip",
            "reason": "未配置 SMTP 用户名/密码。",
            "attempted": attempted,
            "email_config": _email_config_snapshot(),
        }

    if transport == "sendmail":
        _queue_unsent_email(msg, last_error or "sendmail 未配置或未执行成功")
        return {
            "status": "error",
            "reason": f"sendmail 未成功: {last_error}",
            "attempted": attempted,
            "email_config": _email_config_snapshot(),
        }

    if not (smtp_tried or sendmail_tried or api_tried):
        return {
            "status": "skip",
            "reason": "未配置可用邮件发送路径：请设置 SMTP 凭据、sendmail 或邮件 API。",
            "attempted": attempted,
            "email_config": _email_config_snapshot(),
        }

    final_reason = f"SMTP/Sendmail/API 未成功: {'; '.join(attempted)}; 最后错误：{last_error}"
    _queue_unsent_email(msg, final_reason)
    return {"status": "error", "reason": final_reason, "attempted": attempted, "email_config": _email_config_snapshot()}


def _top_words(texts: List[str]) -> set:
    stop = {
        "the",
        "of",
        "and",
        "to",
        "in",
        "for",
        "with",
        "on",
        "a",
        "an",
        "is",
        "are",
        "this",
        "that",
        "from",
        "about",
        "its",
        "it",
        "more",
        "as",
        "into",
        "at",
        "on",
        "by",
        "or",
        "to",
        "we",
        "you",
        "they",
        "have",
        "has",
        "using",
        "via",
        "how",
        "what",
        "who",
        "will",
    }
    tokens = set()
    for s in texts:
        for w in re.findall(r"[a-zA-Z0-9][a-zA-Z0-9+\\-]+", s.lower()):
            if len(w) > 2 and w not in stop:
                tokens.add(w)
    return tokens


def extract_topics(report: Dict) -> set:
    texts = [x.get("title", "") for x in report.get("aiHighlights", [])]
    texts += [str(x.get("repo", "")) for x in report.get("trendProjects", [])]
    texts += [str(x.get("repo", "")) for x in report.get("trendLinks", [])]
    for bucket in report.get("skillTop", {}).values():
        texts += [str(x.get("repo", "")) for x in bucket.get("items", [])]
    for block in report.get("priorityChannelHighlights", []):
        for item in block.get("items", []):
            texts.append(str(item.get("title", "")))
            texts.append(str(item.get("title_original", "")))
            texts.append(str(item.get("summary", "")))
    texts.append(report.get("title", ""))
    return _top_words(texts)


def merge_if_related(history: List[Dict], today_report: Dict) -> tuple[bool, Dict]:
    now = today_report["date"]
    today_dt = datetime.fromisoformat(now)
    window_start = today_dt - timedelta(days=RELATED_WINDOW_DAYS)
    top = _top_words([now])
    top.update(extract_topics(today_report))
    candidates = []
    for idx, rec in enumerate(history):
        try:
            r_date = datetime.fromisoformat(rec["date"])
        except Exception:
            continue
        if r_date < window_start or r_date >= today_dt:
            continue
        overlap = top & extract_topics(rec)
        if len(overlap) >= 2:
            candidates.append((len(overlap), idx, overlap))
    if not candidates:
        return False, today_report
    # merge into the latest matching report
    _, idx, overlap = max(candidates, key=lambda x: (x[0], x[1]))
    target = history[idx]
    existing_titles = {i["title"] for i in target.get("aiHighlights", [])}
    for item in today_report.get("aiHighlights", []):
        if item.get("title") not in existing_titles:
            target.setdefault("aiHighlights", []).append(item)
    existing_repos = {i.get("repo") for i in target.get("trendProjects", [])}
    for p in today_report.get("trendProjects", []):
        if p.get("repo") not in existing_repos:
            target.setdefault("trendProjects", []).append(p)
    if "relatedMergeLog" not in target:
        target["relatedMergeLog"] = []
    target["relatedMergeLog"].append(
        {
            "mergedDate": now,
            "overlapKeywords": sorted(overlap),
            "delta": today_report.get("diffSummary", ""),
        }
    )
    target["updated_at"] = datetime.now(timezone.utc).astimezone().isoformat()
    return True, target


def _last_valid_history_items(history: List[Dict], field: str, max_items: int = 20) -> List[Dict[str, object]]:
    for rec in reversed(history):
        items = rec.get(field, [])
        if not isinstance(items, list):
            continue
        filtered: List[Dict[str, object]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if field == "aiHighlights":
                title = str(item.get("title", ""))
                if "抓取状态" in title:
                    continue
            filtered.append(item)
            if len(filtered) >= max_items:
                break
        if filtered:
            return filtered
    return []


def _mark_history_fallback(items: List[Dict[str, object]], marker: str = "历史复用") -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for item in items:
        it = dict(item)
        src = it.get("source", "")
        if isinstance(src, str) and marker not in src:
            it["source"] = f"{src}（{marker}）"
        out.append(it)
    return out


def collect_priority_channel_highlights() -> List[Dict[str, object]]:
    items = []
    claude = parse_claude_blog_recent_week()
    if claude:
        items.append({"channel": "Claude 官方博文", "source": "Claude 官方博文", "items": claude})
    openai = parse_openai_research_recent_week()
    if openai:
        items.append({"channel": "OpenAI Research", "source": "OpenAI Research", "items": openai})
    return items


def build_today_report(as_of: datetime | None = None, history: List[Dict] | None = None) -> Dict:
    now = (as_of or datetime.now().astimezone()).replace(microsecond=0)
    date_str = now.date().isoformat()
    if history is None:
        history = load_history()
    openai = parse_openai_rss()
    priority_channels = collect_priority_channel_highlights()
    priority_raw: List[Dict[str, str]] = []
    for block in priority_channels:
        for article in block.get("items", []):
            if not isinstance(article, dict):
                continue
            priority_raw.append(
                {
                    "title": str(article.get("title", "")),
                    "source": str(block.get("source", "官方渠道")),
                    "link": str(article.get("link", "")),
                    "time": str(article.get("time", "")),
                    "raw": str(article.get("summary", "")),
                }
            )

    anthropic = parse_anthropic_pages()
    if not anthropic:
        anthropic = parse_anthropic_rss()
    infoq = parse_infoq_feed()
    frontier = collect_frontier_signals()
    agui = parse_agui_and_gh_stars()
    trend = parse_github_trending_weekly()
    if not trend:
        trend = _last_valid_history_items(history, "trendProjects", max_items=12)
        trend = _mark_history_fallback(trend, "历史复用")
    ai_search_updates = collect_ai_search_updates()
    if not ai_search_updates:
        ai_search_updates = _last_valid_history_items(history, "aiHighlights", max_items=6)
        ai_search_updates = _mark_history_fallback(ai_search_updates, "历史复用")
    wechat_top20 = collect_top_wechat_gzh()
    if not wechat_top20:
        wechat_top20 = _last_valid_history_items(history, "wechatTop20", max_items=20)
        wechat_top20 = _mark_history_fallback(wechat_top20, "历史复用")
    twitter_updates = collect_twitter_ai_updates()
    if not twitter_updates:
        twitter_updates = _last_valid_history_items(history, "twitterUpdates", max_items=10)
        twitter_updates = _mark_history_fallback(twitter_updates, "历史复用")
    broker_reports = collect_broker_ai_reports()
    if not broker_reports:
        broker_reports = _last_valid_history_items(history, "brokerReports", max_items=10)
        broker_reports = _mark_history_fallback(broker_reports, "历史复用")

    aiHighlights = []
    if not (openai or anthropic or infoq or frontier or ai_search_updates or priority_raw):
        aiHighlights = [
            {
                "title": "主要来源当前未返回可识别的 AI 快讯，请检查抓取链路或后续新增来源。",
                "source": "抓取状态",
                "link": "",
                "time": "",
                "raw": "本次抓取仅返回状态信息。",
            }
        ]
        merged_signals = []
    else:
        merged_signals = append_source_link([*openai, *anthropic, *infoq, *frontier, *ai_search_updates, *priority_raw])  # 保留统一结构与链接
        seen_titles = set()
        for s in merged_signals[:8]:
            if s["title"] in seen_titles:
                continue
            seen_titles.add(s["title"])
            aiHighlights.append(
                {
                    "title": s["title"],
                    "source": s["source"],
                    "link": s["link"],
                    "time": s.get("time", ""),
                    "coreIdea": "关注该方向对任务可观察性与可执行性的影响。",
                    "value": "可用于评估代理链路的可治理性和交付可靠性。",
                }
            )

        # 额外补充AI检索来源的关键信息（避免重复）
        for s in ai_search_updates:
            if s.get("title") and s["title"] not in seen_titles:
                seen_titles.add(s["title"])
                aiHighlights.append(
                    {
                        "title": s["title"],
                        "source": s["source"],
                        "link": s["link"],
                        "time": s.get("time", ""),
                        "coreIdea": "AI检索补充：支持快速发现最新行业动态与研究线索。",
                        "value": "与 AI 快讯形成交叉验证与补全。",
                    }
                )

        # 控制AI信息条目数量，保留最新和高相关项
        aiHighlights = aiHighlights[:12]

    skill_top = select_top_skills(trend)
    focused = summarize_focus_domains(merged_signals)
    trend_projects = append_source_link(trend[:6]) if trend else []
    topic_summary = summarize_hot_topics(
        {
            "aiHighlights": aiHighlights,
            "trendProjects": trend_projects,
            "skillTop": skill_top,
            "wechatTop20": wechat_top20,
            "twitterUpdates": twitter_updates,
            "brokerReports": broker_reports,
        }
    )

    source_set = {s["source"] for s in merged_signals if s.get("source")}
    source_channels = [
        "Claude 官方博文",
        "OpenAI Research",
    ]
    source_channels = [x for x in source_channels if any(x == p["source"] for p in priority_channels)]
    source_channels += [
        x for x in ["OpenAI 官方", "Anthropic 官方", "INFOQ", "Google AI", "DeepMind", "Hugging Face", "Microsoft AI"] if x in source_set
    ]
    if any("AI检索" in str(s.get("source", "")) for s in ai_search_updates):
        source_channels.append("OpenAI Search")
    if any("Tavily Search" in str(s.get("source", "")) for s in ai_search_updates):
        source_channels.append("Tavily Search")
    if any("DuckDuckGo" in str(s.get("source", "")) for s in ai_search_updates):
        source_channels.append("DuckDuckGo Search")
    source_channels.append("GitHub")
    source_channels.append("AG-UI")
    source_channels.append("AIGCRank 微信AI公告号")
    source_channels.append("Twitter 美国科技公司")
    source_channels.append("新浪研报（10大券商）")

    report = {
        "date": date_str,
        "title": f"AI 研究日报（{date_str}）",
        "timezone": "Asia/Shanghai",
        "fetched_at": now.isoformat(),
        "source_channels": source_channels,
        "priorityChannelHighlights": priority_channels,
        "aiHighlights": aiHighlights,
        "ag_ui": agui,
        "trendProjects": trend_projects,
        "trendLinks": trend_projects,
        "wechatTop20": wechat_top20,
        "twitterUpdates": twitter_updates,
        "brokerReports": broker_reports,
        "topicSummary": topic_summary,
        "skillTop": skill_top,
        "focusedSignals": focused,
        "relatedSignals": [],
        "diffSummary": "AG-UI 与代理执行栈仍向任务可恢复、可观测方向收敛；今日新增数据优先关注 AI 协作、Agent 协议与实盘研判。",
        "suggestions": [
            "优先补齐任务板中的可恢复任务上下文（state+日志快照）。",
            "对已触发较高涨星的 agent 工具仓库做一次本地试用评测。",
            "对接微信 Top20 与新浪研报的同主题项，建立“可落地验证”清单。",
            "关注 Twitter 美国大厂 AI 与治理、合规叙事的差异，补充跨行业对标。",
        ],
    }
    return report


def format_markdown(report: Dict) -> str:
    source_list = report.get("source_channels", [])
    lines = [
        f"# {report['title']}",
        f"- 执行时间：{report['date']}（{report['timezone']}）",
        f"- 数据源：{' / '.join(source_list) if source_list else 'OpenAI / Anthropic / INFOQ / GitHub / AG-UI / 微信 / Twitter / 券商研报'}",
        "",
        "## [优先] Claude / OpenAI Research 近一周官方更新",
    ]
    priority_blocks = report.get("priorityChannelHighlights", [])
    if priority_blocks:
        for block in priority_blocks:
            lines.append(f"### {block.get('channel', '官方渠道')}")
            for item in block.get("items", []):
                title = item.get("title", "")
                source = item.get("source", "")
                link = item.get("link", "")
                time = item.get("time", "")
                summary = item.get("summary", "")
                lines.append(f"- **{title}**（{source}）{(' | ' + time) if time else ''}")
                if summary:
                    lines.append(f"  - 摘要：{summary}")
                if link:
                    lines.append(f"  - 链接：{link}")
                lines.append("")
            lines.append("")
    else:
        lines.append("- 本期未抓到两大官方渠道近一周可确认内容（或页面发布时间不可解析）。")
        lines.append("")
    lines.append("## 明细 Star 的 SKILL（Top3）")
    for key, block in (report.get("skillTop", {}) or {}).items():
        lines.append(f"### {block.get('title', key)}")
        if block.get("description"):
            lines.append(f"- 说明：{block.get('description')}")
        if block.get("status"):
            lines.append(f"- 状态：{block.get('status')}")
        for it in block.get("items", [])[:3]:
            repo = it.get("repo", "N/A")
            link = it.get("link", github_repo_link(repo))
            source = it.get("source", "GitHub")
            lines.append(
                f"- [{repo}]({link})｜来源：{source}｜当前星数 `{it.get('currentStars', 0)}`｜7天增量 `{it.get('delta7d', 0)}`｜{it.get('highlights', it.get('why', ''))}"
            )
        lines.append("")

    lines.extend(["## 定向细分领域追踪（AU- UI / AI地图 / AI搜索 / AI生图）", ""])
    for block in report.get("focusedSignals", []):
        lines.append(f"### {block.get('field', '')}")
        for item in block.get("items", [])[:4]:
            title = item.get("title", "")
            source = item.get("source", "")
            link = item.get("link", "")
            time = item.get("time", "")
            snippet = item.get("snippet", "")
            lines.append(f"- **{title}**（{source}）{('，' + time) if time else ''}")
            if snippet:
                lines.append(f"  - {snippet}")
            if link:
                lines.append(f"  - 链接：{link}")
        lines.append("")

    lines.extend(["## AI 信息", "", ""])
    for item in report.get("aiHighlights", []):
        lines.extend(
            [
                f"### {item['title']}",
                f"- 来源：{item['source']}",
                f"- 时间：{item.get('time', '')}",
                f"- 链接：{item.get('link', '')}",
                f"- 核心：{item.get('coreIdea', '')}",
                "",
            ]
        )
        ag = report.get("ag_ui", {})
    lines.extend(
        [
            "## AG-UI",
            f"- 仓库：[{ag.get('repo', '')}]({ag.get('link', github_repo_link('ag-ui-protocol/ag-ui'))})",
            f"- 链接：{ag.get('link', github_repo_link('ag-ui-protocol/ag-ui'))}",
            f"- 当前星标：{ag.get('stars', 'N/A')}",
            f"- 描述：{ag.get('description', '')}",
            "",
            "## GitHub 周度项目（星标>=1W，7天增量>=0.2W）",
        ]
    )
    for p in report.get("trendProjects", []):
        repo = p.get("repo", "N/A")
        link = p.get("link", github_repo_link(repo))
        src = p.get("source", "GitHub")
        lines.append(
            f"- [{repo}]({link})（{src}）：总星 `{p.get('currentStars')}`，7天增量 `{p.get('delta7d')}`"
        )
    lines.append("")
    lines.extend(["## 今日高频主题（自动提取）", ""])
    for item in report.get("topicSummary", []):
        lines.append(f"- {item.get('term', '')}（{item.get('count', 0)}）")
    if not report.get("topicSummary"):
        lines.append("- 本次抓取未形成可聚合主题。")

    lines.extend(["", "## WeChat Top20 AI 公告号", ""])
    if report.get("wechatTop20"):
        for item in report.get("wechatTop20", []):
            lines.append(f"- {item.get('title', '')}（{item.get('raw', '')}）")
            if item.get("link"):
                lines.append(f"  - 参考：{item.get('link')}")
    else:
        lines.append("- 本轮未抓到微信 Top20 数据（源不可达或页面结构变化）。")

    lines.extend(["", "## Twitter 美国科技公司 AI 动态", ""])
    if report.get("twitterUpdates"):
        for item in report.get("twitterUpdates", []):
            lines.append(f"- {item.get('source', 'Twitter')}：{item.get('title', '')}")
    else:
        lines.append("- 本轮未抓到 Twitter 更新。")

    lines.extend(["", "## 券商 AI 研报（10 大）", ""])
    if report.get("brokerReports"):
        by_broker = {}
        for item in report.get("brokerReports", []):
            by_broker.setdefault(item.get("broker", "未知券商"), []).append(item)
        for broker, items in by_broker.items():
            lines.append(f"### {broker}")
            for item in items[:3]:
                lines.append(f"- {item.get('title', '')}")
                if item.get("link"):
                    lines.append(f"  - 链接：{item.get('link')}")
    else:
        lines.append("- 本轮未抓到 10 大券商可确认的AI研报（源不可达或页面结构变化）。")

    lines.extend(["", "## 执行建议", ""])
    for suggestion in report.get("suggestions", []):
        lines.append(f"- {suggestion}")
    record_policy = report.get("record_policy")
    if isinstance(record_policy, dict):
        lines.append("## 合并与差异")
        lines.append(f"- 今日差异：{report.get('diffSummary', '')}")
        lines.append(
            "- 保留策略：最近{retain}条；清理旧记录 {removed_old} 条；当前保留 {remaining_records} 条；7天关联窗口 {merged_within_7d} 天；今日是否合并：{merged}。".format(
                retain=record_policy.get("retain_last", MAX_RECORDS),
                removed_old=record_policy.get("removed_old", 0),
                remaining_records=record_policy.get("remaining_records", ""),
                merged_within_7d=record_policy.get("merged_within_7d", RELATED_WINDOW_DAYS),
                merged="是" if record_policy.get("merged", False) else "否",
            )
        )
    else:
        lines.extend(["## 合并与差异", f"- 今日差异：{report.get('diffSummary', '')}", "", str(record_policy or "保留策略：最近14条，自动清理旧记录。")])
    lines.append("")
    return "\n".join(lines)


def format_html(report: Dict) -> str:
    ag = report.get("ag_ui", {})
    policy = report.get("record_policy")

    priority_blocks = report.get("priorityChannelHighlights", [])
    priority_html = []
    if priority_blocks:
        for block in priority_blocks:
            channel = escape(block.get("channel", "官方渠道"))
            priority_html.append(f"<section class='card'><h3>{channel}</h3><ul>")
            for item in block.get("items", []):
                title = escape(item.get("title", ""))
                source = escape(item.get("source", ""))
                time = escape(str(item.get("time", "")))
                summary = escape(str(item.get("summary", "")))
                link = escape(str(item.get("link", "")))
                item_html = f"<li><strong>{title}</strong>（{source}{(' | ' + time) if time else ''})"
                if summary:
                    item_html += f"<br/>摘要：{summary}"
                if link:
                    item_html += f"<br/><a href='{link}'>{link}</a>"
                item_html += "</li>"
                priority_html.append(item_html)
            priority_html.append("</ul></section>")
    else:
        priority_html.append("<section class='card'><h3>优先官方渠道</h3><p>本期未抓到两大官方渠道近一周可确认内容（或页面发布时间不可解析）。</p></section>")

    ai_html = []
    for item in report.get("aiHighlights", []):
        ai_html.append(
            "<section class='card'>"
            f"<h3>{escape(item.get('title', ''))}</h3>"
            "<ul>"
            f"<li>来源：{escape(item.get('source', ''))}</li>"
            f"<li>时间：{escape(str(item.get('time', '')))}</li>"
            f"<li>链接：<a href='{escape(item.get('link', ''))}'>{escape(item.get('link', ''))}</a></li>"
            f"<li>核心：{escape(item.get('coreIdea', ''))}</li>"
            "</ul>"
            "</section>"
        )
    if not ai_html:
        ai_html.append(
            "<section class='card'><h3>AI 信息</h3><p>本次抓取为空，请稍后重试。</p></section>"
        )

    topic_list = [f"<li>{escape(str(x.get('term', '')))}（{escape(str(x.get('count', 0)))}）</li>" for x in report.get("topicSummary", [])]

    trend_html = []
    for p in report.get("trendProjects", []):
        trend_html.append(
            "<li><strong><a href='"
            f"{escape(p.get('link', github_repo_link(p.get('repo', ''))))}'>"
            f"{escape(p.get('repo', ''))}</a></strong>（{escape(str(p.get('source', 'GitHub')))}）：总星 "
            f"<code>{escape(str(p.get('currentStars', '')))}</code>，7 天增量 "
            f"<code>{escape(str(p.get('delta7d', '')))}</code></li>"
        )

    wechat_html = []
    if report.get("wechatTop20"):
        for item in report["wechatTop20"]:
            wechat_html.append(f"<li>{escape(item.get('title', ''))} - {escape(item.get('raw', ''))}</li>")
    else:
        wechat_html.append("<li>本轮未抓到微信 Top20 数据（源不可达或页面结构变化）。</li>")

    twitter_html = []
    if report.get("twitterUpdates"):
        for item in report["twitterUpdates"]:
            twitter_html.append(
                f"<li>{escape(item.get('source', 'Twitter'))}：{escape(item.get('title', ''))}<br/>{escape(item.get('link', ''))}</li>"
            )
    else:
        twitter_html.append("<li>本轮未抓到 Twitter 更新。</li>")

    broker_html = []
    if report.get("brokerReports"):
        by_broker = {}
        for item in report["brokerReports"]:
            by_broker.setdefault(item.get("broker", "未知券商"), []).append(item)
        for broker, items in by_broker.items():
            broker_html.append(f"<li><strong>{escape(str(broker))}</strong><ul>")
            for it in items[:3]:
                broker_html.append(f"<li>{escape(str(it.get('title', '')))}")
                if it.get("link"):
                    broker_html.append(
                        f" - <a href='{escape(str(it.get('link', '')))}'>{escape(str(it.get('link', '')))}</a>"
                    )
                broker_html.append("</li>")
            broker_html.append("</ul></li>")
    else:
        broker_html.append("<li>本轮未抓到 10 大券商可确认的AI研报（源不可达或页面结构变化）。</li>")

    skill_html = []
    for key, block in (report.get("skillTop", {}) or {}).items():
        skill_items = ""
        for it in block.get("items", [])[:3]:
            repo = it.get("repo", "")
            link = it.get("link", github_repo_link(repo))
            skill_items += (
                "<li><a href='"
                f"{escape(link)}'>{escape(repo)}</a>｜"
                f"星数: <code>{escape(str(it.get('currentStars', '')))}</code>｜7天增量: <code>{escape(str(it.get('delta7d', '')))}</code>｜"
                f"{escape(str(it.get('highlights', it.get('why', ''))))}</li>"
            )
        skill_html.append(
            f"<section class='card'><h3>{escape(block.get('title', key))}</h3>"
            f"<p>说明：{escape(block.get('description', ''))}</p>"
            f"<p>状态：{escape(block.get('status', ''))}</p>"
            "<ul>"
            f"{skill_items}"
            "</ul></section>"
        )

    policy_html = ""
    if isinstance(policy, dict):
        policy_html = (
            f"<p>保留策略：最近{policy.get('retain_last', MAX_RECORDS)}条；清理旧记录 "
            f"{policy.get('removed_old', 0)} 条；当前保留 {policy.get('remaining_records', '')} 条；"
            f"7天关联窗口 {policy.get('merged_within_7d', RELATED_WINDOW_DAYS)} 天；"
            f"今日是否合并：{'是' if policy.get('merged', False) else '否'}。</p>"
        )

    suggestion_html = "".join([f"<li>{escape(str(s))}</li>" for s in report.get("suggestions", [])])
    trend_html_snippet = "".join(trend_html) if trend_html else "<li>本期未抓到符合条件的周度趋势项目。</li>"
    topic_list_html = "".join(topic_list) if topic_list else "<li>本次抓取未形成可聚合主题。</li>"

    def _shorten(text: str, limit: int = 95) -> str:
        txt = re.sub(r"\s+", " ", str(text).strip())
        if not txt:
            return ""
        return txt if len(txt) <= limit else txt[: limit - 3].rstrip() + "..."

    def _extract_highlights(text: str) -> List[str]:
        if not text:
            return []
        low = text.lower()
        clues = [
            "agent",
            "tool",
            "mcp",
            "orchestration",
            "workflow",
            "api",
            "architecture",
            "模型",
            "推理",
            "memory",
            "retrieval",
            "工具",
            "多模态",
            "调度",
            "routing",
            "编排",
            "评估",
            "fine-tune",
            "微调",
            "rag",
            "reasoning",
            "agentic",
            "multi-agent",
        ]
        out = []
        for c in clues:
            if c in low and c not in out:
                out.append(c)
        return out[:4]

    architecture_trace_order = [
        ("Agent", ("agent", "agents", "agentic", "智能体", "assistant", "代理")),
        ("Workflow", ("workflow", "work flow", "orchestration", "编排", "任务流", "工作流")),
        ("RAG", ("rag", "retrieval", "检索", "检索增强", "检索增强生成")),
        ("Memory", ("memory", "memory store", "记忆", "会话记忆", "记忆库")),
        ("Router", ("router", "routing", "路由", "分发")),
        ("Tools", ("tool", "tools", "tooling", "工具", "function", "tool calling")),
        ("MCP", ("mcp",)),
        ("API", ("api", "gateway", "接口", "endpoint")),
        ("Vector DB", ("vector", "向量", "向量库")),
        ("Policy", ("policy", "guardrail", "guardrails", "安全", "合规")),
    ]

    def _confidence_label(score: int) -> str:
        if score >= 4:
            return "高"
        if score >= 2:
            return "中"
        if score >= 1:
            return "低"
        return "低"

    def _build_architecture_signal_map(items: List[Dict[str, object]]) -> List[tuple[str, int]]:
        scores = {label: 0 for label, _ in architecture_trace_order}
        for item in items:
            title = str(item.get("title", ""))
            summary = str(item.get("summary", ""))
            sections = item.get("architecture_sections")
            section_text = " ".join([str(x) for x in sections]) if isinstance(sections, list) else ""
            nodes = item.get("architecture_nodes", [])
            node_text = " ".join([str(x) for x in nodes]) if isinstance(nodes, list) else ""
            source_text = f"{title} {summary} {section_text} {node_text}".lower()
            for label, keys in architecture_trace_order:
                key_score = 0
                for key in keys:
                    if key in source_text:
                        key_score += 1
                if key_score:
                    # 标题/小节直接出现更权重
                    title_low = title.lower()
                    if any(k in title_low for k in keys):
                        key_score += 1
                    if isinstance(sections, list) and any(key in section_text.lower() for key in keys):
                        key_score += 1
                    if isinstance(nodes, list) and any(key in node_text.lower() for key in keys):
                        key_score += 1
                    scores[label] += key_score
        ordered = [(label, scores[label]) for label, _ in architecture_trace_order if scores.get(label, 0) > 0]
        return ordered

    tech_overview_blocks = []
    merged_priority_items: List[Dict[str, object]] = []
    for block in priority_blocks:
        items = block.get("items", [])
        if isinstance(items, list):
            merged_priority_items.extend([i for i in items if isinstance(i, dict)])

    if merged_priority_items:
        summary_points: List[str] = []
        signal_points = _build_architecture_signal_map(merged_priority_items)
        architecture_trace_points = [
            f"{label}（置信度：{_confidence_label(score)}）"
            for label, score in signal_points
            if label and score > 0
        ]
        summary_points.extend(architecture_trace_points)

        for item in merged_priority_items:
            title = str(item.get("title", "")).strip()
            summary = str(item.get("summary", "")).strip()
            text = f"{title} {summary}"
            for clue in _extract_highlights(text):
                if clue not in summary_points:
                    summary_points.append(clue)

        # 去重并控制条目长度，保留核心信息
        deduped_points: List[str] = []
        for p in summary_points:
            if p and p not in deduped_points:
                deduped_points.append(p)
        summary_points = deduped_points[:12]

        tech_points = "".join(
            [f"<li>{escape(_shorten(item))}</li>" for item in summary_points]
            if summary_points
            else ["<li>当前区间未提取到可判定的关键技术点，仅保留原文摘要供追溯。</li>"]
        )

        tech_overview_blocks.append(
            "<section class='card tech-card'>"
            "<h3>Claude + OpenAI 技术概要</h3>"
            "<div class='card-inner'>"
            "<p>技术要点（按置信度与架构线索优先）</p>"
            f"<ul>{tech_points}</ul>"
            "</div>"
            "</section>"
        )

    if not tech_overview_blocks:
        tech_overview_blocks.append(
            "<section class='card tech-card'><h3>技术概要</h3><p>当前没有可用于技术归纳的 Claude/OpenAI 官方新内容。</p></section>"
        )

    panels = {
        "priority": "".join(priority_html),
        "ai": "".join(ai_html),
        "skill": "".join(skill_html),
        "trend": "".join([
            "<div class='card'><h3>AG-UI</h3><ul>"
            f"<li>仓库：<a href=\"{escape(ag.get('link', 'https://github.com/ag-ui-protocol/ag-ui'))}\">{escape(ag.get('repo', 'ag-ui-protocol/ag-ui'))}</a></li>"
            f"<li>链接：<a href=\"{escape(ag.get('link', 'https://github.com/ag-ui-protocol/ag-ui'))}\">https://github.com/ag-ui-protocol/ag-ui</a></li>"
            f"<li>当前星标：{escape(str(ag.get('stars', 'N/A')))}</li>"
            f"<li>描述：{escape(ag.get('description', ''))}</li>"
            "</ul></div>"
            "<div class='card'><h3>GitHub 周度项目（星标>=1W，7天增量>=0.2W）</h3>"
            f"<ul>{trend_html_snippet}</ul></div>"
        ]),
        "chat": f"<div class='card'><ul>{''.join(wechat_html)}</ul></div>",
        "twitter": f"<div class='card'><ul>{''.join(twitter_html)}</ul></div>",
        "broker": f"<div class='card'><h3>券商 AI 研报（10 大）</h3><ul>{''.join(broker_html)}</ul></div>",
        "suggest": f"<div class='card'><ul>{suggestion_html}</ul></div>",
        "diff": (
            "<div class='card'><p>今日差异："
            f"{escape(report.get('diffSummary', ''))}</p>{policy_html}</div>"
        ),
    }
    topic_panel = f"<div class='card'><h3>今日高频主题</h3><ul>{topic_list_html}</ul></div>"
    panels["trend"] = topic_panel + panels["trend"]

    return f"""
<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <title>{escape(report.get('title', 'AI 研究日报'))}</title>
  <style>
    :root {{
      --rainbow: linear-gradient(120deg, #ff7eb3, #ffbf7e, #7cf7bf, #79d9ff, #7b9dff, #d88bff);
      --ink: #1f2937;
      --subtle: #64748b;
      --line: #e2e8f0;
      --card-bg: #ffffff;
      --card-border: #dbeafe;
      --chip: #eef2ff;
      --chip-active: #dbeafe;
      --shadow-soft: 0 10px 26px rgba(15, 23, 42, 0.09);
    }}
    body {{
      font-family: "PingFang SC", "Microsoft YaHei", Arial, sans-serif;
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 6% 12%, rgba(255, 211, 231, 0.45), transparent 36%),
        radial-gradient(circle at 94% 0%, rgba(186, 233, 255, 0.45), transparent 40%),
        linear-gradient(160deg, #f9fbff 0%, #fff8f0 100%);
      min-height: 100vh;
    }}
    .page {{
      max-width: 1120px;
      margin: 16px auto 28px;
      padding: 0 14px 28px;
    }}
    .hero {{
      background: linear-gradient(135deg, #7f6cff 0%, #46a8ff 55%, #7ce8a8 100%);
      color: #fff;
      border-radius: 14px;
      padding: 16px 20px;
      margin-bottom: 14px;
      box-shadow: var(--shadow-soft);
    }}
    h1 {{
      margin: 0;
      letter-spacing: 0.3px;
      font-size: 1.45rem;
    }}
    .hero p {{
      margin: 8px 0 0;
      color: rgba(255, 255, 255, 0.95);
      line-height: 1.65;
    }}
    h2 {{
      margin: 24px 0 10px;
      padding: 6px 10px;
      border-radius: 10px;
      display: inline-flex;
      align-items: center;
      background: linear-gradient(90deg, rgba(255, 140, 194, 0.2), rgba(124, 218, 255, 0.2), rgba(255, 220, 160, 0.2));
      color: #1f2a44;
      font-size: 1.12rem;
    }}
    .content {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }}
    .card {{
      position: relative;
      background: var(--card-bg);
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid var(--card-border);
      margin-bottom: 12px;
      box-shadow: var(--shadow-soft);
      overflow: hidden;
      backdrop-filter: blur(0.5px);
    }}
    .card::before {{
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 4px;
      background: var(--rainbow);
    }}
    .card::after {{
      content: '';
      position: absolute;
      left: 0;
      top: 4px;
      width: 5px;
      height: calc(100% - 4px);
      background: linear-gradient(180deg, #ff66c4 0%, #ffb16e 25%, #fff38f 50%, #7ef7c1 75%, #7ac3ff 100%);
    }}
    .card h3 {{ margin-top: 2px; position: relative; z-index: 1; }}
    code {{ background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-size: 0.95rem; }}
    a {{ color: #5b4bff; text-underline-offset: 2px; }}
    ul {{ margin: 8px 0 0; padding-left: 20px; }}
    p {{ line-height: 1.7; }}
    .main-title {{ margin: 0 0 12px; display: block; }}
    .tech-wrap {{ display: grid; gap: 12px; }}
    @media (max-width: 920px) {{
    }}
  </style>
</head>
<body>
  <div class=\"page\">
  <div class=\"hero\">
    <h1>{escape(report.get('title', 'AI 研究日报'))}</h1>
    <p>执行时间：{escape(report['date'])}（{escape(report['timezone'])}）<br/>数据源：{escape(' / '.join(report.get('source_channels', [])) or 'OpenAI / Anthropic / INFOQ / GitHub / AG-UI / 微信 / Twitter / 券商研报')}</p>
  </div>

  <div class=\"content\">
    <section class=\"card tech-wrap\">
      <h2 class=\"main-title\">技术概要</h2>
      {''.join(tech_overview_blocks)}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">优先渠道（最近一周）：Claude Blog / OpenAI Research</h2>
      {panels["priority"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">AI 信息</h2>
      {panels["ai"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">SKILL Top3</h2>
      {panels["skill"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">AG-UI 与趋势</h2>
      {panels["trend"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">WeChat Top20 AI 公告号</h2>
      {panels["chat"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">Twitter 美国科技公司 AI 动态</h2>
      {panels["twitter"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">券商 AI 研报（10 大）</h2>
      {panels["broker"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">执行建议</h2>
      {panels["suggest"]}
    </section>
    <section class=\"card\">
      <h2 class=\"main-title\">合并与差异</h2>
      {panels["diff"]}
    </section>
  </div>
</div>
</body>
</html>
""".strip()


def collect_report_catalog() -> List[Dict[str, str]]:
    if not REPORT_OUTPUT_DIR.exists():
        return []
    out: List[Dict[str, str]] = []
    for html_file in sorted(REPORT_OUTPUT_DIR.glob("daily_ai_brief_*.html")):
        match = re.match(r"^daily_ai_brief_(\d{4}-\d{2}-\d{2})\.html$", html_file.name)
        if not match:
            continue
        date_key = match.group(1)
        md_file = REPORT_OUTPUT_DIR / f"daily_ai_brief_{date_key}.md"
        title = f"AI 研究日报（{date_key}）"
        try:
            with html_file.open("r", encoding="utf-8") as f:
                content = f.read(2000)
            t_match = re.search(r"<title>(.*?)</title>", content, flags=re.I | re.S)
            if t_match:
                parsed_title = strip_html(unescape(t_match.group(1).strip()))
                if parsed_title:
                    title = parsed_title
        except Exception:
            pass
        out.append(
            {
                "date": date_key,
                "title": title,
                "html": html_file.name,
                "md": md_file.name if md_file.exists() else "",
            }
        )
    return out


def _build_calendar_block(month: int, year: int, dates_with_report: set[str]) -> str:
    first_day = datetime(year, month, 1)
    first_weekday = first_day.weekday()
    total_days = calendar.monthrange(year, month)[1]
    weekday_labels = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

    cells: List[str] = []
    for _ in range(first_weekday):
        cells.append("<li class='day-cell empty'></li>")

    for day in range(1, total_days + 1):
        dt = first_day.replace(day=day)
        date_str = dt.strftime("%Y-%m-%d")
        if date_str in dates_with_report:
            cells.append(
                f"<li><button type='button' class='day-cell has-report' data-date='{date_str}'>{day:02d}</button></li>"
            )
        else:
            cells.append(
                f"<li><span class='day-cell no-report'>{day:02d}</span></li>"
            )

    # 补齐最后一行，不会影响语义
    while len(cells) % 7:
        cells.append("<li class='day-cell empty'></li>")

    rows = []
    for i in range(0, len(cells), 7):
        week = cells[i : i + 7]
        rows.append("<ul class='week'>" + "".join(week) + "</ul>")

    return "".join(
        [
            f"<section class='month-block'>",
            f"<h4>{month:02d}月</h4>",
            "<div class='calendar-grid'>",
            "<div class='weekday'>",
            "".join([f"<span>{x}</span>" for x in weekday_labels]),
            "</div>",
            "".join(rows),
            "</div>",
            "</section>",
        ]
    )


def build_reports_portal_html(reports: List[Dict[str, str]]) -> str:
    report_by_date: Dict[str, Dict[str, str]] = {}
    if not reports:
        calendar_body = "<p>暂无日报文件，请先执行一次日报生成。</p>"
    else:
        parsed = []
        for item in reports:
            try:
                date_obj = datetime.strptime(item["date"], "%Y-%m-%d")
                parsed.append((item["date"], date_obj.year, date_obj.month, item["title"], item["html"], item["md"]))
            except Exception:
                continue
        parsed.sort(key=lambda x: x[0])
        grouped: Dict[tuple[int, int], List[str]] = {}
        for d, _, _, _, _, _ in parsed:
            grouped.setdefault((int(d[:4]), int(d[5:7])), []).append(d)

        for date_key, _, _, title, html, md in parsed:
            report_by_date[date_key] = {"date": date_key, "title": title, "html": html, "md": md}

        calendar_sections = []
        years: Dict[int, List[tuple[int, List[str]]]] = {}
        for (year, month), dates in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
            years.setdefault(year, []).append((month, dates))

        latest_year = None
        now = datetime.now()
        if years:
            latest_year = max(years.keys())
            if latest_year > now.year:
                latest_year = now.year

        for year in sorted(years.keys()):
            month_sections = []
            for month, dates in years[year]:
                month_sections.append(_build_calendar_block(month=month, year=year, dates_with_report=set(dates)))
            year_open = "open" if year == latest_year else ""
            calendar_sections.append(
                "<details class='year-block' {open_attr}><summary>{year} 年 ({count} 月)</summary>{months}</details>"
                .replace("{open_attr}", f"{year_open}".strip() and "open")
                .replace("{year}", str(year))
                .replace("{count}", str(len(years[year])))
                .replace("{months}", "".join(month_sections))
            )
        calendar_body = "".join(calendar_sections)

    return f"""
<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>AI 研究日报总览</title>
  <style>
    :root {{
      --rainbow: linear-gradient(120deg, #ff7eb3, #ffbf7e, #fff07d, #7cf7bf, #79d9ff, #7b9dff, #d88bff);
      --ink: #1f2937;
      --subtle: #475569;
      --card-bg: rgba(255, 255, 255, 0.95);
      --card-border: rgba(226, 232, 240, 0.95);
    }}
    body {{
      margin: 0;
      font-family: \"PingFang SC\", \"Microsoft YaHei\", Arial, sans-serif;
      background: linear-gradient(160deg, #f8f9ff 0%, #f7fcff 35%, #fff7fb 100%);
      color: var(--ink);
      min-height: 100vh;
    }}
    .page {{
      max-width: 1200px;
      margin: 14px auto 30px;
      padding: 0 14px 30px;
    }}
    .hero {{
      background: linear-gradient(135deg, #7f4be0 0%, #3f84f9 45%, #15c4ff 75%, #7ce8a8 100%);
      color: #fff;
      border-radius: 16px;
      padding: 16px 20px;
      margin-bottom: 14px;
    }}
    .hero h1 {{ margin: 0; font-size: 1.4rem; }}
    .hero p {{ margin: 8px 0 0; color: rgba(255,255,255,0.9); }}
    .top-area {{
      display: flex;
      gap: 16px;
      align-items: stretch;
      margin-bottom: 14px;
    }}
    .calendar-column {{ width: 50%; }}
    .preview-column {{ flex: 1; }}
    .section-title {{
      margin: 0 0 8px;
      font-size: 1.08rem;
      color: #1e293b;
    }}
    .calendar-card, .preview-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
      min-height: 420px;
      position: relative;
      overflow: hidden;
    }}
    .calendar-card::before, .preview-card::before {{
      content: '';
      position: absolute;
      inset: 0 0 auto 0;
      height: 4px;
      background: var(--rainbow);
    }}
    .month-block {{ margin-bottom: 18px; }}
    .month-block h4 {{
      margin: 8px 0 10px;
      padding-left: 4px;
    }}
    .year-block {{
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 8px 10px;
      margin-bottom: 10px;
      background: #f8fafc;
    }}
    .year-block summary {{
      cursor: pointer;
      font-weight: 600;
      color: #1e293b;
      padding: 2px 0;
    }}
    .year-block[open] {{ background: #f1f5ff; }}
    .year-block summary::-webkit-details-marker {{
      display: inline-block;
      margin-right: 6px;
    }}
    .calendar-grid {{
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      padding: 8px;
      background: #f8fafc;
    }}
    .weekday {{
      display: grid;
      grid-template-columns: repeat(7, 1fr);
      text-align: center;
      font-size: 0.84rem;
      color: var(--subtle);
      margin-bottom: 4px;
    }}
    .week {{
      list-style: none;
      display: grid;
      grid-template-columns: repeat(7, 1fr);
      gap: 6px;
      padding: 0;
      margin: 6px 0;
    }}
    .day-cell {{
      width: 100%;
      height: 38px;
      display: flex;
      justify-content: center;
      align-items: center;
      border-radius: 9px;
      font-size: 0.92rem;
      box-sizing: border-box;
    }}
    .day-cell.empty {{ color: transparent; }}
    .day-cell.no-report {{
      color: #94a3b8;
      background: #f1f5f9;
    }}
    .day-cell.has-report {{
      border: 1px solid #a5b4fc;
      background: #ffffff;
      color: #1e293b;
      cursor: pointer;
      transition: all .18s ease;
      padding: 0;
      font-weight: 600;
    }}
    .day-cell.has-report:hover {{ transform: translateY(-1px); background: #eef2ff; }}
    .day-cell.has-report.active {{ outline: 2px solid #6366f1; background: #e0e7ff; }}
    .preview-area {{
      margin-top: 8px;
      background: #ffffff;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      min-height: 460px;
      padding: 10px;
      box-shadow: 0 8px 24px rgba(15,23,42,0.08);
      overflow: hidden;
    }}
    .preview-toolbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      font-size: 0.95rem;
      margin: 2px 4px 8px;
      color: #334155;
      flex-wrap: wrap;
    }}
    .preview-frame {{
      width: 100%;
      height: 520px;
      border: none;
      border-radius: 8px;
      background: #fff;
    }}
    .preview-empty {{ padding: 10px 8px; color: #64748b; }}
    .hint {{
      color: #64748b;
      font-size: 0.9rem;
      margin: 4px 4px 10px;
    }}
    a {{ color: #4f46e5; }}
    @media (max-width: 980px) {{
      .top-area {{ flex-direction: column; }}
      .calendar-column {{ width: 100%; }}
      .preview-frame {{ height: 420px; }}
    }}
  </style>
</head>
<body>
  <div class=\"page\">
    <section class=\"hero\">
      <h1>AI 研究日报总览</h1>
      <p>点击日历中的日期，查看对应日期的日报内容；日历以有稿件日期高亮显示。</p>
    </section>

    <div class=\"top-area\">
      <div class=\"calendar-column\">
        <h2 class=\"section-title\">日报日历（按年月）</h2>
        <div class=\"calendar-card\">
          {calendar_body}
        </div>
      </div>
      <div class=\"preview-column\">
        <h2 class=\"section-title\">日期日报信息</h2>
        <div class=\"preview-card\">
          <div class=\"preview-toolbar\">
            <span id=\"selectedDate\">未选择日期</span>
            <span id=\"selectedMeta\"></span>
          </div>
          <div class=\"hint\" id=\"previewHint\">支持点击日历中的有日报日期；右侧将预览完整 HTML。</div>
          <iframe id=\"reportFrame\" class=\"preview-frame\" title=\"日报预览\"></iframe>
        </div>
      </div>
    </div>

    <div class=\"preview-area\">
      <div class=\"section-title\" style=\"margin: 0 0 10px;\">快速说明</div>
      <div class=\"preview-empty\">
        说明：页面默认展示最近一条日报；所有日报文件位于 report 目录，包含 HTML 与 MD 两种版本。<br/>
        Markdown 版本可直接下载到本地查看：<span id=\"mdLink\"></span>
      </div>
    </div>
  </div>

  <script>
    const reports = {json.dumps(report_by_date, ensure_ascii=False)};
    const dateButtons = Array.from(document.querySelectorAll('.day-cell.has-report'));
    const frame = document.getElementById('reportFrame');
    const selectedDate = document.getElementById('selectedDate');
    const selectedMeta = document.getElementById('selectedMeta');
    const previewHint = document.getElementById('previewHint');
    const mdLinkWrap = document.getElementById('mdLink');

    function showReport(date) {{
      const item = reports[date];
      if (!item) {{
        return;
      }}
      frame.src = item.html;
      selectedDate.textContent = `当前日期：${{date}}`;
      selectedMeta.textContent = item.title || '';
      previewHint.textContent = '已加载 HTML：' + item.html;
      if (item.md) {{
        mdLinkWrap.innerHTML = `<a href=\"${{item.md}}\" target=\"_blank\">打开 Markdown</a>`;
      }} else {{
        mdLinkWrap.textContent = '当前日期未保存 Markdown 文件';
      }}
    }}

    function setActive(button) {{
      dateButtons.forEach(btn => btn.classList.remove('active'));
      if (button) {{
        button.classList.add('active');
      }}
    }}

    dateButtons.forEach(btn => {{
      btn.addEventListener('click', () => {{
        const date = btn.getAttribute('data-date') || '';
        setActive(btn);
        if (date) {{
          showReport(date);
        }}
      }});
    }});

    const defaultDates = Object.keys(reports).sort();
    if (defaultDates.length) {{
      const latest = defaultDates[defaultDates.length - 1];
      const latestBtn = document.querySelector(`button[data-date=\"${{latest}}\"]`);
      if (latestBtn) {{
        setActive(latestBtn);
        showReport(latest);
      }}
    }} else {{
      selectedDate.textContent = '未找到可展示日报';
      previewHint.textContent = '请先执行日报脚本生成 daily_ai_brief_YYYY-MM-DD.html 文件';
      mdLinkWrap.textContent = '';
    }}
  </script>
</body>
</html>
""".strip()


def load_history() -> List[Dict]:
    if not STATE_FILE.exists():
        return []
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return data
    except Exception:
        return []
    return []


def save_history(history: List[Dict]):
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def prune_history(history: List[Dict]) -> Dict:
    removed = 0
    if len(history) > MAX_RECORDS:
        removed = len(history) - MAX_RECORDS
        history = history[removed:]
    return {"history": history, "removed": removed, "remaining": len(history)}


def _safe_parse_report_date(filename: str) -> str | None:
    match = re.match(r"^daily_ai_brief_(\d{4}-\d{2}-\d{2})\.(md|html)$", filename)
    return match.group(1) if match else None


def archive_daily_brief_files(reference_dt: datetime) -> Dict[str, object]:
    """Archive duplicate or historical daily_ai_brief files from project root to report/.

    - For historical files (< reference date): move to report/.
    - For duplicates with report version: keep the newer file and remove older one.
    - For same-day files: also deduplicate by keeping newest and deleting duplicate.
    """
    project_root = Path(__file__).resolve().parent
    archive_dir = REPORT_OUTPUT_DIR
    archive_dir.mkdir(parents=True, exist_ok=True)
    reference_date = reference_dt.date()

    moved: List[Dict[str, str]] = []
    replaced: List[Dict[str, str]] = []
    deleted: List[Dict[str, str]] = []
    skipped: List[str] = []
    errors: List[Dict[str, str]] = []
    processed = 0

    for src in sorted(project_root.glob("daily_ai_brief_*.md")) + sorted(project_root.glob("daily_ai_brief_*.html")):
        date_str = _safe_parse_report_date(src.name)
        if not date_str:
            skipped.append(src.name)
            continue

        processed += 1
        dst = archive_dir / src.name
        src_mtime = src.stat().st_mtime if src.exists() else 0.0
        if dst.exists():
            dst_mtime = dst.stat().st_mtime if dst.exists() else 0.0
            if src_mtime >= dst_mtime:
                try:
                    dst.unlink()
                    src.replace(dst)
                    replaced.append({"from": str(src), "to": str(dst), "reason": "source_newer"})
                except Exception as exc:
                    errors.append({"action": "replace", "source": str(src), "error": str(exc)})
            else:
                try:
                    src.unlink()
                    deleted.append({"file": str(src), "reason": "history_duplicate_older"})
                except Exception as exc:
                    errors.append({"action": "delete_duplicate", "source": str(src), "error": str(exc)})
            continue

        try:
            src.replace(dst)
            action = "moved_current"
            try:
                parsed = datetime.fromisoformat(date_str).date()
                if parsed < reference_date:
                    action = "moved_historical"
            except Exception:
                pass
            moved.append({"from": str(src), "to": str(dst), "reason": action})
        except Exception as exc:
            errors.append({"action": action, "source": str(src), "error": str(exc)})

    return {
        "reference_date": reference_date.isoformat(),
        "scanned_files": processed,
        "moved": moved,
        "replaced": replaced,
        "deleted": deleted,
        "skipped": skipped,
        "errors": errors,
    }


def run() -> Dict:
    now = datetime.now().astimezone().replace(microsecond=0)
    archive_summary = archive_daily_brief_files(now)
    history = load_history()
    executed_dates = {str(item.get("date", "")).strip() for item in history if isinstance(item, dict)}
    target_dates = [now.date() - timedelta(days=i) for i in range(DAILY_AI_BRIEF_CATCHUP_DAYS)]
    missing_dates = [d for d in reversed(target_dates) if d.isoformat() not in executed_dates]

    generated_reports: List[Dict] = []
    merged_any = False
    for d in missing_dates:
        report_for_date = datetime(d.year, d.month, d.day, 9, 15, 0, 0, tzinfo=now.tzinfo)
        report = build_today_report(as_of=report_for_date, history=history)
        merged, merged_target = merge_if_related(history, report)
        if not merged:
            report["diffSummary"] = build_diff_summary(report, None)
            history.append(report)
        else:
            merged_any = True
            report["diffSummary"] = build_diff_summary(report, merged_target)
            merged_target["updated_at"] = datetime.now().astimezone().isoformat()
        generated_reports.append(report)

    purge = prune_history(history)
    policy = {
        "retain_last": MAX_RECORDS,
        "removed_old": purge["removed"],
        "remaining_records": purge["remaining"],
        "merged": bool(merged_any),
        "merged_within_7d": str(RELATED_WINDOW_DAYS),
        "backfill_days": len(missing_dates),
        "backfill_dates": [d.isoformat() for d in missing_dates],
    }
    for h in history:
        h["record_policy"] = policy
    save_history(history)

    generated_outputs = []
    REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for report in generated_reports:
        md = format_markdown(report)
        html = format_html(report)
        out_file = REPORT_OUTPUT_DIR / f"daily_ai_brief_{report['date']}.md"
        out_html_file = REPORT_OUTPUT_DIR / f"daily_ai_brief_{report['date']}.html"
        with out_file.open("w", encoding="utf-8") as f:
            f.write(md)
        with out_html_file.open("w", encoding="utf-8") as f:
            f.write(html)
        generated_outputs.append({"date": report["date"], "markdown": str(out_file), "html": str(out_html_file)})

    report_catalog = collect_report_catalog()
    report_portal_html = build_reports_portal_html(report_catalog)
    portal_file = REPORT_OUTPUT_DIR / "daily_ai_calendar_portal.html"
    legacy_portal_file = REPORT_OUTPUT_DIR / "daily_ai_brief_portal.html"
    with portal_file.open("w", encoding="utf-8") as f:
        f.write(report_portal_html)
    with legacy_portal_file.open("w", encoding="utf-8") as f:
        f.write(report_portal_html)

    if generated_reports:
        sent_report = generated_reports[-1]
    elif history:
        sent_report = history[-1]
    else:
        sent_report = build_today_report(as_of=now, history=history)
        sent_report["diffSummary"] = build_diff_summary(sent_report, None)

    sent_md = format_markdown(sent_report)
    sent_html = format_html(sent_report)
    out_file = REPORT_OUTPUT_DIR / f"daily_ai_brief_{sent_report['date']}.md"
    out_html_file = REPORT_OUTPUT_DIR / f"daily_ai_brief_{sent_report['date']}.html"
    if not out_file.exists():
        with out_file.open("w", encoding="utf-8") as f:
            f.write(sent_md)
    if not out_html_file.exists():
        with out_html_file.open("w", encoding="utf-8") as f:
            f.write(sent_html)
    email_result = send_report_email(sent_md, sent_html, f"{sent_report.get('title', 'AI 研究日报')}", str(out_html_file))
    return {
        "markdown": sent_md,
        "html": sent_html,
        "email": email_result,
        "state_file": str(STATE_FILE),
        "output_file": str(out_file),
        "output_html_file": str(out_html_file),
        "portal_html_file": str(portal_file),
        "portal_html_file_legacy": str(legacy_portal_file),
        "generated_outputs": generated_outputs,
        "generated_dates": [x["date"] for x in generated_reports],
        "backfilled_dates": policy["backfill_dates"],
        "merged": merged_any,
        "prune": purge,
        "archive_daily_ai_brief": archive_summary,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--email-diagnose", action="store_true", help="仅输出邮件配置诊断，不执行发送/抓取")
    parser.add_argument("--email-transport-check", action="store_true", help="检查 SMTP/sendmail/API 可达性")
    args = parser.parse_args()
    if args.email_diagnose:
        print("EMAIL_CONFIG:")
        print(json.dumps(_email_config_snapshot(), ensure_ascii=False, indent=2))
    elif args.email_transport_check:
        print("EMAIL_TRANSPORT_CHECK:")
        print(json.dumps(email_transport_check(), ensure_ascii=False, indent=2))
    elif args.dry_run:
        built = build_today_report()
        print(format_markdown(built))
    else:
        r = run()
        print(r["markdown"])
        print("\n---")
        print(f"STATE: {r['state_file']}")
        print(f"OUTPUT: {r['output_file']}")
        print(f"HTML_OUTPUT: {r.get('output_html_file')}")
        email = r.get("email", {})
        if email.get("status") == "sent":
            print(f"EMAIL: sent -> {email.get('recipients')}")
        elif email.get("status") == "queued":
            print(f"EMAIL: queued -> {email.get('queue_path')}")
            if email.get("terminal_command"):
                print(f"EMAIL_TERMINAL_CMD: {email.get('terminal_command')}")
        elif email.get("status") == "skip":
            print(f"EMAIL: skip -> {email.get('reason')}")
        else:
            print(f"EMAIL: error -> {email.get('reason')}")
        if email.get("status") != "sent":
            print(f"EMAIL_CONFIG: {json.dumps(email.get('email_config', {}), ensure_ascii=False)}")
        print(f"PORTAL: {r.get('portal_html_file')}")
        print(f"PORTAL_LEGACY: {r.get('portal_html_file_legacy')}")
        print(f"MERGED: {r['merged']}")
        print(f"RECORDS: {r['prune']['remaining']} (removed {r['prune']['removed']})")
        if r.get("archive_daily_ai_brief"):
            arch = r["archive_daily_ai_brief"]
            print(
                "ARCHIVE: "
                f"moved={len(arch.get('moved', []))}, "
                f"replaced={len(arch.get('replaced', []))}, "
                f"deleted={len(arch.get('deleted', []))}, "
                f"errors={len(arch.get('errors', []))}"
            )

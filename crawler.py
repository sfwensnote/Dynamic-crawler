#!/usr/bin/env python3
"""
crawler.py - 教育部网站三模块全量爬虫
功能：
  1. 中央文件（静态分页）
  2. 教育部文件（动态分页）
  3. 其他部门文件（静态分页）
  
支持断点续爬、日期命名、错误重试。
"""

import os
import re
import sys
import json
import time
import random
import logging
import argparse
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# ================================================================
# 配置
# ================================================================
BASE_DATA_DIR = "data"
LOG_DIR = "logs"
MANIFEST_FILE = os.path.join(BASE_DATA_DIR, "manifest.jsonl")

SOURCES = [
    {
        "name": "中央文件",
        "dir_name": "中央文件",
        "type": "static",
        "base_url": "http://www.moe.gov.cn/jyb_xxgk/moe_1777/moe_1778/",
    },
    {
        "name": "教育部文件",
        "dir_name": "教育部文件",
        "type": "dynamic",
        "base_url": "http://www.moe.gov.cn/was5/web/search",
        "params": {"channelid": "239993"},
    },
    {
        "name": "其他部门文件",
        "dir_name": "其他部门文件",
        "type": "static",
        "base_url": "http://www.moe.gov.cn/jyb_xxgk/moe_1777/moe_1779/",
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

MAX_RETRIES = 3
DELAY_MIN = 2.0
DELAY_MAX = 5.0

# ================================================================
# 日志
# ================================================================
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "crawler.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ================================================================
# 工具函数
# ================================================================
def sanitize_filename(name: str, max_len: int = 80) -> str:
    """清洗文件名：移除非法字符，截断过长名称"""
    # 移除 Windows / macOS 非法字符
    name = re.sub(r'[\\/:*?"<>|]', '', name)
    # 移除控制字符和多余空格
    name = re.sub(r'\s+', ' ', name).strip()
    # 移除首尾的点号（Windows 限制）
    name = name.strip('.')
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name if name else "untitled"


def make_filename(date_str: str, title: str) -> str:
    """用日期+标题生成文件名"""
    safe_title = sanitize_filename(title)
    return f"{date_str}_{safe_title}.html"


def polite_sleep(min_s: float = DELAY_MIN, max_s: float = DELAY_MAX):
    """随机延迟，避免对服务器造成压力"""
    t = random.uniform(min_s, max_s)
    time.sleep(t)


def fetch_with_retry(url: str, params: dict = None, retries: int = MAX_RETRIES) -> requests.Response | None:
    """带重试的 HTTP GET"""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            resp.encoding = resp.apparent_encoding or "utf-8"
            if resp.status_code == 200:
                return resp
            logger.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt}/{retries})")
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e} (attempt {attempt}/{retries})")
        if attempt < retries:
            polite_sleep(3, 8)
    logger.error(f"All {retries} retries failed for {url}")
    return None


# ================================================================
# 列表页解析
# ================================================================
def _extract_pages_from_js(html: str) -> int | None:
    """从页面 JavaScript 变量中提取总页数。
    MOE 页面在 <script> 中有: var recordCount = 222; var pageSize = 20;
    """
    m_count = re.search(r'var\s+recordCount\s*=\s*(\d+)', html)
    m_size = re.search(r'var\s+pageSize\s*=\s*(\d+)', html)
    if m_count and m_size:
        record_count = int(m_count.group(1))
        page_size = int(m_size.group(1))
        if page_size > 0:
            import math
            return math.ceil(record_count / page_size)
    return None


def get_total_pages_static(html: str) -> int:
    """从静态列表页提取总页数。优先解析 JS 变量。"""
    # 方法1：从 JS 变量解析 (最可靠)
    js_pages = _extract_pages_from_js(html)
    if js_pages:
        return js_pages

    # 方法2：从渲染后的 HTML 文本 (有些页面可能服务端渲染)
    soup = BeautifulSoup(html, "html.parser")
    page_info = soup.find(string=re.compile(r'页数：\d+/\d+'))
    if page_info:
        m = re.search(r'页数：\d+/(\d+)', page_info)
        if m:
            return int(m.group(1))

    # 方法3：查找 "末页" 链接
    last_link = soup.find('a', string=re.compile(r'末页'))
    if last_link and last_link.get('href'):
        m = re.search(r'index_(\d+)\.html', last_link['href'])
        if m:
            return int(m.group(1)) + 1
    return 1


def get_total_pages_dynamic(html: str) -> int:
    """从动态搜索页提取总页数。优先解析 JS 变量。"""
    # 方法1：从 JS 变量
    js_pages = _extract_pages_from_js(html)
    if js_pages:
        return js_pages

    # 方法2：HTML 文本
    soup = BeautifulSoup(html, "html.parser")
    page_info = soup.find(string=re.compile(r'页数：\d+/\d+'))
    if page_info:
        m = re.search(r'页数：\d+/(\d+)', page_info)
        if m:
            return int(m.group(1))

    # 方法3："末页" 链接
    last_link = soup.find('a', string=re.compile(r'末页'))
    if last_link and last_link.get('href'):
        m = re.search(r'page=(\d+)', last_link['href'])
        if m:
            return int(m.group(1))
    return 1


def extract_items_from_static(html: str, base_url: str) -> list[dict]:
    """从静态列表页提取文档条目 [{url, title, date}]"""
    soup = BeautifulSoup(html, "html.parser")
    items = []

    # 静态页面的列表通常在 <ul> 里面，每个 <li> 包含 <a> 和 <span>(日期)
    for li in soup.select("li"):
        a = li.find("a")
        if not a:
            continue
        href = a.get("href", "")
        if not href or href.startswith("javascript") or not href.endswith((".html", ".htm")):
            continue

        # 获取完整标题 — 优先用 title 属性
        title = a.get("title", "") or a.get_text(strip=True)
        if not title:
            continue

        # 过滤掉网站底部的 "网站声明"、"网站地图"等
        if title in ("网站声明", "网站地图", "联系我们"):
            continue

        full_url = urljoin(base_url, href)

        # 提取日期 — 通常在 <span> 中
        date_str = ""
        span = li.find("span")
        if span:
            date_text = span.get_text(strip=True)
            m = re.search(r'(\d{4}-\d{2}-\d{2})', date_text)
            if m:
                date_str = m.group(1)

        # 如果 span 没找到日期，尝试从 li 的文本中搜索
        if not date_str:
            li_text = li.get_text()
            m = re.search(r'(\d{4}-\d{2}-\d{2})', li_text)
            if m:
                date_str = m.group(1)

        # 如果还没有日期，尝试从 URL 中提取
        if not date_str:
            m = re.search(r'/(\d{4})(\d{2})/', full_url)
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}-00"

        items.append({
            "url": full_url,
            "title": title,
            "date": date_str or "unknown-date",
        })

    return items


def extract_items_from_dynamic(html: str, base_url: str) -> list[dict]:
    """从动态搜索页提取文档条目"""
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for li in soup.select("li"):
        a = li.find("a")
        if not a:
            continue
        href = a.get("href", "")
        if not href or href.startswith("javascript") or not href.endswith((".html", ".htm")):
            continue

        title = a.get("title", "") or a.get_text(strip=True)
        if not title:
            continue

        if title in ("网站声明", "网站地图", "联系我们"):
            continue

        full_url = urljoin(base_url, href)

        # 动态页日期通常紧挨着 <a> 后面
        date_str = ""
        # 方法1：<span> 日期
        span = li.find("span")
        if span:
            date_text = span.get_text(strip=True)
            m = re.search(r'(\d{4}-\d{2}-\d{2})', date_text)
            if m:
                date_str = m.group(1)

        # 方法2：li 全文搜索
        if not date_str:
            li_text = li.get_text()
            m = re.search(r'(\d{4}-\d{2}-\d{2})', li_text)
            if m:
                date_str = m.group(1)

        # 方法3：URL 中的日期
        if not date_str:
            m = re.search(r'/(\d{4})(\d{2})/', full_url)
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}-00"

        items.append({
            "url": full_url,
            "title": title,
            "date": date_str or "unknown-date",
        })

    return items


# ================================================================
# Manifest 记录
# ================================================================
def load_existing_manifest() -> set:
    """加载已爬取的 URL 集合（用于断点续爬）"""
    urls = set()
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        urls.add(record.get("url", ""))
                    except json.JSONDecodeError:
                        pass
    return urls


def append_manifest(record: dict):
    """追加一条 manifest 记录"""
    with open(MANIFEST_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ================================================================
# 详情页抓取与保存
# ================================================================
def download_detail(item: dict, save_dir: str, existing_urls: set, source_name: str) -> bool:
    """
    下载单篇文档的详情页。
    返回 True 表示新下载，False 表示跳过。
    """
    url = item["url"]
    if url in existing_urls:
        return False

    filename = make_filename(item["date"], item["title"])
    filepath = os.path.join(save_dir, filename)

    # 如果文件已存在（断点续爬 — 文件存在但 manifest 丢失的情况）
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        # 补充 manifest
        append_manifest({
            "url": url,
            "title": item["title"],
            "date": item["date"],
            "source": source_name,
            "file": filepath,
            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        existing_urls.add(url)
        return False

    polite_sleep()
    resp = fetch_with_retry(url)
    if not resp:
        return False

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(resp.text)

    append_manifest({
        "url": url,
        "title": item["title"],
        "date": item["date"],
        "source": source_name,
        "file": filepath,
        "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    existing_urls.add(url)
    return True


# ================================================================
# 主爬取流程
# ================================================================
def crawl_static_source(source: dict, existing_urls: set, max_pages: int = None):
    """爬取静态分页栏目（中央文件 / 其他部门文件）"""
    name = source["name"]
    base_url = source["base_url"]
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始爬取: {name}")
    logger.info(f"{'='*60}")

    # 第1页
    first_url = urljoin(base_url, "index.html")
    resp = fetch_with_retry(first_url)
    if not resp:
        logger.error(f"无法访问 {name} 首页")
        return

    total_pages = get_total_pages_static(resp.text)
    if max_pages:
        total_pages = min(total_pages, max_pages)
    logger.info(f"{name}: 共 {total_pages} 页")

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    for page_num in range(1, total_pages + 1):
        if page_num == 1:
            page_url = urljoin(base_url, "index.html")
            html = resp.text  # 复用第1页
        else:
            page_url = urljoin(base_url, f"index_{page_num - 1}.html")
            polite_sleep(1, 3)
            page_resp = fetch_with_retry(page_url, retries=1)  # 列表页只试1次
            if not page_resp:
                logger.warning(f"{name}: 第 {page_num} 页获取失败 (404?)，已到达末尾，停止爬取")
                break  # 到达末尾，不再继续
            html = page_resp.text

        items = extract_items_from_static(html, page_url)
        logger.info(f"{name}: 第 {page_num}/{total_pages} 页, 解析到 {len(items)} 条")

        if not items:
            logger.warning(f"{name}: 第 {page_num} 页无有效条目，停止爬取")
            break

        for item in items:
            is_new = download_detail(item, save_dir, existing_urls, name)
            if is_new:
                stats["downloaded"] += 1
                logger.info(f"  ✓ 已下载: {item['date']} {item['title'][:40]}...")
            else:
                stats["skipped"] += 1

    logger.info(f"{name} 完成: 下载 {stats['downloaded']}, 跳过 {stats['skipped']}, 失败 {stats['failed']}")


def crawl_dynamic_source(source: dict, existing_urls: set, max_pages: int = None):
    """爬取动态分页栏目（教育部文件）"""
    name = source["name"]
    base_url = source["base_url"]
    params_template = source.get("params", {})
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始爬取: {name}")
    logger.info(f"{'='*60}")

    # 第1页 — 获取总页数
    params = params_template.copy()
    resp = fetch_with_retry(base_url, params=params)
    if not resp:
        logger.error(f"无法访问 {name} 首页")
        return

    total_pages = get_total_pages_dynamic(resp.text)
    if max_pages:
        total_pages = min(total_pages, max_pages)
    logger.info(f"{name}: 共 {total_pages} 页")

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    for page_num in range(1, total_pages + 1):
        if page_num == 1:
            html = resp.text  # 复用第1页
        else:
            params = params_template.copy()
            params["page"] = page_num
            polite_sleep(1, 3)
            page_resp = fetch_with_retry(base_url, params=params)
            if not page_resp:
                logger.error(f"{name}: 第 {page_num} 页获取失败，跳过")
                stats["failed"] += 1
                continue
            html = page_resp.text

        # 动态页面的 base_url 用于 urljoin（相对链接基准）
        join_base = "http://www.moe.gov.cn/"
        items = extract_items_from_dynamic(html, join_base)
        logger.info(f"{name}: 第 {page_num}/{total_pages} 页, 解析到 {len(items)} 条")

        for item in items:
            is_new = download_detail(item, save_dir, existing_urls, name)
            if is_new:
                stats["downloaded"] += 1
                logger.info(f"  ✓ 已下载: {item['date']} {item['title'][:40]}...")
            else:
                stats["skipped"] += 1

        # 每50页输出一次汇总
        if page_num % 50 == 0:
            logger.info(f"  >>> 进度: {page_num}/{total_pages} 页, 已下载 {stats['downloaded']}, 跳过 {stats['skipped']}")

    logger.info(f"{name} 完成: 下载 {stats['downloaded']}, 跳过 {stats['skipped']}, 失败 {stats['failed']}")


# ================================================================
# 入口
# ================================================================
def main():
    parser = argparse.ArgumentParser(description="教育部网站三模块全量爬虫")
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="测试模式：每个模块只爬取 2 页",
    )
    parser.add_argument(
        "--module",
        choices=["central", "moe", "other", "all"],
        default="all",
        help="指定要爬取的模块 (默认: all)",
    )
    args = parser.parse_args()

    max_pages = 2 if args.test_mode else None

    os.makedirs(BASE_DATA_DIR, exist_ok=True)

    # 加载已爬 URL（断点续爬）
    existing_urls = load_existing_manifest()
    logger.info(f"已加载 {len(existing_urls)} 条已爬取记录（断点续爬）")

    module_map = {
        "central": [SOURCES[0]],
        "moe": [SOURCES[1]],
        "other": [SOURCES[2]],
        "all": SOURCES,
    }

    for source in module_map[args.module]:
        try:
            if source["type"] == "static":
                crawl_static_source(source, existing_urls, max_pages)
            elif source["type"] == "dynamic":
                crawl_dynamic_source(source, existing_urls, max_pages)
        except KeyboardInterrupt:
            logger.warning("用户中断，已安全退出。已下载的文件不会丢失。")
            sys.exit(0)
        except Exception as e:
            logger.error(f"模块 {source['name']} 出错: {e}", exc_info=True)

    logger.info("全部爬取完成！")


if __name__ == "__main__":
    main()

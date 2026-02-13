#!/usr/bin/env python3
"""
incremental_crawler.py - 教育部网站全量爬虫（增量更新版）

功能：
  基于 crawler.py 的核心逻辑，增加“连续已存在判定”机制。
  当连续遇到 20 个已存在的文件时，自动停止当前模块的爬取。
  
  适用于：定期运行（如每日/每周），只抓取最新发布的文件。
"""

import os
import time
import logging
from urllib.parse import urljoin
from crawler import (
    SOURCES, BASE_DATA_DIR, load_existing_manifest,
    fetch_with_retry, get_total_pages_static, get_total_pages_dynamic,
    extract_items_from_static, extract_items_from_dynamic,
    download_detail, polite_sleep, logger
)

# 连续跳过阈值：如果连续跳过 20 个文件，认为后续都是旧文件，停止爬取
CONSECUTIVE_SKIP_LIMIT = 20

def crawl_static_incremental(source: dict, existing_urls: set, max_pages: int = None):
    """增量爬取静态分页栏目"""
    name = source["name"]
    base_url = source["base_url"]
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始增量更新: {name}")
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
    
    # 增量模式通常不需要爬很多页，但我们仍保留翻页逻辑，靠 skip 机制退出
    logger.info(f"{name}:检测到共 {total_pages} 页，将执行增量检查...")

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    consecutive_skips = 0
    stop_signal = False

    for page_num in range(1, total_pages + 1):
        if stop_signal:
            break

        if page_num == 1:
            page_url = urljoin(base_url, "index.html")
            html = resp.text
        else:
            page_url = urljoin(base_url, f"index_{page_num - 1}.html")
            polite_sleep(1, 3)
            page_resp = fetch_with_retry(page_url, retries=1)
            if not page_resp:
                logger.warning(f"{name}: 第 {page_num} 页获取失败 (404?)，已到达末尾")
                break
            html = page_resp.text

        items = extract_items_from_static(html, page_url)
        logger.info(f"{name}: 第 {page_num} 页, 解析到 {len(items)} 条")

        if not items:
            logger.warning(f"{name}: 第 {page_num} 页无有效条目，停止")
            break

        for item in items:
            is_new = download_detail(item, save_dir, existing_urls, name)
            if is_new:
                stats["downloaded"] += 1
                consecutive_skips = 0  # 重置计数器
                logger.info(f"  ✓ 新增: {item['date']} {item['title'][:40]}...")
            else:
                stats["skipped"] += 1
                consecutive_skips += 1
            
            if consecutive_skips >= CONSECUTIVE_SKIP_LIMIT:
                logger.info(f"⚡️ 连续跳过 {consecutive_skips} 个已存在文件，判定为无新内容。")
                logger.info(f"🛑 停止爬取模块: {name}")
                stop_signal = True
                break

    logger.info(f"{name} 增量更新完成: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")


def crawl_dynamic_incremental(source: dict, existing_urls: set, max_pages: int = None):
    """增量爬取动态分页栏目"""
    name = source["name"]
    base_url = source["base_url"]
    params_template = source.get("params", {})
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始增量更新: {name}")
    logger.info(f"{'='*60}")

    # 第1页
    params = params_template.copy()
    resp = fetch_with_retry(base_url, params=params)
    if not resp:
        logger.error(f"无法访问 {name} 首页")
        return

    total_pages = get_total_pages_dynamic(resp.text)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    logger.info(f"{name}:检测到共 {total_pages} 页，将执行增量检查...")

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    consecutive_skips = 0
    stop_signal = False

    # 动态页翻页：通常只需前几页
    for page_num in range(1, total_pages + 1):
        if stop_signal:
            break

        if page_num == 1:
            html = resp.text
            page_url = base_url
        else:
            params = params_template.copy()
            params["page"] = page_num
            polite_sleep(1, 3)
            page_resp = fetch_with_retry(base_url, params=params)
            if not page_resp:
                logger.warning(f"Failed to fetch page {page_num}")
                stats["failed"] += 1
                continue
            html = page_resp.text
            page_url = f"{base_url}?page={page_num}"

        items = extract_items_from_dynamic(html, base_url)
        logger.info(f"{name}: 第 {page_num} 页, 解析到 {len(items)} 条")

        if not items:
            logger.warning(f"{name}: 第 {page_num} 页无数据，停止")
            break

        for item in items:
            is_new = download_detail(item, save_dir, existing_urls, name)
            if is_new:
                stats["downloaded"] += 1
                consecutive_skips = 0
                logger.info(f"  ✓ 新增: {item['date']} {item['title'][:40]}...")
            else:
                stats["skipped"] += 1
                consecutive_skips += 1
            
            if consecutive_skips >= CONSECUTIVE_SKIP_LIMIT:
                logger.info(f"⚡️ 连续跳过 {consecutive_skips} 个已存在文件，判定为无新内容。")
                logger.info(f"🛑 停止爬取模块: {name}")
                stop_signal = True
                break

    logger.info(f"{name} 增量更新完成: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")



def main():
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    existing_urls = load_existing_manifest()
    logger.info(f"已加载 {len(existing_urls)} 条历史记录")

    for source in SOURCES:
        try:
            if source["type"] == "static":
                crawl_static_incremental(source, existing_urls)
            elif source["type"] == "dynamic":
                crawl_dynamic_incremental(source, existing_urls)
        except Exception as e:
            logger.error(f"Source {source['name']} failed: {e}")

if __name__ == "__main__":
    main()

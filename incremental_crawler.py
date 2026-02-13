#!/usr/bin/env python3
"""
incremental_crawler.py - 教育部网站纯增量爬虫

功能：
  专为“定期更新”设计，剔除了全量爬取的复杂逻辑。
  原理：只爬取列表页的前几页，一旦连续遇到 20 个已存在的文件，立即停止。
  
  特点：
  - 极速：不做全量翻页检测，通常仅请求 1-3 页即可完成。
  - 省流：仅下载新文件。
"""

import os
import itertools
from urllib.parse import urljoin
from crawler import (
    SOURCES, BASE_DATA_DIR, load_existing_manifest,
    fetch_with_retry, extract_items_from_static, extract_items_from_dynamic,
    download_detail, polite_sleep, logger
)

# 连续跳过阈值
CONSECUTIVE_SKIP_LIMIT = 20

def crawl_static_incremental(source: dict, existing_urls: set):
    """增量爬取静态分页栏目"""
    name = source["name"]
    base_url = source["base_url"]
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始增量扫描: {name}")
    logger.info(f"{'='*60}")

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    consecutive_skips = 0

    # 无限循环翻页，直到触发停止条件
    for page_num in itertools.count(1):
        if page_num == 1:
            page_url = urljoin(base_url, "index.html")
            resp = fetch_with_retry(page_url)
        else:
            page_url = urljoin(base_url, f"index_{page_num - 1}.html")
            polite_sleep(1, 3)
            resp = fetch_with_retry(page_url, retries=1)

        if not resp:
            logger.info(f"{name}: 第 {page_num} 页无法获取 (可能是翻完或404)，停止扫描")
            break
        
        items = extract_items_from_static(resp.text, page_url)
        logger.info(f"{name}: 第 {page_num} 页, 解析到 {len(items)} 条")

        if not items:
            logger.warning(f"{name}: 第 {page_num} 页无有效内容，停止")
            break

        # 检查本页内容
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
                logger.info(f"⚡️ 连续跳过 {consecutive_skips} 个已存在文件，已追平历史进度。")
                logger.info(f"🛑 停止爬取模块: {name}")
                logger.info(f"{name} 增量扫描完成: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")
                return

    logger.info(f"{name} 扫描结束: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")


def crawl_dynamic_incremental(source: dict, existing_urls: set):
    """增量爬取动态分页栏目"""
    name = source["name"]
    base_url = source["base_url"]
    params_template = source.get("params", {})
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始增量扫描: {name}")
    logger.info(f"{'='*60}")

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    consecutive_skips = 0

    for page_num in itertools.count(1):
        params = params_template.copy()
        if page_num > 1:
            params["page"] = page_num
        
        if page_num > 1:
            polite_sleep(1, 3)
            
        resp = fetch_with_retry(base_url, params=params)
        if not resp:
            logger.warning(f"Failed to fetch page {page_num}")
            stats["failed"] += 1
            if stats["failed"] > 3: # 连续失败几次就停吧
                break
            continue

        items = extract_items_from_dynamic(resp.text, base_url)
        logger.info(f"{name}: 第 {page_num} 页, 解析到 {len(items)} 条")

        if not items:
            logger.info(f"{name}: 第 {page_num} 页无数据，停止")
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
                logger.info(f"⚡️ 连续跳过 {consecutive_skips} 个已存在文件，已追平历史进度。")
                logger.info(f"🛑 停止爬取模块: {name}")
                logger.info(f"{name} 增量扫描完成: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")
                return

    logger.info(f"{name} 扫描结束: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")


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

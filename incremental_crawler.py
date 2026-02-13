#!/usr/bin/env python3
"""
incremental_crawler.py - 教育部网站纯增量爬虫（最保险版）

算法：
  静态栏目（中央文件 / 其他部门文件）：
    → 页数很少（10-20页），每次运行全页扫描，零遗漏。
    
  动态栏目（教育部文件）：
    → 页数多（800+页），采用"连续整页跳过"策略：
      当连续 3 整页的所有条目都已存在时，才停止。
      比"连续N条跳过"更保险，因为颗粒度是整页而非单条。
"""

import os
import itertools
from urllib.parse import urljoin
from crawler import (
    SOURCES, BASE_DATA_DIR, load_existing_manifest,
    fetch_with_retry, extract_items_from_static, extract_items_from_dynamic,
    download_detail, polite_sleep, logger
)

# 动态栏目：连续多少整页全部为旧文件时才停止
FULL_PAGE_SKIP_LIMIT = 3


def crawl_static_full_scan(source: dict, existing_urls: set):
    """
    静态栏目全页扫描。
    页数很少（10-20页），每次都扫完，绝对不遗漏。
    """
    name = source["name"]
    base_url = source["base_url"]
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始全页扫描: {name}")
    logger.info(f"{'='*60}")

    stats = {"downloaded": 0, "skipped": 0}

    for page_num in itertools.count(1):
        if page_num == 1:
            page_url = urljoin(base_url, "index.html")
        else:
            page_url = urljoin(base_url, f"index_{page_num - 1}.html")
            polite_sleep(1, 2)

        resp = fetch_with_retry(page_url, retries=1)
        if not resp:
            logger.info(f"{name}: 第 {page_num} 页不存在，扫描结束")
            break

        items = extract_items_from_static(resp.text, page_url)
        if not items:
            logger.info(f"{name}: 第 {page_num} 页无有效内容，扫描结束")
            break

        page_new = 0
        for item in items:
            is_new = download_detail(item, save_dir, existing_urls, name)
            if is_new:
                stats["downloaded"] += 1
                page_new += 1
                logger.info(f"  ✓ 新增: {item['date']} {item['title'][:40]}...")
            else:
                stats["skipped"] += 1

        logger.info(f"{name}: 第 {page_num} 页完成 (新增 {page_new}, 跳过 {len(items) - page_new})")

    logger.info(f"✅ {name} 扫描完成: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")


def crawl_dynamic_incremental(source: dict, existing_urls: set):
    """
    动态栏目增量扫描。
    采用"连续整页跳过"策略：只有连续 3 整页全部为旧文件时才停止。
    比"连续N条"更保险，即使文件散落在不同位置也能捕获。
    """
    name = source["name"]
    base_url = source["base_url"]
    params_template = source.get("params", {})
    save_dir = os.path.join(BASE_DATA_DIR, source["dir_name"])
    os.makedirs(save_dir, exist_ok=True)

    logger.info(f"{'='*60}")
    logger.info(f"开始增量扫描: {name}")
    logger.info(f"{'='*60}")

    stats = {"downloaded": 0, "skipped": 0}
    consecutive_full_skip_pages = 0  # 连续整页全旧的页数

    for page_num in itertools.count(1):
        params = params_template.copy()
        if page_num > 1:
            params["page"] = page_num
            polite_sleep(1, 3)

        resp = fetch_with_retry(base_url, params=params)
        if not resp:
            logger.warning(f"{name}: 第 {page_num} 页获取失败")
            break

        items = extract_items_from_dynamic(resp.text, base_url)
        if not items:
            logger.info(f"{name}: 第 {page_num} 页无数据，扫描结束")
            break

        # 逐条检查
        page_new = 0
        for item in items:
            is_new = download_detail(item, save_dir, existing_urls, name)
            if is_new:
                stats["downloaded"] += 1
                page_new += 1
                logger.info(f"  ✓ 新增: {item['date']} {item['title'][:40]}...")
            else:
                stats["skipped"] += 1

        logger.info(f"{name}: 第 {page_num} 页完成 (新增 {page_new}, 跳过 {len(items) - page_new})")

        # 判断整页是否全部为旧文件
        if page_new == 0:
            consecutive_full_skip_pages += 1
            if consecutive_full_skip_pages >= FULL_PAGE_SKIP_LIMIT:
                logger.info(f"⚡️ 连续 {FULL_PAGE_SKIP_LIMIT} 整页均为已存在文件，已追平历史进度。")
                logger.info(f"🛑 停止扫描: {name}")
                break
        else:
            consecutive_full_skip_pages = 0  # 有新文件，重置计数

    logger.info(f"✅ {name} 扫描完成: 新增 {stats['downloaded']}, 跳过 {stats['skipped']}")


def main():
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    existing_urls = load_existing_manifest()
    logger.info(f"已加载 {len(existing_urls)} 条历史记录")

    total_new = 0
    for source in SOURCES:
        try:
            if source["type"] == "static":
                crawl_static_full_scan(source, existing_urls)
            elif source["type"] == "dynamic":
                crawl_dynamic_incremental(source, existing_urls)
        except Exception as e:
            logger.error(f"模块 {source['name']} 出错: {e}")

    logger.info("全部模块扫描完成。")


if __name__ == "__main__":
    main()

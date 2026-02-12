#!/usr/bin/env python3
"""
monitor.py - 爬虫进度实时监控
功能：实时显示各模块的爬取进度、速度、预计剩余时间。
用法：python monitor.py  (在另一个终端窗口运行)
"""

import os
import json
import time
import sys
from collections import defaultdict
from datetime import timedelta

BASE_DATA_DIR = "data"
MANIFEST_FILE = os.path.join(BASE_DATA_DIR, "manifest.jsonl")
LOG_FILE = os.path.join("logs", "crawler.log")

# 各模块预估总数
EXPECTED_TOTALS = {
    "中央文件": 222,
    "教育部文件": 13228,
    "其他部门文件": 388,
}

REFRESH_INTERVAL = 3  # 秒


def count_files_by_module() -> dict:
    """统计各模块已下载的文件数"""
    counts = {}
    for module in EXPECTED_TOTALS:
        module_dir = os.path.join(BASE_DATA_DIR, module)
        if os.path.isdir(module_dir):
            files = [f for f in os.listdir(module_dir) if f.endswith(".html")]
            counts[module] = len(files)
        else:
            counts[module] = 0
    return counts


def count_manifest_by_module() -> dict:
    """从 manifest 统计各模块记录数"""
    counts = defaultdict(int)
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        source = record.get("source", "unknown")
                        counts[source] += 1
                    except json.JSONDecodeError:
                        pass
    return dict(counts)


def get_last_log_lines(n: int = 5) -> list[str]:
    """读取最近的 n 行日志"""
    if not os.path.exists(LOG_FILE):
        return ["(日志文件尚未创建)"]
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return [line.rstrip() for line in lines[-n:]]
    except Exception:
        return ["(无法读取日志)"]


def get_manifest_size() -> int:
    """获取 manifest 文件行数"""
    if not os.path.exists(MANIFEST_FILE):
        return 0
    try:
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def format_bar(current: int, total: int, width: int = 30) -> str:
    """生成进度条"""
    if total == 0:
        return "[" + "?" * width + "]"
    ratio = min(current / total, 1.0)
    filled = int(width * ratio)
    bar = "█" * filled + "░" * (width - filled)
    pct = ratio * 100
    return f"[{bar}] {pct:5.1f}%"


def clear_screen():
    os.system("clear" if os.name != "nt" else "cls")


def main():
    start_time = time.time()
    initial_counts = count_files_by_module()
    initial_total = sum(initial_counts.values())

    print("🔍 爬虫监控器已启动，按 Ctrl+C 退出...\n")

    try:
        while True:
            clear_screen()
            elapsed = time.time() - start_time
            elapsed_str = str(timedelta(seconds=int(elapsed)))

            file_counts = count_files_by_module()
            total_files = sum(file_counts.values())
            total_expected = sum(EXPECTED_TOTALS.values())
            new_files = total_files - initial_total

            # 速度计算
            speed = new_files / elapsed if elapsed > 0 else 0
            remaining = total_expected - total_files
            eta = timedelta(seconds=int(remaining / speed)) if speed > 0 else "∞"

            # 头部
            print("╔══════════════════════════════════════════════════════════════╗")
            print("║           📊 教育部网站爬虫 — 实时进度监控                 ║")
            print("╠══════════════════════════════════════════════════════════════╣")
            print(f"║  ⏱  运行时间: {elapsed_str:<12}  📦 总文件: {total_files}/{total_expected:<10}  ║")
            print(f"║  🚀 速度: {speed:.1f} 篇/秒         ⏳ 预计剩余: {str(eta):<12}   ║")
            print("╠══════════════════════════════════════════════════════════════╣")

            # 各模块进度
            for module, expected in EXPECTED_TOTALS.items():
                current = file_counts.get(module, 0)
                bar = format_bar(current, expected, 25)
                print(f"║  {module:<10} {bar} {current:>5}/{expected:<5}  ║")

            print("╠══════════════════════════════════════════════════════════════╣")
            print("║  📋 最近日志:                                              ║")

            # 最近日志
            last_lines = get_last_log_lines(5)
            for line in last_lines:
                # 截断过长的行
                display = line[:58]
                print(f"║  {display:<58}║")

            print("╚══════════════════════════════════════════════════════════════╝")
            print(f"\n  刷新间隔: {REFRESH_INTERVAL}s | Ctrl+C 退出监控 (不影响爬虫)")

            time.sleep(REFRESH_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n✋ 监控已停止 (爬虫仍在后台运行)")


if __name__ == "__main__":
    main()

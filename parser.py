#!/usr/bin/env python3
"""
parser.py - Stage 2: 离线解析器
功能：读取 data/{模块名}/ 下的 HTML 文件，清洗并提取内容，转换为 Markdown。
支持三个模块: 中央文件、教育部文件、其他部门文件
"""

import os
import re
import logging
from bs4 import BeautifulSoup

# --- 配置 ---
BASE_DATA_DIR = "data"
MODULES = ["中央文件", "教育部文件", "其他部门文件"]
MARKDOWN_DIR = os.path.join(BASE_DATA_DIR, "markdown")
LOG_DIR = "logs"

# --- 日志设置 ---
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "parser.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def parse_html(file_path: str) -> str | None:
    """解析单个 HTML 文件并返回 Markdown 内容"""
    with open(file_path, "rb") as f:
        content = f.read()

    # 尝试解码
    for encoding in ("utf-8", "gb18030", "gbk", "latin-1"):
        try:
            html = content.decode(encoding)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        logger.error(f"无法解码: {file_path}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # --- 提取元数据 ---
    meta = {}

    # 从文件名提取日期
    basename = os.path.basename(file_path)
    filename_no_ext = os.path.splitext(basename)[0]
    date_match = re.match(r"(\d{4}-\d{2}-\d{2})_", filename_no_ext)
    if date_match:
        meta["date_from_filename"] = date_match.group(1)

    # 标题
    h1 = soup.find("h1")
    meta["title"] = clean_text(h1.get_text()) if h1 else "Untitled"

    # 提取信息公开表格
    table = soup.find("table", class_="xxgk_table")
    if table:
        tds = table.find_all("td")
        for idx, td in enumerate(tds):
            text = clean_text(td.get_text())
            if idx == 0:
                meta["index_no"] = text
            elif idx == 2:
                meta["agency"] = text
            elif idx == 3:
                meta["date"] = text
            elif idx == 5:
                meta["doc_number"] = text

    # --- 提取正文 ---
    content_div = (
        soup.find("div", class_="trs_editor_view")
        or soup.find("div", id="jyb_xs_content")
        or soup.find("div", class_="moe-detail-box")
    )

    if not content_div:
        logger.warning(f"未找到正文区: {file_path}")
        return None

    # 清洗
    for tag in content_div(["script", "style", "iframe"]):
        tag.decompose()

    # 转换为 Markdown
    lines = []
    lines.append(f"# {meta.get('title', 'Untitled')}")
    lines.append("")
    if "date" in meta:
        lines.append(f"**发布日期**: {meta['date']}")
    elif "date_from_filename" in meta:
        lines.append(f"**发布日期**: {meta['date_from_filename']}")
    if "agency" in meta:
        lines.append(f"**发布机构**: {meta['agency']}")
    if "doc_number" in meta:
        lines.append(f"**发文字号**: {meta['doc_number']}")
    if "index_no" in meta:
        lines.append(f"**索引号**: {meta['index_no']}")
    lines.append("")
    lines.append("---")
    lines.append("")

    for element in content_div.find_all(["p", "h2", "h3", "h4", "ul", "ol", "table"]):
        if element.name in ("h2", "h3", "h4"):
            level = "#" * (int(element.name[1]) - 1 + 1)
            lines.append(f"{level} {clean_text(element.get_text())}")
        elif element.name == "p":
            text = clean_text(element.get_text())
            if text:
                lines.append(text)
                lines.append("")
        elif element.name == "ul":
            for li in element.find_all("li"):
                lines.append(f"- {clean_text(li.get_text())}")
            lines.append("")
        elif element.name == "ol":
            for i, li in enumerate(element.find_all("li"), 1):
                lines.append(f"{i}. {clean_text(li.get_text())}")
            lines.append("")

    return "\n".join(lines)


def main():
    total_parsed = 0
    total_skipped = 0

    for module in MODULES:
        src_dir = os.path.join(BASE_DATA_DIR, module)
        dst_dir = os.path.join(MARKDOWN_DIR, module)

        if not os.path.isdir(src_dir):
            logger.info(f"跳过模块（目录不存在）: {module}")
            continue

        os.makedirs(dst_dir, exist_ok=True)
        files = [f for f in os.listdir(src_dir) if f.endswith(".html")]
        logger.info(f"模块 [{module}]: 找到 {len(files)} 个 HTML 文件")

        for filename in files:
            file_path = os.path.join(src_dir, filename)
            md_filename = os.path.splitext(filename)[0] + ".md"
            md_path = os.path.join(dst_dir, md_filename)

            # 跳过已解析的
            if os.path.exists(md_path):
                total_skipped += 1
                continue

            try:
                markdown_content = parse_html(file_path)
                if markdown_content:
                    with open(md_path, "w", encoding="utf-8") as f:
                        f.write(markdown_content)
                    total_parsed += 1
                    logger.info(f"  ✓ 解析: {filename}")
                else:
                    total_skipped += 1
                    logger.warning(f"  ✗ 跳过: {filename}")
            except Exception as e:
                total_skipped += 1
                logger.error(f"  ✗ 出错: {filename}: {e}")

    logger.info(f"解析完成: 成功 {total_parsed}, 跳过 {total_skipped}")


if __name__ == "__main__":
    main()

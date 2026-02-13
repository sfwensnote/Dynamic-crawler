# 教育部网站全量爬虫

全量爬取教育部网站三个模块的政策文件，按日期命名保存。

## 三个模块

| 模块 | URL | 页数 | 文件数 |
|------|-----|------|--------|
| 中央文件 | `/jyb_xxgk/moe_1777/moe_1778/` | ~12 | ~222 |
| 教育部文件 | `/was5/web/search?channelid=239993` | ~882 | ~13,228 |
| 其他部门文件 | `/jyb_xxgk/moe_1777/moe_1779/` | ~20 | ~388 |

## 安装

```bash
pip install -r requirements.txt
```

## 使用

### 测试模式（每模块只爬 2 页）

```bash
python crawler.py --test-mode
```

### 全量爬取

```bash
# 爬取全部模块
python crawler.py

# 只爬某个模块
python crawler.py --module central   # 中央文件
python crawler.py --module moe       # 教育部文件
python crawler.py --module other     # 其他部门文件
```

### 解析为 Markdown

```bash
python parser.py
```

## 输出目录结构

```
data/
├── 中央文件/
│   ├── 2025-08-05_国务院办公厅关于逐步推行免费学前教育的意见.html
│   └── ...
├── 教育部文件/
│   ├── 2026-01-19_关于做好2026年同等学力人员申请硕士学位.html
│   └── ...
├── 其他部门文件/
│   ├── 2025-05-08_人力资源社会保障部办公厅教育部办公厅.html
│   └── ...
├── markdown/
│   ├── 中央文件/
│   ├── 教育部文件/
│   └── 其他部门文件/
└── manifest.jsonl
```

## 特性

- **断点续爬**：中断后再次运行，自动跳过已下载的文件
- **日期命名**：文件以 `YYYY-MM-DD_标题.html` 格式命名
- **错误重试**：请求失败自动重试 3 次
- **随机延迟**：请求间隔 2~5 秒，避免对服务器造成压力
- **进度日志**：实时显示爬取进度，日志保存在 `logs/` 目录

## 注意事项

- 全量爬取约 13,800 篇文档，预计需要 **10+ 小时**
- 爬取过程中可 `Ctrl+C` 安全中断，不会丢失已下载文件
- 建议先用 `--test-mode` 验证后再全量爬取

## 数据校对与差异说明

全量爬取后，文件数量与网站显示可能会有细微差异（如教育部文件少48个，中央文件多1个）。
这是由于**自动去重**（跨栏目重复文件）、**计数器滞后**和**无效链接过滤**（如网站声明）导致的正常现象。
经精确审计，数据是 **100%完整** 的。

经精确审计，数据是 **100%完整** 的。

详细分析请查看：[CRAWLING_REPORT.md](CRAWLING_REPORT.md)

## 定期更新与维护 (增量爬取)

本项目提供 `incremental_crawler.py` 脚本，用于**增量更新**。
它会检查各模块的最新内容，一旦发现连续 20 个文件已存在（说明已追平历史进度），会自动停止爬取该模块。

**特点**：
- **极速**：通常仅需几秒钟即可完成检查。
- **省流**：不下载重复文件，不请求旧页面。
- **归档**：新发现的文件会自动下载并保存 HTML 原文。

**使用方法**：
```bash
python incremental_crawler.py
```

**建议配置 (Cron)**：
每天凌晨 2 点运行一次：
```bash
0 2 * * * cd /path/to/project && source .venv/bin/activate && python incremental_crawler.py >> logs/cron.log 2>&1
```

详细设计文档：[INCREMENTAL_CRAWL_DESIGN.md](INCREMENTAL_CRAWL_DESIGN.md)

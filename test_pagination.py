#!/usr/bin/env python3
"""
test_pagination.py - 分页逻辑验证脚本 (Multi-Category Edition)
验证教育部网站三大栏目的分页机制：
1. 教育部文件 (Dynamic)
2. 中央文件 (Static)
3. 其他部门文件 (Static)
"""

import requests
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

SOURCES = [
    {
        "name": "教育部文件 (Dynamic)",
        "type": "dynamic",
        "base_url": "http://www.moe.gov.cn/was5/web/search",
        "params": {"channelid": "239993"}
    },
    {
        "name": "中央文件 (Static)",
        "type": "static",
        "base_url": "http://www.moe.gov.cn/jyb_xxgk/moe_1777/moe_1778/"
    },
    {
        "name": "其他部门文件 (Static)",
        "type": "static",
        "base_url": "http://www.moe.gov.cn/jyb_xxgk/moe_1777/moe_1779/"
    }
]

def fetch_url(url, params=None):
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.encoding = resp.apparent_encoding if resp.apparent_encoding else 'utf-8'
        return resp
    except Exception as e:
        print(f"[ERROR] 请求失败 {url}: {e}")
        return None

def test_source(source):
    print(f"\n>>> 测试栏目: {source['name']}")
    
    # --- 第 1 页 ---
    url1 = source['base_url']
    if source['type'] == 'static':
        url1 = urljoin(source['base_url'], "index.html")
        
    print(f"    [1] 请求第1页: {url1}")
    resp1 = fetch_url(url1, params=source.get('params'))
    if not resp1 or resp1.status_code != 200:
        print("    [FAIL] 无法访问第1页")
        return

    # 解析链接数
    soup1 = BeautifulSoup(resp1.text, 'html.parser')
    # 兼容两种列表结构: div.gongkai_wenjian (dynamic) 和 (static 页面可能不同，需检查)
    # Static页面的列表通常在 ul#list 或 similar，先用通用查找 verify
    # 观察 static 页面结构: 通常是 <ul> <li> <a href="./...">...
    items1 = soup1.select('div.gongkai_wenjian li, ul#list li, div.scy_lbsj-right li, li') 
    # 简单的过滤有效链接
    valid_links1 = [i for i in items1 if i.find('a') and i.find('a').get('href')]
    print(f"    解析到条目数: {len(valid_links1)}")

    # --- 第 2 页 ---
    delay = random.uniform(2, 4)
    print(f"    [Wait] 等待 {delay:.1f} 秒...")
    time.sleep(delay)
    
    url2 = ""
    params2 = None
    
    if source['type'] == 'dynamic':
        url2 = source['base_url']
        params2 = source['params'].copy()
        params2['page'] = 2
    else: # static
        # static page 2 is index_1.html
        url2 = urljoin(source['base_url'], "index_1.html")
        
    print(f"    [2] 请求第2页: {url2} (Params: {params2})")
    resp2 = fetch_url(url2, params=params2)
    
    if not resp2 or resp2.status_code != 200:
        print(f"    [FAIL] 无法访问第2页 (Status: {resp2.status_code if resp2 else 'Err'})")
        # 尝试 index_2.html 也就是 page 3? 或者是 index_1.html 确实不存在?
        return

    soup2 = BeautifulSoup(resp2.text, 'html.parser')
    items2 = soup2.select('div.gongkai_wenjian li, ul#list li, div.scy_lbsj-right li, li')
    valid_links2 = [i for i in items2 if i.find('a') and i.find('a').get('href')]
    print(f"    解析到条目数: {len(valid_links2)}")
    
    if len(valid_links1) > 0 and len(valid_links2) > 0:
        print("    [PASS] 分页测试通过")
    else:
        print("    [WARN] 条目数异常，请检查选择器")

def main():
    print("="*60)
    print("全量分页测试开始")
    print("="*60)
    for source in SOURCES:
        test_source(source)
        time.sleep(2)

if __name__ == "__main__":
    main()

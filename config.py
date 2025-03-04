# -*- coding: utf-8 -*-

import os

# 创建保存爬虫数据的目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # 项目根目录
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloaded_images')  # 图片下载目录
CRAWLED_DATA_DIR = os.path.join(BASE_DIR, 'crawled_data')  # 爬取数据存储目录

# 确保目录存在
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CRAWLED_DATA_DIR, exist_ok=True)

# Redis 配置
REDIS_CONFIG = {
    'host': 'localhost',  # Redis 主机地址
    'port': 6379,  # Redis 端口
    'db': 0,  # Redis 数据库编号
    'decode_responses': True  # 自动解码响应为字符串
}

# Redis 键名配置
REDIS_KEYS = {
    'seed_urls': 'seed_urls',  # 种子 URL
    'pending_urls': 'pending_urls',  # 待抓取 URL
    'failed_urls': 'failed_urls',  # 爬取失败的 URL
    'success_urls': 'success_urls',  # 爬取成功的 URL
    'crawler_status': 'crawler_status',  # 爬虫的运行状态
    'parsed_data': 'parsed_data',  # 解析后的数据
    'image_titles': 'image_titles',  # 图片标题等信息
    'crawler_tasks_prefix': 'crawler_tasks'  # 爬虫任务的 Redis key 前缀
}

# 爬虫配置
CRAWLER_CONFIG = {
    'max_workers': 3,  # 最大并发进程数
    'timeout': 10,  # 请求超时设置
    'max_retries': 3,  # 最大重试次数
    'download_path': DOWNLOAD_DIR  # 爬取数据保存路径
}

# 可供爬虫使用的代理 IP 池配置
PROXY_POOL = [
    'http://182.34.102.166:9999',
    'http://183.236.232.160:8080',
    'http://120.220.220.95:8085',
]

# 种子 URL
SEED_URLS = [
    'https://sc.chinaz.com/tupian/dongwutupian.html',
    'https://sc.chinaz.com/tupian/dongwutupian_2.html',
    'https://sc.chinaz.com/tupian/dongwutupian_3.html',
]
# -*- coding: utf-8 -*-
import os

# 创建保存爬虫数据的目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  #
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloaded_images')
CRAWLED_DATA_DIR = os.path.join(BASE_DIR, 'crawled_data')

# 确保目录存在
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CRAWLED_DATA_DIR, exist_ok=True)

# Redis配置
REDIS_CONFIG = {
    'host': 'localhost',  # 本地部署
    'port': 6379,  # 默认端口
    'db': 0,  # redis的数据库编号，默认为0
    'decode_responses': True  # 响应自动解码为字符串
}

# URL配置
SEED_URLS = [
    'https://sc.chinaz.com/tupian/dongwutupian.html',
    'https://sc.chinaz.com/tupian/dongwutupian_2.html',
    'https://sc.chinaz.com/tupian/dongwutupian_3.html',
    'https://sc.chinaz.com/tupian/dongwutupian_4.html',
    'https://sc.chinaz.com/tudpian/dongwutupian_5.html',
]

# 爬虫配置
CRAWLER_CONFIG = {
    'max_workers': 3,  # 最大并发进程数
    'timeout': 10,  # 请求超时设置
    'max_retries': 3,  # 最大重试次数
    'download_path': 'downloaded_images'  # 爬取数据保存路径
}

# 可供爬虫使用的代理IP池配置
PROXY_POOL = [
    'http://182.34.102.166:9999',
    'http://183.236.232.160:8080',
    'http://120.220.220.95:8085',
]

# Redis键名配置
REDIS_KEYS = {
    'seed_urls': 'seed_urls',  # 种子URL
    'pending_urls': 'pending_urls',  # 待抓取URL
    'failed_urls': 'failed_urls',  # 爬取失败的URL
    'success_urls': 'success_urls',  # 爬取成功的URL
    'crawler_status': 'crawler_status',  # 爬虫的运行状态
    'parsed_data': 'parsed_data',  # 解析后的数据
    'image_titles': 'image_titles'  # 图片标题等信息
} 
# -*- coding: utf-8 -*-

from urllib.parse import urlparse  # URL解析模块
import dns.resolver  # DNS解析模块
import redis  # Redis 客户端
import json
from config import REDIS_CONFIG, REDIS_KEYS
import time

class URLManagerFlink:
    def __init__(self, redis_config):
        """初始化URL 管理器"""
        self.redis_config = redis_config  # Redis 配置
        self.redis_client = None

    def connect_redis(self):
        """连接Redis"""
        try:
            if not self.redis_client:
                self.redis_client = redis.Redis(**REDIS_CONFIG)
                # 测试连接
                self.redis_client.ping()
                print("Redis连接成功")
        except redis.ConnectionError as e:
            print(f"Redis连接失败: {str(e)}")
            raise

    def add_seed_urls(self, urls):
        """添加种子 URL 到 Redis"""
        try:
            self.connect_redis()
            added_count = 0
            for url in urls:
                print(f"正在添加种子URL: {url}")
                # 添加到种子URL集合
                if self.redis_client.sadd(REDIS_KEYS['seed_urls'], url):
                    added_count += 1
                # 同时添加到待爬取队列
                if self.redis_client.sadd(REDIS_KEYS['pending_urls'], url):
                    print(f"URL添加到待爬取队列: {url}")
            print(f"成功添加 {added_count} 个新的种子URL")

            # 检查Redis中的URL数量
            pending_count = self.redis_client.scard(REDIS_KEYS['pending_urls'])
            print(f"待爬取队列中现有URL数量: {pending_count}")
        except Exception as e:
            print(f"添加种子 URL 时出错: {str(e)}")

    def add_seed_urls_from_file(self, file_path):
        """从文件批量导入种子 URL 到 Redis"""
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]  # 去除每行的空白字符，过滤空行
            self.add_seed_urls(urls)
        except Exception as e:
            print(f"从文件导入种子 URL 时出错: {str(e)}")

    def get_domain_ip(self, url):
        """DNS解析模块"""
        try:
            domain = urlparse(url).netloc  # 从URL中提取域名
            answers = dns.resolver.resolve(domain, 'A')  # 解析域名的A记录
            ips = [answer.address for answer in answers]  # 提取解析到的IP地址列表
            print(f"域名 {domain} 解析到IP: {ips}")
            return ips
        except Exception as e:
            print(f"DNS解析错误 ({domain}): {str(e)}")
            return None


    def get_pending_url(self):
        """获取待爬取的URL"""
        try:
            self.connect_redis()  # 确保redis连接已建立
            url = self.redis_client.spop(REDIS_KEYS['pending_urls'])  # 弹出一个待爬取的URL
            if url:
                print(f"获取到待爬取URL: {url}")
            return url
        except Exception as e:
            print(f"获取待爬取URL时出错: {str(e)}")
            return None

    def mark_url_status(self, url, status):
        """标记URL状态"""
        try:
            self.connect_redis()
            if status == 'success':
                self.redis_client.sadd(REDIS_KEYS['success_urls'], url)
                print(f"URL已标记为成功: {url}")
            else:
                self.redis_client.sadd(REDIS_KEYS['failed_urls'], url)
                print(f"URL已标记为失败: {url}")
        except Exception as e:
            print(f"标记URL状态时出错: {str(e)}")
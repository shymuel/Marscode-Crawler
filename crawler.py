# -*- coding: utf-8 -*-

import redis
import requests
import time
import os
from fake_useragent import UserAgent
from multiprocessing import Process
from config import REDIS_CONFIG, REDIS_KEYS, CRAWLER_CONFIG, PROXY_POOL
import random
import json
from data_parser import DataParser
from urllib.parse import urljoin
import re


class Crawler:
    def __init__(self, parse_queue):
        """初始化爬虫"""
        self.running = True
        self.base_url = 'https://sc.chinaz.com/'
        self.parse_queue = parse_queue  # 使用传入的共享队列

        # 确保下载目录存在
        print(f"下载目录: {CRAWLER_CONFIG['download_path']}")
        if not os.path.exists(CRAWLER_CONFIG['download_path']):
            os.makedirs(CRAWLER_CONFIG['download_path'])
            print(f"创建下载目录: {CRAWLER_CONFIG['download_path']}")

    def download_image(self, url, crawler_id):
        """下载图片。先保存到redis，再保存到本地"""
        try:
            response = requests.get(url, timeout=CRAWLER_CONFIG['timeout'])
            if response.status_code == 200:  # 200代表请求成功
                # 从URL中提取文件名
                filename = url.split('/')[-1]

                # 获取图片的标题作为文件名
                redis_client = redis.Redis(**REDIS_CONFIG)  # 创建一个redis客户端实例并连接redis
                title = redis_client.hget(REDIS_KEYS['image_titles'], url)  # 从redis哈希表中获取url对应的图片标题。redis里的URL是之前分发来的
                if title:  # 去除非法字符，增加扩展名
                    # 清理文件名中的非法字符
                    title = re.sub(r'[\\/:*?"<>|]', '', title)
                    extension = filename.split('.')[-1] if '.' in filename else 'jpg'
                    filename = f"{title}.{extension}"

                if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
                    filename += '.jpg'

                # 构建保存路径
                save_path = os.path.join(CRAWLER_CONFIG['download_path'], filename)

                # 保存图片
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                print(f"爬虫 {crawler_id} 成功下载图片: {filename}")
                return True
        except Exception as e:
            print(f"下载图片失败 {url}: {str(e)}")
        return False

    def get_proxy(self):
        """获取随机代理"""
        return {'http': random.choice(PROXY_POOL)} if PROXY_POOL else None

    def update_status(self, crawler_id, status, redis_client):
        """更新爬虫状态"""
        try:
            redis_client.hset(
                REDIS_KEYS['crawler_status'],
                f'crawler_{crawler_id}',
                json.dumps({  # 将要存储的信息转化为JSON字符串
                    'status': status,
                    'last_update': time.time()
                })
            )
        except Exception as e:
            print(f"更新爬虫状态失败: {str(e)}")

    def crawler_worker(self, crawler_id):
        """爬虫工作进程"""
        try:
            redis_client = redis.Redis(**REDIS_CONFIG)  # 创建一个redis client并连接
            ua = UserAgent()  # 生成一个随机的用户代理，模拟不同的浏览器或设备

            print(f"爬虫 {crawler_id} 开始工作")

            while self.running:
                try:
                    # 先从爬虫自己的任务队列中获取URL
                    url = redis_client.lpop(f'crawler:{crawler_id}:tasks')
                    if not url:  # 自己的队列中没有URL，则从pending_urls中获取URL
                        url = redis_client.spop(REDIS_KEYS['pending_urls'])  # spop从集合中随机删除一个元素并返回

                    if not url:
                        print(f"爬虫 {crawler_id} 等待任务...")
                        time.sleep(1)
                        continue

                    print(f"爬虫 {crawler_id} 获取到URL: {url}")

                    # 检查是否是图片URL
                    if any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif']):
                        print(f"爬虫 {crawler_id} 检测到图片URL，开始下载...")
                        if self.download_image(url, crawler_id):
                            redis_client.sadd(REDIS_KEYS['success_urls'], url)
                        continue

                    # 如果不是图片URL，则爬取页面内容
                    headers = {
                        'User-Agent': ua.random,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        # 告诉服务器客户端可以接受的响应类型
                        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                        'Referer': self.base_url
                    }

                    print(f"爬虫 {crawler_id} 使用headers: {headers}")

                    proxy = self.get_proxy()
                    if proxy:
                        print(f"爬虫 {crawler_id} 使用代理: {proxy}")

                    # 尝试请求
                    for retry in range(CRAWLER_CONFIG['max_retries']):
                        try:
                            print(f"爬虫 {crawler_id} 第 {retry + 1} 次尝试请求 {url}")
                            response = requests.get(
                                url,
                                headers=headers,
                                proxies=proxy,
                                timeout=CRAWLER_CONFIG['timeout']
                            )

                            print(f"爬虫 {crawler_id} 获得响应状态码: {response.status_code}")

                            if response.status_code == 200:
                                # 设置正确的编码
                                response.encoding = 'utf-8'
                                print(f"爬虫 {crawler_id} 成功获取页面内容，长度: {len(response.text)}")
                                print("正在发送数据到解析器...")
                                try:
                                    data_to_parse = {
                                        'url': url,
                                        'content': response.text,
                                        'crawler_id': crawler_id
                                    }
                                    self.parse_queue.put(data_to_parse)  # 爬取到的非图片内容传入共享队列
                                    print(f"数据已发送到解析器队列，队列大小: {self.parse_queue.qsize()}")
                                    redis_client.sadd(REDIS_KEYS['success_urls'], url)
                                except Exception as e:
                                    print(f"发送数据到解析器失败: {str(e)}")
                                    import traceback
                                    print(traceback.format_exc())
                                break

                        except Exception as e:
                            print(f"爬虫 {crawler_id} 请求失败: {str(e)}")
                            if retry == CRAWLER_CONFIG['max_retries'] - 1:
                                redis_client.sadd(REDIS_KEYS['failed_urls'], url)


                except Exception as e:
                    print(f"爬虫 {crawler_id} 处理URL时发生错误: {str(e)}")

        except Exception as e:
            print(f"爬虫 {crawler_id} 初始化失败: {str(e)}")

    def start_crawlers(self, count=3):
        """启动多个爬虫进程"""
        crawler_processes = []
        for i in range(count):
            # process会创建一个新的子进程，启动时调用self.crawler_worker方法，传递的参数是i
            p = Process(target=self.crawler_worker, args=(i,))
            p.start()  # 启动该进程
            crawler_processes.append(p)
            print(f"爬虫 {i} 已启动")
        return crawler_processes

    def stop_crawlers(self):
        """停止所有爬虫"""
        self.running = False 
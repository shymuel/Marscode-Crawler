# -*- coding: utf-8 -*-

from lxml import etree
import jieba
import json
import redis
from config import REDIS_CONFIG, REDIS_KEYS
from multiprocessing import Process, Queue
from queue import Empty
from urllib.parse import urljoin
import re


class DataParser:
    def __init__(self, parse_queue):
        """初始化解析器"""
        self.parse_queue = parse_queue  # 使用传入的共享队列

    def parse_html(self, html_content, base_url):
        """解析HTML内容"""
        try:
            print("开始解析HTML内容...")

            # 确保内容是字符串类型
            if isinstance(html_content, bytes):  # 字节类型则需要解码
                html_content = html_content.decode('utf-8')

            tree = etree.HTML(html_content)  # 将HTML内容解析为一个HTML树

            # 打印页面内容预览
            print("HTML内容预览:")
            preview = html_content[:200].replace('\n', ' ').replace('\r', '')
            print(preview)

            # 尝试不同的xpath模式来查找图片
            image_patterns = [
                '//img[@class="lazy"]',  # 站长之家常用的懒加载图片类
                '//div[contains(@class, "tupian-list")]//img',
                '//div[contains(@class, "item")]//img',
                '//img[contains(@data-original, ".jpg") or contains(@data-original, ".png")]',
                '//img[contains(@src, ".jpg") or contains(@src, ".png")]',
                '//img'
            ]

            image_data = []
            seen_urls = set()  # 用于去重

            for pattern in image_patterns:
                print(f"\n尝试图片模式: {pattern}")
                images = tree.xpath(pattern)
                print(f"找到 {len(images)} 个图片元素")

                for img in images:  # img是一组tag
                    # 打印图片元素的所有属性
                    print("\n图片元素属性:")
                    for attr in img.attrib:
                        print(f"{attr}: {img.get(attr)}")

                    # 优先使用 data-original 属性
                    image_url = img.get('data-original', '') or img.get('src', '')

                    if image_url and image_url not in seen_urls:
                        seen_urls.add(image_url)
                        # 跳过加载中的图片
                        if 'img-loding.png' in image_url:
                            continue

                        image_info = {
                            'title': img.get('alt', '').strip() or img.get('title', '').strip() or '未命名图片',
                            'image_url': image_url,
                            'description': ''
                        }

                        # 确保图片URL是完整的
                        if not image_info['image_url'].startswith(('http:', 'https:')):
                            image_info['image_url'] = urljoin(base_url, image_info['image_url'])
                        print(f"找到图片: {image_info['image_url']}")
                        image_data.append(image_info)

            print(f"\n成功解析 {len(image_data)} 个图片信息")
            return image_data  # 返回得到的图片信息，每个元素都是一个image_info的字典

        except Exception as e:
            print(f"解析错误: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None

    def clean_data(self, data):
        """清洗数据"""
        if not data:
            return None

        redis_client = redis.Redis(**REDIS_CONFIG)
        cleaned_data = []
        for item in data:
            if item['image_url']:  # 确保有图片URL
                # 清理URL
                url = item['image_url'].strip()
                # 确保是图片URL
                if not re.search(r'\.(jpg|jpeg|png|gif)$', url, re.I):
                    url += '.jpg'

                # 保存图片标题到Redis
                redis_client.hset(REDIS_KEYS['image_titles'], url, item['title'])

                cleaned_item = {
                    'title': item['title'] or '未知标题',
                    'image_url': url,
                    'description': item['description'] or '无描述'
                }
                cleaned_data.append(cleaned_item)
                print(f"清理后的图片URL: {url}")

        print(f"清洗后保留 {len(cleaned_data)} 个有效数据")
        return cleaned_data

    def parse_worker(self):
        """解析工作进程"""
        redis_client = redis.Redis(**REDIS_CONFIG)
        print("解析工作进程已启动，等待数据...")

        while True:
            try:
                print("等待解析队列中的数据...")
                # 从队列获取待解析数据
                try:
                    data = self.parse_queue.get(timeout=1)  # 添加超时
                    print(f"收到数据: {type(data)}")

                    if data == 'STOP':
                        break

                    url = data.get('url')
                    content = data.get('content')
                    crawler_id = data.get('crawler_id')

                    if url == None or content == None or crawler_id==None or url == "" or content == "":
                        print(f"数据不完整: {data}")
                        # print(all([url, content, crawler_id]))
                        # print(f"url:{url==None}, content:{content==None}, crawler_id:{crawler_id==None}")
                        continue

                    print(f"解析进程开始处理来自爬虫 {crawler_id} 的数据")
                    print(f"URL: {url}")
                    print(f"页面内容长度: {len(content)}")

                    # 解析数据
                    parsed_data = self.parse_html(content, url)  # 能用xpath找到一系列图片URL
                    if parsed_data:
                        print(f"解析到 {len(parsed_data)} 个图片数据")
                        # 清洗数据
                        cleaned_data = self.clean_data(parsed_data)
                        if cleaned_data:
                            # 将图片URL添加到待爬取队列
                            for item in cleaned_data:
                                print(f"处理图片: {item['image_url']}")
                                redis_client.sadd(REDIS_KEYS['pending_urls'], item['image_url'])  # 向集合中添加元素
                                print(f"添加图片URL到待爬取队列: {item['image_url']}")

                            # 保存完整数据
                            redis_client.lpush(  # 向列表最左侧推送元素
                                REDIS_KEYS['parsed_data'],
                                json.dumps(cleaned_data, ensure_ascii=False)
                            )
                            print(f"成功保存 {len(cleaned_data)} 条解析数据")
                    else:
                        print("没有解析到任何图片数据")

                except Empty:
                    continue  # 如果队列为空，继续等待

            except Exception as e:
                print(f"解析工作进程错误: {str(e)}")
                import traceback
                print(traceback.format_exc())

    def start_parser(self, worker_count=2):
        """启动多个解析进程"""
        workers = []
        for i in range(worker_count):
            p = Process(target=self.parse_worker)
            p.start()
            workers.append(p)
            print(f"解析进程 {i} 已启动")
        return workers 
# -*- coding: utf-8 -*-

import json
import os
import time
from datetime import datetime
import redis
from config import REDIS_CONFIG, CRAWLER_CONFIG, REDIS_KEYS
import sqlite3


class Storage:
    def __init__(self):
        """初始化存储系统"""
        try:
            self.redis_client = redis.Redis(**REDIS_CONFIG)
            print("Storage: Redis连接成功")

            # 创建数据存储目录
            self.data_dir = 'crawled_data'
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
                print(f"Storage: 创建数据目录 {self.data_dir}")

            # 初始化数据库
            self.init_database()
            print("Storage: 数据库初始化完成")

        except Exception as e:
            print(f"Storage初始化失败: {str(e)}")
            raise

    def init_database(self):
        """初始化SQLite数据库"""
        try:
            db_path = os.path.join(self.data_dir, 'crawled_data.db')
            self.conn = sqlite3.connect(db_path, check_same_thread=False)  # 连接到sqlite数据库
            # check_same_thread=False允许多线程访问数据库
            cursor = self.conn.cursor()  # 数据库游标

            # 创建图片信息表"images"，存储图片的路径，下载状态等信息
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                url TEXT UNIQUE,
                local_path TEXT,
                crawl_time TIMESTAMP,
                status TEXT DEFAULT 'pending',
                tags TEXT
            )
            ''')

            # 创建搜索索引表
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_index (
                keyword TEXT,
                image_id INTEGER,
                FOREIGN KEY (image_id) REFERENCES images (id)
            )
            ''')

            self.conn.commit()  # 提交创建表的事务

        except Exception as e:
            print(f"数据库初始化失败: {str(e)}")
            raise

    def save_data(self, data):
        """保存数据"""
        try:
            cursor = self.conn.cursor()

            # 准备数据
            now = datetime.now()
            title = data.get('title', '未命名')  # 从data中获取图片标题，默认为“未命名”
            url = data.get('image_url', '')

            if not url:
                print("警告: 收到空URL，跳过保存")
                return

            print(f"准备保存图片: {title} - {url}")

            # 生成本地存储路径
            filename = f"{title}_{int(time.time())}.jpg"
            local_path = os.path.join(CRAWLER_CONFIG['download_path'], filename)

            # 存储图片信息到数据库，此时还没有开始爬虫下载图片
            cursor.execute('''
            INSERT OR REPLACE INTO images (title, url, local_path, crawl_time, status)
            VALUES (?, ?, ?, ?, ?)
            ''', (title, url, local_path, now, 'pending'))

            image_id = cursor.lastrowid

            # 处理关键词索引
            if title:
                keywords = set(title.split())  # 使用集合去重
                for keyword in keywords:
                    cursor.execute('''
                    INSERT OR IGNORE INTO search_index (keyword, image_id)
                    VALUES (?, ?)
                    ''', (keyword.lower(), image_id))

            self.conn.commit()
            print(f"✓ 数据保存成功: {title}")

            # 将URL添加到Redis的待处理队列
            self.redis_client.sadd(REDIS_KEYS['pending_urls'], url)

        except Exception as e:
            print(f"× 数据保存失败: {str(e)}")
            self.conn.rollback()

    def update_image_status(self, url, status):
        """更新图片状态"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            UPDATE images SET status = ? WHERE url = ?
            ''', (status, url))
            self.conn.commit()
            print(f"更新图片状态成功: {url} -> {status}")
        except Exception as e:
            print(f"更新图片状态失败: {str(e)}")
            self.conn.rollback()

    def get_statistics(self):
        """获取存储统计"""
        try:
            stats = {}

            # 从Redis获取统计信息
            stats['待下载URL数'] = self.redis_client.scard(REDIS_KEYS['pending_urls'])  # scard返回集合中元素的数量
            stats['已下载URL数'] = self.redis_client.scard(REDIS_KEYS['success_urls'])
            stats['下载失败数'] = self.redis_client.scard(REDIS_KEYS['failed_urls'])
            stats['图片标题数'] = self.redis_client.hlen(REDIS_KEYS['image_titles'])  # hlen返回哈希表中字段数量

            # 获取下载目录大小
            download_size = sum(
                os.path.getsize(os.path.join(CRAWLER_CONFIG['download_path'], f))
                for f in os.listdir(CRAWLER_CONFIG['download_path'])
                if os.path.isfile(os.path.join(CRAWLER_CONFIG['download_path'], f))
            )
            stats['下载目录大小'] = f"{download_size / 1024 / 1024:.2f} MB"

            return stats

        except Exception as e:
            print(f"获取统计信息失败: {str(e)}")
            return {}

    def search_data(self, keyword):
        """搜索图片数据"""
        try:
            results = []
            # 从图片标题中搜索
            all_titles = self.redis_client.hgetall(REDIS_KEYS['image_titles'])
            for url, title in all_titles.items():
                if keyword in title:
                    results.append({
                        'title': title,
                        'url': url,
                        'status': '已下载' if self.redis_client.sismember(REDIS_KEYS['success_urls'], url) else '待下载'
                    })

            print(f"\n找到 {len(results)} 个包含 '{keyword}' 的图片:")
            for item in results:
                print(f"- {item['title']} ({item['status']})")
                print(f"  URL: {item['url']}")

            return results
        except Exception as e:
            print(f"搜索失败: {str(e)}")
            return [] 
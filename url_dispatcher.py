# -*- coding: utf-8 -*-

import redis
import random
import time
from config import REDIS_CONFIG, REDIS_KEYS
from multiprocessing import Process

class URLDispatcher:
    def __init__(self):
        """初始化分发器"""
        self.redis_client = None
        
    def connect_redis(self):
        """连接Redis"""
        try:
            if not self.redis_client:
                self.redis_client = redis.Redis(**REDIS_CONFIG)
                # 测试连接
                self.redis_client.ping()
                print("Redis连接成功")
        except Exception as e:
            print(f"Redis连接失败: {str(e)}")
            raise
        
    def round_robin_dispatch(self, crawler_count):
        """轮询分发策略"""
        try:
            redis_client = redis.Redis(**REDIS_CONFIG)
            current_crawler = 0
            
            while True:  # 持续分发URL
                try:
                    # 获取待处理的URL
                    pending_url = redis_client.spop(REDIS_KEYS['pending_urls'])
                    if not pending_url:
                        time.sleep(1)
                        continue
                    
                    print(f"分发器: 从待处理队列获取URL: {pending_url}")
                    
                    # 轮询分发
                    redis_client.rpush(f'crawler:{current_crawler}:tasks', pending_url)  # 创建了一条新数据
                    print(f"分发器: 将URL {pending_url} 分发给爬虫 {current_crawler}")
                    
                    # 更新爬虫索引
                    current_crawler = (current_crawler + 1) % crawler_count
                    
                except Exception as e:
                    print(f"分发器: URL分发错误: {str(e)}")
                    time.sleep(1)
                    
        except Exception as e:
            print(f"分发器: 初始化失败: {str(e)}")
    
    def start_dispatch(self, crawler_count=3):
        """启动分发器"""
        try:
            print(f"启动URL分发器，爬虫数量: {crawler_count}")
            dispatch_process = Process(
                target=self.round_robin_dispatch,
                args=(crawler_count,)
            )
            dispatch_process.start()
            return dispatch_process
            
        except Exception as e:
            print(f"启动分发器失败: {str(e)}")
            raise 
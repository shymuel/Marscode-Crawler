# -*- coding: utf-8 -*-

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction
import redis
import time

class URLDispatcherFlink:
    def __init__(self, redis_host, redis_port, redis_db, pending_urls_key, crawler_tasks_key_prefix):
        """初始化基于 Flink 的 URL 分发器"""
        self.env = StreamExecutionEnvironment.get_execution_environment()  # flink的流处理环境
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.pending_urls_key = pending_urls_key  # 待爬取 URL 的 Redis key
        self.crawler_tasks_key_prefix = crawler_tasks_key_prefix  # 爬虫任务的 Redis key 前缀

    def round_robin_dispatch(self, crawler_count):
        """轮询分发策略"""
        try:
            # 连接到 Redis
            redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)

            # 从 Redis 待爬取列表读取 URL 流
            pending_urls = redis_client.lrange(self.pending_urls_key, 0, -1)
            # pending_urls = redis_client.spop(self.pending_urls_key)
            print("pending urls: ", pending_urls)
            pending_url_stream = self.env.from_collection(pending_urls)  # 从URL列表转换为flink数据流

            # 轮询分发 URL 到爬虫任务列表
            class DispatchProcessFunction(ProcessFunction):
                """用于处理URL分发逻辑"""
                def __init__(self, crawler_count, crawler_tasks_key_prefix, redis_host, redis_port, redis_db):
                    self.crawler_count = crawler_count
                    self.crawler_tasks_key_prefix = crawler_tasks_key_prefix
                    self.redis_host = redis_host
                    self.redis_port = redis_port
                    self.redis_db = redis_db
                    self.current_crawler = 0  # 当前轮询到的爬虫索引

                def process_element(self, url, context):
                    """处理每个URL的方法"""
                    if not url:
                        return
                    # 动态创建 Redis 连接
                    redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)

                    # 轮询分发
                    # task_key = f"{self.crawler_tasks_key_prefix}_{self.current_crawler}"  # 根据当前爬虫索引，生成对应任务列表的键名
                    # redis_client.rpush(task_key, url)  # 将 URL 写入 Redis
                    redis_client.rpush(f'crawler:{self.current_crawler}:tasks', url)  # 将 URL 写入 Redis
                    print(f"分发器: 将URL {url} 分发给爬虫 {self.current_crawler}")

                    # 更新爬虫索引
                    self.current_crawler = (self.current_crawler + 1) % self.crawler_count

                    # 关闭 Redis 连接
                    redis_client.close()

            # 分发 URL 到爬虫任务列表
            dispatched_stream = pending_url_stream.process(
                DispatchProcessFunction(crawler_count, self.crawler_tasks_key_prefix, self.redis_host, self.redis_port, self.redis_db)
            )

            # 启动 Flink 任务
            self.env.execute("URL Dispatcher Flink Job")
        except Exception as e:
            print(f"URL 分发失败: {str(e)}")

    def random_dispatch(self, crawler_count):
        """随机分发策略"""
        import random
        try:
            # 连接到 Redis
            redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)

            # 从 Redis 待爬取列表读取 URL 流
            pending_urls = redis_client.lrange(self.pending_urls_key, 0, -1)
            # pending_urls = redis_client.spop(self.pending_urls_key)
            print("pending urls: ", pending_urls)
            pending_url_stream = self.env.from_collection(pending_urls)  # 从URL列表转换为flink数据流

            # 随机分发 URL 到爬虫任务列表
            class RandomDispatchProcessFunction(ProcessFunction):
                def __init__(self, crawler_count, crawler_tasks_key_prefix, redis_host, redis_port, redis_db):
                    self.crawler_count = crawler_count
                    self.crawler_tasks_key_prefix = crawler_tasks_key_prefix
                    self.redis_host = redis_host
                    self.redis_port = redis_port
                    self.redis_db = redis_db

                def process_element(self, url, context):
                    """处理每个URL的方法"""
                    if not url:
                        return
                    # 动态创建 Redis 连接
                    redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)

                    # 随机选择爬虫
                    selected_crawler = random.randint(0, self.crawler_count - 1)
                    # task_key = f"{self.crawler_tasks_key_prefix}_{selected_crawler}"  # 每个爬虫有独立的任务列表
                    # redis_client.rpush(task_key, url)  # 将 URL 写入 Redis
                    redis_client.rpush(f'crawler:{selected_crawler}:tasks', url)  # 将 URL 写入 Redis
                    print(f"分发器: 将URL {url} 随机分发给爬虫 {selected_crawler}")

                    # 关闭 Redis 连接
                    redis_client.close()

            # 分发 URL 到爬虫任务列表
            dispatched_stream = pending_url_stream.process(
                RandomDispatchProcessFunction(crawler_count, self.crawler_tasks_key_prefix, self.redis_host,
                                              self.redis_port, self.redis_db)
            )

            # 启动 Flink 任务
            self.env.execute("URL Dispatcher Flink Job")
        except Exception as e:
            print(f"URL 分发失败: {str(e)}")

    def start_dispatch(self, crawler_count=3, dispatch_strategy="roundrobin"):
        """启动分发器"""
        try:
            print(f"启动 URL 分发器，爬虫数量: {crawler_count}")
            if dispatch_strategy == "roundrobin":
                self.round_robin_dispatch(crawler_count)
            elif dispatch_strategy == "random":
                self.random_dispatch(crawler_count)
        except Exception as e:
            print(f"启动分发器失败: {str(e)}")
            raise

if __name__ == "__main__":
    # 初始化 URL 分发器
    redis_host = "localhost"
    redis_port = 6379
    redis_db = 0
    pending_urls_key = "pending_urls"
    crawler_tasks_key_prefix = "crawler_tasks"

    dispatcher = URLDispatcherFlink(redis_host, redis_port, redis_db, pending_urls_key, crawler_tasks_key_prefix)

    # 启动 URL 分发器
    dispatcher.start_dispatch(crawler_count=3)
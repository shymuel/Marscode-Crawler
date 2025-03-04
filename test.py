# -*- coding: utf-8 -*-

import time
import multiprocessing
from multiprocessing import Queue, Process
from url_manager import URLManagerFlink  # Flink 版本的 URLManager
from url_dispatcher_flink import URLDispatcherFlink
from data_parser import DataParser
from crawler import Crawler
from monitor import Monitor
from storage import Storage
from config import (
    SEED_URLS, REDIS_CONFIG, REDIS_KEYS, CRAWLER_CONFIG
)
import redis  # 导入 Redis 客户端


def clear_redis_data(redis_config):
    """清空 Redis 数据"""
    try:
        # 初始化 Redis 客户端
        client = redis.Redis(**redis_config)

        # 清空当前数据库
        client.flushdb()
        print("已清空 Redis 数据库")
    except Exception as e:
        print(f"清空 Redis 数据时出错: {str(e)}")


def start_crawler(crawler_id, redis_config, parse_queue):
    """启动爬虫进程"""
    try:
        print(f"爬虫 {crawler_id} 启动...")
        crawler = Crawler(parse_queue)  # 在子进程中初始化 Crawler
        crawler.crawler_worker(crawler_id)  # 直接调用爬虫的工作方法
    except Exception as e:
        print(f"爬虫 {crawler_id} 运行出错: {str(e)}")


def start_parser(parse_queue, worker_count=2):
    """启动解析器进程"""
    try:
        print("启动解析器进程...")
        parser = DataParser(parse_queue)  # 初始化 DataParser
        parser_processes = parser.start_parser(worker_count)  # 启动多个解析进程
        return parser_processes
    except Exception as e:
        print(f"启动解析器进程失败: {str(e)}")
        raise


def main():
    start_time = time.time()
    print("=== 分布式爬虫系统启动 ===")

    # 创建共享队列，多进程都可访问该队列，交换数据
    parse_queue = Queue()

    # Redis 配置
    redis_config = REDIS_CONFIG  # 使用 config_flink.py 中的 REDIS_CONFIG

    # 0. 清空 Redis 数据
    print("正在清空 Redis 数据...")
    clear_redis_data(redis_config)

    # 1. 初始化各个模块
    url_manager = URLManagerFlink(redis_config)  # 基于 Flink 的 URL 管理
    dispatcher = URLDispatcherFlink(
        redis_config['host'], redis_config['port'], redis_config['db'],
        REDIS_KEYS['pending_urls'], REDIS_KEYS['crawler_tasks_prefix']
    )  # 基于 Flink 的 URL 分发
    monitor = Monitor()  # 监控系统状态
    storage = Storage()  # 数据存储

    try:
        # 2. 添加种子URL
        print("正在初始化种子URL...")
        url_manager.add_seed_urls_from_file("test_urls.txt")

        # 3. 启动数据解析器（先启动解析器）
        print("启动数据解析器...")
        parser_processes = start_parser(parse_queue, worker_count=2)  # 启动 2 个解析器进程

        # 4. 启动爬虫进程
        print("启动爬虫进程...")
        crawler_processes = []
        for i in range(3):  # 启动 3 个爬虫进程
            p = Process(target=start_crawler, args=(i, redis_config, parse_queue))
            p.start()
            crawler_processes.append(p)
            print(f"爬虫 {i} 已启动")

        # 5. 启动 URL 分发器
        print("启动 URL 分发器...")
        dispatcher.start_dispatch(crawler_count=3)

        # 6. 启动监控
        print("启动监控...")
        monitor_process = monitor.start_monitoring()

        # 主程序保持运行
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                end_time = time.time()
                # 计算并输出程序运行时间
                elapsed_time = end_time - start_time
                print(f"程序运行时间: {elapsed_time:.6f} 秒")
                print("\n正在停止所有进程...")
                # 停止爬虫
                for p in crawler_processes:
                    p.terminate()
                # 停止解析器
                for p in parser_processes:
                    p.terminate()
                # 停止监控
                monitor_process.terminate()
                print("系统已停止")
                break

    except Exception as e:
        print(f"系统运行出错: {str(e)}")
        # 确保所有进程都被终止
        try:
            for p in crawler_processes:
                p.terminate()
            for p in parser_processes:
                p.terminate()
            monitor_process.terminate()
        except:
            pass
        raise


if __name__ == '__main__':
    multiprocessing.freeze_support()  # 确保 Windows 上的多进程代码正常运行
    main()
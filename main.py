# -*- coding: utf-8 -*-

import time
import multiprocessing
from multiprocessing import Queue
from url_manager import URLManager
from url_dispatcher import URLDispatcher
from data_parser import DataParser
from crawler import Crawler
from monitor import Monitor
from storage import Storage
from config import SEED_URLS


def main():
    print("=== 分布式爬虫系统启动 ===")

    # 创建共享队列，多进程都可访问该队列，交换数据
    parse_queue = Queue()

    # 1. 初始化各个模块
    url_manager = URLManager()  # URL管理
    dispatcher = URLDispatcher()  # URL分发
    parser = DataParser(parse_queue)  # 数据解析，传入共享队列
    crawler = Crawler(parse_queue)  # 数据爬取，传入共享队列
    monitor = Monitor()  # 监控系统状态
    storage = Storage()  # 数据存储——这个好像还没完成？

    try:
        # 2. 添加种子URL
        print("正在初始化种子URL...")
        url_manager.add_seed_urls(SEED_URLS)

        # 3. 启动数据解析器（先启动解析器）
        print("启动数据解析器...")
        parser_processes = parser.start_parser(worker_count=2)
        time.sleep(1)  # 等待解析器完全启动

        # 4. 启动爬虫进程
        print("启动爬虫进程...")
        crawler_processes = crawler.start_crawlers(count=3)
        time.sleep(1)  # 等待爬虫完全启动

        # 5. 启动URL分发器
        print("启动URL分发器...")
        dispatch_process = dispatcher.start_dispatch(crawler_count=3)
        time.sleep(1)  # 等待分发器完全启动

        # 6. 启动监控
        print("启动监控...")
        monitor_process = monitor.start_monitoring()

        # 主程序保持运行
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                print("\n正在停止所有进程...")
                # 先停止分发器
                dispatch_process.terminate()
                # 然后停止爬虫
                for p in crawler_processes:
                    p.terminate()
                # 最后停止解析器
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
            dispatch_process.terminate()
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
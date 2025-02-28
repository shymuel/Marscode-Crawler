# -*- coding: utf-8 -*-

import redis
import time
import json
import psutil
from datetime import datetime
from config import REDIS_CONFIG, REDIS_KEYS
from multiprocessing import Process


class Monitor:
    def __init__(self):
        """初始化监控"""
        self.running = True

    def get_crawler_status(self, redis_client):
        """获取所有爬虫状态"""
        try:
            status = redis_client.hgetall(REDIS_KEYS['crawler_status'])  # 获取哈希表中所有的键和值
            result = {}
            for crawler_id, status_json in status.items():  # 遍历所有爬虫状态
                status_data = json.loads(status_json)
                last_update = datetime.fromtimestamp(status_data['last_update'])  # 爬虫最后更新时间
                idle_time = (datetime.now() - last_update).seconds  # 爬虫空闲时间

                result[crawler_id] = {
                    'status': status_data['status'],
                    'last_update': last_update.strftime('%Y-%m-%d %H:%M:%S'),
                    'idle_time': f"{idle_time}秒",
                    'warning': idle_time > 300  # 5分钟无响应标记为警告
                }
            return result  # 每个爬虫的状态信息
        except Exception as e:
            print(f"获取爬虫状态失败: {str(e)}")
            return {}

    def get_system_status(self):
        """获取系统状态"""
        try:
            return {
                'CPU使用率': f"{psutil.cpu_percent()}%",
                '内存使用率': f"{psutil.virtual_memory().percent}%",
                '磁盘使用率': f"{psutil.disk_usage('/').percent}%"
            }
        except Exception as e:
            print(f"获取系统状态失败: {str(e)}")
            return {}

    def get_statistics(self, redis_client):
        """获取统计信息"""
        try:
            return {
                '待爬取URL数': redis_client.scard(REDIS_KEYS['pending_urls']),
                '已成功URL数': redis_client.scard(REDIS_KEYS['success_urls']),
                '失败URL数': redis_client.scard(REDIS_KEYS['failed_urls']),
                '解析数据数': redis_client.llen(REDIS_KEYS['parsed_data'])
            }
        except Exception as e:
            print(f"获取统计信息失败: {str(e)}")
            return {}

    def monitor_worker(self):
        """监控工作进程"""
        print("监控进程启动")

        # 在进程内部创建 Redis 连接
        redis_client = redis.Redis(**REDIS_CONFIG)
        print("Monitor: Redis连接成功（工作进程）")

        while self.running:
            try:
                print("\n" + "=" * 50)
                print(f"爬虫监控状态 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 50)

                # 显示系统状态
                print("\n系统状态:")
                for key, value in self.get_system_status().items():
                    print(f"  - {key}: {value}")

                # 显示爬虫状态
                print("\n爬虫状态:")
                status_dict = self.get_crawler_status(redis_client)
                if status_dict:
                    for crawler_id, status in status_dict.items():
                        print(f"爬虫 {crawler_id}:")
                        print(f"  - 状态: {status['status']}")
                        print(f"  - 最后更新: {status['last_update']}")
                        print(f"  - 空闲时间: {status['idle_time']}")
                        if status['warning']:
                            print("  ⚠️ 警告: 爬虫可能已停止响应")
                else:
                    print("暂无爬虫状态信息")

                # 显示统计信息
                print("\n任务统计:")
                for key, value in self.get_statistics(redis_client).items():
                    print(f"  - {key}: {value}")

                time.sleep(5)  # 每5s监控一次

            except Exception as e:
                print(f"监控循环发生错误: {str(e)}")
                time.sleep(5)

    def start_monitoring(self):
        """启动监控"""
        try:
            print("启动监控进程...")
            monitor_process = Process(target=self.monitor_worker)
            monitor_process.start()  # 监控进程只需要一个
            return monitor_process
        except Exception as e:
            print(f"启动监控失败: {str(e)}")
            raise

    def stop_monitoring(self):
        """停止监控"""
        self.running = False 
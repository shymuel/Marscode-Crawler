# -*- coding: utf-8 -*-
# 动态URL分发

import redis
from config import REDIS_CONFIG, REDIS_KEYS
import requests
from fake_useragent import UserAgent
import time

def generate_urls_by_keywords():
    """通过关键词生成URL"""
    redis_client = redis.Redis(**REDIS_CONFIG)
    ua = UserAgent()
    
    # 动物关键词列表（使用拼音，站长之家的URL格式）
    keywords = [
        'xiaogouxiaomaotupian',  # 小狗小猫图片
        'laohutupian',  # 老虎图片
        'madetupian',  # 马的图片
    ]
    
    base_url = 'https://sc.chinaz.com/tupian/'
    
    print("=== 开始生成新的URL ===")
    total_added = 0
    
    for keyword in keywords:
        print(f"\n正在处理关键词: {keyword}")
        
        # 处理多个页面
        for page in range(1, 3):  # 每个关键词处理2页
            try:
                # 构建搜索URL
                if page == 1:
                    url = f"{base_url}{keyword}.html"
                else:
                    url = f"{base_url}{keyword}_{page}.html"
                
                # 检查URL是否已经处理过
                if redis_client.sismember(REDIS_KEYS['success_urls'], url):
                    print(f"URL已存在，跳过: {url}")
                    continue
                
                print(f"添加新URL: {url}")
                redis_client.sadd(REDIS_KEYS['pending_urls'], url)
                total_added += 1
                
                # 避免请求太快
                time.sleep(1)
                
            except Exception as e:
                print(f"处理URL时出错: {str(e)}")
                continue
    
    print(f"\n=== URL生成完成 ===")
    print(f"- 新增URL数: {total_added}")
    print(f"- 待处理URL总数: {redis_client.scard(REDIS_KEYS['pending_urls'])}")
    print(f"- 已处理URL总数: {redis_client.scard(REDIS_KEYS['success_urls'])}")

if __name__ == '__main__':
    generate_urls_by_keywords() 
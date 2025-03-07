# -*- coding: utf-8 -*-
import os
from storage import Storage
from config import CRAWLER_CONFIG

def main():
    storage = Storage()
    
    print("\n=== 系统状态检查 ===")
    
    # 1. 查看统计信息
    print("\n系统统计:")
    stats = storage.get_statistics()
    for key, value in stats.items():
        print(f"- {key}: {value}")
    
    # 2. 查看下载目录中的文件
    print("\n下载目录中的文件:")
    download_path = CRAWLER_CONFIG['download_path']
    total_files = 0
    animal_names = ['老虎', '马', '狗', '猫']
    
    for animal in animal_names:
        count = 0
        print(f"\n{animal}类图片:")
        for filename in os.listdir(download_path):
            if animal in filename:
                print(f"- {filename}")
                count += 1
                total_files += 1
        if count == 0:
            print(f"（暂无{animal}类图片）")
        else:
            print(f"共 {count} 张{animal}类图片")
    
    print(f"\n总计: {total_files} 张符合关键词的图片")

if __name__ == '__main__':
    main() 
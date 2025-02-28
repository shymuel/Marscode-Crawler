# 分布式美食图片爬虫系统

一个基于Redis的分布式图片爬虫系统，专门用于收集美食图片。

## 系统特点

### 1. 分布式架构
- 使用Redis作为中心化存储
- 多进程并行下载
- 可扩展性强

### 2. 双重URL分发
- 静态配置分发：通过config.py配置种子URL
- 动态关键词分发：通过url_generator.py生成新URL

### 3. 智能处理
- 中文图片命名
- 自动去重
- 数据清洗

### 4. 完善的监控
- 实时状态显示
- 资源使用监控
- 便捷的查询工具

## 安装和使用

### 1. 安装依赖
bash
pip install redis requests lxml fake-useragent

### 2. 启动Redis服务
确保Redis服务在本地运行，默认端口6379

### 3. 运行系统
bash
启动主系统
python main.py
生成新的URL（可选）
python url_generator.py
查看系统状态
python check_status.py

## 项目结构
bash
├── main.py # 主程序入口，协调所有组件
├── config.py # 配置文件，包含Redis、URL和爬虫设置
├── crawler.py # 爬虫核心组件，负责下载图片
├── data_parser.py # 数据解析器，处理网页内容
├── storage.py # 存储管理，处理Redis数据存储
├── url_generator.py # URL生成器，第二种URL分发方式
├── check_status.py # 系统状态检查工具
├── monitor.py # 系统监控组件
├── downloaded_images/ # 下载的图片存储目录
└── crawled_data/ # 爬虫数据存储目录

## 系统架构
[URL生成器] --> [Redis队列] --> [爬虫进程1]
--> [爬虫进程2] --> [图片存储]
--> [爬虫进程3]
[监控系统] --> [状态查看]

## 注意事项
- 确保Redis服务正常运行
- 适当调整并发数（在config.py中设置）
- 遵守网站robots.txt规则
- 下载的图片将保存在downloaded_images目录

## 技术栈
- Python 3.x
- Redis
- multiprocessing
- requests
- lxml
- fake-useragent
>>>>>>> 2f38f32 (version0.1)

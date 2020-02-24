# microspider
基于python标准库asyncio实现的微型爬虫框架

用法：

```
#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@File:       demo.py
@Time:       2020/02/13 22:58
@Author:     ly
@Email:      ly_rs90@qq.com
@Desc:       爬虫示例
"""
import re
from microspider.spider.spider import Spider


# 提取url的re匹配模式
pattern = re.compile(r'<a href="(.*?)"')


class DemoSpider(Spider):
    """示例爬虫"""
    
    # 最大并发请求数（默认为20）
    MAX_WORKER = 20

    # 单个域名最大并发请求数（默认为5）
    WORKER_DOMAIN = 5

    # 允许的域名
    ALLOWED_DOMAIN = ['abc.com', 'abd.com']

    async def handle_document(self, response):
        """唯一一个需要重写的方法
           该方法在文档成功获取后调用，
           重写该方法实现自定义操作，如继续爬取新的链接等。
           response为web.response.Response对象
        """
        
        # 解析url
        urls = re.findall(pattern, response.text)
        for url in urls:
            # 拼接获得绝对url并添加到任务队列
            await self.add_task(response.url_join(url))

demo_spider = DemoSpider()
demo_spider.start('www.abc.com')

```
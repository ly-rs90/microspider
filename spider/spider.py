#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@File:       spider.py
@Time:       2020/01/28 20:39
@Author:     ly
@Email:      ly_rs90@qq.com
@Desc:       基于python标准库asyncio实现的异步爬虫
"""
import hashlib
import time
import datetime
import asyncio
import ssl
import gzip
import zlib
import random
from urllib.parse import urlsplit
from asyncio.queues import Queue
from ..log.logger import Logger
from ..web.url import URL
from ..web.response import Response

__all__ = ['Spider']


class Spider:
    """web爬虫"""

    # 总并发请求数
    MAX_REQUEST_TOTAL = 20

    # 单个域名并发请求数
    MAX_REQUEST_DOMAIN = 5

    # 允许的域名
    ALLOWED_DOMAIN = []

    def __init__(self):
        # 具体任务队列，每个key(域名)对应一个队列
        self._task = dict()

        # 每个域名对应的空位
        self._free_domain = dict()

        # 总的空闲队列
        self._free_queue = None

        # 已经处理的url
        self._handled_urls = set()

        # 当前正在处理的url
        self._handling = []

        # 正在运行的任务
        self._running_task = dict()

        # 时间基准
        self._base_time = time.time()

        # 完成的任务数
        self._completed = 0

        # 任务完成事件
        self._task_done = None
    
    async def handle_document(self, response):
        """处理http响应
        重写该方法以实现自定义操作，如继续爬取相关链接
        示例：
        urls = re.findall(response.text)
        self.add_task(*urls)      # 继续爬取新的链接
        """
        pass

    async def _init(self):
        """初始化"""
        loop = asyncio.get_running_loop()
        self._free_queue = Queue(self.MAX_REQUEST_TOTAL)
        self._task_done = loop.create_future()

        # 初始化空闲队列
        for _ in range(self.MAX_REQUEST_TOTAL):
            await self._free_queue.put(1)
        
        asyncio.create_task(self._record())

    async def _task_schedule(self, *url):

        try:
            await self._init()
            await self.add_task(*url)
            await self._task_done

            avg = self._completed / (time.time() - self._base_time) * 60
            Logger.log(f'任务完成，共爬取资源：{self._completed}，平均速度：{round(avg, 2)}/min。耗时：{round(time.time()-self._base_time, 3)}s.')
            return True
        except:
            return False
    
    async def _schedule(self, task_queue, position_queue):
        """执行任务调度"""

        while True:
            url = await task_queue.get()
            self._handling.append(url)
            await position_queue.get()
            await self._free_queue.get()
            task = asyncio.create_task(self._crawl_document(url))
            self._running_task[url] = task
            self._free_queue.task_done()
            position_queue.task_done()
            task_queue.task_done()

    def _gzip_decompress(self, data):
        """gzip解压缩"""

        try:
            return gzip.decompress(data)
        except:
            return None
    
    def _deflate_decompress(self, data):
        """deflate解压"""

        try:
            return zlib.decompress(data, -zlib.MAX_WBITS)
        except:
            return None

    def _check_domain(self, url):
        """检查域名是否在允许的列表内"""

        if not self.ALLOWED_DOMAIN:
            return True

        for domain in self.ALLOWED_DOMAIN:
            host = URL(url).host or ''
            if domain in host:
                return True
        return False

    async def _get_connector(self, url):
        """连接web主机"""

        u = URL(url)
        data = None

        # 不验证网站签名
        if u.scheme == 'https':
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            data = ctx
            
        try:
            return await asyncio.open_connection(u.host, u.port, ssl=data)
        except:
            return None, None
    
    async def add_task(self, *urls):
        """添加新任务"""
        
        # 筛选url添加到任务队列
        for url in urls:
            if self._check_domain(url):
                hash_value = URL.md5_value(url)
                if hash_value not in self._handled_urls:
                    self._handled_urls.add(hash_value)
                    u = URL(url)
                    if self._free_domain.get(u.host):
                        await self._task[u.host].put(url)
                    else:
                        self._free_domain[u.host] = Queue(self.MAX_REQUEST_DOMAIN)
                        for _ in range(self.MAX_REQUEST_DOMAIN):
                            await self._free_domain[u.host].put(1)

                        self._task[u.host] = Queue()
                        await self._task[u.host].put(url)
                        asyncio.create_task(self._schedule(self._task[u.host], self._free_domain[u.host]))
    
    def _get_header(self, url, **param):
        u = URL(url)

        query_user_gen = (f'{key}={value}' for key, value in param.items())
        query = u.query + '&'.join(query_user_gen)
        if query:
            query += '?'

        h = (
            f'GET {u.path + "" if not query else query} HTTP/1.1\r\n'
            f'Host: {u.host}\r\n'
            'Connection: keep-alive\r\n'
            'Pragma: no-cache\r\n'
            'Cache-Control: no-cache\r\n'
            'Upgrade-Insecure-Requests: 1\r\n'
            'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n'
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9\r\n'
            'Accept-Language: zh-CN,zh;q=0.9\r\n'
            'Accept-Encoding: gzip, deflate\r\n'
            '\r\n'
        )

        return h
    
    async def _record(self):
        """状态报告"""

        while True:
            await asyncio.sleep(60)

            now = time.time()
            task_num = 0
            for key, v in self._task.items():
                task_num += v.qsize()
            Logger.log(f'当前任务量：{task_num}。成功爬取文档：{self._completed}，平均速度：{round(self._completed/(now-self._base_time)*60, 2)}/min')
    
    async def _read_chunk_data(self, reader):
        """读取分块传输的内容"""

        content = b''

        try:
            chunk_line = await reader.readline()
            while chunk_line != b'0\r\n':
                chunk_len = int(chunk_line[:-2], 16)
                chunk = await reader.readexactly(chunk_len + 2)
                content += chunk[:-2]
                chunk_line = await reader.readline()
        except Exception as e:
            self._log(f'获取分块传输的内容失败：{e}')

        return content
    
    async def _check_task_done(self):
        """检查任务是否完成"""

        if self._free_queue.qsize() == self.MAX_REQUEST_TOTAL:
            for _, value in self._task.items():
                if value.empty():
                    continue
                else:
                    break
            else:
                self._task_done.set_result(1)
    
    async def _crawl_document(self, url):
        """爬取文档"""

        u = URL(url)
        reader, writer = await self._get_connector(url)
        if not reader or not writer:
            Logger.log(f'GET {url}失败，无法连接主机！')
            await self._free_domain[u.host].put(1)
            await self._free_queue.put(1)
            # 检查任务是否完成
            await self._check_task_done()
            try:
                self._handling.remove(url)
                self._running_task.pop(url)
            except:
                pass
            return

        try:
            writer.write(self._get_header(url).encode())
            await writer.drain()

            response_header = await reader.readuntil(b'\r\n\r\n')
            if not response_header:
                Logger.log(f'GET {url}失败，连接已断开！')
                await self._free_domain[u.host].put(1)
                await self._free_queue.put(1)
                writer.close()
                await writer.wait_closed()
                # 检查任务是否完成
                await self._check_task_done()
                try:
                    self._handling.remove(url)
                    self._running_task.pop(url)
                except:
                    pass
                return

            response = Response(url, response_header)
            # 传输编码
            transfer_encoding = response.get('Transfer-Encoding', '')
            if transfer_encoding == 'chunked':
                body = await self._read_chunk_data(reader)
            else:
                content_length = int(response.get('Content-Length', '0'))
                body = await reader.readexactly(content_length)
            
            # 内容编码
            content_encoding = response.get('Content-Encoding', '')
            if content_encoding == 'gzip':
                body = self._gzip_decompress(body)
            elif content_encoding == 'deflate':
                body = self._deflate_decompress(body)

            response.set_body(body)
            Logger.log(f'({response.code}) GET {url}')
            self._completed += 1
            await self.handle_document(response)
        except Exception as e:
            Logger.log(f'未知错误：{e}')
        finally:
            await self._free_domain[u.host].put(1)
            await self._free_queue.put(1)
            writer.close()
            await writer.wait_closed()
            # 检查任务是否完成
            await self._check_task_done()
            try:
                self._handling.remove(url)
                self._running_task.pop(url)
            except:
                pass
    
    def start(self, *urls):
        """开始爬取"""
        
        asyncio.run(self._task_schedule(*urls))

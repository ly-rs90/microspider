#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@File:       spider.py
@Time:       2020/01/28 20:39
@Author:     ly
@Email:      ly_rs90@qq.com
@Desc:       基于python标准库asyncio实现的异步爬虫
"""
import time
import asyncio
import ssl
import gzip
import zlib
from asyncio.queues import Queue
from ..log.logger import Logger
from ..web.url import URL
from ..web.response import Response
from ..web.agent import UserAgent

__all__ = ['Spider']


class Spider:
    """web爬虫"""

    # 总工作者数量
    MAX_WORKER = 20

    # 单个域名最大工作者数量
    WORKER_DOMAIN = 5

    # 允许的域名
    ALLOWED_DOMAIN = []

    def __init__(self):
        # 具体任务队列，每个key(域名)对应一个队列
        self._url_queue = dict()

        # 总的工位，每一个worker工作时需要先获取一个工位
        self._work_position = None

        # 每个域名对应的工作者
        self._worker = dict()

        # 已经处理的url
        self._handled_urls = set()

        # 正在运行的爬取任务
        self._running_task = None

        # 时间基准
        self._base_time = time.time()

        # 完成的任务数
        self._completed = 0

        # 任务开始标志
        self._start = None
    
    async def add_task(self, *urls):
        """添加新任务"""
        
        for url in urls:
            # 域名不在允许列表内
            if not self._check_domain(url):
                continue

            hash_value = URL.md5_value(url)
            # 重复url
            if hash_value in self._handled_urls:
                continue

            self._handled_urls.add(hash_value)
            u = URL(url)
            if self._url_queue.get(u.host):
                await self._url_queue[u.host].put(url)
            else:
                # 为新的域名创建url队列
                self._url_queue[u.host] = Queue()
                await self._url_queue[u.host].put(url)

                # 初始化工作者
                self._worker[u.host] = asyncio.Semaphore(self.WORKER_DOMAIN)
                # 为新的域名创建一个调度任务
                asyncio.create_task(self._task_monitor(self._url_queue[u.host], self._worker[u.host]))
    
    async def handle_document(self, response):
        """处理http响应
        重写该方法以实现自定义操作，如继续爬取相关链接
        示例：
        urls = re.findall(response.text)
        await self.add_task(*urls)      # 继续爬取新的链接
        """
        pass
    
    def _check_domain(self, url):
        """检查域名是否在允许的列表内"""

        if not self.ALLOWED_DOMAIN:
            return True

        for domain in self.ALLOWED_DOMAIN:
            host = URL(url).host or ''
            if domain in host:
                return True
        return False

    async def _commit_task(self):
        """确认运行的任务"""
        
        while True:
            task = await self._running_task.get()

            # 等待任务完成
            if not task.done():
                await task
            
            self._running_task.task_done()

    @staticmethod
    def _deflate_decompress(data):
        """deflate解压"""

        try:
            return zlib.decompress(data, -zlib.MAX_WBITS)
        except Exception as e:
            Logger.log(f'deflate解压出错，详情：{e}')
            return None

    @staticmethod
    async def _get_connector(url):
        """获取连接到web主机的传输对象"""

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
        except Exception as e:
            Logger.log(f'connect error) GET {url}\n详情：{e}')
            return None, None

    @staticmethod
    def _get_request_header(url, method='GET'):
        """根据url生成对应的HTTP请求头"""

        u = URL(url)

        query = u.query
        if query:
            query += '?'

        h = (
            f'{method} {u.path + "" if not query else query} HTTP/1.1\r\n'
            f'Host: {u.host}\r\n'
            'Connection: keep-alive\r\n'
            'Pragma: no-cache\r\n'
            'Cache-Control: no-cache\r\n'
            'Upgrade-Insecure-Requests: 1\r\n'
            f'User-Agent: {UserAgent.get_agent()}\r\n'
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,'
            'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9\r\n'
            'Accept-Language: zh-CN,zh;q=0.9\r\n'
            'Accept-Encoding: gzip, deflate\r\n'
            '\r\n'
        )

        return h

    @staticmethod
    async def _get_response_header(reader, url):
        """获取HTTP响应头"""

        try:
            header = await reader.readuntil(b'\r\n\r\n')
            return header
        except Exception as e:
            Logger.log(f'(read header error) GET {url}\n详情：{e}')
            return b''

    @staticmethod
    async def _get_chunk_data(reader, url):
        """获取分块传输的内容"""

        content = b''

        try:
            chunk_line = await reader.readline()
            while chunk_line != b'0\r\n':
                chunk_len = int(chunk_line[:-2], 16)
                chunk = await reader.readexactly(chunk_len + 2)
                content += chunk[:-2]
                chunk_line = await reader.readline()
        except Exception as e:
            content = b''
            Logger.log(f'(read chunk body error) GET {url}\n详情：{e}')

        return content

    @staticmethod
    async def _get_body(reader, length, url):
        """获取length长度的响应体"""

        try:
            data = await reader.readexactly(length)
            return data
        except Exception as e:
            Logger.log(f'(read body error) GET {url}\n详情：{e}')
            return b''

    @staticmethod
    def _gzip_decompress(data):
        """gzip解压"""

        try:
            return gzip.decompress(data)
        except Exception as e:
            Logger.log(f'gzip解压出错，详情：{e}')
            return None
    
    async def _record(self):
        """状态报告"""

        while True:
            await asyncio.sleep(60)

            now = time.time()
            task_num = 0
            for key, v in self._url_queue.items():
                task_num += v.qsize()
            Logger.log(f'已完成：{self._completed}，剩余任务：{task_num}，'
                       f'平均速度：{round(self._completed/(now-self._base_time)*60, 2)}/min')
    
    async def _send_request_header(self, writer, url):
        """发送HTTP请求头"""

        try:
            header = self._get_request_header(url)
            writer.write(header.encode())
            await writer.drain()
            return True
        except Exception as e:
            Logger.log(f'(send header error) {url}\n详情：{e}')
            return False
    
    def _task_done(self, url):
        """对应的url处理完成"""

        u = URL(url)
        # 释放工作者
        self._worker[u.host].release()
        # 释放工位
        self._work_position.release()

    async def _init(self):
        """初始化"""
        loop = asyncio.get_running_loop()
        self._work_position = asyncio.Semaphore(self.MAX_WORKER)
        self._running_task = Queue()
        self._start = loop.create_future()
        
        # 创建报告任务，每隔1分钟报告一次当前爬取状态
        asyncio.create_task(self._record())
        # 创建任务检查
        asyncio.create_task(self._commit_task())

    async def _run(self, *url):
        """开始任务，直到任务完成"""

        try:
            await self._init()
            await self.add_task(*url)
            await self._start
            await self._running_task.join()

            avg = self._completed / (time.time() - self._base_time) * 60
            Logger.log(f'任务完成，共获取资源：{self._completed}，平均速度：{round(avg, 2)}/min，'
                       f'耗时：{round(time.time()-self._base_time, 3)}s.')
            return True
        except Exception as e:
            Logger.log(f'运行任务失败，详情：{e}')
            return False
    
    async def _task_monitor(self, url_queue, worker):
        """执行任务调度"""

        while True:
            url = await url_queue.get()
            # 等待工作者
            await worker.acquire()
            # 等待工位
            await self._work_position.acquire()
            # 创建爬取任务
            task = asyncio.create_task(self._download_document(url))
            # 将任务加到运行队列
            await self._running_task.put(task)

            # 任务添加到运行队列后设置开始标志
            if not self._start.done():
                self._start.set_result(1)
    
    async def _download_document(self, url):
        """下载文档"""

        reader, writer = await self._get_connector(url)
        if not reader or not writer:
            self._task_done(url)
            return

        flag = await self._send_request_header(writer, url)
        if not flag:
            self._task_done(url)
            writer.close()
            await writer.wait_closed()
            return
        
        response_header = await self._get_response_header(reader, url)
        if not response_header:
            self._task_done(url)
            writer.close()
            await writer.wait_closed()
            return
        
        response = Response(url, response_header)
        # 传输编码
        transfer_encoding = response.get('Transfer-Encoding', '')
        if transfer_encoding == 'chunked':
            body = await self._get_chunk_data(reader, url)
        else:
            content_length = int(response.get('Content-Length', '0'))
            body = await self._get_body(reader, content_length, url)
        
        # 数据读取完成，关闭连接
        writer.close()
        await writer.wait_closed()
        
        if not body:
            self._task_done(url)
            return
        
        # 内容编码
        content_encoding = response.get('Content-Encoding', '')
        if content_encoding == 'gzip':
            body = self._gzip_decompress(body)
        elif content_encoding == 'deflate':
            body = self._deflate_decompress(body)
        
        response.set_body(body)
        Logger.log(f'({response.code}) GET {url}')
        self._completed += 1

        try:
            await self.handle_document(response)
        except Exception as e:
            Logger.log(f'处理响应出现错误：{e}')
        
        self._task_done(url)
    
    def start(self, *urls):
        """开始爬取"""
        
        asyncio.run(self._run(*urls))

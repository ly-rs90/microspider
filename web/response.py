#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@File:       response.py
@Time:       2020/02/08 16:16
@Author:     ly
@Email:      ly_rs90@qq.com
@Desc:       HTTP响应封装
"""
import re
from .url import URL
from ..log.logger import Logger

class Response:
    """代表一个HTTP响应
       一个合法的HTTP响应应包含一个HTTP响应头
       以及一个可能的响应体，取决于具体的情况
       例如：一个HEAD请求通常只有响应头不会有
       响应体，而一个GET请求通常含有响应体
    """

    def __init__(self, url, header, body=b''):
        self._url = url
        self._header = dict()
        self._body = body
        # 响应状态码
        self._code = None
        # 状态信息描述
        self._info = ''
        # 从响应头中提取编码的正则表达式
        self._encode_pattern = re.compile(r'charset=(.*?)[;\s]')
        
        self._init_header(header)

    @property
    def code(self):
        """HTTP响应码"""
        return self._code

    @property
    def info(self):
        """HTTP响应描述"""

        return self._info

    @property
    def body(self):
        """HTTP响应体 原始二进制形式"""
        
        return self._body
    
    @property
    def text(self):
        """HTTP响应体 文本形式
           尝试用响应头指定的编码格式解码，如果响应头中不存在
           编码格式，则使用UTF-8, 解码成功返回解码后的文本，失败
           返回空字符串
        """

        try:
            encoding = self.encoding if self.encoding else 'UTF-8'
            text = self._body.decode(encoding=encoding, errors='ignore')
            return text
        except:
            return ''

    @property
    def url(self):
        """URL地址"""

        return self._url
    
    @property
    def encoding(self):
        """HTTP响应编码
           没有发现则返回None
        """

        encoding = None

        content_type = self.get('Content-Type')
        if content_type:
            r = re.findall(self._encode_pattern, content_type)
            if r:
                encoding = r[0]

        return encoding
    
    def _init_header(self, header):
        """处理HTTP响应头
           提取出状态码，描述信息，响应行数据
        """

        try:
            # 如果header是二进制形式，先解码
            if isinstance(header, bytes):
                header = header.decode(errors='ignore')
            
            if not header.startswith('HTTP') or not header.endswith('\r\n\r\n'):
                raise Exception(f'非标准HTTP响应格式：{header}')
            
            lines = header.split('\r\n')

            # 提取状态码和描述信息
            line1 = lines[0].split()
            self._code = int(line1[1])
            self._info = ' '.join(line1[2:])

            # 将每个响应行存入字典
            for line in lines[1:]:
                items = line.split(':')
                self._header[items[0]] = ':'.join(items[1:]).strip()
        except Exception as e:
            Logger.log(f'解析HTTP响应头失败，详情：{e}')
    
    def get(self, key, default=None):
        """获取响应字段值
           没有找到时返回default的值
        """

        return self._header.get(key, default)

    def set_body(self, body):
        """设置body"""

        if isinstance(body, bytes):
            self._body = body

    def url_join(self, relative_url):
        """生成绝对URL"""

        return URL.url_join(self._url, relative_url)

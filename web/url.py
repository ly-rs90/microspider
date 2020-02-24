#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@File:       url.py
@Time:       2020/02/08 16:16
@Author:     ly
@Email:      ly_rs90@qq.com
@Desc:       URL解析
"""
import hashlib
from urllib.parse import urlsplit, urljoin


class URL:
    """URL解析类"""

    def __init__(self, url):
        self._url = urlsplit(url)
    
    @property
    def scheme(self):
        """HTTP协议类型"""

        return self._url.scheme or 'http'

    @property
    def host(self):
        """主机名"""

        return self._url.hostname
    
    @property
    def port(self):
        """端口"""

        port = self._url.port
        return port if port else 443 if self.scheme == 'https' else 80
    
    @property
    def path(self):
        """请求路径"""

        return self._url.path or '/'
    
    @property
    def query(self):
        """查询参数"""

        return self._url.query
    
    @staticmethod
    def md5_value(url):
        """计算url的md5值"""

        m = hashlib.md5()
        m.update(url.encode('utf-8'))
        return m.hexdigest()

    @staticmethod
    def url_join(base_url, relative_url):
        """获取绝对URL"""

        return urljoin(base_url, relative_url)

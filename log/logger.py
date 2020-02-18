#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@File:       logger.py
@Time:       2020/02/09 14:59
@Author:     ly
@Email:      ly_rs90@qq.com
@Desc:       日志打印
"""
import time
import datetime


class Logger:
    """日志打印封装类"""

    @staticmethod
    def log(msg):
        """打印日志到标准输出"""

        now = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        print(f'[{now}] {msg}')
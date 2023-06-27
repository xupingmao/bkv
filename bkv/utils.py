# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 16:09:35
LastEditors: xupingmao
LastEditTime: 2023-06-26 00:01:28
FilePath: /bkv/bkv/utils.py
Description: 描述
'''

import os
import time

def memory_info():
    """获取内存信息"""
    import psutil
    p = psutil.Process(pid=os.getpid())
    return p.memory_info()


class QpsCounter:

    def __init__(self):
        self.start_time = time.time()
        self.count = 0

    def add_count(self, count):
        self.count += count

    def set_count(self, count):
        self.count = count

    def qps(self):
        cost_time = time.time() - self.start_time
        if cost_time == 0:
            return 0
        return self.count / cost_time
    
    def cost_time(self):
        return time.time() - self.start_time
# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 16:09:35
LastEditors: xupingmao
LastEditTime: 2023-06-22 16:16:18
FilePath: /bkv/bkv/utils.py
Description: 描述
'''

import os

def memory_info():
    """获取内存信息"""
    import psutil
    p = psutil.Process(pid=os.getpid())
    return p.memory_info()


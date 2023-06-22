# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 12:58:30
LastEditors: xupingmao
LastEditTime: 2023-06-22 13:04:50
FilePath: \bkv\scipts\run-shell.py
Description: 描述
'''
import sys
import os

def get_project_root():
    scripts_dir = os.path.dirname(__file__)
    return os.path.dirname(scripts_dir)

sys.path.append(get_project_root())
from bkv.shell import Shell


if __name__ == "__main__":
    shell = Shell()
    shell.loop()

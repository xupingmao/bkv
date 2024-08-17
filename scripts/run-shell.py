# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 12:58:30
LastEditors: xupingmao
LastEditTime: 2024-08-14 01:42:18
FilePath: /bkv/scripts/run-shell.py
Description: 描述
'''
import sys
import os
import fire

def get_project_root():
    scripts_dir = os.path.dirname(__file__)
    return os.path.dirname(scripts_dir)

sys.path.insert(1, get_project_root())
from bkv.shell import Shell

def run_shell(mem_store_type="mem"):
    shell = Shell(mem_store_type=mem_store_type)
    shell.loop()

if __name__ == "__main__":
    fire.Fire(run_shell)

# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2024-08-17 16:43:53
LastEditors: xupingmao
LastEditTime: 2024-08-17 23:28:19
FilePath: /bkv/scripts/run-server.py
Description: 描述
'''
import fire
import sys
import os
import logging

def get_project_root():
    scripts_dir = os.path.dirname(__file__)
    return os.path.dirname(scripts_dir)

sys.path.insert(1, get_project_root())

from bkv.redis.bkv_redis import BkvRedisImpl
from bkv.server import run_tcp
from bkv.store.config import default_config, FlushType

def run_server(config="./bkv.conf"):
    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s',
            level=logging.DEBUG)
    default_config.flush_type = FlushType.always
    redis_impl = BkvRedisImpl(db_dir="./test-data")
    run_tcp(redis_impl=redis_impl)

if __name__ == '__main__':
    fire.Fire(run_server)
# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2024-08-17 12:02:04
LastEditors: xupingmao
LastEditTime: 2024-08-17 13:03:48
FilePath: /bkv/scripts/run_server.py
Description: 描述
'''
# encoding=utf-8

import fire
import sys
import os
import logging

def get_project_root():
    scripts_dir = os.path.dirname(__file__)
    return os.path.dirname(scripts_dir)

sys.path.insert(1, get_project_root())

import bkv.store
from bkv.server.interfaces import RedisInterface
from bkv.server import run_tcp
from bkv.server import resp

class RedisBkvImpl(RedisInterface):

    def __init__(self, **kw) -> None:
        self.db = bkv.store.DB(db_dir="./test-data", **kw)

    def execute_set(self, key: bytes, value):
        self.db.put(key.decode(), value.decode())
        return resp.OK
    
    def execute_get(self, key: bytes):
        return self.db.get(key.decode())
    
    def execute_del(self, *keys: bytes):
        for key in keys:
            self.db.delete(key.decode())
        return resp.OK
        
    def execute_keys(self, key: bytes, *args):
        return self.db.keys(key.decode())


def run_server(config="./bkv.conf"):
    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s',
            level=logging.DEBUG)
    redis_impl = RedisBkvImpl()
    run_tcp(redis_impl=redis_impl)

if __name__ == '__main__':
    fire.Fire(run_server)

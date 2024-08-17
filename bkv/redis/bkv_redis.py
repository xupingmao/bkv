# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2024-08-17 12:02:04
LastEditors: xupingmao
LastEditTime: 2024-08-17 22:56:10
FilePath: /bkv/bkv/redis/bkv_redis.py
Description: 描述
'''
# encoding=utf-8
import os
import fire
import logging
import bkv.store

from bkv.server.interfaces import RedisInterface
from bkv.server import run_tcp
from bkv.server import resp

class BkvRedisImpl(RedisInterface):

    def __init__(self, db_dir: str, **kw) -> None:
        if not os.path.isdir(db_dir):
            raise Exception(f"db_dir {db_dir} is invalid dirname")
        self.db = bkv.store.DB(db_dir=db_dir, **kw)

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
    
    def execute_compact(self, *args):
        self.db.compact()
        return resp.OK


def run_server(config="./bkv.conf"):
    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s',
            level=logging.DEBUG)
    redis_impl = BkvRedisImpl()
    run_tcp(redis_impl=redis_impl)

if __name__ == '__main__':
    fire.Fire(run_server)

# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2024-08-17 12:02:04
LastEditors: xupingmao
LastEditTime: 2024-08-25 18:04:07
FilePath: /bkv/bkv/redis/bkv_redis.py
Description: 描述
'''
# encoding=utf-8
import os
import bkv.store
import fire
import logging

from typing import Optional
from bkv import utils
from bkv.server.interfaces import RedisInterface
from bkv.server import run_tcp
from bkv.server import resp

class BkvRedisImpl(RedisInterface):

    def __init__(self, db_dir: str, **kw) -> None:
        if not os.path.isdir(db_dir):
            raise Exception(f"db_dir {db_dir} is invalid dirname")
        self.db = bkv.store.DB(db_dir=db_dir, **kw)

    def execute_set(self, key: bytes, value: bytes):
        self.db.put(key.decode(), value.decode())
        return resp.OK
    
    def execute_setnx(self, key: bytes, value: bytes):
        with self.db.lock:
            if self.db.exists(key.decode()):
                return 0
            else:
                self.db.put(key.decode(), value.decode())
                return 1
    
    def execute_get(self, key: bytes) -> Optional[bytes]:
        result = self.db.get(key.decode())
        if result is None:
            return None
        return utils.dump_json_to_bytes(result)
    
    def execute_del(self, *keys: bytes):
        for key in keys:
            self.db.delete(key.decode())
        return resp.OK
        
    def execute_keys(self, key: bytes, *args):
        return self.db.keys(key.decode())
    
    def execute_compact(self, *args):
        self.db.compact()
        return resp.OK
    
    def execute_ping(self, message=""):
        if message == "":
            return "PONG"
        return message


def run_server(db_dir: str):
    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s',
            level=logging.DEBUG)
    redis_impl = BkvRedisImpl(db_dir=db_dir)
    run_tcp(redis_impl=redis_impl)

if __name__ == '__main__':
    fire.Fire(run_server)

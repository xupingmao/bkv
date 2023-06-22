# -*- coding:utf-8 -*-
"""
@Author       : xupingmao
@email        : 578749341@qq.com
@Date         : 2023-02-01 23:15:02
@LastEditors  : xupingmao
@LastEditTime : 2023-02-05 13:45:55
@FilePath     : /xnoted:/projects/learn-python/src/storage/my_kv.py
@Description  : 键值对存储，基于Bitcask模型
"""

import json
import os
import datetime
import time
from bkv.mem_store import MemoryKvStore


meta_version = "1.0.0"

class StoreItem:
    def __init__(self):
        self.k = ""
        self.v = ""

class StoreMeta:

    def __init__(self):
        self.version = meta_version
        self.create_time = ""

def format_datetime(value=None, format='%Y-%m-%d %H:%M:%S'):
    """格式化日期时间
    >>> format_datetime(0)
    '1970-01-01 08:00:00'
    """
    if value == None:
        return time.strftime(format)
    elif isinstance(value, datetime.datetime):
        return value.strftime(format)
    else:
        st = time.localtime(value)
        return time.strftime(format, st)

class StoreFile:
    """db存储，管理1个存储文件"""
    def __init__(self, db_dir="./data", store_file = "./data-1.txt") -> None:
        self.mem_store = MemoryKvStore()
        self.last_pos = 0
        self.db_dir = db_dir
        self.store_file = store_file
        self.load_disk()
    
    def load_disk(self):
        fpath = os.path.join(self.db_dir, self.store_file)
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)

        if not os.path.exists(fpath):
            with open(fpath, "w+") as fp:
                meta = StoreMeta()
                meta.create_time = format_datetime()
                fp.write(json.dumps(meta.__dict__))
                fp.write("\n")
                fp.flush()
        
        with open(fpath, "r+") as fp:
            meta_line = fp.readline()
            if meta_line.strip() == "":
                raise Exception("broken meta")
            self.meta = self._load_meta(meta_line)
            assert self.meta.version == meta_version, "broken meta"

            while True:
                pos = fp.tell()
                line = fp.readline()
                if line.strip() == "":
                    break
                line_data = self._load_item(line)
                key = line_data.k
                self.mem_store.put(key, str(pos))
        
        self.write_fp = open(fpath, "a+")
        self.last_pos = self.write_fp.tell()
    
    def _load_item(self, json_str):
        json_dict = json.loads(json_str)
        item = StoreItem()
        item.__dict__.update(json_dict)
        return item
    
    def _load_meta(self, json_str):
        json_dict = json.loads(json_str)
        item = StoreMeta()
        item.__dict__.update(json_dict)
        return item
    
    def write(self, key, val):
        # TODO 优化下编码器，增加校验和删除标记
        self.write_fp.seek(self.last_pos)
        item = StoreItem()
        item.k = key
        item.v = val
        self.write_fp.write(json.dumps(item.__dict__))
        self.write_fp.write("\n")
        self.write_fp.flush()
        self.last_pos = self.write_fp.tell()
    
    def close(self):
        self.write_fp.close()
    
    def get(self, key, **kw):
        pos_str = self.mem_store.get(key, **kw)
        if pos_str == None:
            return None
        self.write_fp.seek(int(pos_str))
        line_str = self.write_fp.readline()
        return json.loads(line_str).get("v")
    
    def put(self, key, val, **kw):
        pos = self.write_fp.tell()
        self.mem_store.put(key, str(pos), **kw)
        self.write(key, val)

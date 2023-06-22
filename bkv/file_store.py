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
import threading
import fnmatch
from bkv.mem_store import MemoryKvStore
from bkv import utils

class StoreItem:
    def __init__(self):
        self.k = ""
        self.v = ""
        self.d = 0 # 删除标记

class StoreMeta:
    def __init__(self):
        self.version = ""
        self.create_time = ""
        self.data_file = ""

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

class MetaFile:

    meta_version = "1.0.0"
    default_data_file = "data-0.txt"

    def __init__(self, db_dir="./data"):
        meta_file = "./meta.txt"
        self.db_dir = db_dir
        self.meta_file = os.path.join(db_dir, meta_file)
        self.load_meta_file()

    def load_meta_file(self):
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
        
        if not os.path.exists(self.meta_file):
            with open(self.meta_file, "w+"):
                pass

        with open(self.meta_file, "r+") as read_fp:
            json_text = read_fp.read()
            if json_text.strip() == "":
                self.meta = self.create_meta()
            else:
                self.meta = self._load_meta(json_text)
            assert self.meta.version == self.meta_version, "broken meta"

    def create_meta(self):
        with open(self.meta_file, "w+") as fp:
            meta = StoreMeta()
            meta.version = self.meta_version
            meta.data_file = self.default_data_file
            meta.create_time = format_datetime()
            fp.write(json.dumps(meta.__dict__))
            fp.flush()
            return meta
            
    
    def _load_meta(self, json_str):
        json_dict = json.loads(json_str)
        item = StoreMeta()
        item.__dict__.update(json_dict)
        if item.data_file == "":
            item.data_file = "data-0.txt"
        return item
    
    def save(self):
        with open(self.meta_file, "w+") as fp:
            fp.write(json.dumps(self.meta.__dict__))
    
    def create_new_data_file(self):
        for i in range(100):
            fname = "data-%d.txt" % i
            fpath = os.path.join(self.db_dir, fname)
            if not os.path.exists(fpath):
                return fname
        raise Exception("too many store files")
    
    def delete_old_data_files(self):
        for i in range(100):
            fname = "data-%d.txt" % i
            fpath = os.path.join(self.db_dir, fname)
            if os.path.exists(fpath) and fname != self.meta.data_file:
                print("found old data file:", fname)

class DataFile:
    """db存储，管理1个存储文件"""

    def __init__(self, db_dir="./data", data_file="./data-1.txt", **kw):
        self.mem_store = MemoryKvStore(default_value=0)        
        self.last_pos = 0
        self.db_dir = db_dir
        self.data_file = data_file
        self.print_load_stats = kw.get("print_load_stats", False)
        self.load_data_file()
        self.lock = threading.RLock()


    def load_data_file(self):
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
    
        fpath = os.path.join(self.db_dir, self.data_file)
        if not os.path.exists(fpath):
            with open(fpath, "w+") as fp:
                pass
        
        with open(fpath, "r+") as fp:
            count = 0
            while True:
                pos = fp.tell()
                line = fp.readline()
                if line.strip() == "":
                    break
                line_data = self._load_item(line)
                key = line_data.k
                if line_data.d == 1:
                    # 已删除
                    self.mem_store.delete(key)
                else:
                    self.mem_store.put(key, pos)
                
                count+=1
                if self.print_load_stats and count % 10000 == 0:
                    mem_info = utils.memory_info()
                    keys = len(self.mem_store)
                    rss = mem_info.rss/1024/1024
                    print(f"keys:({keys}), memory:({rss:.2f}MB)")
        
        self.write_fp = open(fpath, "a+")
        self.last_pos = self.write_fp.tell()
    
    def _load_item(self, json_str):
        json_dict = json.loads(json_str)
        item = StoreItem()
        item.__dict__.update(json_dict)
        return item

    def write(self, key="", val="", delete=0):
        # TODO 优化下编码器，增加校验和删除标记
        self.write_fp.seek(self.last_pos)
        item = StoreItem()
        item.k = key
        item.v = val
        item.d = delete
        self.write_fp.write(json.dumps(item.__dict__))
        self.write_fp.write("\n")
        # TODO 提供更多flush策略
        self.write_fp.flush()
        self.last_pos = self.write_fp.tell()
    
    def close(self):
        self.write_fp.close()
    
    def get(self, key):
        pos_int = self.mem_store.get(key)
        if pos_int == None:
            return None
        self.write_fp.seek(pos_int)
        line_str = self.write_fp.readline()
        return json.loads(line_str).get("v")
    
    def put(self, key, val):
        pos_int = self.write_fp.tell()
        exist = self.get(key)
        if exist == val:
            # 没变化
            return
        with self.lock:
            self.mem_store.put(key, pos_int)
            self.write(key, val)

    def delete(self, key):
        exist = self.get(key)
        if exist == None:
            # 已经删除
            return
        with self.lock:
            self.mem_store.delete(key)
            self.write(key=key, delete=1)

    def count(self):
        return len(self.mem_store)

    def avg_key_len(self):
        length = 0
        for key, pos in self.mem_store._data:
            length += len(key)
        return length / len(self.mem_store)

    def keys(self, key, limit=100):
        result = []
        for store_key, pos in self.mem_store._data:
            if fnmatch.fnmatch(store_key, key):
                result.append(store_key)
                if len(result) >= limit:
                    break
        return result

    def delete_file(self):
        self.write_fp.close()
        fpath = os.path.join(self.db_dir, self.data_file)
        os.remove(fpath)
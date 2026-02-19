# -*- coding:utf-8 -*-
"""
@Author       : xupingmao
@email        : 578749341@qq.com
@Date         : 2023-02-01 23:15:02
@LastEditors: xupingmao
@LastEditTime: 2023-12-29 19:58:26
@FilePath: \bkv\bkv\file_store.py
@Description  : 键值对存储, 基于Bitcask模型
"""

import json
import os
import datetime
import time
import threading
import fnmatch
import logging

from typing import List, Any
from bkv import utils
from bkv.store.mem_store import HashMemStore, KvInterface, MemoryKvStore, SqliteMemStore
from bkv.store.config import Config, FlushType, default_config, StoreType

class StoreItem:
    value: Any
    def __init__(self):
        self.key = ""
        self.value = None
        self.is_deleted = 0 # 删除标记
        
    def dumps(self):
        result = {}
        result["k"] = self.key
        if self.value != None:
            result["v"] = self.value
        if self.is_deleted != 0:
            result["d"] = self.is_deleted
        return utils.dump_json(result)
    
    @classmethod
    def loads(cls, json_str: str):
        json_dict: dict = json.loads(json_str)
        item = StoreItem()
        item.key = json_dict.get("k", "")
        item.value = json_dict.get("v", None)
        item.is_deleted = json_dict.get("d", 0)
        return item

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

    def __init__(self, config = Config()):
        db_dir = config.db_dir
        meta_file = config.meta_file
        self.db_dir = config.db_dir
        self.default_data_file = config.default_data_file
        self.meta_file = os.path.join(db_dir, meta_file)
        self.config = config
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
            fp.write(utils.dump_json(meta.__dict__))
            fp.flush()
            return meta
            
    
    def _load_meta(self, json_str):
        json_dict = json.loads(json_str)
        item = StoreMeta()
        item.__dict__.update(json_dict)
        if item.data_file == "":
            item.data_file = self.config.default_data_file
        return item
    
    def update_data_file(self, data_file=""):
        self.meta.data_file = data_file
        
    def get_data_file(self):
        return self.meta.data_file
    
    def save(self):
        with open(self.meta_file, "w+") as fp:
            fp.write(utils.dump_json(self.meta.__dict__))
    
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

    logger = logging.getLogger("DataFile")
    mem_store: KvInterface

    def __init__(self, data_file="", config=default_config):
        self.last_pos = 0
        self.db_dir = config.db_dir
        self.mem_store_type = config.mem_store_type
        
        self.logger.debug("mem_store_type=%s", self.mem_store_type)

        if self.mem_store_type == StoreType.sqlite:
            self.mem_store = SqliteMemStore()
        if self.mem_store_type == StoreType.hash:
            self.mem_store = HashMemStore()
        else:
            self.mem_store = MemoryKvStore(default_value="")

        self.data_file = data_file
        self.print_load_stats = config.print_load_stats
        self.load_data_file()
        self.lock = threading.RLock()
        self.config = config

    def load_data_file(self):
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
    
        fpath = os.path.join(self.db_dir, self.data_file)
        if not os.path.exists(fpath):
            with open(fpath, "w+") as fp:
                pass
        
        self.logger.info("load data file %s", self.data_file)

        qps_counter = utils.QpsCounter()
        with open(fpath, "r+") as fp:
            count = 0
            while True:
                pos = fp.tell()
                line = fp.readline()
                if line.strip() == "":
                    break
                line_data = StoreItem.loads(line)
                key = line_data.key
                if line_data.is_deleted == 1:
                    # 已删除
                    self.mem_store.delete(key)
                else:
                    self.mem_store.put(key, pos)
                
                count+=1
                if self.print_load_stats and count % 10000 == 0:
                    qps_counter.set_count(count)
                    mem_info = utils.memory_info()
                    keys = len(self.mem_store)
                    rss = mem_info.rss/1024/1024
                    qps = qps_counter.qps()
                    print(f"keys:({keys}), memory:({rss:.2f}MB), qps:({qps//1})")
        
        self.logger.info("load data file %s done, total count: %d", self.data_file, count)
        self.write_fp = open(fpath, "a+")
        self.last_pos = self.write_fp.tell()

    def write(self, key="", val=None, delete=0):
        # TODO 优化下编码器，增加校验和删除标记
        self.write_fp.seek(self.last_pos)
        item = StoreItem()
        item.key = key
        item.value = val
        item.is_deleted = delete
        self.write_fp.write(item.dumps())
        self.write_fp.write("\n")
        # TODO 提供更多flush策略
        if self.config.flush_type == FlushType.always:
            self.write_fp.flush()
        self.last_pos = self.write_fp.tell()
    
    def close(self):
        self.write_fp.close()

    def exists(self, key: str):
        pos_int = self.mem_store.get(key)
        return pos_int != None
    
    def get(self, key: str):
        pos_int = self.mem_store.get(key)
        if pos_int == None:
            return None
        self.logger.debug("get(%s)->%s", key, pos_int)
        self.write_fp.seek(pos_int)
        line_str = self.write_fp.readline()
        result = StoreItem.loads(line_str)
        return result.value
    
    def put(self, key: str, val: Any):
        # TODO 把data文件切割成多个文件，写满一个文件后自动切换到新的文件
        pos_int = self.write_fp.tell()
        exist = self.get(key)
        if exist == val:
            # 没变化
            return
        with self.lock:
            self.logger.debug("put(%s,%s)", key, pos_int)
            self.mem_store.put(key, pos_int)
            self.write(key, val)

    def delete(self, key: str):
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
        key_length = 0
        for key, pos in self.mem_store:
            key_length += len(key)
        return key_length / len(self.mem_store)

    def keys(self, key: str, limit=100) -> List[str]:
        assert limit <= 1000
        result = []
        for store_key, pos in self.mem_store:
            if fnmatch.fnmatch(store_key, key):
                result.append(store_key)
                if len(result) >= limit:
                    break
        return result

    def delete_file(self):
        self.write_fp.close()
        fpath = os.path.join(self.db_dir, self.data_file)
        os.remove(fpath)
        
    def iter_data(self):
        yield from self.mem_store
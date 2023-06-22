# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 12:23:46
LastEditors: xupingmao
LastEditTime: 2023-06-22 16:07:47
FilePath: \bkv\bkv\__init__.py
Description: 键值对存储，基于Bitcask模型
'''
import threading
import os
from bkv.file_store import MetaFile, StoreFile
from bkv import utils

class DB:
    def __init__(self, db_dir = "./data", **kw):
        self.db_dir = db_dir
        self.meta = MetaFile(db_dir)
        self.store = StoreFile(db_dir, self.meta.meta.store_file, **kw)
        self.lock = threading.RLock()
    
    def compact(self):
        with self.lock:
            new_file = self.meta.create_new_store_file()
            new_store = StoreFile(self.db_dir, new_file)
            for key, pos_int in self.store.mem_store._data:
                val = self.get(key)
                if val != None:
                    new_store.put(key, val)
            # 先更新meta信息
            self.meta.meta.store_file = new_file
            self.meta.save()

            # 更新成功后删除文件
            old_store = self.store
            old_store.delete_file()
            self.store = new_store
    
    def delete_old_files(self):
        with self.lock:
            return self.meta.delete_old_data_files()
    
    def get(self, key):
        return self.store.get(key)
    
    def put(self, key, value):
        return self.store.put(key, value)
    
    def delete(self, key):
        return self.store.delete(key)
    
    def count(self):
        return self.store.count()
    
    def memory_info(self):
        return utils.memory_info()
    
    def avg_key_len(self):
        return self.store.avg_key_len()
    
    def close(self):
        self.store.close()
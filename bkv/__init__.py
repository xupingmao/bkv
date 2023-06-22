# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 12:23:46
LastEditors: xupingmao
LastEditTime: 2023-06-22 12:42:44
FilePath: \bkv\bkv\__init__.py
Description: 键值对存储，基于Bitcask模型
'''

from bkv.file_store import StoreFile

class DB:
    def __init__(self, db_dir = "./data") -> None:
        self.db_dir = db_dir
        self.store = StoreFile(db_dir, "data-1.txt")
    
    def compact(self):
        # TODO 并发控制
        new_store = StoreFile(self.db_dir, "./data-2.txt")
        for key, pos_str in self.store.mem_store._data:
            val = self.get(key)
            if val != None:
                new_store.put(key, val)
        new_store.close()
        # TODO switch data file
    
    def get(self, key):
        return self.store.get(key)
    
    def put(self, key, value):
        return self.store.put(key, value)
    
    def close(self):
        self.store.close()

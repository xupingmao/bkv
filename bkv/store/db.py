# -*- coding:utf-8 -*-
"""
@Author: xupingmao
@email: 578749341@qq.com
@Date: 2023-12-29 19:35:25
@LastEditors: xupingmao
@LastEditTime: 2023-12-29 19:47:48
@FilePath: \bkv\bkv\db.py
@Description: Description
"""

import threading
import copy
from bkv import utils
from bkv.store.file_store import MetaFile, DataFile
from bkv.store.config import Config

class DB:
    def __init__(self, **kw):
        """初始化数据库, 配置key参考 `bkv.config.Config`
        """
        config = Config().load(kw)
        self.db_dir = config.db_dir
        self.meta = MetaFile(config)
        self.store = DataFile(self.meta.get_data_file(), config=config)
        self.lock = threading.RLock()
        self.config = config
    
    def compact(self):
        with self.lock:
            new_file = self.meta.create_new_data_file()
            new_store = DataFile(new_file, self.config)
            for key, pos_int in self.store.iter_data():
                val = self.get(key)
                if val != None:
                    new_store.put(key, val)
            
            # 先更新meta信息
            self.meta.update_data_file(new_file)
            self.meta.save()

            # 更新成功后删除文件
            old_store = self.store
            old_store.delete_file()
            self.store = new_store
    
    def delete_old_files(self):
        with self.lock:
            return self.meta.delete_old_data_files()
    
    def exists(self, key: str):
        return self.store.exists(key)
    
    def get(self, key: str):
        return self.store.get(key)
    
    def put(self, key: str, value: object):
        return self.store.put(key, value)
    
    def delete(self, key: str):
        return self.store.delete(key)
    
    def count(self):
        return self.store.count()
    
    def memory_info(self):
        return utils.memory_info()
    
    def avg_key_len(self):
        return self.store.avg_key_len()
    
    def keys(self, key: str, limit=100):
        return self.store.keys(key, limit=limit)
    
    def close(self):
        self.store.close()
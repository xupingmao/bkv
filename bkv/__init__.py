# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 12:23:46
LastEditors: xupingmao
LastEditTime: 2023-06-22 16:36:07
FilePath: /bkv/bkv/__init__.py
Description: 键值对存储，基于Bitcask模型
'''
import threading
import copy
from bkv.file_store import MetaFile, DataFile
from bkv import utils
from bkv.config import Config

class DB:
    def __init__(self, **kw):
        """初始化数据库, 配置key参考 `bkv.config.Config`
        """
        config = Config().load(kw)
        self.db_dir = config.db_dir
        self.meta = MetaFile(config.db_dir)
        self.store = DataFile(config=config)
        self.lock = threading.RLock()
        self.config = config
    
    def compact(self):
        with self.lock:
            new_file = self.meta.create_new_data_file()
            new_cfg = copy.copy(self.config)
            new_cfg.data_file = new_file
            new_store = DataFile(new_cfg)
            for key, pos_int in self.store.mem_store._data:
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
    
    def keys(self, key, limit=100):
        return self.store.keys(key, limit=limit)
    
    def close(self):
        self.store.close()
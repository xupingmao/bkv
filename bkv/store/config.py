# -*- coding:utf-8 -*-
"""
@Author: xupingmao
@email: 578749341@qq.com
@Date: 2023-12-29 18:43:35
@LastEditors: xupingmao
@LastEditTime: 2023-12-29 19:45:28
@FilePath: \bkv\bkv\config.py
@Description: 配置相关
"""

class FlushType:
    never = "never"
    always = "always"
    everysec = "everysec"

class Config:
    # 数据库目录
    db_dir = "./data"
    
    meta_file = "./meta.txt"
    
    # 文件名称
    default_data_file = "./data-1.txt"
    
    # 内存存储类型, 包括 {bisect, sqlite}
    mem_store_type = "bisect"
    
    # 是否打印加载信息
    print_load_stats = False

    # 刷盘方式
    flush_type = FlushType.always
    
    def load(self, kw={}):
        for key in kw:
            value = kw[key]
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise Exception("unknown config key: %s" % key)
        return self
    
default_config = Config()

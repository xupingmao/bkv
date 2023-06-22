# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 12:42:15
LastEditors: xupingmao
LastEditTime: 2023-06-22 16:17:15
FilePath: /bkv/bkv/shell.py
Description: 描述
'''
import bkv
import traceback

class Shell:
    
    def __init__(self) -> None:
        self.db = bkv.DB(db_dir="./test-data")

    def loop(self):
        while True:
            cmd = input(">>> ")
            parts = cmd.split()
            if len(parts) == 0:
                print("bad command")
                continue
            op = parts[0]
            if op == "quit" or op == "exit":
                print("Bye")
                break
            
            attr = "op_" + op
            if hasattr(self, attr):
                meth = getattr(self, attr)
                try:
                    meth(parts)
                except:
                    traceback.print_exc()
            else:
                self.print_help()

        self.db.close()
        
    def op_put(self, parts: list[str]):
        if len(parts) != 3:
            print("bad put/set command")
        else:
            key = parts[1]
            val = parts[2]
            self.db.put(key, val)
            print("OK")
    
    op_set = op_put
    
    def op_get(self, parts):
        if len(parts) != 2:
            print("bad get command")
        else:
            key = parts[1]
            val = self.db.get(key)
            print(val)
    
    def op_delete(self, parts):
        if len(parts) != 2:
            print("bad delete command")
        else:
            key = parts[1]
            self.db.delete(key)
            print("OK")
    
    def op_delete_old_files(self, parts):
        self.db.delete_old_files()
        print("OK")
    
    def op_compact(self, parts):
        self.db.compact()
        print("OK")
    
    def op_memory_info(self, parts):
        print(self.db.memory_info())
        print("OK")

    def print_help(self):
        print("bad command, supported commands:")
        print("- get $key            get value by key")
        print("- put $key $value     set value by key")
        print("- set $key $value     set value by key")
        print("- delete $key         delete key")
        print("- compact             compact data files")
        print("- memory_info         print memory info")
        print()

if __name__ == "__main__":
    shell = Shell()
    shell.loop()

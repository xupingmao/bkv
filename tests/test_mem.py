# encoding=utf-8
import sys
import os
import fire

def get_project_root():
    scripts_dir = os.path.dirname(__file__)
    return os.path.dirname(scripts_dir)

sys.path.insert(1, get_project_root())
import bkv


def load_big_data(mem_store_type="sqlite"):
    db = bkv.DB("./test-data/big", print_load_stats = True, mem_store_type = mem_store_type)
    print(db.store.mem_store)

if __name__ == "__main__":
    fire.Fire(load_big_data)


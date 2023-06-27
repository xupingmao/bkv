# encoding=utf-8
import redis
import sys
import os
import time

def get_project_root():
    scripts_dir = os.path.dirname(__file__)
    return os.path.dirname(scripts_dir)

sys.path.insert(1, get_project_root())
import bkv


def write_data_to_redis():
    r = redis.Redis(host="192.168.50.190")
    db = bkv.DB("./test-data/big", print_load_stats = True)
    count = 0
    counter = bkv.utils.QpsCounter()
    for key, value in db.store.mem_store._data:
        count += 1
        if count % 1000 == 0:
            counter.add_count(count)
            qps = counter.qps()
            cost_time = counter.cost_time()
            print(f"count:{count}, cost_time:{cost_time:.2f}ms, qps:{qps}")
        r.set(key, value)

if __name__ == "__main__":
    write_data_to_redis()

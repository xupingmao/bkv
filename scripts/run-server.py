import fire
import sys
import os
import logging

def get_project_root():
    scripts_dir = os.path.dirname(__file__)
    return os.path.dirname(scripts_dir)

sys.path.insert(1, get_project_root())

from bkv.redis.bkv_redis import BkvRedisImpl
from bkv.server import run_tcp


def run_server(config="./bkv.conf"):
    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s',
            level=logging.DEBUG)
    redis_impl = BkvRedisImpl(db_dir="./test-data")
    run_tcp(redis_impl=redis_impl)

if __name__ == '__main__':
    fire.Fire(run_server)
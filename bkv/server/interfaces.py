# encoding=utf-8
import logging
import typing
from typing import Optional
from . import resp

class KeyType:
    def __init__(self, type_):
        self.type_ = type_


MUTABLE_KEY = object()
KEY_SET = KeyType(set)
KEY_STRING = KeyType(bytes)


class RedisInterface:

    def __init__(self):
        pass

    def add_watch(self, queue):
        pass

    def remove_watch(self, queue):
        pass

    def on_change(self, key):
        pass

    def execute_single(self, command: bytes, *args):
        command_str = command.decode("utf-8")
        command_lower = command_str.lower()
        if "." in command_lower:
            command_lower = command_lower.replace(".", "_")
        meth = getattr(self, "execute_" + command_lower, None)
        if meth is None:
            return resp.Error('ERR', f'command {command_str} not implemented')
        logging.debug("%s %s", command, args)
        return meth(*args)

    def execute_set(self, key: bytes, value):
        return resp.OK

    def execute_get(self, key: bytes) -> Optional[bytes]:
        return b''

    def execute_incrby(self, key: bytes, value):
        return 0

    def execute_decrby(self, key: bytes, value):
        return 0

    def execute_del(self, *keys: bytes) -> object:
        return resp.Errors.NOT_IMPLEMENTED

    def execute_scan(self, cursor):
        return resp.Errors.NOT_IMPLEMENTED

    def execute_sadd(self, key: bytes, *args):
        return resp.Errors.NOT_IMPLEMENTED

    def execute_spop(self, key: bytes):
        return resp.Errors.NOT_IMPLEMENTED

    def execute_scard(self, key: bytes):
        return resp.Errors.NOT_IMPLEMENTED


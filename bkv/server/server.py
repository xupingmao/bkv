# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2024-08-17 11:43:01
LastEditors: xupingmao
LastEditTime: 2024-08-25 18:25:06
FilePath: /bkv/bkv/server/server.py
Description: 描述
'''
# encoding=utf-8
# Modified from https://github.com/chekart/rediserver/

import asyncio
import asyncio.streams
import asyncio.exceptions

from threading import Thread, Event
from typing import Optional

from .interfaces import RedisInterface
from . import resp
from .queue import CommandQueue


def create_redis_server(redis_impl: RedisInterface):
    async def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        transaction = CommandQueue(redis_impl)

        try:
            while True:
                command, *command_args = await resp.read_command(reader)
                try:
                    result = transaction.execute(command, *command_args)
                except resp.Error as e:
                    response = resp.dump_response(e)
                    transaction.reset()
                except Exception as e:
                    response = resp.dump_response(
                        resp.Error('UNKNOWN', str(e))
                    )
                    transaction.reset()
                else:
                    response = resp.dump_response(result)

                writer.write(response)
                await writer.drain()
        except asyncio.exceptions.IncompleteReadError:
            writer.close()

    return redis_impl, on_connect


def run_tcp(redis_impl: RedisInterface, host='127.0.0.1', port=6379):
    loop, socket_server = _create_with_host_and_port(redis_impl=redis_impl, host=host, port=port)
    _run_forever(loop, socket_server)


def run_socket(redis_impl: RedisInterface, path: str):
    loop, socket_server = _create_socket(redis_impl=redis_impl, unix_domain_socket=path)
    _run_forever(loop, socket_server)


def _create_with_host_and_port(redis_impl: RedisInterface, host='', port=6379):
    redis_instance, on_connect = create_redis_server(redis_impl)
    loop = asyncio.get_event_loop()
    socket_server = asyncio.start_server(on_connect, host=host, port=port)
    return loop, socket_server

def _create_socket(redis_impl: RedisInterface, unix_domain_socket=None):
    assert isinstance(redis_impl, RedisInterface)
    assert unix_domain_socket is not None

    redis_instance, on_connect = create_redis_server(redis_impl)
    loop = asyncio.get_event_loop()
    socket_server = asyncio.start_unix_server(on_connect, path=unix_domain_socket) 
    return loop, socket_server


def _run_forever(loop: asyncio.AbstractEventLoop, socket_server, started_event=None):
    server = loop.run_until_complete(socket_server)

    if started_event:
        started_event.set()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

def run_threaded(unix_domain_socket):
    started_event = Event()

    class Data:
        def __init__(self):
            self.redis_instance = None
            self.loop = None

    data = Data()

    def thread_target():
        data.redis_instance, data.loop, socket_server = _create_socket(unix_domain_socket=unix_domain_socket)
        _run_forever(data.loop, socket_server, started_event)

    thread = Thread(target=thread_target)
    thread.start()
    started_event.wait()

    def shutdown():
        for task in asyncio.Task.all_tasks():
            task.cancel()
        data.loop.stop()

    def shutdown_callback():
        data.loop.call_soon_threadsafe(shutdown)

    return data.redis_instance, thread, shutdown_callback

# -*- coding:utf-8 -*-
'''
Author: xupingmao
email: 578749341@qq.com
Date: 2023-06-22 12:24:53
LastEditors: xupingmao
LastEditTime: 2023-06-27 23:34:22
FilePath: /bkv/bkv/mem_store.py
Description: 键值对存储，基于Bitcask模型, copied from leveldbpy
'''

# -*- coding:utf-8 -*-
#!/usr/bin/env python
#
# Copyright (C) 2012 Space Monkey, Inc.
#               2023 xupingmao 578749341@qq.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import bisect
import threading


class KvInterface(object):

    def get(self, key):
        return None
    
    def put(self, key, val):
        pass

    def delete(self, key):
        pass

class MemoryKvStore(KvInterface):

    def __init__(self, data=None, default_value=""):
        if data is None:
            self._data = []
        else:
            self._data = data
        self._lock = threading.RLock()
        self.default_value = default_value

    def close(self):
        with self._lock:
            self._data = []

    def put(self, key, val):
        assert isinstance(key, str)
        with self._lock:
            idx = bisect.bisect_left(self._data, (key, val))
            if 0 <= idx < len(self._data) and self._data[idx][0] == key:
                self._data[idx] = (key, val)
            else:
                self._data.insert(idx, (key, val))

    def delete(self, key):
        with self._lock:
            idx = bisect.bisect_left(self._data, (key, self.default_value))
            if 0 <= idx < len(self._data) and self._data[idx][0] == key:
                del self._data[idx]

    def get(self, key):
        idx = bisect.bisect_left(self._data, (key, self.default_value))
        if 0 <= idx < len(self._data) and self._data[idx][0] == key:
            return self._data[idx][1]
        return None
        
    def __len__(self):
        return len(self._data)

    # pylint: disable=W0212
    def write(self, batch):
        with self._lock:
            for key, val in batch._puts.iteritems():
                self.put(key, val)
            for key in batch._deletes:
                self.delete(key)

    def iterator(self, **_kwargs):
        # WARNING: huge performance hit.
        # leveldb iterators are actually lightweight snapshots of the data. in
        # real leveldb, an iterator won't change its idea of the full database
        # even if puts or deletes happen while the iterator is in use. to
        # simulate this, there isn't anything simple we can do for now besides
        # just copy the whole thing.
        with self._lock:
            return _IteratorMemImpl(self._data[:])

    def __iter__(self):
        yield from self._data


class _IteratorMemImpl(object):

    __slots__ = ["_data", "_idx"]

    def __init__(self, memdb_data):
        self._data = memdb_data
        self._idx = -1
        self.default_value = ""

    def valid(self):
        return 0 <= self._idx < len(self._data)

    def key(self):
        return self._data[self._idx][0]

    def val(self):
        return self._data[self._idx][1]

    def seek(self, key):
        self._idx = bisect.bisect_left(self._data, (key, self.default_value))

    def seekFirst(self):
        self._idx = 0

    def seekLast(self):
        self._idx = len(self._data) - 1

    def prev(self):
        self._idx -= 1

    def next(self):
        self._idx += 1

    def close(self):
      self._data = []
      self._idx = -1


class SqliteMemStore(KvInterface):

    def __init__(self):
        import sqlite3
        self.db = sqlite3.connect(":memory:")
        self.execute_sql("create table kv (k text primary key, v bigint)")
        self._lock = threading.RLock()

    def close(self):
        self.db.close()

    def put(self, key, val):
        assert isinstance(key, str)
        with self._lock:
            result = self.execute_sql("select k from kv where k=?", key)
            if len(result) == 0:
                self.execute_sql("insert into kv(k,v) values(?,?)", key, val)
            else:
                self.execute_sql("update kv set v=? where k=?", val, key)

    def execute_sql(self, sql, *args):
        cursorobj = self.db.cursor()
        try:
            cursorobj.execute(sql, args)
            # dict_result = []
            result = cursorobj.fetchall()
            # for single in result:
            #     resultMap = {}
            #     for i, desc in enumerate(cursorobj.description):
            #         name = desc[0]
            #         resultMap[name] = single[i]
            #     dict_result.append(resultMap)
            self.db.commit()
            return result
        except Exception as e:
            self.db.rollback()
            raise e
        finally:
            cursorobj.close()


    def delete(self, key):
        with self._lock:
            self.execute_sql("delete from kv where k=?", key)

    def get(self, key):
        result = self.execute_sql("select v from kv where k=?", key)
        if len(result) > 0:
            return result[0][0]
        return None
        
    def __len__(self):
        result = self.execute_sql("select count(*) as amount from kv")
        return result[0][0]
    
    def __iter__(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("select k,v from kv")
            for k,v in cursor:
                yield k,v
        finally:
            cursor.close()



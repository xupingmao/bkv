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

"""
@Author       : xupingmao
@email        : 578749341@qq.com
@Date         : 2023-02-01 23:15:02
@LastEditors  : xupingmao
@LastEditTime : 2023-02-05 13:45:55
@FilePath     : /xnoted:/projects/learn-python/src/storage/my_kv.py
@Description  : 键值对存储，基于Bitcask模型, copied from leveldbpy
"""
import bisect
import threading


class MemoryKvStore(object):

    __slots__ = ["_data", "_lock", "_is_snapshot"]

    def __init__(self, data=None, is_snapshot=False):
        if data is None:
            self._data = []
        else:
            self._data = data
        self._lock = threading.RLock()
        self._is_snapshot = is_snapshot

    def close(self):
        with self._lock:
            self._data = []

    def put(self, key, val, **_kwargs):
        if self._is_snapshot:
            raise TypeError("cannot put on leveldb snapshot")
        assert isinstance(key, str)
        assert isinstance(val, str)
        with self._lock:
            idx = bisect.bisect_left(self._data, (key, ""))
            if 0 <= idx < len(self._data) and self._data[idx][0] == key:
                self._data[idx] = (key, val)
            else:
                self._data.insert(idx, (key, val))

    def delete(self, key, **_kwargs):
        if self._is_snapshot:
            raise TypeError("cannot delete on leveldb snapshot")
        with self._lock:
            idx = bisect.bisect_left(self._data, (key, ""))
            if 0 <= idx < len(self._data) and self._data[idx][0] == key:
                del self._data[idx]

    def get(self, key, **_kwargs):
        with self._lock:
            idx = bisect.bisect_left(self._data, (key, ""))
            if 0 <= idx < len(self._data) and self._data[idx][0] == key:
                return self._data[idx][1]
            return None

    # pylint: disable=W0212
    def write(self, batch, **_kwargs):
        if self._is_snapshot:
            raise TypeError("cannot write on leveldb snapshot")
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



class _IteratorMemImpl(object):

    __slots__ = ["_data", "_idx"]

    def __init__(self, memdb_data):
        self._data = memdb_data
        self._idx = -1

    def valid(self):
        return 0 <= self._idx < len(self._data)

    def key(self):
        return self._data[self._idx][0]

    def val(self):
        return self._data[self._idx][1]

    def seek(self, key):
        self._idx = bisect.bisect_left(self._data, (key, ""))

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

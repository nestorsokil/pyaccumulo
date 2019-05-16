#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from array import array
from accumulo.ttypes import ColumnUpdate, Key, WriterOptions, Range as AccumuloRange
from pyaccumulo.utils import _following_key
from collections import namedtuple

Cell = namedtuple("Cell", "row cf cq cv ts val")


class Mutation(object):
    def __init__(self, row):
        super(Mutation, self).__init__()
        self.row = row
        self.updates = []

    def put(self, cf='', cq='', cv=None, ts=None, val='', is_delete=None):
        self.updates.append(ColumnUpdate(colFamily=cf, colQualifier=cq, colVisibility=cv, timestamp=ts, value=val,
                                         deleteCell=is_delete))


class Range(object):
    def __init__(self,
                 srow=None, scf=None, scq=None, scv=None, sts=None, sinclude=True,
                 erow=None, ecf=None, ecq=None, ecv=None, ets=None, einclude=True):

        super(Range, self).__init__()

        self.srow = srow
        self.scf = scf
        self.scq = scq
        self.scv = scv
        self.sts = sts
        self.sinclude = sinclude

        self.erow = erow
        self.ecf = ecf
        self.ecq = ecq
        self.ecv = ecv
        self.ets = ets
        self.einclude = einclude

    @staticmethod
    def following_prefix(prefix):
        """Returns a String that sorts just after all Strings beginning with a prefix"""
        prefix_bytes = array('B', prefix)

        change_index = len(prefix_bytes) - 1
        while change_index >= 0 and prefix_bytes[change_index] == 0xff:
            change_index = change_index - 1
        if change_index < 0:
            return None
        new_bytes = array('B', prefix[0:change_index + 1])
        new_bytes[change_index] = new_bytes[change_index] + 1
        return new_bytes.tostring()

    @staticmethod
    def prefix(row_prefix):
        """Returns a Range that covers all rows beginning with a prefix"""
        fp = Range.following_prefix(row_prefix)
        return Range(srow=row_prefix, sinclude=True, erow=fp, einclude=False)

    def to_range(self) -> AccumuloRange:
        r = AccumuloRange()
        r.startInclusive = self.sinclude
        r.stopInclusive = self.einclude

        if self.srow:
            r.start = Key(row=self.srow, colFamily=self.scf, colQualifier=self.scq, colVisibility=self.scv,
                          timestamp=self.sts)
            if not self.sinclude:
                r.start = _following_key(r.start)

        if self.erow:
            r.stop = Key(row=self.erow, colFamily=self.ecf, colQualifier=self.ecq, colVisibility=self.ecv,
                         timestamp=self.ets)
            if self.einclude:
                r.stop = _following_key(r.stop)

        return r


class BatchWriter(object):
    """docstring for BatchWriter"""

    def __init__(self, conn, table, max_memory=10 * 1024, latency_ms=30 * 1000, timeout_ms=5 * 1000, threads=10):
        super(BatchWriter, self).__init__()
        self._conn = conn
        self._writer = conn.client.createWriter(self._conn.login, table,
                                                WriterOptions(maxMemory=max_memory, latencyMs=latency_ms,
                                                              timeoutMs=timeout_ms, threads=threads))
        self._is_closed = False

    ''' muts - a list of Mutation objects '''

    def add_mutations(self, muts):
        if self._is_closed:
            raise Exception("Cannot write to a closed writer")

        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        self._conn.client.update(self._writer, cells)

    ''' mut - a Muation object '''

    def add_mutation(self, mut):
        if self._is_closed:
            raise Exception("Cannot write to a closed writer")
        self._conn.client.update(self._writer, {mut.row: mut.updates})

    def flush(self):
        if self._is_closed:
            raise Exception("Cannot flush a closed writer")
        self._conn.client.flush(self._writer)

    def close(self):
        self._conn.client.closeWriter(self._writer)
        self._is_closed = True

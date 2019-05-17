from contextlib import contextmanager
from threading import RLock

from pyaccumulo.accumulo import Accumulo


class AccumuloPool:
    def __init__(self, host, port, user, password, size=10):
        self._conn_params = {'host': host, 'port': port, 'user': user, 'password': password}
        self._lock = RLock()
        self._pool = []
        self._max_size = size

    @contextmanager
    def get(self):
        conn: Accumulo = self._check_out()
        yield conn
        self._check_in(conn)

    def _new_conn(self):
        return Accumulo(**self._conn_params)

    def _check_in(self, conn):
        with self._lock:
            while len(self._pool) >= self._max_size:
                self._pool.pop(0)
            self._pool.append(conn)

    def _check_out(self):
        with self._lock:
            conn = None
            if len(self._pool) > 0:
                conn = self._pool.pop(0)
            if not conn:
                conn = self._new_conn()
            return conn

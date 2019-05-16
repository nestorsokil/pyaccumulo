from contextlib import contextmanager, asynccontextmanager
from threading import RLock
from pyaccumulo.accumulo import Accumulo
import asyncio
import random


class AccumuloPool:
    def __init__(self, host, port, user, password, size=10):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self._lock = RLock()
        self._pool = []
        self._max_size = size

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

    def _new_conn(self):
        return Accumulo(host=self.host, port=self.port, user=self.user, password=self.password)

    @contextmanager
    def get(self):
        conn: Accumulo = self._check_out()
        yield conn
        self._check_in(conn)


class AccumuloAsyncPool:
    def __init__(self, host, port, user, password, size=10):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self._lock = asyncio.Lock()
        self._pool = []
        self._actual_size = 0
        self._max_size = size

    def _is_available(self) -> bool:
        return len(self._pool) > 0

    async def _check_out(self):
        while True:
            async with self._lock:
                if self._is_available():
                    return self._pool.pop()
                if self._actual_size < self._max_size:
                    self._actual_size = self._actual_size + 1
                    return self._new_conn()
            sleep_time = random.uniform(0.001, 0.005)
            await asyncio.sleep(sleep_time)  # TODO maybe there's smth better

    async def _check_in(self, conn):
        async with self._lock:
            self._pool.append(conn)

    def _new_conn(self):
        return Accumulo(host=self.host, port=self.port, user=self.user, password=self.password)

    @asynccontextmanager
    async def get(self):
        conn = await self._check_out()
        yield conn
        await self._check_in(conn)

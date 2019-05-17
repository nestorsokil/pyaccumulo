import asyncio
from contextlib import asynccontextmanager
from pyaccumulo.accumulo import Accumulo


class AccumuloAsyncPool:
    def __init__(self, host, port, user, password, size=10):
        self._conn_params = {'host': host, 'port': port, 'user': user, 'password': password}
        self._max_size = size
        self._actual_size = 0
        self._pool = []
        self._lock = asyncio.Lock()
        self._available = asyncio.Event()

    @asynccontextmanager
    async def get(self):
        conn = await self._check_out()
        yield conn
        await self._check_in(conn)

    def _new_conn(self):
        return Accumulo(**self._conn_params)

    def _is_available(self) -> bool:
        return self._available.is_set()

    def _make_available(self):
        self._available.set()

    def _make_unavailable(self):
        self._available.clear()

    async def _check_out(self):
        while True:
            async with self._lock:
                if self._is_available():
                    conn = self._pool.pop()
                    if len(self._pool) == 0:
                        self._make_unavailable()
                    return conn
                if self._actual_size < self._max_size:
                    self._actual_size = self._actual_size + 1
                    return self._new_conn()
            await self._available.wait()

    async def _check_in(self, conn):
        async with self._lock:
            self._pool.append(conn)
            if not self._is_available():
                self._make_available()

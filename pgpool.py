import sys
import time
from threading import Lock
import psycopg2
from psycopg2 import Error as PoolError
from psycopg2 import extensions as _ext

sys.tracebacklimit = 0


class ConnectionPool:
    def __init__(
        self,
        min_pool_size: int = 0,
        max_pool_size: int = 2,
        con_min_life_ts: int = 3600,
        **kwargs: dict
    ):
        """
        min_pool_size: int
            initialize with minimum number of connection in the pool
        max_pool_size: int
            maximum connection that pool can hold
        con_min_life_ts: int
            lifespan of a connection in seconds
        kwargs: dict
            connection parameters
        """
        self.kwargs = kwargs
        self._min_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._pool = []
        self._track_conn_in_use = {}
        self._con_min_life_ts = con_min_life_ts
        self._track_connection_life_ts = {}
        self._connection_counter = 0
        self._lock = Lock()

        if self._max_pool_size > 10:
            raise PoolError("Minimum pool size should be < 10")

        if con_min_life_ts > 3600:
            self._con_min_life_ts = 3600

        for _ in range(self._min_size):
            conn = self._connect()
            self._pool.append(conn)

    def _connect(self):
        try:
            conn = psycopg2.connect(
                user=self.kwargs.get("user"),
                password=self.kwargs.get("password"),
                host=self.kwargs.get("host"),
                port=self.kwargs.get("port"),
                dbname=self.kwargs.get("dbname"),
            )
            self._track_connection_life_ts[id(conn)] = int(time.time())
            self._connection_counter += 1
            return conn
        except:
            raise PoolError("Invalid DB credentials")

    def get_connection(self) -> _ext.connection:
        try:
            self._lock.acquire()
            if self._pool:
                conn = self._pool.pop(0)
                self._track_conn_in_use[id(conn)] = conn
                return conn
            else:
                if self._connection_counter > self._max_pool_size:
                    raise PoolError("Pool exhausted")
                conn = self._connect()
                self._track_conn_in_use[id(conn)] = conn
                return conn
        finally:
            self._lock.release()

    def close(self, conn):
        try:
            self._lock.acquire()
            if not conn.closed:
                if (
                    self._con_min_life_ts > 0
                    and int(time.time()) - self._track_connection_life_ts.get(id(conn))
                    > self._con_min_life_ts
                ):
                    conn.close()
                else:
                    status = conn.info.transaction_status
                    if status == _ext.TRANSACTION_STATUS_UNKNOWN:
                        conn.close()
                    elif status != _ext.TRANSACTION_STATUS_IDLE:
                        conn.rollback()
                        self._pool.append(self._track_conn_in_use[id(conn)])
                        del self._track_conn_in_use[id(conn)]
                    else:
                        self._pool.append(self._track_conn_in_use[id(conn)])
                        del self._track_conn_in_use[id(conn)]
        finally:
            self._lock.release()

    def closeall(self) -> None:
        try:
            self._lock.acquire()
            if self._pool:
                raise PoolError("No connection exist in pool")
            for con in self._pool:
                con.close()
            self._track_conn_in_use = {}
        finally:
            self._lock.release()

    @property
    def available_connections(self) -> int:
        """number of connection available to use"""
        return len(self._pool)

    @property
    def total_connection_pool(self) -> int:
        """number of connection available + in use"""
        return self._connection_counter

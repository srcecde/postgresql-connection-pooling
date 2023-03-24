import sys
import time
import psycopg2
from psycopg2 import Error as PoolError
from psycopg2 import extensions as _ext

sys.tracebacklimit = 0

class ConnectionPool:
    def __init__(self, min_pool_size=0, max_pool_size=2, con_min_life_ts=3600, **kwargs):
        self.kwargs = kwargs
        self._min_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._pool = []
        self._track_conn_in_use = {}
        self._con_min_life_ts = con_min_life_ts
        self._track_connection_life_ts = {}
        self._connection_counter = 0

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

    def close(self, conn):
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

    def closeall(self) -> None:
        if self._pool:
            raise PoolError("No connection exist in pool")
        for con in self._pool:
            con.close()
        self._track_conn_in_use = {}

    @property
    def pool_size(self) -> int:
        return len(self._pool)

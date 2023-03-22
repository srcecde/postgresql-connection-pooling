import time
import psycopg2
from psycopg2 import Error as PoolError
from psycopg2 import extensions as _ext


class ConnectionPool:
    def __init__(self, min_size=5, con_min_life_ts=3600, **kwargs):
        self.kwargs = kwargs
        self._min_size = min_size
        self._pool = []
        self._track_conn_in_use = {}
        self.con_min_life_ts = con_min_life_ts
        self.track_connection_life_ts = {}

        if self._min_size > 5:
            PoolError("Minimum pool size should be < 5")

        if con_min_life_ts > 3600:
            self.con_min_life_ts = 3600

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
            self.track_connection_life_ts[id(conn)] = int(time.time())
            return conn
        except:
            PoolError("Invalid DB credentials")

    def get_connection(self) -> _ext.connection:
        if self._pool:
            conn = self._pool.pop(0)
            self._track_conn_in_use[id(conn)] = conn
            return conn
        else:
            conn = self._connect()
            self._track_conn_in_use[id(conn)] = conn
            return conn

    def close(self, conn):
        if not conn.closed:
            if (
                self.con_min_life_ts > 0
                and int(time.time()) - self.track_connection_life_ts.get(id(conn))
                > self.con_min_life_ts
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
            PoolError("No connection exist in pool")
        for con in self._pool:
            con.close()
        self._track_conn_in_use = {}

    @property
    def pool_length(self) -> int:
        return len(self._pool)

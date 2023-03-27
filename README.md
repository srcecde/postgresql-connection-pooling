# PostgreSQL Connection Pooling
Naive implementation of the connection pooling for learning purpose.

## Usage
- - -
```
from pgpool import ConnectionPool
con_obj = ConnectionPool(
        min_pool_size=5,
        max_pool_size=10,
        user="admin",
        password="admin",
        host="localhost",
        port="5432",
        dbname="postgres",
    )

print(f"Available connections: {con_obj.available_connections}")
print(f"Total connections: {con_obj.total_connection_pool}")

# get connection from pool
conn1 = con_obj.get_connection()

print(f"Available connections: {con_obj.available_connections}")
print(f"Total connections: {con_obj.total_connection_pool}")

# query execution
with conn1.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchall())

con_obj.close(conn1)

print(f"Available connections: {con_obj.available_connections}")
print(f"Total connections: {con_obj.total_connection_pool}")
```

## WIP
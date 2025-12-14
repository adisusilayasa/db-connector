"""
db_connector - A high-performance PostgreSQL connector for Python, written in Rust

Example usage:

    # Quick start with connection string
    from db_connector import connect_url
    
    with connect_url("postgresql://user:pass@localhost:5432/mydb") as conn:
        rows = conn.query("SELECT * FROM users WHERE id = $1", [1])
        for row in rows:
            print(row)

    # Production setup with SSL and timeouts
    from db_connector import ConnectionConfig, SslMode, create_pool
    
    config = ConnectionConfig(
        host="db.example.com",
        port=5432,
        user="app_user",
        password="secret",
        database="production_db",
        pool_size=20,
        ssl_mode=SslMode.Require,
        connect_timeout_secs=10,
        statement_timeout_secs=30
    )
    
    with create_pool(config) as pool:
        users = pool.query("SELECT * FROM users")
        pool.execute("INSERT INTO logs (msg) VALUES ($1)", ["connected"])
"""

from db_connector._internal import (
    SslMode,
    ConnectionConfig,
    AsyncPool,
    Connection,
    create_pool,
    connect,
    connect_url,
)

__version__ = "0.1.0"
__all__ = [
    "SslMode",
    "ConnectionConfig",
    "AsyncPool", 
    "Connection",
    "create_pool",
    "connect",
    "connect_url",
]

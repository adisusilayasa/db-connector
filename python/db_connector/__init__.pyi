"""
Type stubs for db_connector

These type hints enable IDE autocompletion and type checking.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from enum import IntEnum

class SslMode(IntEnum):
    """SSL connection modes."""
    Disable = 0
    Prefer = 1
    Require = 2

class ConnectionConfig:
    """Database connection configuration."""
    
    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int
    ssl_mode: SslMode
    connect_timeout_secs: int
    statement_timeout_secs: int
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "",
        database: str = "postgres",
        pool_size: int = 10,
        ssl_mode: SslMode = SslMode.Disable,
        connect_timeout_secs: int = 30,
        statement_timeout_secs: int = 30,
    ) -> None: ...
    
    @staticmethod
    def from_url(url: str) -> "ConnectionConfig":
        """Create config from connection URL."""
        ...
    
    def with_pool_size(self, pool_size: int) -> "ConnectionConfig":
        """Return copy with modified pool size."""
        ...
    
    def with_ssl(self, ssl_mode: SslMode) -> "ConnectionConfig":
        """Return copy with modified SSL mode."""
        ...
    
    def with_timeouts(
        self, 
        connect_timeout_secs: int, 
        statement_timeout_secs: int
    ) -> "ConnectionConfig":
        """Return copy with modified timeouts."""
        ...

class Connection:
    """Single database connection."""
    
    def __init__(
        self, 
        config: ConnectionConfig, 
        accept_invalid_certs: bool = False
    ) -> None: ...
    
    def query(
        self, 
        sql: str, 
        params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute query and return rows as list of dicts."""
        ...
    
    def execute(
        self, 
        sql: str, 
        params: Optional[List[Any]] = None
    ) -> int:
        """Execute statement and return affected row count."""
        ...
    
    def is_closed(self) -> bool:
        """Check if connection is closed."""
        ...
    
    def close(self) -> None:
        """Close the connection."""
        ...
    
    def __enter__(self) -> "Connection": ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...

class AsyncPool:
    """Connection pool for production use."""
    
    def __init__(
        self, 
        config: ConnectionConfig, 
        accept_invalid_certs: bool = False
    ) -> None: ...
    
    def query(
        self, 
        sql: str, 
        params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute query and return rows as list of dicts."""
        ...
    
    def fetch_one(
        self, 
        sql: str, 
        params: Optional[List[Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch single row or None."""
        ...
    
    def execute(
        self, 
        sql: str, 
        params: Optional[List[Any]] = None
    ) -> int:
        """Execute statement and return affected row count."""
        ...
    
    def execute_many(
        self, 
        statements: List[Tuple[str, Optional[List[Any]]]]
    ) -> List[int]:
        """Execute multiple statements in a transaction."""
        ...
    
    def execute_batch(
        self, 
        sql: str, 
        params_list: List[List[Any]]
    ) -> int:
        """High-performance bulk insert with prepared statement reuse."""
        ...
    
    def execute_raw(self, sql: str) -> None:
        """Execute raw SQL batch (multiple statements separated by semicolons)."""
        ...
    
    def is_healthy(self) -> bool:
        """Check if pool connections are healthy."""
        ...
    
    def pool_status(self) -> Dict[str, int]:
        """Get pool statistics: size, available, waiting."""
        ...
    
    def close(self) -> None:
        """Close all connections in the pool."""
        ...
    
    def __enter__(self) -> "AsyncPool": ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...

def create_pool(
    config: ConnectionConfig, 
    accept_invalid_certs: bool = False
) -> AsyncPool:
    """Create a connection pool."""
    ...

def connect(
    config: ConnectionConfig, 
    accept_invalid_certs: bool = False
) -> Connection:
    """Create a single connection."""
    ...

def connect_url(
    url: str, 
    accept_invalid_certs: bool = False
) -> Connection:
    """Create connection from URL string."""
    ...

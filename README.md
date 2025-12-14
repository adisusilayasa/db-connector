# db_connector 🦀🐍

A high-performance PostgreSQL connector for Python, written in Rust. Built for production with connection pooling, SSL/TLS, timeouts, and automatic type conversion.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)

## Features

- ⚡ **High Performance** - Built on `tokio-postgres` for async I/O
- 🏊 **Connection Pooling** - Production-ready pool with `deadpool-postgres`
- 🔒 **SSL/TLS Support** - Secure connections with configurable SSL modes
- ⏱️ **Timeouts** - Connection and statement timeouts prevent hanging
- 🔐 **Type Safe** - Automatic Python ↔ PostgreSQL type conversion
- 📦 **Transactions** - Execute multiple statements atomically
- 🛡️ **SQL Injection Safe** - Parameterized queries with `$1, $2, ...` syntax
- 🐍 **Pythonic API** - Context managers, type hints, familiar interface

---

## Installation

### From Source (Recommended for Development)

```bash
# 1. Install Rust (if not installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. Clone and build
git clone https://github.com/yourusername/db-connector.git
cd db-connector

# 3. Create virtual environment and install
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install maturin
maturin develop --release

# 4. Verify installation
python -c "from db_connector import connect_url; print('✅ Installed!')"
```

### Build Wheel for Distribution

```bash
maturin build --release
pip install target/wheels/db_connector-*.whl
```

---

## Quick Start

### Basic Usage

```python
from db_connector import connect_url

# Connect and query
conn = connect_url("postgresql://postgres:password@localhost:5432/mydb")
rows = conn.query("SELECT * FROM users WHERE age > $1", [21])

for row in rows:
    print(f"{row['name']}: {row['age']} years old")

conn.close()
```

### Using Context Manager (Recommended)

```python
from db_connector import connect_url

with connect_url("postgresql://user:pass@localhost:5432/mydb") as conn:
    # Connection automatically closes when exiting the block
    users = conn.query("SELECT * FROM users")
    conn.execute("INSERT INTO logs (msg) VALUES ($1)", ["User list viewed"])
```

### Using Connection Pool (Production)

```python
from db_connector import ConnectionConfig, SslMode, create_pool

# Configure for production
config = ConnectionConfig(
    host="db.example.com",
    port=5432,
    user="app_user",
    password="secret123",
    database="production",
    pool_size=20,                 # Max connections
    ssl_mode=SslMode.Require,     # Require SSL
    connect_timeout_secs=10,      # Connection timeout
    statement_timeout_secs=30,    # Query timeout
)

# Create pool (reuse across your application)
with create_pool(config) as pool:
    # Pool manages connections automatically
    users = pool.query("SELECT * FROM users LIMIT 10")
    user = pool.fetch_one("SELECT * FROM users WHERE id = $1", [123])
    
    # Check pool health
    print(pool.pool_status())  # {'size': 20, 'available': 19, 'waiting': 0}
```

---

## Using in Your Python Project

### 1. Add as Dependency

If you've built a wheel:

```bash
pip install /path/to/db_connector-0.1.0-*.whl
```

Or install directly from the source directory:

```bash
pip install /path/to/db-connector
```

### 2. Create a Database Module

Create `db.py` in your project:

```python
"""Database connection module."""
import os
from db_connector import ConnectionConfig, SslMode, create_pool, AsyncPool

# Load from environment variables (recommended)
_config = ConnectionConfig(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("DB_NAME", "myapp"),
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    ssl_mode=SslMode.Require if os.getenv("DB_SSL") == "true" else SslMode.Disable,
    connect_timeout_secs=10,
    statement_timeout_secs=30,
)

# Global pool instance
_pool: AsyncPool | None = None

def get_pool() -> AsyncPool:
    """Get or create the connection pool."""
    global _pool
    if _pool is None:
        _pool = create_pool(_config)
    return _pool

def close_pool():
    """Close the connection pool."""
    global _pool
    if _pool:
        _pool.close()
        _pool = None
```

### 3. Use in Your Application

```python
from db import get_pool, close_pool

def get_user(user_id: int) -> dict | None:
    pool = get_pool()
    return pool.fetch_one(
        "SELECT * FROM users WHERE id = $1", 
        [user_id]
    )

def create_user(name: str, email: str) -> int:
    pool = get_pool()
    pool.execute(
        "INSERT INTO users (name, email) VALUES ($1, $2)",
        [name, email]
    )
    result = pool.fetch_one("SELECT lastval()")
    return result["lastval"]

def transfer_funds(from_id: int, to_id: int, amount: float):
    """Transfer with transaction safety."""
    pool = get_pool()
    pool.execute_many([
        ("UPDATE accounts SET balance = balance - $1 WHERE id = $2", [amount, from_id]),
        ("UPDATE accounts SET balance = balance + $1 WHERE id = $2", [amount, to_id]),
        ("INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)", 
         [from_id, to_id, amount]),
    ])

# Clean up on shutdown
import atexit
atexit.register(close_pool)
```

### 4. With FastAPI

```python
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from db import get_pool, close_pool

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: pool is created on first use
    yield
    # Shutdown: close pool
    close_pool()

app = FastAPI(lifespan=lifespan)

@app.get("/users/{user_id}")
def get_user(user_id: int):
    pool = get_pool()
    user = pool.fetch_one("SELECT * FROM users WHERE id = $1", [user_id])
    if not user:
        raise HTTPException(404, "User not found")
    return user

@app.get("/users")
def list_users(limit: int = 10):
    pool = get_pool()
    return pool.query("SELECT * FROM users LIMIT $1", [limit])
```

### 5. With Flask

```python
from flask import Flask, g
from db import get_pool, close_pool

app = Flask(__name__)

@app.teardown_appcontext
def shutdown_pool(exception=None):
    # Pool persists across requests, only close on app shutdown
    pass

@app.route('/users/<int:user_id>')
def get_user(user_id):
    pool = get_pool()
    user = pool.fetch_one("SELECT * FROM users WHERE id = $1", [user_id])
    return user or ("Not found", 404)

# Register cleanup
import atexit
atexit.register(close_pool)
```

---

## API Reference

### SslMode

```python
from db_connector import SslMode

SslMode.Disable  # No SSL (development only)
SslMode.Prefer   # Try SSL, fallback to plain
SslMode.Require  # Must use SSL (production)
```

### ConnectionConfig

```python
config = ConnectionConfig(
    host="localhost",           # Database host
    port=5432,                  # Database port
    user="postgres",            # Username
    password="secret",          # Password
    database="mydb",            # Database name
    pool_size=10,               # Max pool connections
    ssl_mode=SslMode.Disable,   # SSL mode
    connect_timeout_secs=30,    # Connection timeout
    statement_timeout_secs=30,  # Query timeout
)

# From URL (supports sslmode and connect_timeout params)
config = ConnectionConfig.from_url(
    "postgresql://user:pass@host:5432/db?sslmode=require&connect_timeout=10"
)

# Builder pattern
config = config.with_pool_size(20).with_ssl(SslMode.Require)
```

### Connection

```python
conn = connect(config)
# or
conn = connect_url("postgresql://...")

conn.query(sql, params=None)    # Returns List[Dict]
conn.execute(sql, params=None)  # Returns int (affected rows)
conn.is_closed()                # Returns bool
conn.close()                    # Close connection

# Context manager
with connect_url("...") as conn:
    rows = conn.query("SELECT 1")
```

### AsyncPool

```python
pool = create_pool(config)

pool.query(sql, params=None)       # Returns List[Dict]
pool.fetch_one(sql, params=None)   # Returns Dict or None
pool.execute(sql, params=None)     # Returns int
pool.execute_many(statements)      # Transaction, returns List[int]
pool.is_healthy()                  # Returns bool
pool.pool_status()                 # Returns {'size': N, 'available': N, 'waiting': N}
pool.close()                       # Close all connections

# Context manager
with create_pool(config) as pool:
    rows = pool.query("SELECT 1")
```

---

## Type Mappings

| PostgreSQL | Python | Notes |
|------------|--------|-------|
| `BOOL` | `bool` | |
| `INT2/4/8` | `int` | |
| `FLOAT4/8` | `float` | |
| `NUMERIC` | `float` | May lose precision |
| `TEXT/VARCHAR` | `str` | |
| `BYTEA` | `bytes` | |
| `UUID` | `str` | UUID string format |
| `JSON/JSONB` | `dict`/`list` | Auto-parsed |
| `DATE` | `datetime.date` | |
| `TIME` | `datetime.time` | |
| `TIMESTAMP` | `datetime.datetime` | |
| `TIMESTAMPTZ` | `datetime.datetime` | With timezone |
| `NULL` | `None` | |

---

## Error Handling

```python
from db_connector import connect_url

try:
    conn = connect_url("postgresql://user:wrong@localhost/db")
except ConnectionError as e:
    print(f"Connection failed: {e}")

try:
    conn.query("SELECT * FROM huge_table")  # Takes too long
except TimeoutError as e:
    print(f"Query timed out: {e}")

try:
    conn.query("INVALID SQL")
except RuntimeError as e:
    print(f"Query error: {e}")
```

---

## Best Practices

### ✅ Do

```python
# Use connection pooling for web apps
pool = create_pool(config)

# Use parameterized queries
pool.query("SELECT * FROM users WHERE id = $1", [user_id])

# Use context managers
with connect_url("...") as conn:
    conn.query("...")

# Set appropriate timeouts
config = ConnectionConfig(..., statement_timeout_secs=30)

# Use SSL in production
config = ConnectionConfig(..., ssl_mode=SslMode.Require)
```

### ❌ Don't

```python
# Don't create new pools per request
def handle_request():
    pool = create_pool(config)  # BAD! Create once, reuse

# Don't concatenate SQL strings
pool.query(f"SELECT * FROM users WHERE id = {user_id}")  # SQL INJECTION!

# Don't use Disable SSL in production
config = ConnectionConfig(..., ssl_mode=SslMode.Disable)  # Insecure!
```

---

## Troubleshooting

### Connection Refused
```
ConnectionError: Connection failed: connection refused
```
- Check PostgreSQL is running: `pg_isready -h localhost -p 5432`
- Verify `pg_hba.conf` allows your connection

### SSL Required
```
ConnectionError: SSL Connection failed: ...
```
- Ensure PostgreSQL has SSL enabled
- For self-signed certs: `connect_url("...", accept_invalid_certs=True)`

### Timeout
```
TimeoutError: Query timed out after 30s
```
- Increase `statement_timeout_secs` or optimize query
- Check for table locks or slow queries

### Pool Exhausted
```
Pool error: Timeout waiting for connection
```
- Increase `pool_size`
- Ensure connections are released (use context managers)
- Check for connection leaks

---

## Performance

### High-Performance Bulk Insert

Use `execute_batch()` for inserting many rows - it prepares the statement once and reuses it:

```python
# 10x faster than execute_many for bulk inserts
pool.execute_batch(
    "INSERT INTO users (name, email) VALUES ($1, $2)",
    [
        ["Alice", "alice@example.com"],
        ["Bob", "bob@example.com"],
        ["Charlie", "charlie@example.com"],
        # ... thousands more rows
    ]
)
```

### Raw SQL Batch Execution

Use `execute_raw()` for DDL or migrations:

```python
pool.execute_raw("""
    CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT);
    CREATE INDEX IF NOT EXISTS idx_users_name ON users(name);
    INSERT INTO users (name) VALUES ('admin');
""")
```

### Build Optimizations

The release build includes maximum optimizations:

| Optimization | Setting | Benefit |
|--------------|---------|---------|
| LTO | `lto = "fat"` | Cross-module optimization |
| Codegen Units | `codegen-units = 1` | Better inlining |
| Opt Level | `opt-level = 3` | Maximum speed |
| Panic | `panic = "abort"` | Smaller, faster binary |

Always build with `--release` for production:
```bash
maturin build --release
```

### Performance Tips

1. **Use `execute_batch()`** for bulk inserts (10x faster than loops)
2. **Reuse pool instances** - Create once, use everywhere  
3. **Set appropriate pool size** - Start with 10-20, adjust based on load
4. **Use `fetch_one()`** instead of `query()` for single rows
5. **Add LIMIT** - Don't fetch more rows than needed
6. **Index your columns** - Ensure WHERE columns are indexed

---

## Development

```bash
# Setup
git clone https://github.com/yourusername/db-connector
cd db-connector
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest

# Build
maturin develop

# Test (requires PostgreSQL)
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
pytest tests/ -v

# Build release wheel
maturin build --release
```

---

## License

MIT
# db-connector

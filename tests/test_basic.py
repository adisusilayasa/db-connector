"""
Basic tests for db_connector

To run these tests, you need a PostgreSQL instance running.
Set the DATABASE_URL environment variable or use the default:
postgresql://postgres:postgres@localhost:5432/postgres
"""

import os
import pytest
from datetime import datetime, date, time

# Import will fail until the library is built
try:
    from db_connector import ConnectionConfig, create_pool, connect, connect_url
    LIBRARY_AVAILABLE = True
except ImportError:
    LIBRARY_AVAILABLE = False


DATABASE_URL = os.environ.get(
    "DATABASE_URL", 
    "postgresql://postgres:postgres@localhost:5432/postgres"
)


@pytest.fixture
def config():
    """Create a test configuration."""
    return ConnectionConfig.from_url(DATABASE_URL)


@pytest.fixture
def pool(config):
    """Create a test pool."""
    return create_pool(config)


@pytest.fixture
def connection(config):
    """Create a test connection."""
    conn = connect(config)
    yield conn
    conn.close()


@pytest.mark.skipif(not LIBRARY_AVAILABLE, reason="Library not built")
class TestConnectionConfig:
    def test_from_url(self):
        config = ConnectionConfig.from_url("postgresql://user:pass@localhost:5432/testdb")
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.user == "user"
        assert config.password == "pass"
        assert config.database == "testdb"

    def test_constructor(self):
        config = ConnectionConfig(
            host="myhost",
            port=5433,
            user="myuser",
            password="mypass",
            database="mydb",
            pool_size=20
        )
        assert config.host == "myhost"
        assert config.port == 5433
        assert config.pool_size == 20


@pytest.mark.skipif(not LIBRARY_AVAILABLE, reason="Library not built")
class TestConnection:
    def test_connect_url(self):
        """Test connecting with URL."""
        conn = connect_url(DATABASE_URL)
        assert conn is not None
        conn.close()

    def test_query(self, connection):
        """Test basic query."""
        rows = connection.query("SELECT 1 as num, 'hello' as greeting")
        assert len(rows) == 1
        assert rows[0]["num"] == 1
        assert rows[0]["greeting"] == "hello"

    def test_query_with_params(self, connection):
        """Test parameterized query."""
        rows = connection.query(
            "SELECT $1::int as a, $2::text as b", 
            [42, "world"]
        )
        assert rows[0]["a"] == 42
        assert rows[0]["b"] == "world"


@pytest.mark.skipif(not LIBRARY_AVAILABLE, reason="Library not built")
class TestPool:
    def test_pool_creation(self, config):
        """Test pool creation."""
        pool = create_pool(config)
        assert pool is not None
        assert pool.is_healthy()

    def test_pool_status(self, pool):
        """Test pool status."""
        status = pool.pool_status()
        assert "size" in status
        assert "available" in status
        assert "waiting" in status

    def test_query(self, pool):
        """Test query via pool."""
        rows = pool.query("SELECT 'test'::text as value")
        assert len(rows) == 1
        assert rows[0]["value"] == "test"

    def test_fetch_one(self, pool):
        """Test fetching single row."""
        row = pool.fetch_one("SELECT 123 as num")
        assert row is not None
        assert row["num"] == 123

    def test_fetch_one_no_result(self, pool):
        """Test fetching when no rows match."""
        row = pool.fetch_one("SELECT 1 WHERE false")
        assert row is None

    def test_execute(self, pool):
        """Test execute (no results)."""
        # Create temp table
        pool.execute("CREATE TEMP TABLE test_exec (id int)")
        
        # Insert
        count = pool.execute("INSERT INTO test_exec VALUES (1), (2), (3)")
        assert count == 3
        
        # Update
        count = pool.execute("UPDATE test_exec SET id = id + 10 WHERE id > 1")
        assert count == 2
        
        # Delete
        count = pool.execute("DELETE FROM test_exec WHERE id > 10")
        assert count == 2


@pytest.mark.skipif(not LIBRARY_AVAILABLE, reason="Library not built")
class TestTypeConversions:
    def test_integer_types(self, pool):
        """Test integer type conversions."""
        row = pool.fetch_one("""
            SELECT 
                1::smallint as small,
                1000::integer as medium,
                9999999999::bigint as big
        """)
        assert row["small"] == 1
        assert row["medium"] == 1000
        assert row["big"] == 9999999999

    def test_float_types(self, pool):
        """Test float type conversions."""
        row = pool.fetch_one("""
            SELECT 
                3.14::real as float4,
                3.14159265359::double precision as float8
        """)
        assert abs(row["float4"] - 3.14) < 0.01
        assert abs(row["float8"] - 3.14159265359) < 0.0001

    def test_string_types(self, pool):
        """Test string type conversions."""
        row = pool.fetch_one("""
            SELECT 
                'hello'::text as txt,
                'world'::varchar(10) as vc
        """)
        assert row["txt"] == "hello"
        assert row["vc"] == "world"

    def test_boolean(self, pool):
        """Test boolean conversion."""
        row = pool.fetch_one("SELECT true as yes, false as no")
        assert row["yes"] is True
        assert row["no"] is False

    def test_null(self, pool):
        """Test NULL conversion."""
        row = pool.fetch_one("SELECT NULL::text as nothing")
        assert row["nothing"] is None

    def test_json(self, pool):
        """Test JSON/JSONB conversion."""
        row = pool.fetch_one("""
            SELECT 
                '{"key": "value"}'::jsonb as obj,
                '[1, 2, 3]'::jsonb as arr
        """)
        assert row["obj"] == {"key": "value"}
        assert row["arr"] == [1, 2, 3]

    def test_uuid(self, pool):
        """Test UUID conversion."""
        row = pool.fetch_one("SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid as id")
        assert row["id"] == "550e8400-e29b-41d4-a716-446655440000"

    def test_date_types(self, pool):
        """Test date/time type conversions."""
        row = pool.fetch_one("""
            SELECT 
                '2024-01-15'::date as d,
                '2024-01-15 10:30:00'::timestamp as ts
        """)
        assert isinstance(row["d"], date)
        assert row["d"].year == 2024
        assert row["d"].month == 1
        assert row["d"].day == 15
        
        assert isinstance(row["ts"], datetime)
        assert row["ts"].hour == 10
        assert row["ts"].minute == 30


@pytest.mark.skipif(not LIBRARY_AVAILABLE, reason="Library not built")
class TestTransactions:
    def test_execute_many(self, pool):
        """Test transaction with multiple statements."""
        # Create temp table
        pool.execute("CREATE TEMP TABLE tx_test (name text, value int)")
        
        # Execute multiple inserts in transaction
        counts = pool.execute_many([
            ("INSERT INTO tx_test VALUES ($1, $2)", ["a", 1]),
            ("INSERT INTO tx_test VALUES ($1, $2)", ["b", 2]),
            ("INSERT INTO tx_test VALUES ($1, $2)", ["c", 3]),
        ])
        
        assert counts == [1, 1, 1]
        
        # Verify all were inserted
        rows = pool.query("SELECT * FROM tx_test ORDER BY value")
        assert len(rows) == 3
        assert rows[0]["name"] == "a"
        assert rows[2]["name"] == "c"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

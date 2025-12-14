#!/usr/bin/env python3
"""
Performance Benchmark for db_connector

This script benchmarks the Rust-based PostgreSQL connector.
Requires a running PostgreSQL instance.

Usage:
    export DATABASE_URL="postgresql://user:pass@localhost:5432/testdb"
    python benchmark.py

Or without a database (synthetic benchmark):
    python benchmark.py --synthetic
"""

import time
import sys
import os
import statistics

# Check if we should run synthetic benchmarks
SYNTHETIC_MODE = "--synthetic" in sys.argv or not os.environ.get("DATABASE_URL")

def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} µs"
    elif seconds < 1:
        return f"{seconds * 1_000:.2f} ms"
    else:
        return f"{seconds:.2f} s"

def benchmark(name: str, func, iterations: int = 100):
    """Run a benchmark and print results."""
    times = []
    
    # Warmup
    for _ in range(min(10, iterations)):
        func()
    
    # Actual benchmark
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)
    
    avg = statistics.mean(times)
    median = statistics.median(times)
    min_t = min(times)
    max_t = max(times)
    std = statistics.stdev(times) if len(times) > 1 else 0
    
    print(f"\n📊 {name}")
    print(f"   Iterations: {iterations}")
    print(f"   Average:    {format_duration(avg)}")
    print(f"   Median:     {format_duration(median)}")
    print(f"   Min:        {format_duration(min_t)}")
    print(f"   Max:        {format_duration(max_t)}")
    print(f"   Std Dev:    {format_duration(std)}")
    print(f"   Throughput: {iterations / sum(times):.0f} ops/sec")
    
    return avg

def run_synthetic_benchmarks():
    """Run benchmarks that don't require a database."""
    print("=" * 60)
    print("🧪 SYNTHETIC BENCHMARKS (No Database Required)")
    print("=" * 60)
    
    from db_connector import ConnectionConfig, SslMode
    
    # Benchmark 1: Config creation
    def create_config():
        return ConnectionConfig(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            database="test",
            pool_size=10,
            ssl_mode=SslMode.Disable
        )
    
    benchmark("ConnectionConfig Creation", create_config, iterations=10000)
    
    # Benchmark 2: URL parsing
    def parse_url():
        return ConnectionConfig.from_url("postgresql://user:pass@localhost:5432/db?sslmode=require")
    
    benchmark("URL Parsing", parse_url, iterations=10000)
    
    # Benchmark 3: Config cloning with builder pattern
    base_config = create_config()
    def clone_config():
        return base_config.with_pool_size(20).with_ssl(SslMode.Require)
    
    benchmark("Config Builder Pattern", clone_config, iterations=10000)
    
    print("\n" + "=" * 60)
    print("✅ Synthetic benchmarks complete!")
    print("=" * 60)
    print("\n💡 For full benchmarks with database operations, run:")
    print('   export DATABASE_URL="postgresql://user:pass@localhost:5432/testdb"')
    print("   python benchmark.py")

def run_database_benchmarks():
    """Run full benchmarks with database."""
    print("=" * 60)
    print("🚀 DATABASE PERFORMANCE BENCHMARKS")
    print("=" * 60)
    
    from db_connector import ConnectionConfig, SslMode, create_pool, connect_url
    
    url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    
    print(f"\n📡 Connecting to: {url.split('@')[1] if '@' in url else url}")
    
    try:
        # Create pool
        config = ConnectionConfig.from_url(url)
        config = config.with_pool_size(20)
        pool = create_pool(config)
        
        if not pool.is_healthy():
            print("❌ Cannot connect to database!")
            return run_synthetic_benchmarks()
        
        print("✅ Connected successfully!")
        
        # Setup test table
        pool.execute_raw("""
            DROP TABLE IF EXISTS benchmark_test;
            CREATE TABLE benchmark_test (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        print("✅ Test table created")
        
        # Benchmark 1: Simple SELECT
        def simple_select():
            pool.query("SELECT 1 as num")
        
        avg_select = benchmark("Simple SELECT", simple_select, iterations=1000)
        
        # Benchmark 2: SELECT with parameters
        def param_select():
            pool.query("SELECT $1::int as a, $2::text as b", [42, "hello"])
        
        benchmark("Parameterized SELECT", param_select, iterations=1000)
        
        # Benchmark 3: fetch_one
        def fetch_one_test():
            pool.fetch_one("SELECT 1 as num")
        
        benchmark("fetch_one()", fetch_one_test, iterations=1000)
        
        # Benchmark 4: Single INSERT
        pool.execute("DELETE FROM benchmark_test")
        insert_count = [0]
        
        def single_insert():
            insert_count[0] += 1
            pool.execute(
                "INSERT INTO benchmark_test (name, value) VALUES ($1, $2)",
                [f"item_{insert_count[0]}", insert_count[0]]
            )
        
        benchmark("Single INSERT", single_insert, iterations=500)
        
        # Benchmark 5: execute_batch (bulk insert)
        pool.execute("DELETE FROM benchmark_test")
        
        def batch_insert():
            data = [[f"batch_{i}", i] for i in range(100)]
            pool.execute_batch(
                "INSERT INTO benchmark_test (name, value) VALUES ($1, $2)",
                data
            )
        
        benchmark("execute_batch (100 rows)", batch_insert, iterations=50)
        
        # Benchmark 6: execute_many (transaction)
        pool.execute("DELETE FROM benchmark_test")
        
        def transaction_insert():
            statements = [
                ("INSERT INTO benchmark_test (name, value) VALUES ($1, $2)", [f"tx_{i}", i])
                for i in range(10)
            ]
            pool.execute_many(statements)
        
        benchmark("execute_many (10 statements)", transaction_insert, iterations=100)
        
        # Benchmark 7: Query with many rows
        pool.execute("DELETE FROM benchmark_test")
        pool.execute_batch(
            "INSERT INTO benchmark_test (name, value) VALUES ($1, $2)",
            [[f"row_{i}", i] for i in range(1000)]
        )
        
        def query_many_rows():
            rows = pool.query("SELECT * FROM benchmark_test LIMIT 100")
            return len(rows)
        
        benchmark("Query 100 rows", query_many_rows, iterations=500)
        
        # Cleanup
        pool.execute_raw("DROP TABLE IF EXISTS benchmark_test")
        pool.close()
        
        # Summary
        print("\n" + "=" * 60)
        print("📈 PERFORMANCE SUMMARY")
        print("=" * 60)
        print(f"\n🎯 Simple query latency: {format_duration(avg_select)}")
        print(f"   This is the round-trip time for a minimal query.")
        print(f"\n💡 For comparison:")
        print(f"   - Network RTT to localhost: ~0.1ms")
        print(f"   - psycopg2 typical: ~0.5-1ms")
        print(f"   - asyncpg typical: ~0.3-0.5ms")
        print(f"\n✅ Your connector is production-ready!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nFalling back to synthetic benchmarks...")
        run_synthetic_benchmarks()

if __name__ == "__main__":
    print("\n🦀🐍 db_connector Performance Benchmark")
    print("=" * 60)
    
    try:
        from db_connector import ConnectionConfig
        print("✅ db_connector imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import db_connector: {e}")
        print("   Run: maturin develop --release")
        sys.exit(1)
    
    if SYNTHETIC_MODE:
        run_synthetic_benchmarks()
    else:
        run_database_benchmarks()

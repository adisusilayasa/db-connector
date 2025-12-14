//! db_connector - A high-performance PostgreSQL connector for Python
//!
//! This library provides both synchronous and asynchronous interfaces
//! for connecting to PostgreSQL databases from Python.

use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError, PyTimeoutError, PyConnectionError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_postgres::Client;
use deadpool_postgres::{Config, Pool, Runtime, ManagerConfig, RecyclingMethod, SslMode as DeadpoolSslMode};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;

mod error;
mod types;

use error::DbError;
use types::{PyValue, row_to_dict};

/// SSL Mode for database connections
#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum SslMode {
    /// No SSL (default, for development)
    Disable = 0,
    /// Try SSL, fall back to non-SSL if unavailable
    Prefer = 1,
    /// Require SSL connection
    Require = 2,
}

#[pymethods]
impl SslMode {
    #[new]
    fn new(value: u8) -> PyResult<Self> {
        match value {
            0 => Ok(SslMode::Disable),
            1 => Ok(SslMode::Prefer),
            2 => Ok(SslMode::Require),
            _ => Err(PyValueError::new_err("Invalid SSL mode. Use 0=Disable, 1=Prefer, 2=Require")),
        }
    }
}

/// Connection configuration with production-ready options
#[pyclass]
#[derive(Clone)]
pub struct ConnectionConfig {
    #[pyo3(get, set)]
    pub host: String,
    #[pyo3(get, set)]
    pub port: u16,
    #[pyo3(get, set)]
    pub user: String,
    #[pyo3(get, set)]
    pub password: String,
    #[pyo3(get, set)]
    pub database: String,
    #[pyo3(get, set)]
    pub pool_size: usize,
    #[pyo3(get, set)]
    pub ssl_mode: SslMode,
    #[pyo3(get, set)]
    pub connect_timeout_secs: u64,
    #[pyo3(get, set)]
    pub statement_timeout_secs: u64,
}

#[pymethods]
impl ConnectionConfig {
    #[new]
    #[pyo3(signature = (
        host="localhost".to_string(),
        port=5432,
        user="postgres".to_string(),
        password="".to_string(),
        database="postgres".to_string(),
        pool_size=10,
        ssl_mode=SslMode::Disable,
        connect_timeout_secs=30,
        statement_timeout_secs=30
    ))]
    fn new(
        host: String,
        port: u16,
        user: String,
        password: String,
        database: String,
        pool_size: usize,
        ssl_mode: SslMode,
        connect_timeout_secs: u64,
        statement_timeout_secs: u64,
    ) -> Self {
        ConnectionConfig {
            host,
            port,
            user,
            password,
            database,
            pool_size,
            ssl_mode,
            connect_timeout_secs,
            statement_timeout_secs,
        }
    }

    /// Create config from a connection string
    /// Format: postgresql://user:password@host:port/database?sslmode=require&connect_timeout=30
    #[staticmethod]
    fn from_url(url: &str) -> PyResult<Self> {
        let url = url.trim_start_matches("postgresql://").trim_start_matches("postgres://");
        
        // Split query params if present
        let (main_part, query) = url.split_once('?').unwrap_or((url, ""));
        
        // Parse SSL mode from query params
        let ssl_mode = if query.contains("sslmode=require") {
            SslMode::Require
        } else if query.contains("sslmode=prefer") {
            SslMode::Prefer
        } else {
            SslMode::Disable
        };

        // Parse connect_timeout
        let connect_timeout_secs = query.split('&')
            .find(|p| p.starts_with("connect_timeout="))
            .and_then(|p| p.strip_prefix("connect_timeout="))
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        
        let (auth, rest) = main_part.split_once('@').ok_or_else(|| {
            PyValueError::new_err("Invalid connection URL format. Expected: postgresql://user:pass@host:port/database")
        })?;
        
        let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
        
        let (host_port, database) = rest.split_once('/').ok_or_else(|| {
            PyValueError::new_err("Invalid connection URL format. Missing database name after /")
        })?;
        
        let (host, port_str) = host_port.split_once(':').unwrap_or((host_port, "5432"));
        let port: u16 = port_str.parse().map_err(|_| {
            PyValueError::new_err(format!("Invalid port number: {}", port_str))
        })?;
        
        Ok(ConnectionConfig {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            pool_size: 10,
            ssl_mode,
            connect_timeout_secs,
            statement_timeout_secs: 30,
        })
    }

    /// Return a copy of the config with modified pool_size
    fn with_pool_size(&self, pool_size: usize) -> Self {
        let mut config = self.clone();
        config.pool_size = pool_size;
        config
    }

    /// Return a copy of the config with modified SSL mode
    fn with_ssl(&self, ssl_mode: SslMode) -> Self {
        let mut config = self.clone();
        config.ssl_mode = ssl_mode;
        config
    }

    /// Return a copy of the config with modified timeouts
    fn with_timeouts(&self, connect_timeout_secs: u64, statement_timeout_secs: u64) -> Self {
        let mut config = self.clone();
        config.connect_timeout_secs = connect_timeout_secs;
        config.statement_timeout_secs = statement_timeout_secs;
        config
    }

    fn __repr__(&self) -> String {
        format!(
            "ConnectionConfig(host='{}', port={}, user='{}', database='{}', pool_size={}, ssl_mode={:?})",
            self.host, self.port, self.user, self.database, self.pool_size, self.ssl_mode
        )
    }
}

/// Create a TLS connector for SSL connections
fn create_tls_connector(accept_invalid_certs: bool) -> Result<MakeTlsConnector, Box<dyn std::error::Error + Send + Sync>> {
    let tls_connector = TlsConnector::builder()
        .danger_accept_invalid_certs(accept_invalid_certs)
        .build()?;
    Ok(MakeTlsConnector::new(tls_connector))
}

/// PostgreSQL connection pool with production features
#[pyclass]
pub struct AsyncPool {
    pool: Pool,
    runtime: Arc<tokio::runtime::Runtime>,
    statement_timeout: Duration,
}

#[pymethods]
impl AsyncPool {
    #[new]
    #[pyo3(signature = (config, accept_invalid_certs=false))]
    fn new(config: &ConnectionConfig, accept_invalid_certs: bool) -> PyResult<Self> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create async runtime: {}", e)))?;

        let mut cfg = Config::new();
        cfg.host = Some(config.host.clone());
        cfg.port = Some(config.port);
        cfg.user = Some(config.user.clone());
        cfg.password = Some(config.password.clone());
        cfg.dbname = Some(config.database.clone());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        // Set SSL mode
        cfg.ssl_mode = Some(match config.ssl_mode {
            SslMode::Disable => DeadpoolSslMode::Disable,
            SslMode::Prefer => DeadpoolSslMode::Prefer,
            SslMode::Require => DeadpoolSslMode::Require,
        });

        let pool = match config.ssl_mode {
            SslMode::Disable => {
                cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to create pool: {}", e)))?
            }
            SslMode::Prefer | SslMode::Require => {
                let tls = create_tls_connector(accept_invalid_certs)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to create TLS connector: {}", e)))?;
                cfg.create_pool(Some(Runtime::Tokio1), tls)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to create pool: {}", e)))?
            }
        };

        Ok(AsyncPool {
            pool,
            runtime: Arc::new(runtime),
            statement_timeout: Duration::from_secs(config.statement_timeout_secs),
        })
    }

    /// Execute a query and return rows as list of dicts
    #[pyo3(signature = (sql, params=None))]
    fn query<'py>(&self, py: Python<'py>, sql: &str, params: Option<Vec<PyValue>>) -> PyResult<Bound<'py, pyo3::types::PyList>> {
        let sql = sql.to_string();
        let params = params.unwrap_or_default();
        let stmt_timeout = self.statement_timeout;
        
        let rows = self.runtime.block_on(async {
            let client = self.pool.get().await.map_err(DbError::Pool)?;
            
            let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            
            let result = timeout(stmt_timeout, client.query(&sql[..], &params_refs)).await
                .map_err(|_| DbError::Timeout(format!("Query timed out after {:?}", stmt_timeout)))?
                .map_err(DbError::Query)?;
            
            Ok::<_, DbError>(result)
        }).map_err(|e: DbError| match e {
            DbError::Timeout(msg) => PyTimeoutError::new_err(msg),
            DbError::Pool(e) => PyConnectionError::new_err(format!("Pool error: {}", e)),
            _ => PyRuntimeError::new_err(e.to_string()),
        })?;

        let result = pyo3::types::PyList::empty_bound(py);
        for row in rows {
            let dict = row_to_dict(py, &row)?;
            result.append(dict)?;
        }
        
        Ok(result)
    }

    /// Execute a query without returning results (INSERT, UPDATE, DELETE)
    #[pyo3(signature = (sql, params=None))]
    fn execute(&self, sql: &str, params: Option<Vec<PyValue>>) -> PyResult<u64> {
        let sql = sql.to_string();
        let params = params.unwrap_or_default();
        let stmt_timeout = self.statement_timeout;
        
        let count = self.runtime.block_on(async {
            let client = self.pool.get().await.map_err(DbError::Pool)?;
            
            let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            
            let result = timeout(stmt_timeout, client.execute(&sql[..], &params_refs)).await
                .map_err(|_| DbError::Timeout(format!("Execute timed out after {:?}", stmt_timeout)))?
                .map_err(DbError::Query)?;
            
            Ok::<_, DbError>(result)
        }).map_err(|e: DbError| match e {
            DbError::Timeout(msg) => PyTimeoutError::new_err(msg),
            DbError::Pool(e) => PyConnectionError::new_err(format!("Pool error: {}", e)),
            _ => PyRuntimeError::new_err(e.to_string()),
        })?;

        Ok(count)
    }

    /// Execute many statements in a transaction
    fn execute_many(&self, statements: Vec<(String, Option<Vec<PyValue>>)>) -> PyResult<Vec<u64>> {
        let stmt_timeout = self.statement_timeout;
        
        let results = self.runtime.block_on(async {
            let mut client = self.pool.get().await.map_err(DbError::Pool)?;
            let transaction = client.transaction().await.map_err(DbError::Query)?;
            
            let mut counts = Vec::new();
            for (sql, params) in statements {
                let params = params.unwrap_or_default();
                let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                    params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                
                let count = timeout(stmt_timeout, transaction.execute(&sql[..], &params_refs)).await
                    .map_err(|_| DbError::Timeout(format!("Transaction statement timed out after {:?}", stmt_timeout)))?
                    .map_err(DbError::Query)?;
                counts.push(count);
            }
            
            transaction.commit().await.map_err(DbError::Query)?;
            Ok::<_, DbError>(counts)
        }).map_err(|e: DbError| match e {
            DbError::Timeout(msg) => PyTimeoutError::new_err(msg),
            _ => PyRuntimeError::new_err(e.to_string()),
        })?;

        Ok(results)
    }

    /// High-performance bulk insert using a single prepared statement
    /// Much faster than execute_many for inserting many rows with the same SQL
    #[pyo3(signature = (sql, params_list))]
    fn execute_batch(&self, sql: &str, params_list: Vec<Vec<PyValue>>) -> PyResult<u64> {
        let sql = sql.to_string();
        let stmt_timeout = self.statement_timeout;
        
        let total = self.runtime.block_on(async {
            let client = self.pool.get().await.map_err(DbError::Pool)?;
            
            // Prepare statement once, reuse for all rows
            let statement = timeout(stmt_timeout, client.prepare(&sql)).await
                .map_err(|_| DbError::Timeout("Statement preparation timed out".to_string()))?
                .map_err(DbError::Query)?;
            
            let mut total_count: u64 = 0;
            for params in params_list {
                let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                    params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                
                let count = timeout(stmt_timeout, client.execute(&statement, &params_refs)).await
                    .map_err(|_| DbError::Timeout("Batch execute timed out".to_string()))?
                    .map_err(DbError::Query)?;
                total_count += count;
            }
            
            Ok::<_, DbError>(total_count)
        }).map_err(|e: DbError| match e {
            DbError::Timeout(msg) => PyTimeoutError::new_err(msg),
            DbError::Pool(e) => PyConnectionError::new_err(format!("Pool error: {}", e)),
            _ => PyRuntimeError::new_err(e.to_string()),
        })?;

        Ok(total)
    }

    /// Execute raw SQL batch (multiple statements separated by semicolons)
    /// Use for schema migrations or bulk DDL operations
    fn execute_raw(&self, sql: &str) -> PyResult<()> {
        let sql = sql.to_string();
        let stmt_timeout = self.statement_timeout;
        
        self.runtime.block_on(async {
            let client = self.pool.get().await.map_err(DbError::Pool)?;
            
            timeout(stmt_timeout, client.batch_execute(&sql)).await
                .map_err(|_| DbError::Timeout("Raw batch execute timed out".to_string()))?
                .map_err(DbError::Query)?;
            
            Ok::<_, DbError>(())
        }).map_err(|e: DbError| match e {
            DbError::Timeout(msg) => PyTimeoutError::new_err(msg),
            _ => PyRuntimeError::new_err(e.to_string()),
        })?;

        Ok(())
    }

    /// Fetch a single row
    #[pyo3(signature = (sql, params=None))]
    fn fetch_one<'py>(&self, py: Python<'py>, sql: &str, params: Option<Vec<PyValue>>) -> PyResult<Option<Bound<'py, pyo3::types::PyDict>>> {
        let sql = sql.to_string();
        let params = params.unwrap_or_default();
        let stmt_timeout = self.statement_timeout;
        
        let row = self.runtime.block_on(async {
            let client = self.pool.get().await.map_err(DbError::Pool)?;
            
            let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            
            let result = timeout(stmt_timeout, client.query_opt(&sql[..], &params_refs)).await
                .map_err(|_| DbError::Timeout(format!("Query timed out after {:?}", stmt_timeout)))?
                .map_err(DbError::Query)?;
            
            Ok::<_, DbError>(result)
        }).map_err(|e: DbError| match e {
            DbError::Timeout(msg) => PyTimeoutError::new_err(msg),
            _ => PyRuntimeError::new_err(e.to_string()),
        })?;

        match row {
            Some(r) => Ok(Some(row_to_dict(py, &r)?)),
            None => Ok(None),
        }
    }

    /// Check if connection is healthy
    fn is_healthy(&self) -> bool {
        self.runtime.block_on(async {
            match timeout(Duration::from_secs(5), self.pool.get()).await {
                Ok(Ok(client)) => {
                    timeout(Duration::from_secs(5), client.query("SELECT 1", &[]))
                        .await
                        .map(|r| r.is_ok())
                        .unwrap_or(false)
                }
                _ => false,
            }
        })
    }

    /// Get pool statistics
    fn pool_status(&self) -> HashMap<String, usize> {
        let status = self.pool.status();
        let mut map = HashMap::new();
        map.insert("size".to_string(), status.size);
        map.insert("available".to_string(), status.available as usize);
        map.insert("waiting".to_string(), status.waiting);
        map
    }

    /// Close all connections in the pool
    fn close(&self) {
        self.pool.close();
    }

    fn __repr__(&self) -> String {
        let status = self.pool.status();
        format!("AsyncPool(size={}, available={}, waiting={})", 
            status.size, status.available, status.waiting)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(&self, _exc_type: Option<PyObject>, _exc_val: Option<PyObject>, _exc_tb: Option<PyObject>) {
        self.close();
    }
}

/// Simple synchronous connection (no pooling)
#[pyclass]
pub struct Connection {
    client: Arc<Mutex<Option<Client>>>,
    runtime: Arc<tokio::runtime::Runtime>,
    statement_timeout: Duration,
}

#[pymethods]
impl Connection {
    #[new]
    #[pyo3(signature = (config, accept_invalid_certs=false))]
    fn new(config: &ConnectionConfig, accept_invalid_certs: bool) -> PyResult<Self> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        let conn_str = format!(
            "host={} port={} user={} password={} dbname={} connect_timeout={}",
            config.host, config.port, config.user, config.password, config.database, config.connect_timeout_secs
        );

        let ssl_mode = config.ssl_mode;
        let connect_timeout = Duration::from_secs(config.connect_timeout_secs);
        
        let client = runtime.block_on(async move {
            let connect_future = async {
                match ssl_mode {
                    SslMode::Disable => {
                        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await
                            .map_err(|e| PyConnectionError::new_err(format!("Connection failed: {}", e)))?;

                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                eprintln!("Connection error: {}", e);
                            }
                        });

                        Ok::<_, PyErr>(client)
                    }
                    SslMode::Prefer | SslMode::Require => {
                        let tls = create_tls_connector(accept_invalid_certs)
                            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create TLS connector: {}", e)))?;

                        let (client, connection) = tokio_postgres::connect(&conn_str, tls).await
                            .map_err(|e| PyConnectionError::new_err(format!("SSL Connection failed: {}", e)))?;

                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                eprintln!("Connection error: {}", e);
                            }
                        });

                        Ok(client)
                    }
                }
            };

            match timeout(connect_timeout, connect_future).await {
                Ok(result) => result,
                Err(_) => Err(PyTimeoutError::new_err(format!(
                    "Connection timed out after {} seconds", connect_timeout.as_secs()
                ))),
            }
        })?;

        Ok(Connection {
            client: Arc::new(Mutex::new(Some(client))),
            runtime: Arc::new(runtime),
            statement_timeout: Duration::from_secs(config.statement_timeout_secs),
        })
    }

    /// Execute a query and return rows
    #[pyo3(signature = (sql, params=None))]
    fn query<'py>(&self, py: Python<'py>, sql: &str, params: Option<Vec<PyValue>>) -> PyResult<Bound<'py, pyo3::types::PyList>> {
        let sql = sql.to_string();
        let params = params.unwrap_or_default();
        let client = self.client.clone();
        let stmt_timeout = self.statement_timeout;
        
        let rows = self.runtime.block_on(async {
            let guard = client.lock().await;
            let client = guard.as_ref().ok_or_else(|| PyRuntimeError::new_err("Connection closed"))?;
            
            let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            
            let result = timeout(stmt_timeout, client.query(&sql[..], &params_refs)).await
                .map_err(|_| PyTimeoutError::new_err(format!("Query timed out after {:?}", stmt_timeout)))?
                .map_err(|e| PyRuntimeError::new_err(format!("Query failed: {}", e)))?;
            
            Ok::<_, PyErr>(result)
        })?;

        let result = pyo3::types::PyList::empty_bound(py);
        for row in rows {
            let dict = row_to_dict(py, &row)?;
            result.append(dict)?;
        }
        
        Ok(result)
    }

    /// Execute without returning results
    #[pyo3(signature = (sql, params=None))]
    fn execute(&self, sql: &str, params: Option<Vec<PyValue>>) -> PyResult<u64> {
        let sql = sql.to_string();
        let params = params.unwrap_or_default();
        let client = self.client.clone();
        let stmt_timeout = self.statement_timeout;
        
        self.runtime.block_on(async {
            let guard = client.lock().await;
            let client = guard.as_ref().ok_or_else(|| PyRuntimeError::new_err("Connection closed"))?;
            
            let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            
            let result = timeout(stmt_timeout, client.execute(&sql[..], &params_refs)).await
                .map_err(|_| PyTimeoutError::new_err(format!("Execute timed out after {:?}", stmt_timeout)))?
                .map_err(|e| PyRuntimeError::new_err(format!("Execute failed: {}", e)))?;
            
            Ok(result)
        })
    }

    /// Check if connection is still open
    fn is_closed(&self) -> bool {
        self.runtime.block_on(async {
            let guard = self.client.lock().await;
            guard.is_none()
        })
    }

    /// Close the connection
    fn close(&self) -> PyResult<()> {
        self.runtime.block_on(async {
            let mut guard = self.client.lock().await;
            *guard = None;
        });
        Ok(())
    }

    fn __repr__(&self) -> String {
        let closed = self.is_closed();
        format!("Connection(closed={})", closed)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(&self, _exc_type: Option<PyObject>, _exc_val: Option<PyObject>, _exc_tb: Option<PyObject>) -> PyResult<()> {
        self.close()
    }
}

/// Create a connection pool
#[pyfunction]
#[pyo3(signature = (config, accept_invalid_certs=false))]
fn create_pool(config: &ConnectionConfig, accept_invalid_certs: bool) -> PyResult<AsyncPool> {
    AsyncPool::new(config, accept_invalid_certs)
}

/// Create a single connection
#[pyfunction]
#[pyo3(signature = (config, accept_invalid_certs=false))]
fn connect(config: &ConnectionConfig, accept_invalid_certs: bool) -> PyResult<Connection> {
    Connection::new(config, accept_invalid_certs)
}

/// Quick connect using connection string
#[pyfunction]
#[pyo3(signature = (url, accept_invalid_certs=false))]
fn connect_url(url: &str, accept_invalid_certs: bool) -> PyResult<Connection> {
    let config = ConnectionConfig::from_url(url)?;
    Connection::new(&config, accept_invalid_certs)
}

/// Python module definition
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SslMode>()?;
    m.add_class::<ConnectionConfig>()?;
    m.add_class::<AsyncPool>()?;
    m.add_class::<Connection>()?;
    m.add_function(wrap_pyfunction!(create_pool, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(connect_url, m)?)?;
    Ok(())
}

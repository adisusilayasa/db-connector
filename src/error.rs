//! Error types for the database connector

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Connection pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),

    #[error("Query execution error: {0}")]
    Query(#[from] tokio_postgres::Error),

    #[error("Operation timed out: {0}")]
    Timeout(String),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

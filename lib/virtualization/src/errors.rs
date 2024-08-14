use bigquery_polars::errors::BigQueryExecutorError;
use gcp_bigquery_client::error::BQError;
use polars::prelude::PolarsError;
use pyo3::PyErr;
use thiserror::*;
use url::ParseError;

#[derive(Error, Debug)]
pub enum ChrontextError {
    #[error(transparent)]
    PyVirtualizedDatabaseError(#[from] PyErr),
    #[error(transparent)]
    PolarsError(#[from] PolarsError),
    #[error("Problem creating dataframe from arrow: `{0}`")]
    TranslationError(String),
    #[error("Invalid node id `{0}`")]
    InvalidNodeIdError(String),
    #[error("Only grouped and basic query types are supported")]
    VirtualizedQueryTypeNotSupported,
    #[error(transparent)]
    ReadFileError(#[from] std::io::Error),
    #[error(transparent)]
    ReadJSONError(#[from] serde_json::Error),
    #[error(transparent)]
    BigQueryExecutorError(#[from] BigQueryExecutorError),
    #[error(transparent)]
    BigQueryKeyPathParseError(#[from] ParseError),
    #[error(transparent)]
    BigQueryError(#[from] BQError),
}

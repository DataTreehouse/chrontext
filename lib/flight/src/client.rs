use polars::prelude::PolarsError;
use query_processing::errors::QueryProcessingError;
use representation::solution_mapping::SolutionMappings;
use secrecy::SecretString;
use std::collections::HashMap;
use thiserror::*;
use tonic::Status;

#[derive(Error, Debug)]
pub enum ChrontextFlightClientError {
    #[error("Cannot create endpoint `{0}`")]
    IpcError(String),
    #[error(transparent)]
    QueryExecutionError(Status),
    #[error(transparent)]
    TypesDeserializationError(bincode::Error),
    #[error(transparent)]
    PolarsDeserializationError(PolarsError),
    #[error(transparent)]
    UnionError(QueryProcessingError),
    #[error(transparent)]
    ConnectError(tonic::transport::Error),
}

#[derive(Clone)]
pub struct ChrontextFlightClient {
    uri: String,
}

impl ChrontextFlightClient {
    pub fn new(uri: &str) -> ChrontextFlightClient {
        Self {
            uri: uri.to_string(),
        }
    }

    pub async fn query(
        &mut self,
        _query: &str,
        _metadata: &HashMap<String, SecretString>,
    ) -> Result<SolutionMappings, ChrontextFlightClientError> {
        unimplemented!("Contact Data Treehouse to try")
    }
}

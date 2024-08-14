use crate::combiner::CombinerError;
use crate::splitter::QueryParseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChrontextError {
    #[error("Missing SPARQL database")]
    NoSPARQLDatabaseDefined,
    #[error("Error creating SPARQL database `{0}`")]
    CreateSPARQLDatabaseError(String),
    #[error("No timeseries database defined")]
    NoTimeseriesDatabaseDefined,
    #[error(transparent)]
    QueryParseError(#[from] QueryParseError),
    #[error(transparent)]
    CombinerError(#[from] CombinerError),
}

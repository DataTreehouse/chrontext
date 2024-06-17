use gcp_bigquery_client::error::BQError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BigQueryExecutorError {
    #[error(transparent)]
    ClientError(BQError),
    #[error("Job reference missing")]
    JobReferenceMissingError,
    #[error("Job id is none")]
    JobIdNoneError,
    #[error("Schema is missing")]
    SchemaMissing,
}

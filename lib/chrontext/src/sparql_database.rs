pub mod sparql_embedded_oxigraph;
pub mod sparql_endpoint;

use crate::sparql_database::sparql_embedded_oxigraph::EmbeddedOxigraphError;
use crate::sparql_database::sparql_endpoint::SparqlEndpointQueryExecutionError;
use async_trait::async_trait;
use sparesults::QuerySolution;
use spargebra::Query;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum SparqlQueryError {
    #[error(transparent)]
    EmbeddedOxigraphError(#[from] EmbeddedOxigraphError),
    #[error(transparent)]
    SparqlEndpointQueryExecutionError(#[from] SparqlEndpointQueryExecutionError),
}

#[async_trait]
pub trait SparqlQueryable: Send + Sync {
    async fn execute(&self, query: &Query) -> Result<Vec<QuerySolution>, SparqlQueryError>;
}

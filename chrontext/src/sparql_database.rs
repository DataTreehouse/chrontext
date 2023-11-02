pub mod sparql_embedded_oxigraph;
pub mod sparql_endpoint;

use async_trait::async_trait;
use sparesults::QuerySolution;
use spargebra::Query;
use std::error::Error;

#[async_trait]
pub trait SparqlQueryable: Send {
    async fn execute(&mut self, query: &Query) -> Result<Vec<QuerySolution>, Box<dyn Error>>;
}

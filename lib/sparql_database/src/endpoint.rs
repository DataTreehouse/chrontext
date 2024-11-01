use super::{parse_json_text, SparqlQueryError, SparqlQueryable};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, USER_AGENT};
use sparesults::{QueryResultsSyntaxError, QuerySolution};
use spargebra::Query;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SparqlEndpointQueryExecutionError {
    #[error(transparent)]
    RequestError(reqwest::Error),
    #[error("Bad status code `{0}`")]
    BadStatusCode(String),
    #[error("Results parse error `{0}`")]
    ResultsParseError(QueryResultsSyntaxError),
    #[error("Solution parse error `{0}`")]
    SolutionParseError(QueryResultsSyntaxError),
    #[error("Wrong result type, expected solutions")]
    WrongResultType,
}

pub struct SparqlEndpoint {
    pub endpoint: String,
}
#[async_trait]
impl SparqlQueryable for SparqlEndpoint {
    async fn execute(&self, query: &Query) -> Result<Vec<QuerySolution>, SparqlQueryError> {
        let client = reqwest::Client::new();
        let response = client
            .get(&self.endpoint)
            .header(ACCEPT, "application/sparql-results+json,application/json,text/javascript,application/javascript")
            .header(USER_AGENT, "chrontext")
            .query(&[("query",query.to_string())])
            .query(&[("format", "json"), ("output", "json"), ("results", "json")])
            .send()
            .await;
        match response {
            Ok(proper_response) => {
                if proper_response.status().as_u16() != 200 {
                    Err(SparqlEndpointQueryExecutionError::BadStatusCode(
                        proper_response.status().to_string(),
                    )
                    .into())
                } else {
                    parse_json_text(&proper_response.text().await.expect("Read text error"))
                }
            }
            Err(error) => Err(SparqlEndpointQueryExecutionError::RequestError(error).into()),
        }
    }
}

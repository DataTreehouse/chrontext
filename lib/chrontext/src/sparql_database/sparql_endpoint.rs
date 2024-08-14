use super::{SparqlQueryError, SparqlQueryable};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, USER_AGENT};
use sparesults::{
    FromReadQueryResultsReader, QueryResultsFormat, QueryResultsParseError, QueryResultsParser,
    QuerySolution,
};
use spargebra::Query;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SparqlEndpointQueryExecutionError {
    #[error(transparent)]
    RequestError(reqwest::Error),
    #[error("Bad status code `{0}`")]
    BadStatusCode(String),
    #[error("Results parse error `{0}`")]
    ResultsParseError(QueryResultsParseError),
    #[error("Solution parse error `{0}`")]
    SolutionParseError(QueryResultsParseError),
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
                    let text = proper_response.text().await.expect("Read text error");
                    let json_parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
                    let parsed_results = json_parser.parse_read(text.as_bytes());
                    match parsed_results {
                        Ok(reader) => {
                            let mut solns = vec![];
                            if let FromReadQueryResultsReader::Solutions(solutions) = reader {
                                for s in solutions {
                                    match s {
                                        Ok(query_solution) => solns.push(query_solution),
                                        Err(parse_error) => return Err(
                                            SparqlEndpointQueryExecutionError::SolutionParseError(
                                                parse_error,
                                            )
                                            .into(),
                                        ),
                                    }
                                }
                                Ok(solns)
                            } else {
                                Err(SparqlEndpointQueryExecutionError::WrongResultType.into())
                            }
                        }
                        Err(parse_error) => Err(
                            SparqlEndpointQueryExecutionError::ResultsParseError(parse_error)
                                .into(),
                        ),
                    }
                }
            }
            Err(error) => Err(SparqlEndpointQueryExecutionError::RequestError(error).into()),
        }
    }
}

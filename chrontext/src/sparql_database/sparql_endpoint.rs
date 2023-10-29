use super::SparqlQueryable;
use async_trait::async_trait;
use reqwest::header::CONTENT_TYPE;
use reqwest::StatusCode;
use sparesults::{
    ParseError, QueryResultsFormat, QueryResultsParser, QueryResultsReader, QuerySolution,
};
use spargebra::Query;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum QueryExecutionError {
    RequestError(reqwest::Error),
    BadStatusCode(StatusCode),
    ResultsParseError(ParseError),
    SolutionParseError(ParseError),
    WrongResultType,
}

impl Display for QueryExecutionError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self {
            QueryExecutionError::RequestError(reqerr) => std::fmt::Display::fmt(&reqerr, f),
            QueryExecutionError::BadStatusCode(status_code) => {
                std::fmt::Display::fmt(&status_code, f)
            }
            QueryExecutionError::ResultsParseError(parseerr) => {
                std::fmt::Display::fmt(&parseerr, f)
            }
            QueryExecutionError::SolutionParseError(parseerr) => {
                std::fmt::Display::fmt(&parseerr, f)
            }
            QueryExecutionError::WrongResultType => {
                write!(f, "Wrong result type, expected solutions")
            }
        }
    }
}

impl Error for QueryExecutionError {}

pub struct SparqlEndpoint {
    pub endpoint: String,
}
#[async_trait]
impl SparqlQueryable for SparqlEndpoint {
    async fn execute(&mut self, query: &Query) -> Result<Vec<QuerySolution>, Box<dyn Error>> {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.endpoint)
            .header(CONTENT_TYPE, "application/sparql-query")
            .body(query.to_string())
            .send()
            .await;
        match response {
            Ok(proper_response) => {
                if proper_response.status().as_u16() != 200 {
                    Err(Box::new(QueryExecutionError::BadStatusCode(
                        proper_response.status(),
                    )))
                } else {
                    let text = proper_response.text().await.expect("Read text error");
                    let json_parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
                    let parsed_results = json_parser.read_results(text.as_bytes());
                    match parsed_results {
                        Ok(reader) => {
                            let mut solns = vec![];
                            if let QueryResultsReader::Solutions(solutions) = reader {
                                for s in solutions {
                                    match s {
                                        Ok(query_solution) => solns.push(query_solution),
                                        Err(parse_error) => {
                                            return Err(Box::new(
                                                QueryExecutionError::SolutionParseError(
                                                    parse_error,
                                                ),
                                            ))
                                        }
                                    }
                                }
                                Ok(solns)
                            } else {
                                Err(Box::new(QueryExecutionError::WrongResultType))
                            }
                        }
                        Err(parse_error) => Err(Box::new(QueryExecutionError::ResultsParseError(
                            parse_error,
                        ))),
                    }
                }
            }
            Err(error) => Err(Box::new(QueryExecutionError::RequestError(error))),
        }
    }
}

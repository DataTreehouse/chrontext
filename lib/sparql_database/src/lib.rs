pub mod embedded_oxigraph;
pub mod endpoint;

use async_trait::async_trait;
use embedded_oxigraph::EmbeddedOxigraphError;
use endpoint::SparqlEndpointQueryExecutionError;
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QuerySolution, SliceQueryResultsParserOutput,
};
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

fn parse_json_text(text: &str) -> Result<Vec<QuerySolution>, SparqlQueryError> {
    let json_parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
    let parsed_results = json_parser.for_slice(text.as_bytes());
    match parsed_results {
        Ok(reader) => {
            let mut solns = vec![];
            if let SliceQueryResultsParserOutput::Solutions(solutions) = reader {
                for s in solutions {
                    match s {
                        Ok(query_solution) => solns.push(query_solution),
                        Err(syntax_error) => {
                            return Err(SparqlEndpointQueryExecutionError::SolutionParseError(
                                syntax_error,
                            )
                            .into())
                        }
                    }
                }
                Ok(solns)
            } else {
                Err(SparqlEndpointQueryExecutionError::WrongResultType.into())
            }
        }
        Err(parse_error) => {
            Err(SparqlEndpointQueryExecutionError::ResultsParseError(parse_error).into())
        }
    }
}

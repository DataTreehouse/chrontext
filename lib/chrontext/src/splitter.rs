use spargebra::{Query, SparqlSyntaxError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryParseError {
    #[error(transparent)]
    Parse(SparqlSyntaxError),
    #[error("Not a select query")]
    NotSelectQuery,
    #[error("Unsupported construct: `{0}`")]
    Unsupported(String),
}

pub fn parse_sparql_select_query(query_str: &str) -> Result<Query, QueryParseError> {
    let q_res = Query::parse(query_str, None);
    match q_res {
        Ok(q) => match q {
            Query::Select {
                dataset,
                pattern,
                base_iri,
            } => {
                let mut unsupported_constructs = vec![];
                if dataset.is_some() {
                    unsupported_constructs.push("Dataset")
                }
                if base_iri.is_some() {
                    unsupported_constructs.push("BaseIri")
                }
                if unsupported_constructs.len() > 0 {
                    Err(QueryParseError::Unsupported(
                        unsupported_constructs.join(","),
                    ))
                } else {
                    Ok(Query::Select {
                        dataset,
                        pattern,
                        base_iri,
                    })
                }
            }
            _ => Err(QueryParseError::NotSelectQuery),
        },
        Err(e) => Err(QueryParseError::Parse(e)),
    }
}

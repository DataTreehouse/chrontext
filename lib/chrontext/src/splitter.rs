use spargebra::{ParseError, Query};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub enum SelectQueryErrorKind {
    Parse(ParseError),
    NotSelectQuery,
    Unsupported(String),
}

#[derive(Debug)]
pub struct SelectQueryError {
    kind: SelectQueryErrorKind,
}

impl Display for SelectQueryError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self.kind {
            SelectQueryErrorKind::Parse(pe) => std::fmt::Display::fmt(&pe, f),
            SelectQueryErrorKind::NotSelectQuery => {
                write!(f, "Not a select query")
            }
            SelectQueryErrorKind::Unsupported(s) => {
                write!(f, "Unsupported construct: {}", s)
            }
        }
    }
}

impl Error for SelectQueryError {}

pub fn parse_sparql_select_query(query_str: &str) -> Result<Query, SelectQueryError> {
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
                    Err(SelectQueryError {
                        kind: SelectQueryErrorKind::Unsupported(unsupported_constructs.join(",")),
                    })
                } else {
                    Ok(Query::Select {
                        dataset,
                        pattern,
                        base_iri,
                    })
                }
            }
            _ => Err(SelectQueryError {
                kind: SelectQueryErrorKind::NotSelectQuery,
            }),
        },
        Err(e) => Err(SelectQueryError {
            kind: SelectQueryErrorKind::Parse(e),
        }),
    }
}

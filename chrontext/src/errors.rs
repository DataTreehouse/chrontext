use std::fmt::{Display, Formatter};
use thiserror::Error;
#[derive(Debug, Error)]
pub enum ChrontextError {
    FromJSONFileError(String),
    NoSPARQLDatabaseDefined,
    CreateSPARQLDatabaseError(String),
    DeserializeFromJSONFileError(String),
    NoTimeseriesDatabaseDefined,
}

impl Display for ChrontextError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChrontextError::FromJSONFileError(s) => {
                write!(f, "Error reading engine config from JSON: {}", s)
            }
            ChrontextError::NoSPARQLDatabaseDefined => write!(f, "Missing SPARQL database"),
            ChrontextError::CreateSPARQLDatabaseError(s) => {
                write!(f, "Error creating SPARQL database {}", s)
            }
            ChrontextError::DeserializeFromJSONFileError(s) => {
                write!(f, "Error deserializing config from JSON file {}", s)
            }
            ChrontextError::NoTimeseriesDatabaseDefined => {
                write!(f, "No timeseries database defined")
            }
        }
    }
}

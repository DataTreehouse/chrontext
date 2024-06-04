use oxrdf::IriParseError;
use chrontext::errors::ChrontextError as RustChrontextError;
use thiserror::Error;
use pyo3::{create_exception, exceptions::PyException, prelude::*};

#[derive(Error, Debug)]
pub enum PyQueryError {
    #[error(transparent)]
    DatatypeIRIParseError(#[from] IriParseError),
    #[error(transparent)]
    QueryExecutionError(Box<dyn std::error::Error>),
    #[error("Missing time series database")]
    MissingTimeseriesDatabaseError,
    #[error("Missing sparql database")]
    MissingSPARQLDatabaseError,
    #[error("Time series database defined multiple times")]
    MultipleTimeseriesDatabases,
    #[error("Sparql database defined multiple times")]
    MultipleSPARQLDatabases,
    #[error(transparent)]
    ChrontextError(RustChrontextError)
}

impl std::convert::From<PyQueryError> for PyErr {
    fn from(pqe: PyQueryError) -> Self {
        match pqe {
            PyQueryError::ChrontextError(e) => {
                ChrontextError::new_err(format!("{}", e))
            }
            PyQueryError::DatatypeIRIParseError(err) => {
                DatatypeIRIParseError::new_err(format!("{}", err))
            }
            PyQueryError::QueryExecutionError(err) => {
                QueryExecutionError::new_err(format!("{}", err))
            }
            PyQueryError::MissingTimeseriesDatabaseError => {
                MissingTimeseriesDatabaseError::new_err("")
            }
            PyQueryError::MultipleTimeseriesDatabases => MultipleTimeseriesDatabases::new_err(""),
            PyQueryError::MissingSPARQLDatabaseError => MissingSPARQLDatabaseError::new_err(""),
            PyQueryError::MultipleSPARQLDatabases => MultipleSPARQLDatabases::new_err(""),
        }
    }
}

create_exception!(exceptions, DatatypeIRIParseError, PyException);
create_exception!(exceptions, QueryExecutionError, PyException);
create_exception!(exceptions, MissingTimeseriesDatabaseError, PyException);
create_exception!(exceptions, MultipleTimeseriesDatabases, PyException);
create_exception!(exceptions, MissingSPARQLDatabaseError, PyException);
create_exception!(exceptions, MultipleSPARQLDatabases, PyException);
create_exception!(exceptions, ChrontextError, PyException);

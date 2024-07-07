use chrontext::errors::ChrontextError as RustChrontextError;
use oxrdf::IriParseError;
use pyo3::{create_exception, exceptions::PyException, prelude::*};
use spargebra::SparqlSyntaxError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PyChrontextError {
    #[error(transparent)]
    DatatypeIRIParseError(#[from] IriParseError),
    #[error(transparent)]
    DataProductQueryParseError(#[from] SparqlSyntaxError),
    #[error(transparent)]
    QueryExecutionError(Box<dyn std::error::Error>),
    #[error("Missing time series database")]
    MissingSPARQLDatabaseError,
    #[error("Time series database defined multiple times")]
    MultipleSPARQLDatabasesError,
    #[error(transparent)]
    ChrontextError(RustChrontextError),
}

impl std::convert::From<PyChrontextError> for PyErr {
    fn from(pqe: PyChrontextError) -> Self {
        match pqe {
            PyChrontextError::ChrontextError(e) => ChrontextError::new_err(format!("{}", e)),
            PyChrontextError::DatatypeIRIParseError(err) => {
                DatatypeIRIParseError::new_err(format!("{}", err))
            }
            PyChrontextError::QueryExecutionError(err) => {
                QueryExecutionError::new_err(format!("{}", err))
            }
            PyChrontextError::MissingSPARQLDatabaseError => MissingSPARQLDatabaseError::new_err(""),
            PyChrontextError::MultipleSPARQLDatabasesError => MultipleSPARQLDatabasesError::new_err(""),
            PyChrontextError::DataProductQueryParseError(e) => {
                DataProductQueryParseError::new_err(format!("{}", e))
            }
        }
    }
}

create_exception!(exceptions, DatatypeIRIParseError, PyException);
create_exception!(exceptions, DataProductQueryParseError, PyException);
create_exception!(exceptions, QueryExecutionError, PyException);
create_exception!(exceptions, MissingSPARQLDatabaseError, PyException);
create_exception!(exceptions, MultipleSPARQLDatabasesError, PyException);
create_exception!(exceptions, ChrontextError, PyException);

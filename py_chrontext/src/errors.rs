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
    MissingTimeseriesDatabaseError,
    #[error("Missing sparql database")]
    MissingSPARQLDatabaseError,
    #[error("Time series database defined multiple times")]
    MultipleTimeseriesDatabases,
    #[error("Sparql database defined multiple times")]
    MultipleSPARQLDatabases,
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
            PyChrontextError::MissingTimeseriesDatabaseError => {
                MissingTimeseriesDatabaseError::new_err("")
            }
            PyChrontextError::MultipleTimeseriesDatabases => {
                MultipleTimeseriesDatabases::new_err("")
            }
            PyChrontextError::MissingSPARQLDatabaseError => MissingSPARQLDatabaseError::new_err(""),
            PyChrontextError::MultipleSPARQLDatabases => MultipleSPARQLDatabases::new_err(""),
            PyChrontextError::DataProductQueryParseError(e) => {
                DataProductQueryParseError::new_err(format!("{}", e))
            }
        }
    }
}

create_exception!(exceptions, DatatypeIRIParseError, PyException);
create_exception!(exceptions, DataProductQueryParseError, PyException);
create_exception!(exceptions, QueryExecutionError, PyException);
create_exception!(exceptions, MissingTimeseriesDatabaseError, PyException);
create_exception!(exceptions, MultipleTimeseriesDatabases, PyException);
create_exception!(exceptions, MissingSPARQLDatabaseError, PyException);
create_exception!(exceptions, MultipleSPARQLDatabases, PyException);
create_exception!(exceptions, ChrontextError, PyException);

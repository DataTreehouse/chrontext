use chrontext::errors::ChrontextError as RustChrontextError;
use oxrdf::IriParseError;
use pyo3::{create_exception, exceptions::PyException, prelude::*};
use spargebra::SparqlSyntaxError;
use thiserror::Error;
use flight::client::ChrontextFlightClientError;
use flight::server::ChrontextFlightServerError;


#[derive(Error, Debug)]
pub enum PyChrontextError {
    #[error(transparent)]
    DatatypeIRIParseError(#[from] IriParseError),
    #[error(transparent)]
    DataProductQueryParseError(#[from] SparqlSyntaxError),
    #[error("Missing SPARQL database")]
    MissingSPARQLDatabaseError,
    #[error("SPARQL database defined multiple times")]
    MultipleSPARQLDatabasesError,
    #[error(transparent)]
    ChrontextError(RustChrontextError),
    #[error("Missing virtualized database")]
    MissingVirtualizedDatabaseError,
    #[error("Virtualized database defined multiple times")]
    MultipleVirtualizedDatabasesError,
    #[error(transparent)]
    FlightClientError(ChrontextFlightClientError),
    #[error(transparent)]
    FlightServerError(ChrontextFlightServerError),
}

impl std::convert::From<PyChrontextError> for PyErr {
    fn from(pqe: PyChrontextError) -> Self {
        match pqe {
            PyChrontextError::ChrontextError(e) => ChrontextError::new_err(format!("{}", e)),
            PyChrontextError::DatatypeIRIParseError(err) => {
                DatatypeIRIParseError::new_err(format!("{}", err))
            }
            PyChrontextError::MissingSPARQLDatabaseError => MissingSPARQLDatabaseError::new_err(""),
            PyChrontextError::MultipleSPARQLDatabasesError => {
                MultipleSPARQLDatabasesError::new_err("")
            }
            PyChrontextError::DataProductQueryParseError(e) => {
                DataProductQueryParseError::new_err(format!("{}", e))
            }
            PyChrontextError::MissingVirtualizedDatabaseError => {
                MissingVirtualizedDatabaseError::new_err("")
            }
            PyChrontextError::MultipleVirtualizedDatabasesError => {
                MultipleVirtualizedDatabasesError::new_err("")
            }
            PyChrontextError::FlightClientError(x) => {
                FlightClientError::new_err(x.to_string())
            }
            PyChrontextError::FlightServerError(x) => {
                FlightServerError::new_err(x.to_string())
            }
        }
    }
}

create_exception!(exceptions, DatatypeIRIParseError, PyException);
create_exception!(exceptions, DataProductQueryParseError, PyException);
create_exception!(exceptions, MissingSPARQLDatabaseError, PyException);
create_exception!(exceptions, MultipleSPARQLDatabasesError, PyException);
create_exception!(exceptions, MissingVirtualizedDatabaseError, PyException);
create_exception!(exceptions, MultipleVirtualizedDatabasesError, PyException);
create_exception!(exceptions, FlightClientError, PyException);
create_exception!(exceptions, FlightServerError, PyException);
create_exception!(exceptions, ChrontextError, PyException);

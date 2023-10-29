use oxrdf::IriParseError;
use std::io;
use thiserror::Error;

use chrontext::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLError as RustArrowFlightSQLError;
use oxigraph::store::{LoaderError, StorageError};
use pyo3::{create_exception, exceptions::PyException, prelude::*};

#[derive(Error, Debug)]
pub enum PyQueryError {
    #[error(transparent)]
    ArrowFlightSQLError(#[from] RustArrowFlightSQLError),
    #[error(transparent)]
    DatatypeIRIParseError(#[from] IriParseError),
    #[error(transparent)]
    QueryExecutionError(Box<dyn std::error::Error>),
    #[error("Missing time series database")]
    MissingTimeSeriesDatabaseError,
    #[error("Missing sparql database")]
    MissingSPARQLDatabaseError,
    #[error("Time series database defined multiple times")]
    MultipleTimeSeriesDatabases,
    #[error("Sparql database defined multiple times")]
    MultipleSPARQLDatabases,
    #[error("Oxigraph storage error `{0}`")]
    OxigraphStorageError(StorageError),
    #[error("Read ntriples file error `{0}`")]
    ReadNTriplesFileError(io::Error),
    #[error("Load triples from file error in oxigraph `{0}`")]
    OxigraphLoaderError(LoaderError),
}

impl std::convert::From<PyQueryError> for PyErr {
    fn from(pqe: PyQueryError) -> Self {
        match pqe {
            PyQueryError::ArrowFlightSQLError(err) => {
                ArrowFlightSQLError::new_err(format!("{}", err))
            }
            PyQueryError::DatatypeIRIParseError(err) => {
                DatatypeIRIParseError::new_err(format!("{}", err))
            }
            PyQueryError::QueryExecutionError(err) => {
                QueryExecutionError::new_err(format!("{}", err))
            }
            PyQueryError::MissingTimeSeriesDatabaseError => {
                MissingTimeSeriesDatabaseError::new_err("")
            }
            PyQueryError::MultipleTimeSeriesDatabases => MultipleTimeSeriesDatabases::new_err(""),
            PyQueryError::OxigraphStorageError(o) => {
                OxigraphStorageError::new_err(format!("{}", o))
            }
            PyQueryError::ReadNTriplesFileError(e) => {
                ReadNTriplesFileError::new_err(format!("{}", e))
            }
            PyQueryError::OxigraphLoaderError(l) => OxigraphLoaderError::new_err(format!("{}", l)),
            PyQueryError::MissingSPARQLDatabaseError => MissingSPARQLDatabaseError::new_err(""),
            PyQueryError::MultipleSPARQLDatabases => MultipleSPARQLDatabases::new_err(""),
        }
    }
}

create_exception!(exceptions, ArrowFlightSQLError, PyException);
create_exception!(exceptions, DatatypeIRIParseError, PyException);
create_exception!(exceptions, QueryExecutionError, PyException);
create_exception!(exceptions, MissingTimeSeriesDatabaseError, PyException);
create_exception!(exceptions, MultipleTimeSeriesDatabases, PyException);
create_exception!(exceptions, OxigraphStorageError, PyException);
create_exception!(exceptions, ReadNTriplesFileError, PyException);
create_exception!(exceptions, OxigraphLoaderError, PyException);
create_exception!(exceptions, MissingSPARQLDatabaseError, PyException);
create_exception!(exceptions, MultipleSPARQLDatabases, PyException);

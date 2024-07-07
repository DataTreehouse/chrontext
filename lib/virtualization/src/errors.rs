use pyo3::PyErr;
use thiserror::*;

#[derive(Error, Debug)]
pub enum VirtualizedDatabaseError {
    #[error(transparent)]
    PyVirtualizedDatabaseError(#[from] PyErr)
}
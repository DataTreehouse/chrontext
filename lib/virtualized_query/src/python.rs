use crate::VirtualizedQuery;
use pyo3::prelude::*;

#[pyclass(name = "VirtualizedQuery")]
pub struct PyVirtualizedQuery {
    pub vq: VirtualizedQuery,
}

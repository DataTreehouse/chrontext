use pyo3::prelude::*;
use crate::VirtualizedQuery;

#[pyclass(name="VirtualizedQuery")]
pub struct PyVirtualizedQuery {
    pub vq: VirtualizedQuery
}

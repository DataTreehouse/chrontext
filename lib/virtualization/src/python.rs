use std::collections::HashSet;
use polars::prelude::DataFrame;
use pydf_io::to_rust::polars_df_to_rust_df;
use pyo3::prelude::*;
use virtualized_query::pushdown_setting::{all_pushdowns, PushdownSetting};
use virtualized_query::python::PyVirtualizedQuery;
use virtualized_query::VirtualizedQuery;

#[derive(Clone, Debug)]
#[pyclass(name="VirtualizedDatabase")]
pub struct PyVirtualizedDatabase {
    pub db_module:String
}

#[pymethods]
impl PyVirtualizedDatabase {
    #[new]
    pub fn new(db_module:String) -> PyVirtualizedDatabase {
        PyVirtualizedDatabase{
            db_module
        }
    }
}

impl PyVirtualizedDatabase {
    pub fn pushdown_settings(&self) -> HashSet<PushdownSetting> {
        all_pushdowns()
    }

    pub fn query(&self, vq:&VirtualizedQuery) -> PyResult<DataFrame> {
        let pyvq = PyVirtualizedQuery {vq:vq.clone()};
        Python::with_gil(|py| {
            let db_mod = PyModule::import_bound(py, self.db_module.as_str())?;
            let query_func = db_mod.getattr("query")?;
            let py_df = query_func.call1((pyvq,))?;
            polars_df_to_rust_df(&py_df)

        })
    }
}
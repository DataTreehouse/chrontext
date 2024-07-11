use polars::prelude::DataFrame;
use pydf_io::to_rust::polars_df_to_rust_df;
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use virtualized_query::pushdown_setting::{all_pushdowns, PushdownSetting};
use virtualized_query::python::PyVirtualizedQuery;
use virtualized_query::VirtualizedQuery;

#[derive(Clone, Debug)]
#[pyclass(name = "VirtualizedDatabase")]
pub struct VirtualizedPythonDatabase {
    pub database: Py<PyAny>,
    pub resource_sql_map: Option<HashMap<String, Py<PyAny>>>,
    pub sql_dialect: Option<String>,
}

#[pymethods]
impl VirtualizedPythonDatabase {
    #[new]
    pub fn new(
        database: Py<PyAny>,
        resource_sql_map: Option<HashMap<String, Py<PyAny>>>,
        sql_dialect: Option<String>,
    ) -> VirtualizedPythonDatabase {
        VirtualizedPythonDatabase {
            database,
            resource_sql_map,
            sql_dialect,
        }
    }
}

impl VirtualizedPythonDatabase {
    pub fn pushdown_settings(&self) -> HashSet<PushdownSetting> {
        all_pushdowns()
    }

    pub fn query(&self, vq: &VirtualizedQuery) -> PyResult<DataFrame> {
        Python::with_gil(|py| {
            let pyvq = PyVirtualizedQuery::new(vq.clone(), py)?;
            let query_func = self.database.getattr(py, "query")?;
            let py_df = query_func.call1(py, (pyvq,))?;
            polars_df_to_rust_df(&py_df.into_bound(py))
        })
    }
}

pub fn translate_sql(vq: &VirtualizedQuery, x: &HashMap<String, Py<PyAny>>) -> PyResult<String> {
    Python::with_gil(|py| {
        let pyvq = PyVirtualizedQuery::new(vq.clone(), py)?;
        let db_mod = PyModule::import_bound(py, "my_db")?;
        let translate_sql_func = db_mod.getattr("translate_sql")?;
        let query_string = translate_sql_func.call1((pyvq,))?;
        query_string.extract::<String>()
    })
}

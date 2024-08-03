mod sql_translation;

use polars::prelude::DataFrame;
use pydf_io::to_rust::polars_df_to_rust_df;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sql_translation::PYTHON_CODE;
use std::collections::HashSet;
use virtualized_query::pushdown_setting::{all_pushdowns, PushdownSetting};
use virtualized_query::python::PyVirtualizedQuery;
use virtualized_query::VirtualizedQuery;

#[derive(Clone, Debug)]
#[pyclass]
pub struct VirtualizedPythonDatabase {
    pub database: Py<PyAny>,
    pub resource_sql_map: Option<Py<PyDict>>,
    pub sql_dialect: Option<String>,
}

#[pymethods]
impl VirtualizedPythonDatabase {
    #[new]
    pub fn new(
        database: Py<PyAny>,
        resource_sql_map: Option<Py<PyDict>>,
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
            let py_df = if let Some(resource_sql_map) = &self.resource_sql_map {
                let s = translate_sql(
                    vq,
                    resource_sql_map,
                    self.sql_dialect.as_ref().unwrap().as_str(),
                )?;
                let query_func = self.database.getattr(py, "query")?;
                query_func.call1(py, (s,))?
            } else {
                let pyvq = PyVirtualizedQuery::new(vq.clone(), py)?;
                let query_func = self.database.getattr(py, "query")?;
                query_func.call1(py, (pyvq,))?
            };
            polars_df_to_rust_df(&py_df.into_bound(py))
        })
    }
}

pub fn translate_sql(
    vq: &VirtualizedQuery,
    resource_sql_map: &Py<PyDict>,
    dialect: &str,
) -> PyResult<String> {
    Python::with_gil(|py| {
        let pyvq = PyVirtualizedQuery::new(vq.clone(), py)?;
        let db_mod = PyModule::from_code_bound(py, PYTHON_CODE, "my_translator", "my_translator")?;
        let translate_sql_func = db_mod.getattr("translate_sql")?;
        let query_string = translate_sql_func.call((pyvq, dialect, resource_sql_map), None)?;
        query_string.extract::<String>()
    })
}

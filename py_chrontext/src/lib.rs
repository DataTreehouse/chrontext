pub mod errors;

use std::thread;
//The below snippet controlling alloc-library is from https://github.com/pola-rs/polars/blob/main/py-polars/src/lib.rs
//And has a MIT license:
//Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use crate::errors::PyQueryError;
use arrow_python_utils::to_python::to_py_df;
use chrontext::engine::Engine as RustEngine;
use chrontext::pushdown_setting::{all_pushdowns, PushdownSetting};
use chrontext::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLDatabase as RustArrowFlightSQLDatabase;
use chrontext::timeseries_database::bigquery_database::BigQueryDatabase as RustBigQueryDatabase;
use chrontext::timeseries_database::opcua_history_read::OPCUAHistoryRead as RustOPCUAHistoryRead;
use chrontext::timeseries_database::timeseries_sql_rewrite::TimeSeriesTable as RustTimeSeriesTable;
use log::debug;
use oxrdf::{IriParseError, NamedNode};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};

#[pyclass(unsendable)]
pub struct Engine {
    engine: Option<RustEngine>,
    endpoint: String,
}

#[pymethods]
impl Engine {
    #[new]
    pub fn new(endpoint: &str) -> Box<Engine> {
        Box::new(Engine {
            engine: None,
            endpoint: endpoint.to_string(),
        })
    }

    pub fn set_arrow_flight_sql(&mut self, db: &ArrowFlightSQLDatabase) -> PyResult<()> {
        if self.engine.is_some() {
            return Err(PyQueryError::TimeSeriesDatabaseAlreadyDefined.into());
        }
        let endpoint = format!("http://{}:{}", &db.host, &db.port);
        let mut new_tables = vec![];
        for t in &db.tables {
            new_tables.push(t.to_rust_table().map_err(PyQueryError::from)?);
        }

        let afsqldb_result = Runtime::new()
            .unwrap()
            .block_on(RustArrowFlightSQLDatabase::new(
                &endpoint,
                &db.username,
                &db.password,
                new_tables,
            ));
        let db = afsqldb_result.map_err(PyQueryError::from)?;
        self.engine = Some(RustEngine::new(
            all_pushdowns(),
            Box::new(db),
            self.endpoint.clone(),
        ));
        Ok(())
    }

    pub fn set_bigquery_database(&mut self, db: &BigQueryDatabase) -> PyResult<()> {
        if self.engine.is_some() {
            return Err(PyQueryError::TimeSeriesDatabaseAlreadyDefined.into());
        }
        let mut new_tables = vec![];
        for t in &db.tables {
            new_tables.push(t.to_rust_table().map_err(PyQueryError::from)?);
        }
        let key = db.key.clone();
        let db = thread::spawn(|| {
            RustBigQueryDatabase::new(key, new_tables)
        })
        .join()
        .unwrap();

        self.engine = Some(RustEngine::new(
            all_pushdowns(),
            Box::new(db),
            self.endpoint.clone(),
        ));
        Ok(())
    }

    pub fn set_opcua_history_read(&mut self, db: &OPCUAHistoryRead) -> PyResult<()> {
        if self.engine.is_some() {
            return Err(PyQueryError::TimeSeriesDatabaseAlreadyDefined.into());
        }
        let actual_db = RustOPCUAHistoryRead::new(&db.endpoint, db.namespace);
        self.engine = Some(RustEngine::new(
            [PushdownSetting::GroupBy].into(),
            Box::new(actual_db),
            self.endpoint.clone(),
        ));
        Ok(())
    }

    pub fn execute_hybrid_query(&mut self, py: Python<'_>, sparql: &str) -> PyResult<PyObject> {
        if self.engine.is_none() {
            return Err(PyQueryError::MissingTimeSeriesDatabaseError.into());
        }
        let res = env_logger::try_init();
        match res {
            Ok(_) => {}
            Err(_) => {
                debug!("Tried to initialize logger which is already initialize")
            }
        }
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let df_result = builder
            .build()
            .unwrap()
            .block_on(self.engine.as_mut().unwrap().execute_hybrid_query(sparql));
        match df_result {
            Ok(mut df) => {
                let names_vec: Vec<String> = df
                    .get_column_names()
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect();
                let names: Vec<&str> = names_vec.iter().map(|x| x.as_str()).collect();
                let chunk = df.as_single_chunk().iter_chunks().next().unwrap();
                let pyarrow = PyModule::import(py, "pyarrow")?;
                let polars = PyModule::import(py, "polars")?;
                to_py_df(&chunk, names.as_slice(), py, pyarrow, polars)
            }
            Err(err) => Err(PyErr::from(PyQueryError::QueryExecutionError(err))),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ArrowFlightSQLDatabase {
    host: String,
    port: u16,
    username: String,
    password: String,
    tables: Vec<TimeSeriesTable>,
}

#[pymethods]
impl ArrowFlightSQLDatabase {
    #[new]
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        tables: Vec<TimeSeriesTable>,
    ) -> ArrowFlightSQLDatabase {
        ArrowFlightSQLDatabase {
            username,
            password,
            host,
            port,
            tables,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct BigQueryDatabase {
    tables: Vec<TimeSeriesTable>,
    key: String,
}

#[pymethods]
impl BigQueryDatabase {
    #[new]
    pub fn new(tables: Vec<TimeSeriesTable>, key: String) -> BigQueryDatabase {
        BigQueryDatabase { tables, key }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct OPCUAHistoryRead {
    namespace: u16,
    endpoint: String,
}

#[pymethods]
impl OPCUAHistoryRead {
    #[new]
    pub fn new(endpoint: String, namespace: u16) -> OPCUAHistoryRead {
        OPCUAHistoryRead {
            namespace,
            endpoint,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TimeSeriesTable {
    pub resource_name: String,
    pub schema: Option<String>,
    pub time_series_table: String,
    pub value_column: String,
    pub timestamp_column: String,
    pub identifier_column: String,
    pub value_datatype: String,
    pub year_column: Option<String>,
    pub month_column: Option<String>,
    pub day_column: Option<String>,
}

#[pymethods]
impl TimeSeriesTable {
    #[new]
    pub fn new(
        resource_name: String,
        time_series_table: String,
        value_column: String,
        timestamp_column: String,
        identifier_column: String,
        value_datatype: String,
        schema: Option<String>,
        year_column: Option<String>,
        month_column: Option<String>,
        day_column: Option<String>,
    ) -> TimeSeriesTable {
        TimeSeriesTable {
            resource_name,
            schema,
            time_series_table,
            value_column,
            timestamp_column,
            identifier_column,
            value_datatype,
            year_column,
            month_column,
            day_column,
        }
    }
}

impl TimeSeriesTable {
    fn to_rust_table(&self) -> Result<RustTimeSeriesTable, IriParseError> {
        Ok(RustTimeSeriesTable {
            resource_name: self.resource_name.clone(),
            schema: self.schema.clone(),
            time_series_table: self.time_series_table.clone(),
            value_column: self.value_column.clone(),
            timestamp_column: self.timestamp_column.clone(),
            identifier_column: self.identifier_column.clone(),
            value_datatype: NamedNode::new(&self.value_datatype)?,
            year_column: self.year_column.clone(),
            month_column: self.month_column.clone(),
            day_column: self.day_column.clone(),
        })
    }
}

#[pymodule]
fn _chrontext(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<TimeSeriesTable>()?;
    m.add_class::<ArrowFlightSQLDatabase>()?;
    m.add_class::<BigQueryDatabase>()?;
    m.add_class::<OPCUAHistoryRead>()?;
    Ok(())
}

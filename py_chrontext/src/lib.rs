pub mod errors;

use std::collections::HashSet;
use std::fs::{File, read_to_string};
use std::io::BufReader;
use std::path::Path;
use std::thread;
use std::time::SystemTime;
use filesize::PathExt;
use std::io::Write;

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
use chrontext::sparql_database::embedded_oxigraph::EmbeddedOxigraph;
use chrontext::sparql_database::sparql_endpoint::SparqlEndpoint;
use chrontext::sparql_database::SparqlQueryable;
use chrontext::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLDatabase as RustArrowFlightSQLDatabase;
use chrontext::timeseries_database::bigquery_database::BigQueryDatabase as RustBigQueryDatabase;
use chrontext::timeseries_database::opcua_history_read::OPCUAHistoryRead as RustOPCUAHistoryRead;
use chrontext::timeseries_database::timeseries_sql_rewrite::TimeseriesTable as RustTimeseriesTable;
use chrontext::timeseries_database::TimeseriesQueryable;
use log::debug;
use oxigraph::io::DatasetFormat;
use oxrdf::{IriParseError, NamedNode};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};

const TTL_FILE_METADATA:&str = "ttl_file_data.txt";

#[pyclass(unsendable)]
pub struct Engine {
    timeseries_opcua_db: Option<TimeseriesOPCUADatabase>,
    timeseries_bigquery_db: Option<TimeseriesBigQueryDatabase>,
    timeseries_dremio_db: Option<TimeseriesDremioDatabase>,
    engine: Option<RustEngine>,
    sparql_endpoint: Option<String>,
    sparql_embedded_oxigraph: Option<SparqlEmbeddedOxigraph>,
}

#[pymethods]
impl Engine {
    #[new]
    #[pyo3(text_signature = "(sparql_endpoint: Optional[String],
sparql_embedded_oxigraph: Optional[SparqlEmbeddedOxigraph],
timeseries_dremio_db: Optional[TimeseriesDremioDatabase],
timeseries_bigquery_db: Optional[TimeseriesBigQueryDatabase],
timeseries_opcua_db: Optional[TimeseriesOPCUADatabase])")]
    pub fn new(
        sparql_endpoint: Option<String>,
        sparql_embedded_oxigraph: Option<SparqlEmbeddedOxigraph>,
        timeseries_dremio_db: Option<TimeseriesDremioDatabase>,
        timeseries_bigquery_db: Option<TimeseriesBigQueryDatabase>,
        timeseries_opcua_db: Option<TimeseriesOPCUADatabase>,
    ) -> PyResult<Engine> {
        let num_sparql =
            sparql_endpoint.is_some() as usize + sparql_embedded_oxigraph.is_some() as usize;
        let num_ts = timeseries_dremio_db.is_some() as usize
            + timeseries_bigquery_db.is_some() as usize
            + timeseries_opcua_db.is_some() as usize;

        if num_sparql == 0 {
            return Err(PyQueryError::MissingSPARQLDatabaseError.into());
        }
        if num_sparql > 1 {
            return Err(PyQueryError::MultipleSPARQLDatabases.into());
        }

        if num_ts == 0 {
            return Err(PyQueryError::MissingTimeseriesDatabaseError.into());
        }
        if num_ts > 1 {
            return Err(PyQueryError::MultipleTimeseriesDatabases.into());
        }

        let mut engine = Engine {
            engine: None,
            sparql_endpoint,
            sparql_embedded_oxigraph,
            timeseries_dremio_db,
            timeseries_bigquery_db,
            timeseries_opcua_db,
        };
        Ok(engine)
    }

    pub fn init(&mut self) -> PyResult<()> {
        let (pushdown_settings, time_series_db) = if let Some(db) = &self.timeseries_opcua_db {
            create_opcua_history_read(&db.clone())?
        } else if let Some(db) = &self.timeseries_bigquery_db {
            create_bigquery_database(&db.clone())?
        } else if let Some(db) = &self.timeseries_dremio_db {
            create_arrow_flight_sql(&db.clone())?
        } else {
            return Err(PyQueryError::MissingTimeseriesDatabaseError.into());
        };

        let sparql_db = if self.engine.is_some() {
            self.engine.as_mut().unwrap().sparql_database.take()
        } else {
            None
        };

        let sparql_db = if let Some(sparql_db) = sparql_db {
            sparql_db
        } else if let Some(endpoint) = &self.sparql_endpoint {
            Box::new(SparqlEndpoint {
                endpoint: endpoint.to_string(),
            })
        } else if let Some(oxi) = &self.sparql_embedded_oxigraph {
            create_oxigraph(oxi)?
        } else {
            return Err(PyQueryError::MissingSPARQLDatabaseError.into());
        };

        self.engine = Some(RustEngine::new(
            pushdown_settings,
            time_series_db,
            sparql_db,
        ));
        Ok(())
    }

    pub fn query(&mut self, py: Python<'_>, sparql: &str) -> PyResult<PyObject> {
        if self.engine.is_none()
            || !self.engine.as_ref().unwrap().has_time_series_db()
            || !self.engine.as_ref().unwrap().has_sparql_db()
        {
            self.init()?;
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
pub struct SparqlEmbeddedOxigraph {
    path: Option<String>,
    ntriples_file: String,
}

#[pymethods]
impl SparqlEmbeddedOxigraph {
    #[new]
    pub fn new(ntriples_file: String, path: Option<String>) -> SparqlEmbeddedOxigraph {
        SparqlEmbeddedOxigraph {
            path,
            ntriples_file,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TimeseriesDremioDatabase {
    host: String,
    port: u16,
    username: String,
    password: String,
    tables: Vec<TimeseriesTable>,
}

#[pymethods]
impl TimeseriesDremioDatabase {
    #[new]
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        tables: Vec<TimeseriesTable>,
    ) -> TimeseriesDremioDatabase {
        TimeseriesDremioDatabase {
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
pub struct TimeseriesBigQueryDatabase {
    tables: Vec<TimeseriesTable>,
    key: String,
}

#[pymethods]
impl TimeseriesBigQueryDatabase {
    #[new]
    pub fn new(tables: Vec<TimeseriesTable>, key: String) -> TimeseriesBigQueryDatabase {
        TimeseriesBigQueryDatabase { tables, key }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TimeseriesOPCUADatabase {
    namespace: u16,
    endpoint: String,
}

#[pymethods]
impl TimeseriesOPCUADatabase {
    #[new]
    pub fn new(endpoint: String, namespace: u16) -> TimeseriesOPCUADatabase {
        TimeseriesOPCUADatabase {
            namespace,
            endpoint,
        }
    }
}

pub fn create_arrow_flight_sql(
    db: &TimeseriesDremioDatabase,
) -> PyResult<(HashSet<PushdownSetting>, Box<dyn TimeseriesQueryable>)> {
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
    Ok((all_pushdowns(), Box::new(db)))
}

pub fn create_bigquery_database(
    db: &TimeseriesBigQueryDatabase,
) -> PyResult<(HashSet<PushdownSetting>, Box<dyn TimeseriesQueryable>)> {
    let mut new_tables = vec![];
    for t in &db.tables {
        new_tables.push(t.to_rust_table().map_err(PyQueryError::from)?);
    }
    let key = db.key.clone();
    let db = thread::spawn(|| RustBigQueryDatabase::new(key, new_tables))
        .join()
        .unwrap();

    Ok((all_pushdowns(), Box::new(db)))
}

fn create_opcua_history_read(
    db: &TimeseriesOPCUADatabase,
) -> PyResult<(HashSet<PushdownSetting>, Box<dyn TimeseriesQueryable>)> {
    let actual_db = RustOPCUAHistoryRead::new(&db.endpoint, db.namespace);
    Ok(([PushdownSetting::GroupBy].into(), Box::new(actual_db)))
}

fn create_oxigraph(db: &SparqlEmbeddedOxigraph) -> PyResult<Box<dyn SparqlQueryable>> {
    let ntriples_path = Path::new(&db.ntriples_file);
    let ntriples_file_metadata = file_metadata_string(ntriples_path)?;

    let store = if let Some(p) = &db.path {
        oxigraph::store::Store::open(Path::new(p))
            .map_err(|x| PyQueryError::OxigraphStorageError(x))?

    } else {
        oxigraph::store::Store::new().unwrap()
    };

    let need_read_file = if let Some(p) =  &db.path {
        let mut pb = Path::new(p).to_path_buf();
        pb.push(Path::new(TTL_FILE_METADATA));
        let dbdata_path = pb.as_path();
        if dbdata_path.exists() {
            let existing_db_ntriples_metadata = read_to_string(dbdata_path)?;
            existing_db_ntriples_metadata != ntriples_file_metadata
        } else {
            true
        }
    } else {
        true
    };

    if need_read_file {
        let file = File::open(&db.ntriples_file).map_err(|x| PyQueryError::ReadNTriplesFileError(x))?;
        let reader = BufReader::new(file);
        store
            .bulk_loader()
            .load_dataset(reader, DatasetFormat::NQuads, None)
            .map_err(|x| PyQueryError::OxigraphLoaderError(x))?;
        if let Some(p) = &db.path {
            let mut pb = Path::new(p).to_path_buf();
            pb.push(TTL_FILE_METADATA);
            let mut f = File::create(pb).unwrap();
            write!(f, "{}", ntriples_file_metadata)?;
        }
    }
    let oxi = EmbeddedOxigraph { store };

    Ok(Box::new(oxi))
}

#[pyclass]
#[derive(Clone)]
pub struct TimeseriesTable {
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
impl TimeseriesTable {
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
    ) -> TimeseriesTable {
        TimeseriesTable {
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

impl TimeseriesTable {
    fn to_rust_table(&self) -> Result<RustTimeseriesTable, IriParseError> {
        Ok(RustTimeseriesTable {
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

fn file_metadata_string(p:&Path) -> Result<String, std::io::Error>{
    let size = p.size_on_disk()?;
    let changed = p.metadata()?.created()?.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
    Ok(format!("{}_{}", size, changed))
}

#[pymodule]
fn _chrontext(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize");
        }
    }

    m.add_class::<Engine>()?;
    m.add_class::<TimeseriesTable>()?;
    m.add_class::<TimeseriesDremioDatabase>()?;
    m.add_class::<TimeseriesBigQueryDatabase>()?;
    m.add_class::<TimeseriesOPCUADatabase>()?;
    m.add_class::<SparqlEmbeddedOxigraph>()?;
    Ok(())
}

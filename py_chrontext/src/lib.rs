pub mod errors;

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

use crate::errors::PyChrontextError;
use chrontext::engine::{Engine as RustEngine, EngineConfig};
use chrontext::sparql_database::sparql_embedded_oxigraph::EmbeddedOxigraphConfig;
use log::debug;
use oxrdf::{IriParseError, NamedNode};
use polars::prelude::{DataFrame, IntoLazy};
use postgres::catalog::{Catalog as RustCatalog, DataProduct as RustDataProduct};
use postgres::server::{start_server, Config};
use pydf_io::to_python::{df_to_py_df, dtypes_map, fix_cats_and_multicolumns};
use pyo3::prelude::*;
use representation::multitype::{
    compress_actual_multitypes, lf_column_from_categorical, multi_columns_to_string_cols,
};
use representation::{BaseRDFNodeType as RustBaseRDFNodeType, RDFNodeType};
use std::collections::HashMap;
use timeseries_query::TimeseriesTable as RustTimeseriesTable;
use tokio::runtime::Builder;

#[pyclass]
pub struct Engine {
    timeseries_opcua_db: Option<TimeseriesOPCUADatabase>,
    timeseries_bigquery_db: Option<TimeseriesBigQueryDatabase>,
    engine: Option<RustEngine>,
    sparql_endpoint: Option<String>,
    sparql_embedded_oxigraph: Option<SparqlEmbeddedOxigraph>,
}

#[pymethods]
impl Engine {
    #[new]
    pub fn new(
        sparql_endpoint: Option<String>,
        sparql_embedded_oxigraph: Option<SparqlEmbeddedOxigraph>,
        timeseries_bigquery_db: Option<TimeseriesBigQueryDatabase>,
        timeseries_opcua_db: Option<TimeseriesOPCUADatabase>,
    ) -> PyResult<Engine> {
        let num_sparql =
            sparql_endpoint.is_some() as usize + sparql_embedded_oxigraph.is_some() as usize;
        let num_ts =
            timeseries_bigquery_db.is_some() as usize + timeseries_opcua_db.is_some() as usize;

        if num_sparql == 0 {
            return Err(PyChrontextError::MissingSPARQLDatabaseError.into());
        }
        if num_sparql > 1 {
            return Err(PyChrontextError::MultipleSPARQLDatabases.into());
        }

        if num_ts == 0 {
            return Err(PyChrontextError::MissingTimeseriesDatabaseError.into());
        }
        if num_ts > 1 {
            return Err(PyChrontextError::MultipleTimeseriesDatabases.into());
        }

        let engine = Engine {
            engine: None,
            sparql_endpoint,
            sparql_embedded_oxigraph,
            timeseries_bigquery_db,
            timeseries_opcua_db,
        };
        Ok(engine)
    }

    pub fn init(&mut self) -> PyResult<()> {
        if self.engine.is_none() {
            let (timeseries_opcua_endpoint, timeseries_opcua_namespace) =
                if let Some(db) = &self.timeseries_opcua_db {
                    (Some(db.endpoint.to_string()), Some(db.namespace))
                } else {
                    (None, None)
                };

            let (timeseries_bigquery_tables, timeseries_bigquery_key_file) =
                if let Some(db) = &self.timeseries_bigquery_db {
                    let mut tables = vec![];
                    for t in &db.tables {
                        tables.push(
                            t.to_rust_table()
                                .map_err(|x| PyChrontextError::DatatypeIRIParseError(x))?,
                        );
                    }
                    (Some(tables), Some(db.key.clone()))
                } else {
                    (None, None)
                };

            let sparql_endpoint = if let Some(endpoint) = &self.sparql_endpoint {
                Some(endpoint.clone())
            } else {
                None
            };

            let sparql_oxigraph_config = if let Some(oxi) = &self.sparql_embedded_oxigraph {
                Some(oxi.as_config())
            } else {
                None
            };

            let config = EngineConfig {
                sparql_oxigraph_config,
                sparql_endpoint,
                timeseries_bigquery_tables,
                timeseries_bigquery_key_file,
                timeseries_opcua_endpoint,
                timeseries_opcua_namespace,
            };

            self.engine = Some(
                RustEngine::from_config(config).map_err(|x| PyChrontextError::ChrontextError(x))?,
            );
        }
        Ok(())
    }

    pub fn query(
        &mut self,
        py: Python<'_>,
        sparql: &str,
        multi_to_strings: Option<bool>,
    ) -> PyResult<PyObject> {
        if self.engine.is_none() {
            self.init()?;
        }

        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let (mut df, mut datatypes) = builder
            .build()
            .unwrap()
            .block_on(self.engine.as_mut().unwrap().execute_hybrid_query(sparql))
            .map_err(|err| PyChrontextError::QueryExecutionError(err))?;

        (df, datatypes) =
            fix_cats_and_multicolumns(df, datatypes, multi_to_strings.unwrap_or(false));
        let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
        Ok(pydf)
    }

    pub fn serve_postgres(&mut self, catalog: Catalog) -> PyResult<()> {
        if self.engine.is_none() {
            self.init()?;
        }
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let config = Config::default();
        let catalog = catalog.to_rust()?;
        builder
            .build()
            .unwrap()
            .block_on(start_server(self.engine.take().unwrap(), config, catalog))
            .unwrap();
        Ok(())
    }
}

#[pyclass]
#[derive(Clone)]
pub struct SparqlEmbeddedOxigraph {
    path: Option<String>,
    ntriples_file: String,
}

impl SparqlEmbeddedOxigraph {
    pub fn as_config(&self) -> EmbeddedOxigraphConfig {
        EmbeddedOxigraphConfig {
            path: self.path.clone(),
            ntriples_file: self.ntriples_file.clone(),
        }
    }
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
pub struct TimeseriesBigQueryDatabase {
    pub tables: Vec<TimeseriesTable>,
    pub key: String,
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

#[pyclass]
#[derive(Clone)]
pub struct TimeseriesTable {
    pub resource_name: String,
    pub schema: Option<String>,
    pub time_series_table: String,
    pub value_column: String,
    pub timestamp_column: String,
    pub identifier_column: String,
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
            year_column: self.year_column.clone(),
            month_column: self.month_column.clone(),
            day_column: self.day_column.clone(),
        })
    }
}

#[pyclass]
#[derive(Clone)]
pub struct Catalog {
    pub data_products: HashMap<String, DataProduct>,
}

#[pymethods]
impl Catalog {
    #[new]
    pub fn new(data_products: HashMap<String, DataProduct>) -> Catalog {
        Catalog { data_products }
    }
    //
    // pub fn to_json(&self) -> String {
    //     self.to_rust()?.to_json()
    // }
    //
    // pub fn from_json_string(json_string:String) -> Catalog {
    //
    // }
    //
    // pub fn from_json()
}

impl Catalog {
    pub fn to_rust(&self) -> Result<RustCatalog, PyChrontextError> {
        let mut data_products = HashMap::new();
        for (k, v) in &self.data_products {
            data_products.insert(k.clone(), v.to_rust()?);
        }
        Ok(RustCatalog { data_products })
    }
}

#[pyclass]
#[derive(Clone)]
pub struct DataProduct {
    pub query: String,
    pub types: HashMap<String, RDFType>,
}

#[pymethods]
impl DataProduct {
    #[new]
    pub fn new(query: String, types: HashMap<String, RDFType>) -> DataProduct {
        DataProduct { query, types }
    }
}

impl DataProduct {
    pub fn to_rust(&self) -> Result<RustDataProduct, PyChrontextError> {
        let mut rdf_node_types = HashMap::new();
        for (k, v) in &self.types {
            rdf_node_types.insert(k.clone(), v.to_rust()?);
        }
        let mut rdp = RustDataProduct {
            query_string: self.query.clone(),
            parsed_query: None,
            rdf_node_types,
        };
        rdp.init()
            .map_err(|x| PyChrontextError::DataProductQueryParseError(x))?;
        Ok(rdp)
    }
}

#[pyclass]
#[derive(Clone)]
pub enum RDFType {
    IRI {},
    BlankNode {},
    Literal { iri: String },
    Unknown {},
}

impl RDFType {
    pub fn to_rust(&self) -> Result<RustBaseRDFNodeType, PyChrontextError> {
        Ok(match self {
            RDFType::IRI { .. } => RustBaseRDFNodeType::IRI,
            RDFType::BlankNode { .. } => RustBaseRDFNodeType::BlankNode,
            RDFType::Literal { iri } => RustBaseRDFNodeType::Literal(
                NamedNode::new(iri).map_err(|x| PyChrontextError::DatatypeIRIParseError(x))?,
            ),
            RDFType::Unknown { .. } => RustBaseRDFNodeType::None,
        })
    }
}

#[pymodule]
#[pyo3(name = "chrontext")]
fn _chrontext(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize");
        }
    }

    m.add_class::<Engine>()?;
    m.add_class::<TimeseriesTable>()?;
    m.add_class::<TimeseriesBigQueryDatabase>()?;
    m.add_class::<TimeseriesOPCUADatabase>()?;
    m.add_class::<SparqlEmbeddedOxigraph>()?;
    m.add_class::<RDFType>()?;
    m.add_class::<DataProduct>()?;
    m.add_class::<Catalog>()?;
    Ok(())
}

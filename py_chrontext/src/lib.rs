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
use chrontext::engine::{Engine, EngineConfig};
use chrontext::sparql_database::sparql_embedded_oxigraph::EmbeddedOxigraphConfig;
use flight::client::ChrontextFlightClient;
use flight::server::ChrontextFlightServer;
use log::{debug, info};
use oxrdfio::RdfFormat;
use postgres::catalog::{Catalog, DataProduct};
use postgres::server::{start_server, Config};
use pydf_io::to_python::{df_to_py_df, fix_cats_and_multicolumns};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use representation::python::{PyIRI, PyLiteral, PyPrefix, PyRDFType, PyVariable, PyXSDDuration};
use representation::solution_mapping::EagerSolutionMappings;
use representation::BaseRDFNodeType;
use std::collections::HashMap;
use std::sync::Arc;
use secrecy::SecretString;
use templates::python::{a, py_triple, PyArgument, PyInstance, PyParameter, PyTemplate, PyXSD};
use tokio::runtime::Builder;
use virtualization::bigquery::VirtualizedBigQueryDatabase;
#[cfg(feature = "opcua")]
use virtualization::opcua::VirtualizedOPCUADatabase;
use virtualization::python::VirtualizedPythonDatabase;
use virtualization::{Virtualization, VirtualizedDatabase};
use virtualized_query::python::{
    PyAggregateExpression, PyExpression, PyOrderExpression, PyVirtualizedQuery,
};

#[pyclass(name = "Engine")]
pub struct PyEngine {
    engine: Option<Engine>,
    sparql_endpoint: Option<String>,
    sparql_embedded_oxigraph: Option<PySparqlEmbeddedOxigraph>,
    virtualized_python_database: Option<VirtualizedPythonDatabase>,
    virtualized_bigquery_database: Option<PyVirtualizedBigQueryDatabase>,
    #[cfg(feature = "opcua")]
    virtualized_opcua_database: Option<PyVirtualizedOPCUADatabase>,
    resources: HashMap<String, PyTemplate>,
}

impl PyEngine {
    pub fn new_impl(
        resources: HashMap<String, PyTemplate>,
        virtualized_python_database: Option<VirtualizedPythonDatabase>,
        virtualized_bigquery_database: Option<PyVirtualizedBigQueryDatabase>,
        #[cfg(feature = "opcua")] virtualized_opcua_database: Option<PyVirtualizedOPCUADatabase>,
        sparql_endpoint: Option<String>,
        sparql_embedded_oxigraph: Option<PySparqlEmbeddedOxigraph>,
    ) -> PyResult<PyEngine> {
        let num_sparql =
            sparql_endpoint.is_some() as usize + sparql_embedded_oxigraph.is_some() as usize;

        if num_sparql == 0 {
            return Err(PyChrontextError::MissingSPARQLDatabaseError.into());
        }
        if num_sparql > 1 {
            return Err(PyChrontextError::MultipleSPARQLDatabasesError.into());
        }

        #[cfg(feature = "opcua")]
        let num_virtualized = virtualized_bigquery_database.is_some() as usize
            + virtualized_opcua_database.is_some() as usize
            + virtualized_python_database.is_some() as usize;

        #[cfg(not(feature = "opcua"))]
        let num_virtualized = virtualized_bigquery_database.is_some() as usize
            + virtualized_python_database.is_some() as usize;

        if num_virtualized == 0 {
            return Err(PyChrontextError::MissingVirtualizedDatabaseError.into());
        }
        if num_virtualized > 1 {
            return Err(PyChrontextError::MultipleVirtualizedDatabasesError.into());
        }

        let engine = PyEngine {
            engine: None,
            sparql_endpoint,
            sparql_embedded_oxigraph,
            virtualized_python_database,
            virtualized_bigquery_database,
            #[cfg(feature = "opcua")]
            virtualized_opcua_database,
            resources,
        };
        Ok(engine)
    }
}

#[pymethods]
impl PyEngine {
    #[cfg(feature = "opcua")]
    #[new]
    pub fn new<'py>(
        resources: HashMap<String, PyTemplate>,
        virtualized_python_database: Option<VirtualizedPythonDatabase>,
        virtualized_bigquery_database: Option<PyVirtualizedBigQueryDatabase>,
        virtualized_opcua_database: Option<PyVirtualizedOPCUADatabase>,
        sparql_endpoint: Option<String>,
        sparql_embedded_oxigraph: Option<PySparqlEmbeddedOxigraph>,
    ) -> PyResult<PyEngine> {
        Self::new_impl(
            resources,
            virtualized_python_database,
            virtualized_bigquery_database,
            virtualized_opcua_database,
            sparql_endpoint,
            sparql_embedded_oxigraph,
        )
    }

    #[cfg(not(feature = "opcua"))]
    #[new]
    pub fn new<'py>(
        resources: HashMap<String, PyTemplate>,
        virtualized_python_database: Option<VirtualizedPythonDatabase>,
        virtualized_bigquery_database: Option<PyVirtualizedBigQueryDatabase>,
        sparql_endpoint: Option<String>,
        sparql_embedded_oxigraph: Option<PySparqlEmbeddedOxigraph>,
    ) -> PyResult<PyEngine> {
        Self::new_impl(
            resources,
            virtualized_python_database,
            virtualized_bigquery_database,
            sparql_endpoint,
            sparql_embedded_oxigraph,
        )
    }

    pub fn init(&mut self) -> PyResult<()> {
        if self.engine.is_none() {
            let virtualized_database = if let Some(db) = &self.virtualized_bigquery_database {
                VirtualizedDatabase::VirtualizedBigQueryDatabase(VirtualizedBigQueryDatabase::new(
                    db.key_json_path.clone(),
                    db.resource_sql_map.clone(),
                ))
            } else if let Some(db) = &self.virtualized_python_database {
                VirtualizedDatabase::VirtualizedPythonDatabase(db.clone())
            } else {
                #[cfg(feature = "opcua")]
                if let Some(db) = &self.virtualized_opcua_database {
                    VirtualizedDatabase::VirtualizedOPCUADatabase(VirtualizedOPCUADatabase::new(
                        &db.endpoint,
                        db.namespace,
                    ))
                } else {
                    panic!("Should never happen");
                }
                #[cfg(not(feature = "opcua"))]
                panic!("Should never happen");
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

            let mut virtualization_map = HashMap::new();
            for (k, v) in &self.resources {
                virtualization_map.insert(k.clone(), v.template.clone());
            }
            let virtualization = Virtualization {
                resources: virtualization_map,
            };

            let config = EngineConfig {
                sparql_oxigraph_config,
                virtualized_database,
                sparql_endpoint,
                virtualization,
            };

            self.engine =
                Some(Engine::from_config(config).map_err(|x| PyChrontextError::ChrontextError(x))?);
        }
        Ok(())
    }

    pub fn query(
        &mut self,
        sparql: &str,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        if self.engine.is_none() {
            self.init()?;
        }

        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let (mut df, mut datatypes, pushdown_contexts) = py.allow_threads(move || {
            builder
                .build()
                .unwrap()
                .block_on(self.engine.as_mut().unwrap().query(sparql))
                .map_err(|err| PyChrontextError::ChrontextError(err))
        })?;

        (df, datatypes) =
            fix_cats_and_multicolumns(df, datatypes, native_dataframe.unwrap_or(false));
        let pydf = df_to_py_df(
            df,
            datatypes,
            Some(pushdown_contexts),
            include_datatypes.unwrap_or(false),
            py,
        )?;
        Ok(pydf)
    }

    pub fn serve_postgres(&mut self, catalog: PyCatalog, py: Python) -> PyResult<()> {
        py.allow_threads(move || {
            if self.engine.is_none() {
                self.init()?;
            }
            let catalog = catalog.to_rust()?;

            let config = Config::default();
            let mut builder = Builder::new_multi_thread();
            builder.enable_all();
            builder
                .build()
                .unwrap()
                .block_on(start_server(self.engine.take().unwrap(), config, catalog))
                .unwrap();
            Ok(())
        })
    }

    pub fn serve_flight(&mut self, address: &str, py:Python) -> PyResult<()> {
        py.allow_threads(move || {
            if self.engine.is_none() {
                self.init()?;
            }
            let flight_server = ChrontextFlightServer::new(Some(Arc::new(self.engine.take().unwrap())));
            let mut builder = Builder::new_multi_thread();
            builder.enable_all();
            builder
                .build()
                .unwrap()
                .block_on(flight_server.serve(address))
                .map_err(|x|PyChrontextError::FlightServerError(x))?;
            Ok(())
        })
    }
}

#[derive(Clone)]
#[pyclass(name = "FlightClient")]
pub struct PyFlightClient {
    uri:String,
    metadata: HashMap<String, SecretString>
}

#[pymethods]
impl PyFlightClient {
    #[new]
    pub fn new(uri:String, metadata: Option<HashMap<String, String>>) -> PyResult<Self> {
        let mut metadata_s = HashMap::new();
        if let Some(metadata) = metadata {
            for (k,v) in metadata {
                metadata_s.insert(k, SecretString::from(v));
            }
        }
        Ok(Self {uri, metadata:metadata_s})
    }

    pub fn query(
        &mut self,
        sparql: &str,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        py: Python,
    ) -> PyResult<PyObject> {
        let sparql = sparql.to_string();
        let res = py.allow_threads(move || {

            let sparql = sparql;
            let mut builder = Builder::new_multi_thread();
            builder.enable_all();
            info!("Connecting to server {}", &self.uri);
            let mut client = ChrontextFlightClient::new(&self.uri);
            info!("Connected to server, sending query");
            let sm = builder
                .build()
                .unwrap()
                .block_on(client.query(&sparql, &self.metadata))
                .map_err(|x|PyChrontextError::FlightClientError(x))?;
            Ok(sm)
        });
        match res  {
            Ok(sm) => {
                let EagerSolutionMappings {
                    mut mappings,
                    mut rdf_node_types,
                } = sm.as_eager();
                (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                    mappings,
                    rdf_node_types,
                    native_dataframe.unwrap_or(false),
                );
                let pydf = df_to_py_df(
                    mappings,
                    rdf_node_types,
                    None,
                    include_datatypes.unwrap_or(false),
                    py,
                )?;
                Ok(pydf)
            },
            Err(e) => Err(e)
        }

    }
}

#[derive(Clone)]
#[pyclass(name = "SparqlEmbeddedOxigraph")]
pub struct PySparqlEmbeddedOxigraph {
    path: Option<String>,
    rdf_file: String,
    rdf_format: Option<RdfFormat>,
}

impl PySparqlEmbeddedOxigraph {
    pub fn as_config(&self) -> EmbeddedOxigraphConfig {
        EmbeddedOxigraphConfig {
            path: self.path.clone(),
            rdf_file: self.rdf_file.clone(),
            rdf_format: self.rdf_format.clone(),
        }
    }
}

#[pymethods]
impl PySparqlEmbeddedOxigraph {
    #[new]
    pub fn new(
        rdf_file: String,
        rdf_format: Option<String>,
        path: Option<String>,
    ) -> PySparqlEmbeddedOxigraph {
        let rdf_format = if let Some(format) = rdf_format {
            Some(resolve_format(&format))
        } else {
            None
        };
        PySparqlEmbeddedOxigraph {
            path,
            rdf_file,
            rdf_format,
        }
    }
}

fn resolve_format(format: &str) -> RdfFormat {
    match format.to_lowercase().as_str() {
        "ntriples" => RdfFormat::NTriples,
        "turtle" => RdfFormat::Turtle,
        "rdf/xml" | "xml" | "rdfxml" => RdfFormat::RdfXml,
        _ => unimplemented!("Unknown format {}", format),
    }
}

#[pyclass(name = "VirtualizedBigQueryDatabase")]
#[derive(Clone)]
pub struct PyVirtualizedBigQueryDatabase {
    pub resource_sql_map: Py<PyDict>,
    pub key_json_path: String,
}

#[pymethods]
impl PyVirtualizedBigQueryDatabase {
    #[new]
    pub fn new(
        resource_sql_map: Py<PyDict>,
        key_json_path: String,
    ) -> PyVirtualizedBigQueryDatabase {
        Self {
            resource_sql_map,
            key_json_path,
        }
    }
}
#[cfg(feature = "opcua")]
#[pyclass(name = "VirtualizedOPCUADatabase")]
#[derive(Clone)]
pub struct PyVirtualizedOPCUADatabase {
    namespace: u16,
    endpoint: String,
}

#[cfg(feature = "opcua")]
#[pymethods]
impl PyVirtualizedOPCUADatabase {
    #[new]
    pub fn new(endpoint: String, namespace: u16) -> PyVirtualizedOPCUADatabase {
        Self {
            namespace,
            endpoint,
        }
    }
}

#[pyclass(name = "Catalog")]
#[derive(Clone)]
pub struct PyCatalog {
    pub data_products: HashMap<String, PyDataProduct>,
}

#[pymethods]
impl PyCatalog {
    #[new]
    pub fn new(data_products: HashMap<String, PyDataProduct>) -> PyCatalog {
        PyCatalog { data_products }
    }
}

impl PyCatalog {
    pub fn to_rust(&self) -> Result<Catalog, PyChrontextError> {
        let mut data_products = HashMap::new();
        for (k, v) in &self.data_products {
            data_products.insert(k.clone(), v.to_rust()?);
        }
        Ok(Catalog { data_products })
    }
}

#[pyclass(name = "DataProduct")]
#[derive(Clone)]
pub struct PyDataProduct {
    pub query: String,
    pub types: HashMap<String, PyRDFType>,
}

#[pymethods]
impl PyDataProduct {
    #[new]
    pub fn new(query: String, types: HashMap<String, PyRDFType>) -> PyDataProduct {
        PyDataProduct { query, types }
    }
}

impl PyDataProduct {
    pub fn to_rust(&self) -> Result<DataProduct, PyChrontextError> {
        let mut rdf_node_types = HashMap::new();
        for (k, v) in &self.types {
            rdf_node_types.insert(
                k.clone(),
                BaseRDFNodeType::from_rdf_node_type(&v.as_rdf_node_type()),
            );
        }
        let mut rdp = DataProduct {
            query_string: self.query.clone(),
            parsed_query: None,
            rdf_node_types,
        };
        rdp.init()
            .map_err(|x| PyChrontextError::DataProductQueryParseError(x))?;
        Ok(rdp)
    }
}

#[pymodule]
#[pyo3(name = "chrontext")]
fn _chrontext(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize");
        }
    }

    m.add_class::<PyEngine>()?;
    m.add_class::<PySparqlEmbeddedOxigraph>()?;
    m.add_class::<VirtualizedPythonDatabase>()?;
    m.add_class::<PyVirtualizedBigQueryDatabase>()?;
    #[cfg(feature = "opcua")]
    m.add_class::<PyVirtualizedOPCUADatabase>()?;
    m.add_class::<PyDataProduct>()?;
    m.add_class::<PyCatalog>()?;
    m.add_class::<PyRDFType>()?;
    m.add_class::<PyPrefix>()?;
    m.add_class::<PyVariable>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyIRI>()?;
    m.add_class::<PyParameter>()?;
    m.add_class::<PyArgument>()?;
    m.add_class::<PyTemplate>()?;
    m.add_class::<PyInstance>()?;
    m.add_class::<PyXSD>()?;
    m.add_function(wrap_pyfunction!(py_triple, m)?)?;
    m.add_function(wrap_pyfunction!(a, m)?)?;

    m.add_class::<PyFlightClient>()?;

    let child = PyModule::new_bound(m.py(), "vq")?;
    child.add_class::<PyVirtualizedQuery>()?;
    child.add_class::<PyExpression>()?;
    child.add_class::<PyOrderExpression>()?;
    child.add_class::<PyAggregateExpression>()?;
    child.add_class::<PyXSDDuration>()?;
    m.add_submodule(&child)?;

    _py.import_bound("sys")?
        .getattr("modules")?
        .set_item("chrontext.vq", child)?;
    Ok(())
}

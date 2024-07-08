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
use chrontext::engine::{Engine, EngineConfig, Virtualization};
use chrontext::sparql_database::sparql_embedded_oxigraph::EmbeddedOxigraphConfig;
use log::debug;
use postgres::catalog::{Catalog, DataProduct};
use postgres::server::{start_server, Config};
use pydf_io::to_python::{df_to_py_df, dtypes_map, fix_cats_and_multicolumns};
use pyo3::prelude::*;
use representation::python::{PyRDFType,PyIRI};
use representation::BaseRDFNodeType;
use std::collections::HashMap;
use templates::python::{
    a, py_triple, xsd, PyArgument, PyInstance, PyLiteral, PyParameter, PyPrefix, PyTemplate,
    PyVariable,
};
use tokio::runtime::Builder;
use virtualization::python::PyVirtualizedDatabase;
use virtualization::VirtualizedDatabase;

#[pyclass(name = "Engine")]
pub struct PyEngine {
    engine: Option<Engine>,
    sparql_endpoint: Option<String>,
    sparql_embedded_oxigraph: Option<PySparqlEmbeddedOxigraph>,
    virtualized_database: PyVirtualizedDatabase,
    resources: HashMap<String, PyTemplate>,
}

#[pymethods]
impl PyEngine {
    #[new]
    pub fn new<'py>(
        virtualized_database: PyVirtualizedDatabase,
        resources: HashMap<String, PyTemplate>,
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

        let engine = PyEngine {
            engine: None,
            sparql_endpoint,
            sparql_embedded_oxigraph,
            virtualized_database,
            resources,
        };
        Ok(engine)
    }

    pub fn init(&mut self) -> PyResult<()> {
        if self.engine.is_none() {
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

            let virtualized_database = VirtualizedDatabase::PyVirtualizedDatabase(self.virtualized_database.clone());
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
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        if self.engine.is_none() {
            self.init()?;
        }

        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let (mut df, mut datatypes) = builder
            .build()
            .unwrap()
            .block_on(self.engine.as_mut().unwrap().query(sparql))
            .map_err(|err| PyChrontextError::QueryExecutionError(err))?;

        (df, datatypes) =
            fix_cats_and_multicolumns(df, datatypes, native_dataframe.unwrap_or(false));
        let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
        Ok(pydf)
    }

    pub fn serve_postgres(&mut self, catalog: PyCatalog) -> PyResult<()> {
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

#[derive(Clone)]
#[pyclass(name = "SparqlEmbeddedOxigraph")]
pub struct PySparqlEmbeddedOxigraph {
    path: Option<String>,
    ntriples_file: String,
}

impl PySparqlEmbeddedOxigraph {
    pub fn as_config(&self) -> EmbeddedOxigraphConfig {
        EmbeddedOxigraphConfig {
            path: self.path.clone(),
            ntriples_file: self.ntriples_file.clone(),
        }
    }
}

#[pymethods]
impl PySparqlEmbeddedOxigraph {
    #[new]
    pub fn new(ntriples_file: String, path: Option<String>) -> PySparqlEmbeddedOxigraph {
        PySparqlEmbeddedOxigraph {
            path,
            ntriples_file,
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
fn _chrontext(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize");
        }
    }

    m.add_class::<PyEngine>()?;
    m.add_class::<PySparqlEmbeddedOxigraph>()?;
    m.add_class::<PyVirtualizedDatabase>()?;
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
    m.add_function(wrap_pyfunction!(py_triple, m)?)?;
    m.add_function(wrap_pyfunction!(a, m)?)?;
    m.add_function(wrap_pyfunction!(xsd, m)?)?;
    Ok(())
}

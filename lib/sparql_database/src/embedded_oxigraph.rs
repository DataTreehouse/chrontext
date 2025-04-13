use super::{parse_json_text, SparqlQueryError, SparqlQueryable};
use async_trait::async_trait;
use pyo3::types::PyAnyMethods;
use pyo3::{Py, PyAny, Python};
use sparesults::QuerySolution;
use spargebra::Query;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum EmbeddedOxigraphError {
    #[error("Oxigraph evaluation error")]
    EvaluationError(String),
}

pub struct EmbeddedOxigraph {
    pub store: Py<PyAny>,
}

#[async_trait]
impl SparqlQueryable for EmbeddedOxigraph {
    async fn execute(&self, query: &Query) -> Result<Vec<QuerySolution>, SparqlQueryError> {
        Python::with_gil(|py| {
            let json_format = py
                .import("pyoxigraph")
                .unwrap()
                .getattr("QueryResultsFormat")
                .unwrap()
                .getattr("JSON")
                .unwrap();
            let json = self
                .store
                .call_method1(py, "query", (query.to_string(),))
                .unwrap()
                .call_method1(py, "serialize", ((), json_format))
                .unwrap();
            let json_bytes: Vec<u8> = json.extract(py).unwrap();
            let json_string = String::from_utf8(json_bytes).unwrap();
            parse_json_text(&json_string)
        })
    }
}

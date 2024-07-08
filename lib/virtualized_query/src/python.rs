use crate::{BasicVirtualizedQuery, VirtualizedQuery};
use pyo3::prelude::*;
use representation::python::PyIRI;
use spargebra::algebra::Expression;
use templates::python::{PyLiteral, PyVariable};

#[pyclass(name = "VirtualizedQuery")]
pub enum PyVirtualizedQuery {
    BasicVirtualizedQuery { basic: PyBasicVirtualizedQuery},
    FilteredVirtualizedQuery {filtered: PyFilteredVirtualizedQuery},
}

impl PyVirtualizedQuery {
    pub fn new(vq:VirtualizedQuery) -> PyVirtualizedQuery {
        match vq {
            VirtualizedQuery::Basic(basic) => PyVirtualizedQuery::BasicVirtualizedQuery {
                basic:PyBasicVirtualizedQuery::new(basic)
            },
            VirtualizedQuery::Filtered(inner, expression) => todo!(),
            _ => todo!()
        }
    }
}


#[derive(Clone)]
#[pyclass(name = "BasicVirtualizedQuery")]
pub struct PyBasicVirtualizedQuery {
    inner: BasicVirtualizedQuery
}

impl PyBasicVirtualizedQuery {
    pub fn new(bvq:BasicVirtualizedQuery) -> PyBasicVirtualizedQuery {
        PyBasicVirtualizedQuery { inner:bvq }
    }
}

#[pymethods]
impl PyBasicVirtualizedQuery {
    #[getter]
    fn resource(&self) -> String {
        self.inner.resource.as_ref().unwrap().clone()
    }

    #[getter]
    fn ids(&self) -> Vec<String> {
        self.inner.ids.as_ref().unwrap().clone()
    }
}

#[derive(Clone)]
#[pyclass(name="FilteredVirtualizedQuery")]
pub struct PyFilteredVirtualizedQuery {
    pub virtualized: Py<PyVirtualizedQuery>,
    pub filter: Py<PyExpression>,
}

#[derive(Clone)]
#[pyclass(name="Expression")]
pub enum PyExpression {
    Greater{ left:Py<PyExpression>, right:Py<PyExpression>},
    Less{left:Py<PyExpression>, right:Py<PyExpression>},
    And{left:Py<PyExpression>, right:Py<PyExpression>},
    Variable{variable:Py<PyVariable>},
    IRI{iri:Py<PyIRI>},
    Literal{literal:Py<PyLiteral>}
}

impl PyExpression {
    pub fn new(expression:Expression, py:Python) -> PyResult<PyExpression> {
        Ok(match expression {
            Expression::And(left, right) => {
                PyExpression::And {
                    left:Py::new(py,
                    PyExpression::new( * left,
                    py)?)?,
                    right:Py::new(py,
                                  PyExpression::new( * right,
                                                     py)?)?,
                }
            },
            Expression::Greater(_, _) => {todo!()},
            Expression::Less(_, _) => {todo!()},
            Expression::Variable(_) => {todo!()},
            _ => {todo!()}
        })
    }
}
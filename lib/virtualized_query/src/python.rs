use crate::{BasicVirtualizedQuery, VirtualizedQuery};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use representation::python::{PyIRI, PyLiteral, PyVariable};
use spargebra::algebra::Expression;
use thiserror::*;
//
// #[derive(Error, Debug)]
// pub enum PyExpressionError {
//     #[error("Bad method call: `{0}`")]
//     BadMethodCallError(String),
// }
//
// impl std::convert::From<PyExpressionError> for PyErr {
//     fn from(err: PyExpressionError) -> PyErr {
//         match &err {
//             PyExpressionError::BadMethodCallError(err) => {
//                 BadMethodCallErrorException::new_err(format!("{}", err))
//             }
//         }
//     }
// }
//
// create_exception!(exceptions, BadMethodCallErrorException, PyException);

#[pyclass(name = "VirtualizedQuery")]
pub enum PyVirtualizedQuery {
    BasicVirtualizedQuery {
        basic: PyBasicVirtualizedQuery,
    },
    FilteredVirtualizedQuery {
        filtered: PyFilteredVirtualizedQuery,
    },
}

impl PyVirtualizedQuery {
    pub fn new(vq: VirtualizedQuery, py: Python) -> PyResult<PyVirtualizedQuery> {
        Ok(match vq {
            VirtualizedQuery::Basic(basic) => PyVirtualizedQuery::BasicVirtualizedQuery {
                basic: PyBasicVirtualizedQuery::new(basic),
            },
            VirtualizedQuery::Filtered(inner, expression) => {
                PyVirtualizedQuery::FilteredVirtualizedQuery {
                    filtered: PyFilteredVirtualizedQuery {
                        virtualized: Py::new(py, PyVirtualizedQuery::new(*inner, py)?)?,
                        filter: Py::new(py, PyExpression::new(expression, py)?)?,
                    },
                }
            }
            _ => todo!(),
        })
    }
}

#[derive(Clone)]
#[pyclass(name = "BasicVirtualizedQuery")]
pub struct PyBasicVirtualizedQuery {
    inner: BasicVirtualizedQuery,
}

impl PyBasicVirtualizedQuery {
    pub fn new(bvq: BasicVirtualizedQuery) -> PyBasicVirtualizedQuery {
        PyBasicVirtualizedQuery { inner: bvq }
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
#[pyclass(name = "FilteredVirtualizedQuery")]
pub struct PyFilteredVirtualizedQuery {
    pub virtualized: Py<PyVirtualizedQuery>,
    pub filter: Py<PyExpression>,
}

#[pymethods]
impl PyFilteredVirtualizedQuery {
    #[getter]
    pub fn virtualized(&self, py: Python) -> Py<PyVirtualizedQuery> {
        self.virtualized.clone_ref(py)
    }

    #[getter]
    pub fn filter(&self, py: Python) -> Py<PyExpression> {
        self.filter.clone_ref(py)
    }
}

#[derive(Clone, Debug)]
#[pyclass(name = "Expression")]
pub enum PyExpression {
    Greater {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Less {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    And {
        left: Py<PyExpression>,
        right: Py<PyExpression>,
    },
    Variable {
        variable: Py<PyVariable>,
    },
    IRI {
        iri: Py<PyIRI>,
    },
    Literal {
        literal: Py<PyLiteral>,
    },
}

#[pymethods]
impl PyExpression {
    fn expression_type(&self) -> &str {
        match self {
            PyExpression::Greater { .. } => "Greater",
            PyExpression::Less { .. } => "Less",
            PyExpression::And { .. } => "And",
            PyExpression::Variable { .. } => "Variable",
            PyExpression::IRI { .. } => "IRI",
            PyExpression::Literal { .. } => "Literal",
        }
    }

    #[getter]
    fn left(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyExpression::Greater { left, .. }
            | PyExpression::Less { left, .. }
            | PyExpression::And { left, .. } => Some(left.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn right(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyExpression::Greater { right, .. }
            | PyExpression::Less { right, .. }
            | PyExpression::And { right, .. } => Some(right.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn variable(&self, py: Python) -> Option<Py<PyVariable>> {
        match self {
            PyExpression::Variable { variable } => Some(variable.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn literal(&self, py: Python) -> Option<Py<PyLiteral>> {
        match self {
            PyExpression::Literal { literal } => Some(literal.clone_ref(py)),
            _ => None,
        }
    }
}

impl PyExpression {
    pub fn new(expression: Expression, py: Python) -> PyResult<PyExpression> {
        Ok(match expression {
            Expression::And(left, right) => PyExpression::And {
                left: Py::new(py, PyExpression::new(*left, py)?)?,
                right: Py::new(py, PyExpression::new(*right, py)?)?,
            },
            Expression::Greater(left, right) => PyExpression::Greater {
                left: Py::new(py, PyExpression::new(*left, py)?)?,
                right: Py::new(py, PyExpression::new(*right, py)?)?,
            },
            Expression::Less(left, right) => PyExpression::Less {
                left: Py::new(py, PyExpression::new(*left, py)?)?,
                right: Py::new(py, PyExpression::new(*right, py)?)?,
            },
            Expression::Variable(variable) => PyExpression::Variable {
                variable: Py::new(py, PyVariable { variable })?,
            },
            Expression::Literal(l) => PyExpression::Literal {
                literal: Py::new(py, PyLiteral::from_literal(l))?,
            },
            _ => todo!(),
        })
    }
}

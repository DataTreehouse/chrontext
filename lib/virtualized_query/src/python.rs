use crate::{BasicVirtualizedQuery, VirtualizedQuery};
use polars::export::ahash::{HashMap, HashMapExt};
use polars::prelude::AnyValue;
use pyo3::prelude::*;
use representation::python::{PyIRI, PyLiteral, PyVariable};
use representation::query_context::Context;
use spargebra::algebra::{AggregateExpression, AggregateFunction, Expression};
use spargebra::term::TermPattern;
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

#[derive(Clone)]
#[pyclass(name = "VirtualizedQuery")]
pub enum PyVirtualizedQuery {
    Basic {
        identifier_name: String,
        column_mapping: HashMap<String, String>,
        resource: String,
        ids: Vec<String>,
        grouping_column_name: Option<String>,
        id_to_grouping_mapping: Option<HashMap<String, i64>>,
    },
    Filtered {
        filter: Py<PyExpression>,
        query: Py<PyVirtualizedQuery>,
    },
    Grouped {
        query: Py<PyVirtualizedQuery>,
        by: Vec<PyVariable>,
        aggregations: Vec<(Py<PyVariable>, Py<PyAggregateExpression>)>,
    },
}

#[pymethods]
impl PyVirtualizedQuery {
    fn type_name(&self) -> &str {
        match self {
            PyVirtualizedQuery::Basic { .. } => "Basic",
            PyVirtualizedQuery::Filtered { .. } => "Filtered",
            PyVirtualizedQuery::Grouped { .. } => "Grouped",
        }
    }

    #[getter]
    fn filter(&self, py: Python) -> Option<Py<PyExpression>> {
        match self {
            PyVirtualizedQuery::Filtered { filter, .. } => Some(filter.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn identifier_name(&self) -> Option<String> {
        match self {
            PyVirtualizedQuery::Basic {
                identifier_name, ..
            } => Some(identifier_name.clone()),
            _ => None,
        }
    }
    #[getter]
    fn resource(&self) -> Option<String> {
        match self {
            PyVirtualizedQuery::Basic { resource, .. } => Some(resource.clone()),
            _ => None,
        }
    }
    #[getter]
    fn ids(&self) -> Option<Vec<String>> {
        match self {
            PyVirtualizedQuery::Basic { ids, .. } => Some(ids.clone()),
            _ => None,
        }
    }
    #[getter]
    fn column_mapping(&self) -> Option<HashMap<String, String>> {
        match self {
            PyVirtualizedQuery::Basic { column_mapping, .. } => Some(column_mapping.clone()),
            _ => None,
        }
    }
    #[getter]
    fn id_to_grouping_mapping(&self) -> Option<HashMap<String, i64>> {
        match self {
            PyVirtualizedQuery::Basic {
                id_to_grouping_mapping,
                ..
            } => {
                if let Some(id_to_grouping_mapping) = id_to_grouping_mapping {
                    Some(id_to_grouping_mapping.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    #[getter]
    fn grouping_column_name(&self) -> Option<String> {
        match self {
            PyVirtualizedQuery::Basic {
                grouping_column_name,
                ..
            } => {
                if let Some(grouping_column_name) = grouping_column_name {
                    Some(grouping_column_name.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    #[getter]
    fn query(&self, py: Python) -> Option<Py<PyVirtualizedQuery>> {
        match self {
            PyVirtualizedQuery::Filtered { query, .. } => Some(query.clone_ref(py)),
            _ => None,
        }
    }

    #[getter]
    fn by(&self) -> Option<Vec<PyVariable>> {
        match self {
            PyVirtualizedQuery::Grouped { by, .. } => {
                Some(by.clone())
            }
            _ => {None}
        }
    }

    #[getter]
    fn aggregations(&self) -> Option<Vec<(Py<PyVariable>, Py<PyAggregateExpression>)>> {
        match self {
            PyVirtualizedQuery::Grouped { aggregations, .. } => {
                Some(aggregations.clone())
            }
            _ => None
        }
    }
}

impl PyVirtualizedQuery {
    pub fn new(vq: VirtualizedQuery, py: Python) -> PyResult<PyVirtualizedQuery> {
        Ok(match vq {
            VirtualizedQuery::Basic(basic) => {
                let mut column_mapping = HashMap::new();
                for (k, v) in &basic.column_mapping {
                    if let TermPattern::Variable(v) = v {
                        column_mapping.insert(k.as_str().to_string(), v.as_str().to_string());
                    }
                }
                let id_to_grouping_mapping = if let Some(df) = basic.grouping_mapping {
                    let mut id_to_grouping_mapping = HashMap::new();
                    let id_iter = df
                        .column(basic.identifier_variable.as_str())
                        .unwrap()
                        .iter();
                    let group_iter = df
                        .column(basic.grouping_col.as_ref().unwrap())
                        .unwrap()
                        .iter();
                    for (id, group) in id_iter.zip(group_iter) {
                        if let (AnyValue::String(id), AnyValue::Int64(group)) = (id, group) {
                            id_to_grouping_mapping.insert(id.to_string(), group);
                        } else {
                            panic!("Should never happen")
                        }
                    }
                    Some(id_to_grouping_mapping)
                } else {
                    None
                };
                PyVirtualizedQuery::Basic {
                    identifier_name: basic.identifier_variable.as_str().to_string(),
                    column_mapping,
                    resource: basic.resource.unwrap(),
                    ids: basic.ids.unwrap(),
                    grouping_column_name: basic.grouping_col,
                    id_to_grouping_mapping,
                }
            }
            VirtualizedQuery::Filtered(inner, expression) => PyVirtualizedQuery::Filtered {
                query: Py::new(py, PyVirtualizedQuery::new(*inner, py)?)?,
                filter: Py::new(py, PyExpression::new(&expression, py)?)?,
            },
            VirtualizedQuery::Grouped(grouped) => {
                let mut by = vec![];
                for v in &grouped.by {
                    by.push(PyVariable::new(v.as_str().to_string())?);
                }
                let mut aggregations = vec![];
                for (v, a) in &grouped.aggregations {
                    aggregations.push((
                        Py::new(py, PyVariable::new(v.as_str().to_string())?)?,
                        Py::new(py, PyAggregateExpression::new(a, py)?)?,
                    ))
                }
                PyVirtualizedQuery::Grouped {
                    query: Py::new(py, PyVirtualizedQuery::new(*grouped.vq, py)?)?,
                    by,
                    aggregations,
                }
            }
            _ => todo!()
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
    pub query: Py<PyVirtualizedQuery>,
    pub filter: Py<PyExpression>,
}

#[pymethods]
impl PyFilteredVirtualizedQuery {
    #[getter]
    pub fn query(&self, py: Python) -> Py<PyVirtualizedQuery> {
        self.query.clone_ref(py)
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
    pub fn new(expression: &Expression, py: Python) -> PyResult<PyExpression> {
        Ok(match expression {
            Expression::And(left, right) => PyExpression::And {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Greater(left, right) => PyExpression::Greater {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Less(left, right) => PyExpression::Less {
                left: Py::new(py, PyExpression::new(left, py)?)?,
                right: Py::new(py, PyExpression::new(right, py)?)?,
            },
            Expression::Variable(variable) => PyExpression::Variable {
                variable: Py::new(
                    py,
                    PyVariable {
                        variable: variable.clone(),
                    },
                )?,
            },
            Expression::Literal(l) => PyExpression::Literal {
                literal: Py::new(py, PyLiteral::from_literal(l.clone()))?,
            },
            _ => todo!(),
        })
    }
}

#[pyclass(name = "AggregateExpression")]
#[derive(Clone)]
pub struct PyAggregateExpression {
    pub name: AggregateFunction,
    pub expression: Option<Py<PyExpression>>,
}

impl PyAggregateExpression {
    pub fn new(
        aggregate_expression: &AggregateExpression,
        py: Python,
    ) -> PyResult<PyAggregateExpression> {
        Ok(match aggregate_expression {
            AggregateExpression::CountSolutions { .. } => PyAggregateExpression {
                name: AggregateFunction::Count,
                expression: None,
            },
            AggregateExpression::FunctionCall { name, expr, .. } => PyAggregateExpression {
                name: name.clone(),
                expression: Some(Py::new(py, PyExpression::new(expr, py)?)?),
            },
        })
    }
}

#[pymethods]
impl PyAggregateExpression {
    #[getter]
    fn name(&self) -> String {
        self.name.to_string()
    }

    #[getter]
    fn expression(&self) -> Option<Py<PyExpression>> {
        self.expression.clone()
    }
}

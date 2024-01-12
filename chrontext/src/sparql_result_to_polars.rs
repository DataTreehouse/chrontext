use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Term};
use polars::prelude::{DataFrame, LiteralValue};
use sparesults::QuerySolution;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use representation::RDFNodeType;
use representation::sparql_to_polars::{polars_literal_values_to_series, sparql_term_to_polars_literal_value};

pub(crate) fn create_static_query_dataframe(
    static_query: &Query,
    static_query_solutions: Vec<QuerySolution>,
) -> (DataFrame, HashMap<String, RDFNodeType>) {
    let column_variables;
    if let Query::Select {
        dataset: _,
        pattern,
        base_iri: _,
    } = static_query
    {
        if let GraphPattern::Project { variables, .. } = pattern {
            column_variables = variables.clone();
        } else if let GraphPattern::Distinct { inner } = pattern {
            if let GraphPattern::Project { variables, .. } = inner.as_ref() {
                column_variables = variables.clone();
            } else {
                panic!("");
            }
        } else {
            panic!("");
        }
    } else {
        panic!("");
    }

    let mut series_vec = vec![];
    let mut datatypes = HashMap::new();
    'outer: for c in &column_variables {
        let c_str = c.as_str();
        for s in &static_query_solutions {
            if let Some(term) = s.get(c) {
                match term {
                    Term::NamedNode(_) => {
                        datatypes.insert(c_str.to_string(), xsd::ANY_URI.into_owned());
                    }
                    Term::Literal(l) => {
                        datatypes.insert(c_str.to_string(), l.datatype().into_owned());
                    }
                    _ => {
                        panic!("Not supported")
                    } //Blank node
                }
                continue 'outer;
            }
        }
    }

    for c in &column_variables {
        let mut literal_values = vec![];
        for s in &static_query_solutions {
            literal_values.push(if let Some(term) = s.get(c) {
                sparql_term_to_polars_literal_value(term)
            } else {
                LiteralValue::Null
            });
        }

        let series = polars_literal_values_to_series(literal_values, c.as_str());
        series_vec.push(series);
    }
    let df = DataFrame::new(series_vec).expect("Create df problem");
    (df, datatypes)
}


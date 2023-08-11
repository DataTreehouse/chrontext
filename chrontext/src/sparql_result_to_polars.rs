use std::collections::HashMap;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, Term};
use polars::export::chrono::{DateTime, NaiveDateTime, Utc};
use polars::prelude::{DataFrame, LiteralValue, Series, TimeUnit};
use sparesults::QuerySolution;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::str::FromStr;

pub(crate) fn create_static_query_dataframe(
    static_query: &Query,
    static_query_solutions: Vec<QuerySolution>,
) -> (DataFrame, HashMap<String, NamedNode>) {
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
                    Term::NamedNode(_) => {datatypes.insert(c_str.to_string(), xsd::ANY_URI.into_owned());}
                    Term::Literal(l) => {datatypes.insert(c_str.to_string(), l.datatype().into_owned());}
                    _ => {panic!("Not supported")} //Blank node
                }
                continue 'outer
            }
        }
    }

    for c in &column_variables {
        let mut literal_values = vec![];
        for s in &static_query_solutions {
            literal_values.push(
            if let Some(term) = s.get(c) {
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

pub(crate) fn sparql_term_to_polars_literal_value(term: &Term) -> polars::prelude::LiteralValue {
    match term {
        Term::NamedNode(named_node) => sparql_named_node_to_polars_literal_value(named_node),
        Term::Literal(lit) => sparql_literal_to_polars_literal_value(lit),
        _ => {
            panic!("Not supported")
        }
    }
}

pub(crate) fn sparql_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::Utf8(named_node.as_str().to_string())
}

pub(crate) fn sparql_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
    let datatype = lit.datatype();
    let value = lit.value();
    let literal_value = if datatype == xsd::STRING {
        LiteralValue::Utf8(value.to_string())
    } else if datatype == xsd::UNSIGNED_INT {
        let u = u32::from_str(value).expect("Integer parsing error");
        LiteralValue::UInt32(u)
    } else if datatype == xsd::UNSIGNED_LONG {
        let u = u64::from_str(value).expect("Integer parsing error");
        LiteralValue::UInt64(u)
    } else if datatype == xsd::INTEGER {
        let i = i64::from_str(value).expect("Integer parsing error");
        LiteralValue::Int64(i)
    } else if datatype == xsd::LONG {
        let i = i64::from_str(value).expect("Integer parsing error");
        LiteralValue::Int64(i)
    } else if datatype == xsd::INT {
        let i = i32::from_str(value).expect("Integer parsing error");
        LiteralValue::Int32(i)
    } else if datatype == xsd::DOUBLE {
        let d = f64::from_str(value).expect("Integer parsing error");
        LiteralValue::Float64(d)
    } else if datatype == xsd::FLOAT {
        let f = f32::from_str(value).expect("Integer parsing error");
        LiteralValue::Float32(f)
    } else if datatype == xsd::BOOLEAN {
        let b = bool::from_str(value).expect("Boolean parsing error");
        LiteralValue::Boolean(b)
    } else if datatype == xsd::DATE_TIME {
        let dt_without_tz = value.parse::<NaiveDateTime>();
        if let Ok(dt) = dt_without_tz {
            LiteralValue::DateTime(dt.timestamp_nanos(), TimeUnit::Nanoseconds, None)
        } else {
            let dt_without_tz = value.parse::<DateTime<Utc>>();
            if let Ok(dt) = dt_without_tz {
                LiteralValue::DateTime(dt.naive_utc().timestamp_nanos(), TimeUnit::Nanoseconds, None)
            } else {
                panic!("Could not parse datetime: {}", value);
            }
        }
    } else if datatype == xsd::DECIMAL {
        let d = f64::from_str(value).expect("Decimal parsing error");
        LiteralValue::Float64(d)
    } else {
        todo!("Not implemented!")
    };
    literal_value
}

fn polars_literal_values_to_series(literal_values: Vec<LiteralValue>, name: &str) -> Series {
    let mut anys = vec![];
        for l in &literal_values {
            anys.push(l.to_anyvalue().unwrap());
        }
    return Series::from_any_values(name, anys.as_slice(), false).unwrap();

}

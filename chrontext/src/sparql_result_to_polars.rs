use oxrdf::{Term, Variable};
use polars::prelude::{as_struct, col, DataFrame, IntoLazy, LiteralValue};
use polars_core::prelude::{NamedFrom, Series};
use representation::multitype::{MULTI_DT_COL, MULTI_IRI_DT, MULTI_LANG_COL, MULTI_VALUE_COL};
use representation::sparql_to_polars::{
    polars_literal_values_to_series, sparql_term_to_polars_literal_value,
};
use representation::RDFNodeType;
use sparesults::QuerySolution;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;

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

    let datatypes: HashMap<_, _> = column_variables
        .iter()
        .map(|c: &Variable| {
            let mut dtype = None;
            for s in &static_query_solutions {
                if let Some(term) = s.get(c) {
                    match term {
                        Term::NamedNode(_) => {
                            if let Some(dt) = &dtype {
                                if dt == &RDFNodeType::IRI {
                                    continue;
                                } else {
                                    dtype = Some(RDFNodeType::MultiType);
                                }
                            } else {
                                dtype = Some(RDFNodeType::IRI);
                            }
                        }
                        Term::BlankNode(_) => {
                            if let Some(dt) = &dtype {
                                if dt == &RDFNodeType::BlankNode {
                                    continue;
                                } else {
                                    dtype = Some(RDFNodeType::MultiType);
                                }
                            } else {
                                dtype = Some(RDFNodeType::BlankNode);
                            }
                        }
                        Term::Literal(l) => {
                            if let Some(dt) = &dtype {
                                if let RDFNodeType::Literal(nn) = dt {
                                    if l.datatype() == nn.as_ref() {
                                        continue;
                                    } else {
                                        dtype = Some(RDFNodeType::MultiType);
                                    }
                                } else {
                                    dtype = Some(RDFNodeType::MultiType);
                                }
                            } else {
                                dtype = Some(RDFNodeType::Literal(l.datatype().into_owned()));
                            }
                        }
                        Term::Triple(_) => {
                            todo!()
                        }
                    }
                }
                if let Some(RDFNodeType::MultiType) = dtype {
                    break;
                }
            }
            (c.as_str().to_string(), dtype.unwrap_or(RDFNodeType::None))
        })
        .collect();

    let series: Vec<_> = column_variables
        .iter()
        .map(|c| {
            let c = c.as_str();
            if datatypes.get(c).unwrap() == &RDFNodeType::MultiType {
                let mut values = vec![];
                let mut dtypes = vec![];
                let mut langs = vec![];
                for s in &static_query_solutions {
                    if let Some(t) = s.get(c) {
                        match t {
                            Term::NamedNode(nn) => {
                                values.push(nn.to_string());
                                dtypes.push(MULTI_IRI_DT.to_string());
                                langs.push(None);
                            }
                            Term::BlankNode(bl) => {
                                values.push(bl.to_string());
                                dtypes.push(MULTI_IRI_DT.to_string());
                                langs.push(None);
                            }
                            Term::Literal(l) => {
                                values.push(l.value().to_string());
                                dtypes.push(l.datatype().to_string());
                                let lang = if let Some(lang) = l.language() {
                                    Some(lang.to_string())
                                } else {
                                    None
                                };
                                langs.push(lang);
                            }
                            Term::Triple(_) => {
                                todo!()
                            }
                        }
                    }
                }
                let values_ser = Series::new(MULTI_VALUE_COL, values);
                let dtypes_ser = Series::new(MULTI_DT_COL, dtypes);
                let langs_ser = Series::new(MULTI_LANG_COL, langs);

                let mut df = DataFrame::new(vec![values_ser, dtypes_ser, langs_ser]).unwrap();
                df = df
                    .lazy()
                    .with_column(
                        as_struct(vec![
                            col(MULTI_VALUE_COL),
                            col(MULTI_DT_COL),
                            col(MULTI_LANG_COL),
                        ])
                        .alias(c),
                    )
                    .collect()
                    .unwrap();
                df.drop_in_place(c).unwrap()
            } else {
                let mut literal_values = vec![];
                for s in &static_query_solutions {
                    literal_values.push(if let Some(term) = s.get(c) {
                        sparql_term_to_polars_literal_value(term)
                    } else {
                        LiteralValue::Null
                    });
                }
                polars_literal_values_to_series(literal_values, c)
            }
        })
        .collect();
    let df = DataFrame::new(series).expect("Create df problem");
    (df, datatypes)
}

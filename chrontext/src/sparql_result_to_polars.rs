use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{as_struct, col, DataFrame, Expr, IntoLazy, lit};
use polars_core::prelude::{AnyValue, NamedFrom, Series};
use representation::multitype::{all_multi_cols, MULTI_BLANK_DT, multi_has_this_type_column, MULTI_IRI_DT, MULTI_NONE_DT, non_multi_type_string};
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, RDFNodeType};
use sparesults::QuerySolution;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use representation::literals::sparql_literal_to_any_value;

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
        column_variables = get_projected_variables(pattern)
    } else {
        panic!("");
    }
    let mut var_col_map = HashMap::new();
    for v in &column_variables {
        let mut i = 0;
        let mut col_map: HashMap<String, Vec<AnyValue>> = HashMap::new();
        for s in &static_query_solutions {
            let (k, anyval) = if let Some(term) = s.get(v) {
                match term {
                    Term::NamedNode(n) => {
                        (MULTI_IRI_DT, AnyValue::StringOwned(n.to_string().into()))
                    }
                    Term::BlankNode(b) => {
                        (MULTI_BLANK_DT, AnyValue::StringOwned(b.to_string().into()))
                    }
                    Term::Literal(l) => {
                        (l.datatype().as_str(), sparql_literal_to_any_value(l.value(), l.language(), &Some(l.datatype())).0)
                    }
                    _ => {todo!()}
                }
            } else {
                (MULTI_NONE_DT, AnyValue::Null)
            };

            if let Some(v) = col_map.get_mut(k) {
                v.push(anyval)
            } else if k != MULTI_NONE_DT {
                let mut v: Vec<_> = (0..i).map(|_|AnyValue::Null).collect();
                v.push(anyval);
                col_map.insert(k.to_string(), v);
            }
            push_none_all_others(k, &mut col_map);
            i += 1;
        }
        if col_map.len() == 0 {
            col_map.insert(MULTI_NONE_DT.to_string(), (0..i).map(|_|AnyValue::Null).collect());
        }
        let mut new_col_map = HashMap::new();
        for (c, v) in col_map {
            let dt = if c == MULTI_IRI_DT {
                BaseRDFNodeType::IRI
            } else if c == MULTI_BLANK_DT {
                BaseRDFNodeType::BlankNode
            } else if c == MULTI_NONE_DT {
                BaseRDFNodeType::None
            } else {
                BaseRDFNodeType::Literal(NamedNode::new_unchecked(c))
            };
            new_col_map.insert(dt, v);
        }

        var_col_map.insert(v.as_str().to_string(), new_col_map);
    }
    let mut rdf_node_types = HashMap::new();
    let mut all_series: Vec<_> = vec![] ;
    for (c, m) in var_col_map {
            let mlen = m.len();
            let mut series = vec![];
            let mut types = vec![];
            for (t, v) in m {
                let name = if mlen > 1 {
                    non_multi_type_string(&t)
                } else {
                    c.clone()
                };

                let ser = Series::from_any_values_and_dtype(&name, v.as_slice(), &t.polars_data_type(), false).unwrap();
                if mlen > 1 && t.is_lang_string() {
                    series.push(ser.struct_().unwrap().field_by_name(LANG_STRING_VALUE_FIELD).unwrap());
                    series.push(ser.struct_().unwrap().field_by_name(LANG_STRING_LANG_FIELD).unwrap());
                } else {
                    series.push(ser);
                }
                types.push(t);
            }
            if series.len() == 1 {
                all_series.push(series.pop().unwrap());
                rdf_node_types.insert(c.to_string(), types.pop().unwrap().as_rdf_node_type());
            } else {
                let mut lf = DataFrame::new(series).unwrap().lazy();
                let mut struct_exprs = vec![];
                for c in all_multi_cols(&types) {
                    struct_exprs.push(col(&c));
                }
                let mut is_exprs: Vec<Expr> = vec![];
                let mut need_none = false;
                for t in &types {
                    if &BaseRDFNodeType::None == t {
                        need_none = true;
                    } else {
                        is_exprs.push(
                            col(&non_multi_type_string(t)).is_null().alias(
                                &multi_has_this_type_column(t)));
                    }
                }
                if need_none {
                    let mut is_iter = is_exprs.iter();
                    let mut e = if let Some(e) = is_iter.next() {
                        e.clone()
                    } else {
                        lit(true)
                    };
                    for other_e in is_iter {
                        e = e.and(other_e.clone().not())
                    }
                    e = e.alias(&multi_has_this_type_column(&BaseRDFNodeType::None));
                    is_exprs.push(e);
                }
                struct_exprs.extend(is_exprs);
                lf = lf.with_column(as_struct(struct_exprs).alias(&c)).select([col(&c)]);
                let mut df = lf.collect().unwrap();

                types.sort();
                rdf_node_types.insert(c.to_string(), RDFNodeType::MultiType(types));
                all_series.push(df.drop_in_place(&c).unwrap());
            }
        }
    let df = DataFrame::new(all_series).expect("Create df problem");
    (df, rdf_node_types)
}

fn get_projected_variables(g:&GraphPattern) -> Vec<Variable> {
    match g {
        GraphPattern::Union { left, right } => {
            let left_vars = get_projected_variables(left);
            let right_vars = get_projected_variables(right);
            let mut all_vars = HashSet::new();
            all_vars.extend(left_vars.into_iter());
            all_vars.extend(right_vars.into_iter());
            all_vars.into_iter().collect()
        }
        GraphPattern::Project { variables, .. } => {
            variables.clone()
        }
        GraphPattern::Distinct { inner } => {
            get_projected_variables(inner)
        }
        GraphPattern::Reduced { inner } => {
            get_projected_variables(inner)
        }
        GraphPattern::Slice { inner, .. } => {
            get_projected_variables(inner)
        }
        _ => panic!("Should not happen!")
    }
}

fn push_none_all_others(k_not:&str, map:&mut HashMap<String, Vec<AnyValue>>) {
    for (k, v) in map.iter_mut() {
        if k != k_not {
            v.push(AnyValue::Null);
        }
    }
}
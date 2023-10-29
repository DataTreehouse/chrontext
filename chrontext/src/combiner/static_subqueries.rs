use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::time_series_queries::complete_basic_time_series_queries;
use crate::combiner::CombinerError;
use crate::query_context::Context;
use crate::sparql_result_to_polars::create_static_query_dataframe;
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, NamedNodeRef, Variable};
use polars::prelude::{col, Expr, IntoLazy};
use polars_core::datatypes::AnyValue;
use polars_core::prelude::{JoinArgs, JoinType, UniqueKeepStrategy};
use polars_core::series::SeriesIter;
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;
use spargebra::Query;
use std::collections::{HashMap, HashSet};

impl Combiner {
    pub async fn execute_static_query(
        &mut self,
        query: &Query,
        solution_mappings: Option<SolutionMappings>,
    ) -> Result<SolutionMappings, CombinerError> {
        let use_query;
        let use_solution_mappings;
        if let Some(mappings) = solution_mappings {
            let (new_query, new_solution_mappings) = constrain_query(query, mappings);
            use_query = new_query;
            use_solution_mappings = Some(new_solution_mappings)
        } else {
            use_query = query.clone();
            use_solution_mappings = solution_mappings;
        }
        debug!("Static query: {}", use_query.to_string());
        let solutions = self
            .sparql_database
            .execute(&use_query)
            .await
            .map_err(|x| CombinerError::StaticQueryExecutionError(x))?;
        complete_basic_time_series_queries(
            &solutions,
            &mut self.prepper.basic_time_series_queries,
        )?;
        let (df, mut datatypes) = create_static_query_dataframe(&use_query, solutions);
        debug!("Static query results:\n {}", df);
        let mut columns: HashSet<String> = df
            .get_column_names()
            .iter()
            .map(|x| x.to_string())
            .collect();
        if columns.is_empty() {
            return Ok(use_solution_mappings.unwrap());
        }
        let mut lf = df.lazy();
        if let Some(SolutionMappings {
            mappings: input_lf,
            columns: input_columns,
            datatypes: input_datatypes,
        }) = use_solution_mappings
        {
            let on: Vec<&String> = columns.intersection(&input_columns).collect();
            let on_cols: Vec<Expr> = on.iter().map(|x| col(x)).collect();
            let join_type = if on_cols.is_empty() {
                JoinType::Cross
            } else {
                JoinType::Inner
            };
            lf = lf.join(
                input_lf,
                on_cols.as_slice(),
                on_cols.as_slice(),
                JoinArgs::new(join_type),
            );

            columns.extend(input_columns);
            datatypes.extend(input_datatypes);
        }
        Ok(SolutionMappings::new(lf, columns, datatypes))
    }
}

pub(crate) fn split_static_queries(
    static_queries: &mut HashMap<Context, Query>,
    context: &Context,
) -> HashMap<Context, Query> {
    let mut split_keys = vec![];
    for k in static_queries.keys() {
        if k.path.iter().zip(&context.path).all(|(x, y)| x == y) {
            split_keys.push(k.clone())
        }
    }
    let mut new_map = HashMap::new();
    for k in split_keys {
        let q = static_queries.remove(&k).unwrap();
        new_map.insert(k, q);
    }
    new_map
}

pub(crate) fn split_static_queries_opt(
    static_queries: &mut Option<HashMap<Context, Query>>,
    context: &Context,
) -> Option<HashMap<Context, Query>> {
    if let Some(static_queries) = static_queries {
        Some(split_static_queries(static_queries, context))
    } else {
        None
    }
}

fn constrain_query(
    query: &Query,
    mut solution_mappings: SolutionMappings,
) -> (Query, SolutionMappings) {
    solution_mappings.mappings = solution_mappings.mappings.collect().unwrap().lazy();
    let projected_variables = get_variable_set(query);

    let mut constrain_variables = vec![];
    for v in projected_variables {
        if solution_mappings.columns.contains(v.as_str()) {
            constrain_variables.push(v.clone());
        }
    }
    if constrain_variables.is_empty() {
        return (query.clone(), solution_mappings);
    }

    let constrain_columns: Vec<Expr> = constrain_variables
        .iter()
        .map(|x| col(x.as_str()))
        .collect();
    let variable_columns = solution_mappings
        .mappings
        .clone()
        .select(constrain_columns)
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .unwrap();

    let mut bindings = vec![];
    let height = variable_columns.height();
    let datatypes: Vec<NamedNode> = constrain_variables
        .iter()
        .map(|x| {
            solution_mappings
                .datatypes
                .get(x.as_str())
                .expect("Datatype did not exist")
                .clone()
        })
        .collect();
    let datatypes_nnref: Vec<NamedNodeRef> = datatypes.iter().map(|x| x.as_ref()).collect();
    let mut series_iters: Vec<SeriesIter> = variable_columns.iter().map(|x| x.iter()).collect();
    for _i in 0..height {
        let mut binding = vec![];
        for (j, iter) in series_iters.iter_mut().enumerate() {
            binding.push(any_to_ground_term(
                iter.next().unwrap(),
                datatypes.get(j).unwrap(),
                datatypes_nnref.get(j).unwrap(),
            ));
        }
        bindings.push(binding)
    }
    let values_pattern = GraphPattern::Values {
        variables: constrain_variables,
        bindings,
    };
    (
        constrain_query_with_values(query, values_pattern),
        solution_mappings,
    )
}

fn constrain_query_with_values(query: &Query, values_pattern: GraphPattern) -> Query {
    if let Query::Select {
        dataset,
        pattern,
        base_iri,
        ..
    } = query
    {
        Query::Select {
            dataset: dataset.clone(),
            pattern: constrain_pattern_with_values(pattern, values_pattern),
            base_iri: base_iri.clone(),
        }
    } else {
        panic!("Only select queries permitted");
    }
}

fn constrain_pattern_with_values(
    pattern: &GraphPattern,
    values_pattern: GraphPattern,
) -> GraphPattern {
    match pattern {
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(GraphPattern::Join {
                left: Box::new(values_pattern),
                right: inner.clone(),
            }),
            variables: variables.clone(),
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(constrain_pattern_with_values(inner, values_pattern)),
        },
        _ => {
            panic!("This should never happen")
        }
    }
}

fn get_variable_set(query: &Query) -> Vec<&Variable> {
    if let Query::Select { pattern, .. } = query {
        if let GraphPattern::Project { variables, .. } = pattern {
            return variables.iter().collect();
        } else {
            panic!("Non project graph pattern in query")
        }
    } else {
        panic!("Non select query not supported")
    }
}

fn any_to_ground_term(
    any: AnyValue,
    datatype: &NamedNode,
    datatype_nnref: &NamedNodeRef,
) -> Option<GroundTerm> {
    #[allow(unreachable_patterns)]
    match any {
        AnyValue::Null => None,
        AnyValue::Boolean(b) => Some(GroundTerm::Literal(Literal::from(b))),
        AnyValue::Utf8(s) => {
            if datatype_nnref == &xsd::STRING {
                Some(GroundTerm::Literal(Literal::new_simple_literal(s)))
            } else {
                Some(GroundTerm::NamedNode(NamedNode::new_unchecked(s)))
            }
        }
        AnyValue::UInt8(u) => Some(GroundTerm::Literal(Literal::new_typed_literal(
            u.to_string(),
            datatype.to_owned(),
        ))),
        AnyValue::UInt16(u) => Some(GroundTerm::Literal(Literal::from(u))),
        AnyValue::UInt32(u) => Some(GroundTerm::Literal(Literal::from(u))),
        AnyValue::UInt64(u) => Some(GroundTerm::Literal(Literal::from(u))),
        AnyValue::Int8(i) => Some(GroundTerm::Literal(Literal::new_typed_literal(
            i.to_string(),
            datatype.to_owned(),
        ))),
        AnyValue::Int16(i) => Some(GroundTerm::Literal(Literal::from(i))),
        AnyValue::Int32(i) => Some(GroundTerm::Literal(Literal::from(i))),
        AnyValue::Int64(i) => Some(GroundTerm::Literal(Literal::from(i))),
        AnyValue::Float32(f) => Some(GroundTerm::Literal(Literal::from(f))),
        AnyValue::Float64(f) => Some(GroundTerm::Literal(Literal::from(f))),
        AnyValue::Date(_) => {
            todo!("No support for date yet")
        }
        AnyValue::Datetime(_, _, _) => {
            todo!("No support for datetime yet")
        }
        AnyValue::Duration(_, _) => {
            todo!("No support for duration yet")
        }
        AnyValue::Time(_) => {
            todo!("No support for time yet")
        }
        AnyValue::Categorical(..) => {
            todo!("No support for categorical yet")
        }
        AnyValue::List(_) => {
            todo!("No support for list yet")
        }
        AnyValue::Utf8Owned(s) => {
            if datatype_nnref == &xsd::STRING {
                Some(GroundTerm::Literal(Literal::new_simple_literal(s)))
            } else {
                Some(GroundTerm::NamedNode(NamedNode::new_unchecked(s)))
            }
        }
        _ => {
            unimplemented!("Not implemented for {}", any)
        }
    }
}

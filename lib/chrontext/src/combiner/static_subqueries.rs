use super::Combiner;
use crate::combiner::virtualized_queries::complete_basic_virtualized_queries;
use crate::combiner::CombinerError;
use crate::sparql_result_to_polars::create_static_query_dataframe;
use log::debug;
use oxrdf::{Term, Variable};
use polars::export::rayon::iter::{IntoParallelIterator, ParallelIterator};
use polars::prelude::{col, Expr, IntoLazy, JoinType, UniqueKeepStrategy};
use query_processing::graph_patterns::join;
use representation::polars_to_rdf::{df_as_result, QuerySolutions};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;
use spargebra::Query;
use std::collections::HashMap;

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
        complete_basic_virtualized_queries(
            &solutions,
            &mut self.prepper.basic_virtualized_queries,
        )?;
        let (df, datatypes) = create_static_query_dataframe(&use_query, solutions);
        debug!("Static query results:\n {}", df);
        let mut out_solution_mappings = SolutionMappings::new(df.lazy(), datatypes);
        if let Some(use_solution_mappings) = use_solution_mappings {
            out_solution_mappings = join(
                out_solution_mappings,
                use_solution_mappings,
                JoinType::Inner,
            )?;
        }
        Ok(out_solution_mappings)
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
        if solution_mappings.rdf_node_types.contains_key(v.as_str()) {
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

    let QuerySolutions {
        variables,
        solutions,
    } = df_as_result(variable_columns, &solution_mappings.rdf_node_types);
    let bindings = solutions
        .into_par_iter()
        .map(|x| {
            x.into_iter()
                .map(|y: Option<Term>| {
                    if let Some(y) = y {
                        Some(match y {
                            Term::NamedNode(nn) => GroundTerm::NamedNode(nn),
                            Term::BlankNode(_) => {
                                panic!()
                            }
                            Term::Literal(l) => GroundTerm::Literal(l),
                            Term::Triple(_) => {
                                todo!()
                            }
                        })
                    } else {
                        None
                    }
                })
                .collect()
        })
        .collect();
    let values_pattern = GraphPattern::Values {
        variables,
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

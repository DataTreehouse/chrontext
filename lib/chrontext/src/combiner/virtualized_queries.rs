use super::Combiner;
use crate::combiner::CombinerError;
use crate::preparing::grouping_col_type;
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::Term;
use polars::prelude::{
    col, CategoricalOrdering, DataFrame, DataType, Expr, IntoLazy, JoinArgs, JoinType, Series,
    SortMultipleOptions,
};
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::query_context::Context;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeType};
use sparesults::QuerySolution;
use std::collections::{HashMap, HashSet};
use virtualized_query::pushdown_setting::PushdownSetting;
use virtualized_query::{BasicVirtualizedQuery, VirtualizedQuery};

impl Combiner {
    pub fn attach_expected_empty_results(
        &self,
        vq: &VirtualizedQuery,
        mut solution_mappings: SolutionMappings,
    ) -> SolutionMappings {
        let mut expected_cols: Vec<_> = vq.expected_columns().into_iter().collect();
        expected_cols.sort();
        let drop_cols = get_drop_cols(vq);
        let mut series_vec = vec![];
        for e in expected_cols {
            if !drop_cols.contains(e) {
                series_vec.push(Series::new_empty(
                    e,
                    &BaseRDFNodeType::None.polars_data_type(),
                ));
                solution_mappings
                    .rdf_node_types
                    .insert(e.to_string(), RDFNodeType::None);
            }
        }
        let df = DataFrame::new(series_vec).unwrap();
        for d in drop_cols {
            if solution_mappings.rdf_node_types.contains_key(&d) {
                solution_mappings.rdf_node_types.remove(&d);
                solution_mappings.mappings = solution_mappings.mappings.drop(vec![d]);
            }
        }
        solution_mappings.mappings = solution_mappings.mappings.join(
            df.lazy(),
            vec![],
            vec![],
            JoinArgs::new(JoinType::Cross),
        );
        solution_mappings
    }

    pub async fn execute_attach_virtualized_query(
        &mut self,
        mut vq: VirtualizedQuery,
        mut solution_mappings: SolutionMappings,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Executing time series query: {:?}", vq);
        //Filter out degenerate VQs here.
        if !vq.has_identifiers() || !vq.has_resources() {
            return Ok(self.attach_expected_empty_results(&vq, solution_mappings));
        }

        //Find the columns we should join on:
        let on_cols = get_join_columns(&vq, &solution_mappings.rdf_node_types);

        if self
            .prepper
            .pushdown_settings
            .contains(&PushdownSetting::Ordering)
        {
            vq = vq.add_sorting_pushdown(&on_cols);
        }
        let EagerSolutionMappings {
            mappings,
            mut rdf_node_types,
        } = self
            .virtualized_database
            .query(&vq)
            .await
            .map_err(|x| CombinerError::VirtualizedDatabaseError(x))?;

        // We allow empty (no columns & rows) result for compatibility with e.g. Azure Kusto.
        if mappings.height() == 0 && mappings.get_columns().is_empty() {
            return Ok(self.attach_expected_empty_results(&vq, solution_mappings));
        }

        vq.validate(&mappings)
            .map_err(|x| CombinerError::TimeseriesValidationError(x))?;
        let mut mappings = mappings.lazy();
        let drop_cols = get_drop_cols(&vq);
        let mut groupby_cols: Vec<_> = vq
            .get_groupby_columns()
            .into_iter()
            .filter(|x| on_cols.contains(x))
            .collect();
        groupby_cols.sort();
        let id_cols: Vec<_> = vq
            .get_identifier_variables()
            .into_iter()
            .map(|x| x.as_str().to_string())
            .filter(|x| on_cols.contains(x))
            .collect();
        for c in groupby_cols {
            //When there are no results we need to cast to the appropriate type
            if let Some(&RDFNodeType::None) = rdf_node_types.get(c) {
                let coltype = grouping_col_type();
                rdf_node_types.insert(
                    c.to_string(),
                    polars_type_to_literal_type(&coltype).unwrap().to_owned(),
                );
                mappings = mappings.with_column(col(c).cast(coltype.clone()));
            };
        }

        for id_col in id_cols {
            //When there are no results we need to cast to the appropriate type
            if let Some(&RDFNodeType::None) = rdf_node_types.get(&id_col) {
                if let Some(e) = solution_mappings.rdf_node_types.get(&id_col) {
                    if e != &RDFNodeType::None {
                        let coltype = DataType::String;
                        mappings = mappings.with_column(col(&id_col).cast(coltype.clone()));
                        rdf_node_types.insert(
                            id_col.clone(),
                            polars_type_to_literal_type(&coltype).unwrap().to_owned(),
                        );
                    };
                }
            }

            mappings = mappings.with_column(
                col(&id_col).cast(DataType::Categorical(None, CategoricalOrdering::Lexical)),
            );
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(&id_col).cast(DataType::Categorical(None, CategoricalOrdering::Lexical)),
            );
        }
        let on_cols: Vec<Expr> = on_cols.into_iter().map(|x| col(&x)).collect();

        for (k, v) in rdf_node_types {
            solution_mappings.rdf_node_types.insert(k, v);
        }

        solution_mappings.mappings = solution_mappings.mappings.collect().unwrap().lazy();
        let sort_opts = SortMultipleOptions::new()
            .with_order_descending(false)
            .with_maintain_order(false)
            .with_nulls_last(false);
        mappings = mappings.sort_by_exprs(on_cols.as_slice(), sort_opts.clone());
        solution_mappings.mappings = solution_mappings
            .mappings
            .sort_by_exprs(on_cols.as_slice(), sort_opts);

        solution_mappings.mappings = solution_mappings
            .mappings
            .join(
                mappings,
                on_cols.as_slice(),
                on_cols.as_slice(),
                JoinArgs::new(JoinType::Inner),
            )
            .drop(drop_cols.iter());
        for c in &drop_cols {
            solution_mappings.rdf_node_types.remove(c);
        }
        return Ok(solution_mappings);
    }
}

pub(crate) fn split_virtualized_queries(
    virtualized_queries: &mut Option<HashMap<Context, Vec<VirtualizedQuery>>>,
    context: &Context,
) -> Option<HashMap<Context, Vec<VirtualizedQuery>>> {
    if let Some(vqs) = virtualized_queries {
        let mut split_keys = vec![];
        for k in vqs.keys() {
            if k.path.iter().zip(&context.path).all(|(x, y)| x == y) {
                split_keys.push(k.clone())
            }
        }
        let mut new_map = HashMap::new();
        for k in split_keys {
            let vq = vqs.remove(&k).unwrap();
            new_map.insert(k, vq);
        }
        Some(new_map)
    } else {
        None
    }
}

fn get_drop_cols(vq: &VirtualizedQuery) -> HashSet<String> {
    let mut drop_cols = HashSet::new();
    drop_cols.extend(
        vq.get_resource_variables()
            .iter()
            .map(|x| x.as_str().to_string()),
    );
    for c in vq.get_groupby_columns() {
        drop_cols.insert(c.clone());
    }
    drop_cols.extend(
        vq.get_identifier_variables()
            .iter()
            .map(|x| x.as_str().to_string()),
    );
    drop_cols
}

pub(crate) fn complete_basic_virtualized_queries(
    static_query_solutions: &Vec<QuerySolution>,
    basic_virtualized_queries: &mut Vec<BasicVirtualizedQuery>,
) -> Result<(), CombinerError> {
    for basic_query in basic_virtualized_queries {
        let mut ids = HashSet::new();
        for sqs in static_query_solutions {
            if let Some(Term::Literal(lit)) = sqs.get(&basic_query.identifier_variable) {
                if lit.datatype() == xsd::STRING {
                    ids.insert(lit.value().to_string());
                } else {
                    todo!()
                }
            }
        }

        for sqs in static_query_solutions {
            if let Some(Term::Literal(lit)) = sqs.get(&basic_query.resource_variable) {
                if basic_query.resource.is_none() {
                    if lit.datatype() != xsd::STRING {
                        return Err(CombinerError::ResourceIsNotString(
                            basic_query.query_source_context.as_str().to_string(),
                            lit.datatype().to_string(),
                        ));
                    }
                    basic_query.resource = Some(lit.value().into());
                } else if let Some(res) = &basic_query.resource {
                    if res != lit.value() {
                        return Err(CombinerError::InconsistentResourceName(
                            basic_query.query_source_context.as_str().to_string(),
                            res.clone(),
                            lit.value().to_string(),
                        ));
                    }
                }
            }
        }

        let mut ids_vec: Vec<String> = ids.into_iter().collect();
        ids_vec.sort();
        basic_query.ids = Some(ids_vec);
    }
    Ok(())
}

pub fn get_join_columns(
    vq: &VirtualizedQuery,
    existing: &HashMap<String, RDFNodeType>,
) -> Vec<String> {
    let mut new_order = vec![];
    let expected = vq.expected_columns();
    let mut grouping_cols: Vec<_> = vq.get_groupby_columns().into_iter().collect();
    grouping_cols.sort();
    let id_vars = vq.get_identifier_variables();
    let mut expected_group_and_id = HashSet::new();
    for g in grouping_cols {
        if expected.contains(g.as_str()) {
            expected_group_and_id.insert(g.as_str());
            new_order.push(g.clone());
        }
    }
    for i in id_vars {
        if expected.contains(i.as_str()) {
            expected_group_and_id.insert(i.as_str());
            new_order.push(i.as_str().to_string());
        }
    }

    for s in existing.keys() {
        if expected.contains(s.as_str()) && !expected_group_and_id.contains(s.as_str()) {
            new_order.push(s.clone());
        }
    }
    new_order
}

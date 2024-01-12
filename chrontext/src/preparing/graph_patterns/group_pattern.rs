use representation::query_context::{Context, PathEntry};
use log::debug;
use std::collections::{HashMap, HashSet};

use super::TimeseriesQueryPrepper;
use representation::solution_mapping::SolutionMappings;
use crate::constants::GROUPING_COL;
use crate::find_query_variables::find_all_used_variables_in_aggregate_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::pushdown_setting::PushdownSetting;
use crate::timeseries_query::{GroupedTimeseriesQuery, TimeseriesQuery};
use oxrdf::Variable;
use polars::prelude::{DataFrameJoinOps, JoinArgs, JoinType, IntoLazy};
use polars_core::prelude::{UniqueKeepStrategy};
use polars_core::series::Series;
use spargebra::algebra::{AggregateExpression, GraphPattern};

impl TimeseriesQueryPrepper {
    pub fn prepare_group(
        &mut self,
        graph_pattern: &GraphPattern,
        by: &Vec<Variable>,
        aggregations: &Vec<(Variable, AggregateExpression)>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        debug!(
            "Prepare group by graph pattern at context {}",
            context.as_str()
        );
        if try_groupby_complex_query {
            return GPPrepReturn::fail_groupby_complex_query();
        }
        let inner_context = &context.extension_with(PathEntry::GroupInner);
        let mut try_graph_pattern_prepare =
            self.prepare_graph_pattern(graph_pattern, true, solution_mappings, &inner_context);
        if !try_graph_pattern_prepare.fail_groupby_complex_query
            && self.pushdown_settings.contains(&PushdownSetting::GroupBy)
        {
            if try_graph_pattern_prepare.time_series_queries.len() == 1 {
                let (_c, mut tsqs) = try_graph_pattern_prepare
                    .time_series_queries
                    .drain()
                    .next()
                    .unwrap();
                if tsqs.len() == 1 {
                    let mut tsq = tsqs.remove(0);
                    let in_scope =
                        check_aggregations_are_in_scope(&tsq, inner_context, aggregations);

                    if in_scope {
                        let grouping_col = self.add_grouping_col(solution_mappings, by);
                        tsq =
                            add_basic_groupby_mapping_values(tsq, solution_mappings, &grouping_col);
                        let tsfuncs = tsq.get_timeseries_functions(context);
                        let mut keep_by = vec![Variable::new_unchecked(&grouping_col)];
                        for v in by {
                            for (v2, _) in &tsfuncs {
                                if v2.as_str() == v.as_str() {
                                    keep_by.push(v.clone())
                                }
                            }
                        }
                        //TODO: For OPC UA we must ensure that mapping df is 1:1 with identities, or alternatively group on these
                        tsq = TimeseriesQuery::Grouped(GroupedTimeseriesQuery {
                            context: context.clone(),
                            tsq: Box::new(tsq),
                            by: keep_by,
                            aggregations: aggregations.clone(),
                        });
                        return GPPrepReturn::new(HashMap::from([(context.clone(), vec![tsq])]));
                    }
                }
            }
        }
        debug!("Group by pushdown failed at context {:?}", context);
        self.prepare_graph_pattern(graph_pattern, false, solution_mappings, &inner_context)
    }

    fn add_grouping_col(
        &mut self,
        solution_mappings: &mut SolutionMappings,
        by: &Vec<Variable>,
    ) -> String {
        let grouping_col = format!("{}_{}", GROUPING_COL, self.grouping_counter);
        self.grouping_counter += 1;
        let by_names: Vec<String> = by
            .iter()
            .filter(|x| solution_mappings.rdf_node_types.contains_key(x.as_str()))
            .map(|x| x.as_str().to_string())
            .collect();
        solution_mappings.mappings = solution_mappings.mappings.clone().collect().unwrap().lazy();
        let mut df = solution_mappings
            .mappings
            .clone()
            .collect()
            .unwrap()
            .select(by_names.as_slice())
            .unwrap()
            .unique(Some(by_names.as_slice()), UniqueKeepStrategy::First, None)
            .unwrap();
        let mut series = Series::from_iter(0..(df.height() as i64));
        series.rename(&grouping_col);
        df.with_column(series).unwrap();
        solution_mappings.mappings = solution_mappings
            .mappings
            .clone()
            .collect()
            .unwrap()
            .join(
                &df,
                by_names.as_slice(),
                by_names.as_slice(),
                JoinArgs::new(JoinType::Inner),
            )
            .unwrap()
            .lazy();
        grouping_col
    }
}

fn check_aggregations_are_in_scope(
    tsq: &TimeseriesQuery,
    context: &Context,
    aggregations: &Vec<(Variable, AggregateExpression)>,
) -> bool {
    for (_, ae) in aggregations {
        let mut used_vars = HashSet::new();
        find_all_used_variables_in_aggregate_expression(ae, &mut used_vars);
        for v in &used_vars {
            if tsq.has_equivalent_timestamp_variable(v, context) {
                continue;
            } else if tsq.has_equivalent_value_variable(v, context) {
                continue;
            } else {
                debug!("Variable {:?} in aggregate expression not in scope", v);
                return false;
            }
        }
    }
    true
}

fn add_basic_groupby_mapping_values(
    tsq: TimeseriesQuery,
    solution_mappings: &mut SolutionMappings,
    grouping_col: &str,
) -> TimeseriesQuery {
    match tsq {
        TimeseriesQuery::Basic(b) => {
            let by_vec = vec![
                grouping_col,
                b.identifier_variable.as_ref().unwrap().as_str(),
            ];
            solution_mappings.mappings =
                solution_mappings.mappings.clone().collect().unwrap().lazy();
            let mapping_values = solution_mappings
                .mappings
                .clone()
                .collect()
                .unwrap()
                .select(by_vec)
                .unwrap();
            TimeseriesQuery::GroupedBasic(b, mapping_values, grouping_col.to_string())
        }
        TimeseriesQuery::Filtered(tsq, f) => TimeseriesQuery::Filtered(
            Box::new(add_basic_groupby_mapping_values(
                *tsq,
                solution_mappings,
                grouping_col,
            )),
            f,
        ),
        TimeseriesQuery::InnerSynchronized(inners, syncs) => {
            let mut tsq_added = vec![];
            for tsq in inners {
                tsq_added.push(Box::new(add_basic_groupby_mapping_values(
                    *tsq,
                    solution_mappings,
                    grouping_col,
                )))
            }
            TimeseriesQuery::InnerSynchronized(tsq_added, syncs)
        }
        TimeseriesQuery::ExpressionAs(tsq, v, e) => TimeseriesQuery::ExpressionAs(
            Box::new(add_basic_groupby_mapping_values(
                *tsq,
                solution_mappings,
                grouping_col,
            )),
            v,
            e,
        ),
        TimeseriesQuery::Grouped(_) => {
            panic!("Should never happen")
        }
        TimeseriesQuery::GroupedBasic(_, _, _) => {
            panic!("Should never happen")
        }
    }
}

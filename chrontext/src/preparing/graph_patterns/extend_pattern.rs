use super::TimeSeriesQueryPrepper;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::find_query_variables::find_all_used_variables_in_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

impl TimeSeriesQueryPrepper {
    pub(crate) fn prepare_extend(
        &mut self,
        inner: &GraphPattern,
        var: &Variable,
        expr: &Expression,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        let inner_context = context.extension_with(PathEntry::ExtendInner);
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            solution_mappings,
            &inner_context,
        );
        if try_groupby_complex_query {
            let mut expression_vars = HashSet::new();
            find_all_used_variables_in_expression(expr, &mut expression_vars);
            let mut found_i = None;
            let mut found_context = None;

            for (c, tsqs) in &inner_prepare.time_series_queries {
                for (i, tsq) in tsqs.iter().enumerate() {
                    let mut found_all = true;
                    let mut found_some = false;
                    for expression_var in &expression_vars {
                        if tsq.has_equivalent_value_variable(expression_var, context) {
                            found_some = true;
                        } else if tsq.has_equivalent_timestamp_variable(expression_var, context) {
                            found_some = true;
                        } else {
                            found_all = false;
                            break;
                        }
                    }
                    if found_all && found_some {
                        found_i = Some(i);
                        found_context = Some(c.clone());
                    }
                }
            }
            if let (Some(i), Some(c)) = (found_i, found_context) {
                let inner_tsq = inner_prepare
                    .time_series_queries
                    .get_mut(&c)
                    .unwrap()
                    .remove(i);
                if inner_prepare
                    .time_series_queries
                    .get(&c)
                    .unwrap()
                    .is_empty()
                {
                    inner_prepare.time_series_queries.remove(&c);
                }
                let new_tsq =
                    TimeSeriesQuery::ExpressionAs(Box::new(inner_tsq), var.clone(), expr.clone());
                if !inner_prepare.time_series_queries.contains_key(context) {
                    inner_prepare
                        .time_series_queries
                        .insert(context.clone(), vec![]);
                }
                inner_prepare
                    .time_series_queries
                    .get_mut(context)
                    .unwrap()
                    .push(new_tsq);
                inner_prepare
            } else {
                GPPrepReturn::fail_groupby_complex_query()
            }
        } else {
            inner_prepare
        }
    }
}

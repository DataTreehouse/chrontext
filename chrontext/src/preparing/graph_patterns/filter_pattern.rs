use super::TimeseriesQueryPrepper;
use crate::change_types::ChangeType;
use representation::solution_mapping::SolutionMappings;
use crate::preparing::graph_patterns::filter_expression_rewrites::rewrite_filter_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use representation::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;
use log::debug;

impl TimeseriesQueryPrepper {
    pub fn prepare_filter(
        &mut self,
        expression: &Expression,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        let expression_prepare = self.prepare_expression(
            expression,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::FilterExpression),
        );
        debug!("Expression prepare: {:?}", expression_prepare);
        let inner_filter_context = context.extension_with(PathEntry::FilterInner);
        let inner_prepare = self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            solution_mappings,
            &inner_filter_context,
        );
        if expression_prepare.fail_groupby_complex_query || inner_prepare.fail_groupby_complex_query
        {
            return GPPrepReturn::fail_groupby_complex_query();
        }

        let mut out_tsqs = HashMap::new();
        out_tsqs.extend(expression_prepare.time_series_queries);
        for (inner_context, tsqs) in inner_prepare.time_series_queries {
            let mut out_tsq_vec = vec![];
            for t in tsqs {
                let use_change_type = if try_groupby_complex_query {
                    ChangeType::NoChange
                } else {
                    ChangeType::Relaxed
                };
                let conj_vec = conjunction_to_vec(self.rewritten_filters.get(&context));
                let (time_series_condition, lost_value) = rewrite_filter_expression(
                    &t,
                    expression,
                    &use_change_type,
                    context,
                    &conj_vec,
                    &self.pushdown_settings,
                );
                if try_groupby_complex_query && (lost_value || time_series_condition.is_none()) {
                    return GPPrepReturn::fail_groupby_complex_query();
                }
                if let Some(expr) = time_series_condition {
                    out_tsq_vec.push(TimeseriesQuery::Filtered(Box::new(t), expr));
                } else {
                    out_tsq_vec.push(t);
                }
            }
            if !out_tsq_vec.is_empty() {
                out_tsqs.insert(inner_context, out_tsq_vec);
            }
        }
        GPPrepReturn::new(out_tsqs)
    }
}

fn conjunction_to_vec(expr_opt: Option<&Expression>) -> Option<Vec<&Expression>> {
    let mut out = vec![];
    if let Some(expr) = expr_opt {
        match expr {
            Expression::And(left, right) => {
                let left_conj = conjunction_to_vec(Some(left));
                if let Some(left_vec) = left_conj {
                    out.extend(left_vec);
                }
                let right_conj = conjunction_to_vec(Some(right));
                if let Some(right_vec) = right_conj {
                    out.extend(right_vec);
                }
            }
            _ => {
                out.push(expr);
            }
        }
    }
    if out.len() > 0 {
        Some(out)
    } else {
        None
    }
}

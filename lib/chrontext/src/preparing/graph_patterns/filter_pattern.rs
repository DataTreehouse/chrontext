use super::TimeseriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::preparing::graph_patterns::expression_rewrites::rewrite_filter_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use log::debug;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

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

        let mut out_vqs = HashMap::new();
        out_vqs.extend(expression_prepare.virtualized_queries);
        let mut lost_any = false;
        for (inner_context, vqs) in inner_prepare.virtualized_queries {
            let mut out_vq_vec = vec![];
            for t in vqs {
                let use_change_type = if try_groupby_complex_query {
                    ChangeType::NoChange
                } else {
                    ChangeType::Relaxed
                };
                let conj_vec = conjunction_to_vec(self.rewritten_filters.get(&context));
                let (virtualized_condition, lost_value) = rewrite_filter_expression(
                    &t,
                    expression,
                    &use_change_type,
                    context,
                    &conj_vec,
                    &self.pushdown_settings,
                );
                lost_any = lost_value || lost_any;
                if try_groupby_complex_query && (lost_value || virtualized_condition.is_none()) {
                    return GPPrepReturn::fail_groupby_complex_query();
                }
                if let Some(expr) = virtualized_condition {
                    out_vq_vec.push(VirtualizedQuery::Filtered(Box::new(t), expr));
                } else {
                    out_vq_vec.push(t);
                }
            }
            if !out_vq_vec.is_empty() {
                if !lost_any && inner_context.path.len() == context.path.len() + 1 {
                    out_vqs.insert(context.clone(), out_vq_vec);
                } else {
                    out_vqs.insert(inner_context, out_vq_vec);
                }
            }
        }
        GPPrepReturn::new(out_vqs)
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

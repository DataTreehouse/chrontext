use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use std::collections::HashMap;

use crate::preparing::graph_patterns::expression_rewrites::rewrite_order_expressions;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{GraphPattern, OrderExpression};
use virtualized_query::VirtualizedQuery;

impl TimeseriesQueryPrepper {
    pub fn prepare_order_by(
        &mut self,
        inner: &GraphPattern,
        order_expressions: &Vec<OrderExpression>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        let expression_prepare = self.prepare_order_expressions(
            order_expressions,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::FilterExpression),
        );

        let inner_prepare = self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::OrderByInner),
        );
        if expression_prepare.fail_groupby_complex_query || inner_prepare.fail_groupby_complex_query
        {
            return GPPrepReturn::fail_groupby_complex_query();
        }
        let mut out_vqs = HashMap::new();
        for (inner_context, vqs) in inner_prepare.virtualized_queries {
            let mut out_vq_vec = vec![];
            for vq in vqs {
                let (rewritten, lost_value) = rewrite_order_expressions(
                    &vq,
                    order_expressions,
                    context,
                    &self.pushdown_settings,
                );
                if try_groupby_complex_query && lost_value {
                    return GPPrepReturn::fail_groupby_complex_query();
                } else {
                    if let Some(ordering) = rewritten {
                        out_vq_vec.push(VirtualizedQuery::Ordered(Box::new(vq), ordering));
                    } else {
                        out_vq_vec.push(vq);
                    }
                }
            }
            out_vqs.insert(inner_context, out_vq_vec);
        }
        GPPrepReturn::new(out_vqs)
    }
}

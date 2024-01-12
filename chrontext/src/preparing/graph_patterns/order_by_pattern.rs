use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;

use representation::solution_mapping::SolutionMappings;
use representation::query_context::{Context, PathEntry};
use log::debug;
use spargebra::algebra::{GraphPattern, OrderExpression};

impl TimeseriesQueryPrepper {
    pub fn prepare_order_by(
        &mut self,
        inner: &GraphPattern,
        _order_expressions: &Vec<OrderExpression>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!("Encountered graph inside order by, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query();
        } else {
            let inner_prepare = self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::OrderByInner),
            );
            inner_prepare
        }
    }
}

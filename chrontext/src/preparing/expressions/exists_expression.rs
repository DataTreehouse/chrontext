use super::TimeSeriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;
use crate::combiner::solution_mapping::SolutionMappings;

impl TimeSeriesQueryPrepper {
    pub fn prepare_exists_expression(
        &mut self,
        wrapped: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        if try_groupby_complex_query {
            EXPrepReturn::fail_groupby_complex_query()
        } else {
            let wrapped_prepare = self.prepare_graph_pattern(
                wrapped,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::Exists),
            );
            if wrapped_prepare.fail_groupby_complex_query {
                EXPrepReturn::fail_groupby_complex_query()
            } else {
                EXPrepReturn::new(wrapped_prepare.time_series_queries)
            }
        }
    }
}

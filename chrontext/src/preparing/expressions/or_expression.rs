use super::TimeSeriesQueryPrepper;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;

impl TimeSeriesQueryPrepper {
    pub fn prepare_or_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::OrLeft),
        );
        let right_prepare = self.prepare_expression(
            right,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::OrRight),
        );
        if left_prepare.fail_groupby_complex_query || right_prepare.fail_groupby_complex_query {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        left_prepare.with_time_series_queries_from(right_prepare);
        left_prepare
    }
}

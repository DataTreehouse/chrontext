use super::TimeseriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;

impl TimeseriesQueryPrepper {
    pub fn prepare_and_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        // We allow translations of left- or right hand sides of And-expressions to be None.
        // This allows us to enforce the remaining conditions that were not removed due to a prepare
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::AndLeft),
        );
        let right_prepare = self.prepare_expression(
            right,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::AndRight),
        );
        if left_prepare.fail_groupby_complex_query || right_prepare.fail_groupby_complex_query {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        left_prepare.with_virtualized_queries_from(right_prepare);
        left_prepare
    }
}

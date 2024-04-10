use super::TimeseriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;

impl TimeseriesQueryPrepper {
    pub fn prepare_not_expression(
        &mut self,
        wrapped: &Expression,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let wrapped_prepare = self.prepare_expression(
            wrapped,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::Not),
        );
        wrapped_prepare
    }
}

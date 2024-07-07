use super::TimeseriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;

impl TimeseriesQueryPrepper {
    pub fn prepare_if_expression(
        &mut self,
        left: &Expression,
        mid: &Expression,
        right: &Expression,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::IfLeft),
        );
        let mid_prepare = self.prepare_expression(
            mid,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::IfMiddle),
        );
        let right_prepare = self.prepare_expression(
            right,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::IfRight),
        );
        left_prepare.with_virtualized_queries_from(mid_prepare);
        left_prepare.with_virtualized_queries_from(right_prepare);
        left_prepare
    }
}

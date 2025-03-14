use super::TimeseriesQueryPrepper;
use crate::combiner::CombinerError;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;

impl TimeseriesQueryPrepper {
    pub fn prepare_exists_expression(
        &mut self,
        wrapped: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<EXPrepReturn, CombinerError> {
        if try_groupby_complex_query {
            Ok(EXPrepReturn::fail_groupby_complex_query())
        } else {
            let wrapped_prepare = self.prepare_graph_pattern(
                wrapped,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::Exists),
            )?;
            if wrapped_prepare.fail_groupby_complex_query {
                Ok(EXPrepReturn::fail_groupby_complex_query())
            } else {
                Ok(EXPrepReturn::new(wrapped_prepare.virtualized_queries))
            }
        }
    }
}

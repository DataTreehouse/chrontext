use super::TimeseriesQueryPrepper;
use crate::combiner::CombinerError;
use crate::preparing::graph_patterns::GPPrepReturn;
use log::debug;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;

impl TimeseriesQueryPrepper {
    pub fn prepare_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<GPPrepReturn, CombinerError> {
        if try_groupby_complex_query {
            debug!("Encountered minus inside groupby, not supported for complex groupby pushdown");
            Ok(GPPrepReturn::fail_groupby_complex_query())
        } else {
            let mut left_prepare = self.prepare_graph_pattern(
                left,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::MinusLeftSide),
            )?;
            let right_prepare = self.prepare_graph_pattern(
                right,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::MinusRightSide),
            )?;
            left_prepare.with_virtualized_queries_from(right_prepare);
            Ok(left_prepare)
        }
    }
}

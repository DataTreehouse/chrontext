use super::TimeseriesQueryPrepper;
use crate::combiner::CombinerError;
use crate::preparing::graph_patterns::GPPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;

impl TimeseriesQueryPrepper {
    pub fn prepare_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<GPPrepReturn, CombinerError> {
        let mut left_prepare = self.prepare_graph_pattern(
            left,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::JoinLeftSide),
        )?;
        if left_prepare.fail_groupby_complex_query {
            return Ok(left_prepare);
        }

        let right_prepare = self.prepare_graph_pattern(
            right,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::JoinRightSide),
        )?;
        if right_prepare.fail_groupby_complex_query {
            return Ok(right_prepare);
        }

        left_prepare.with_virtualized_queries_from(right_prepare);
        if try_groupby_complex_query && left_prepare.virtualized_queries.len() > 1 {
            return Ok(GPPrepReturn::fail_groupby_complex_query());
            //TODO: Fix synchronized queries
        }
        Ok(left_prepare)
    }
}

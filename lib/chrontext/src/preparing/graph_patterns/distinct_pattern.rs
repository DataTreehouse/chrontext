use super::TimeseriesQueryPrepper;
use log::debug;

use crate::combiner::CombinerError;
use crate::preparing::graph_patterns::GPPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;

impl TimeseriesQueryPrepper {
    pub fn prepare_distinct(
        &mut self,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<GPPrepReturn, CombinerError> {
        if try_groupby_complex_query {
            debug!(
                "Encountered distinct inside groupby, not supported for complex groupby pushdown"
            );
            return Ok(GPPrepReturn::fail_groupby_complex_query());
        }

        self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::DistinctInner),
        )
    }
}

use super::TimeseriesQueryPrepper;
use log::debug;

use crate::combiner::CombinerError;
use crate::preparing::graph_patterns::GPPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;

impl TimeseriesQueryPrepper {
    pub fn prepare_graph(
        &mut self,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<GPPrepReturn, CombinerError> {
        if try_groupby_complex_query {
            debug!("Encountered graph inside groupby, not supported for complex groupby pushdown");
            Ok(GPPrepReturn::fail_groupby_complex_query())
        } else {
            self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::GraphInner),
            )
        }
    }
}

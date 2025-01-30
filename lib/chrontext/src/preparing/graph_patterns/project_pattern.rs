use super::TimeseriesQueryPrepper;
use crate::combiner::CombinerError;
use crate::preparing::graph_patterns::GPPrepReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;

impl TimeseriesQueryPrepper {
    pub fn prepare_project(
        &mut self,
        inner: &GraphPattern,
        _variables: &[Variable],
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<GPPrepReturn, CombinerError> {
        let inner_context = context.extension_with(PathEntry::ProjectInner);

        self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            solution_mappings,
            &inner_context,
        )
    }
}

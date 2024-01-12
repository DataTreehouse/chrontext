use super::TimeseriesQueryPrepper;
use representation::solution_mapping::SolutionMappings;
use crate::preparing::graph_patterns::GPPrepReturn;
use representation::query_context::{Context, PathEntry};
use log::debug;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;

impl TimeseriesQueryPrepper {
    pub fn prepare_project(
        &mut self,
        inner: &GraphPattern,
        _variables: &Vec<Variable>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!("Encountered graph inside project, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query();
        } else {
            let inner_rewrite = self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::ProjectInner),
            );
            inner_rewrite
        }
    }
}

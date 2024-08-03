use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use log::debug;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;
use virtualized_query::VirtualizedQuery;

impl TimeseriesQueryPrepper {
    pub fn prepare_slice(
        &mut self,
        start: usize,
        length: Option<usize>,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        let inner_context = context.extension_with(PathEntry::SliceInner);
        if try_groupby_complex_query {
            debug!("Encountered graph inside slice, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query();
        } else {
            let mut inner_prepare = self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                solution_mappings,
                &inner_context,
            );
            if !inner_prepare.fail_groupby_complex_query {
                for (c, vqs) in &mut inner_prepare.virtualized_queries {
                    let mut found_noncompatible = false;
                    for i in inner_context.path.len()..c.path.len() {
                        if c.path[i] != PathEntry::ProjectInner {
                            found_noncompatible = true;
                            break;
                        }
                    }
                    if !found_noncompatible {
                        if vqs.len() == 1 {
                            let vq =
                                VirtualizedQuery::Sliced(Box::new(vqs.remove(0)), start, length);
                            *vqs = vec![vq];
                        }
                    }
                }
            }
            inner_prepare
        }
    }
}

use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use log::debug;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;
use timeseries_query::TimeseriesQuery;

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
        let inner_context = context.extension_with(PathEntry::ReducedInner);
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
            if !inner_prepare.fail_groupby_complex_query && start == 0 {
                if let Some(length) = length {
                    if let Some(mut tsqs) = inner_prepare.time_series_queries.remove(&inner_context)
                    {
                        if tsqs.len() == 1 {
                            let tsq = TimeseriesQuery::Limited(Box::new(tsqs.remove(0)), length);
                            return GPPrepReturn::new(HashMap::from_iter([(
                                context.clone(),
                                vec![tsq],
                            )]));
                        }
                    }
                }
            }
            inner_prepare
        }
    }
}

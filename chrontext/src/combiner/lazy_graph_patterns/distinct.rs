use super::Combiner;
use representation::solution_mapping::SolutionMappings;
use crate::combiner::CombinerError;
use representation::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use log::debug;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use query_processing::graph_patterns::distinct;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_distinct(
        &mut self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        static_query_map: HashMap<Context, Query>,
        prepared_time_series_queries: Option<HashMap<Context, Vec<TimeseriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing distinct graph pattern");
        let solution_mappings = self
            .lazy_graph_pattern(
                inner,
                solution_mappings,
                static_query_map,
                prepared_time_series_queries,
                &context.extension_with(PathEntry::DistinctInner),
            )
            .await?;
        Ok(distinct(solution_mappings)?)
    }
}

use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::CombinerError;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use async_recursion::async_recursion;
use log::debug;
use polars_core::frame::UniqueKeepStrategy;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_distinct(
        &mut self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        static_query_map: HashMap<Context, Query>,
        prepared_time_series_queries: Option<HashMap<Context, Vec<TimeSeriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing distinct graph pattern");
        let SolutionMappings {
            mappings,
            columns,
            datatypes,
        } = self
            .lazy_graph_pattern(
                inner,
                solution_mappings,
                static_query_map,
                prepared_time_series_queries,
                &context.extension_with(PathEntry::DistinctInner),
            )
            .await?;
        Ok(SolutionMappings::new(
            mappings.unique_stable(None, UniqueKeepStrategy::First),
            columns,
            datatypes,
        ))
    }
}

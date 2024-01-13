use super::Combiner;
use representation::solution_mapping::SolutionMappings;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use representation::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use log::debug;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use query_processing::graph_patterns::minus;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_time_series_queries: Option<HashMap<Context, Vec<TimeseriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing minus graph pattern");
        let left_context = context.extension_with(PathEntry::MinusLeftSide);
        let right_context = context.extension_with(PathEntry::MinusRightSide);
        let left_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &left_context);
        let right_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &right_context);
        let left_static_query_map = split_static_queries(&mut static_query_map, &left_context);
        let right_static_query_map = split_static_queries(&mut static_query_map, &right_context);
        assert!(static_query_map.is_empty());
        assert!(if let Some(tsqs) = &prepared_time_series_queries {
            tsqs.is_empty()
        } else {
            true
        });
        self.counter += 1;
        let left_solution_mappings = self
            .lazy_graph_pattern(
                left,
                solution_mappings,
                left_static_query_map,
                left_prepared_time_series_queries,
                &left_context,
            )
            .await?;
        let right_solution_mappings = self
            .lazy_graph_pattern(
                right,
                Some(left_solution_mappings.clone()),
                right_static_query_map,
                right_prepared_time_series_queries,
                &right_context,
            )
            .await?;
        Ok(minus(left_solution_mappings, right_solution_mappings)?)
    }
}

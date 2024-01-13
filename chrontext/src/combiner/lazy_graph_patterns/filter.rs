use super::Combiner;
use representation::solution_mapping::SolutionMappings;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use representation::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use log::debug;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::Query;
use std::collections::HashMap;
use query_processing::graph_patterns::filter;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_filter(
        &mut self,
        inner: &GraphPattern,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_time_series_queries: Option<HashMap<Context, Vec<TimeseriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing filter graph pattern");
        let inner_context = context.extension_with(PathEntry::FilterInner);
        let expression_context = context.extension_with(PathEntry::FilterExpression);
        let inner_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &inner_context);
        let expression_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &expression_context);
        let inner_static_query_map = split_static_queries(&mut static_query_map, &inner_context);
        let expression_static_query_map =
            split_static_queries(&mut static_query_map, &expression_context);
        assert!(static_query_map.is_empty());
        assert!(if let Some(tsqs) = prepared_time_series_queries {
            tsqs.is_empty()
        } else {
            true
        });

        let mut output_solution_mappings = self
            .lazy_graph_pattern(
                inner,
                input_solution_mappings,
                inner_static_query_map,
                inner_prepared_time_series_queries,
                &inner_context,
            )
            .await?;
        output_solution_mappings = self
            .lazy_expression(
                expression,
                output_solution_mappings,
                Some(expression_static_query_map),
                expression_prepared_time_series_queries,
                &expression_context,
            )
            .await?;
        Ok(filter(output_solution_mappings, &expression_context)?)
    }
}

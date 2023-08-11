use super::Combiner;
use crate::combiner::lazy_graph_patterns::SolutionMappings;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{concat, UnionArgs};
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use async_recursion::async_recursion;
use log::debug;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_time_series_queries: Option<HashMap<Context, Vec<TimeSeriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing union graph pattern");
        let left_context = context.extension_with(PathEntry::UnionLeftSide);
        let right_context = context.extension_with(PathEntry::UnionRightSide);
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
        let SolutionMappings {
            mappings: left_mappings,
            columns: mut left_columns,
            datatypes: mut left_datatypes,
        } = self
            .lazy_graph_pattern(
                &left,
                solution_mappings.clone(),
                left_static_query_map,
                left_prepared_time_series_queries,
                &left_context,
            )
            .await?;

        let SolutionMappings {
            mappings: right_mappings,
            columns: right_columns,
            datatypes: mut right_datatypes,
        } = self
            .lazy_graph_pattern(
                right,
                solution_mappings,
                right_static_query_map,
                right_prepared_time_series_queries,
                &right_context,
            )
            .await?;

        let output_mappings =
            concat(vec![left_mappings, right_mappings], UnionArgs::default()).expect("Concat problem");
        left_columns.extend(right_columns);
        for (v, dt) in right_datatypes.drain() {
            if let Some(left_dt) = left_datatypes.get(&v) {
                assert_eq!(&dt, left_dt);
            } else {
                left_datatypes.insert(v, dt);
            }
        }
        Ok(SolutionMappings::new(
            output_mappings,
            left_columns,
            left_datatypes,
        ))
    }
}

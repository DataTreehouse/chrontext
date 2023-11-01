use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use log::debug;
use polars::prelude::{col, Expr, IntoLazy, LiteralValue};
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use std::ops::Not;

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
        let minus_column = left_context.as_str();
        self.counter += 1;
        let mut left_solution_mappings = self
            .lazy_graph_pattern(
                left,
                solution_mappings,
                left_static_query_map,
                left_prepared_time_series_queries,
                &left_context,
            )
            .await?;

        let mut left_df = left_solution_mappings
            .mappings
            .with_column(Expr::Literal(LiteralValue::Int64(1)).alias(&minus_column))
            .with_column(col(&minus_column).cumsum(false).keep_name())
            .collect()
            .expect("Minus collect left problem");
        left_solution_mappings.mappings = left_df.clone().lazy();
        let left_columns = left_solution_mappings.columns.clone();
        let left_datatypes = left_solution_mappings.datatypes.clone();

        //TODO: determine only variables actually used before copy
        let right_solution_mappings = self
            .lazy_graph_pattern(
                right,
                Some(left_solution_mappings),
                right_static_query_map,
                right_prepared_time_series_queries,
                &right_context,
            )
            .await?;

        let SolutionMappings {
            mappings: right_mappings,
            ..
        } = right_solution_mappings;

        let right_df = right_mappings
            .select([col(&minus_column)])
            .collect()
            .expect("Minus right df collect problem");
        left_df = left_df
            .filter(
                &left_df
                    .column(&minus_column)
                    .unwrap()
                    .is_in(right_df.column(&minus_column).unwrap())
                    .unwrap()
                    .not(),
            )
            .expect("Filter minus left hand side problem");
        left_df = left_df.drop(&minus_column).unwrap();
        Ok(SolutionMappings::new(
            left_df.lazy(),
            left_columns,
            left_datatypes,
        ))
    }
}

//Uses code from https://github.com/magbak/maplib/blob/main/triplestore/src/sparql/lazy_graph_patterns/left_join.rs

use super::Combiner;
use crate::combiner::solution_mapping::{is_string_col, SolutionMappings};
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use log::debug;
use polars::prelude::{col, Expr, IntoLazy, JoinType, JoinArgs};
use polars_core::datatypes::DataType;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::Query;
use std::collections::HashMap;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression: &Option<Expression>,
        solution_mapping: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_time_series_queries: Option<HashMap<Context, Vec<TimeseriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing left join graph pattern");
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
        let left_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &left_context);
        let right_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &right_context);
        let expression_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &right_context);
        let left_static_query_map = split_static_queries(&mut static_query_map, &left_context);
        let right_static_query_map = split_static_queries(&mut static_query_map, &right_context);
        let expression_static_query_map =
            split_static_queries(&mut static_query_map, &expression_context);
        assert!(static_query_map.is_empty());
        assert!(if let Some(tsqs) = &prepared_time_series_queries {
            tsqs.is_empty()
        } else {
            true
        });
        let mut left_solution_mappings = self
            .lazy_graph_pattern(
                left,
                solution_mapping,
                left_static_query_map,
                left_prepared_time_series_queries,
                &left_context,
            )
            .await?;

        left_solution_mappings.mappings = left_solution_mappings.mappings.collect().unwrap().lazy();

        let mut right_solution_mappings = self
            .lazy_graph_pattern(
                right,
                Some(left_solution_mappings.clone()),
                right_static_query_map,
                right_prepared_time_series_queries,
                &right_context,
            )
            .await?;

        if let Some(expr) = expression {
            right_solution_mappings = self
                .lazy_expression(
                    expr,
                    right_solution_mappings,
                    Some(expression_static_query_map),
                    expression_prepared_time_series_queries,
                    &expression_context,
                )
                .await?;
            right_solution_mappings.mappings = right_solution_mappings
                .mappings
                .filter(col(&expression_context.as_str()))
                .drop_columns([&expression_context.as_str()]);
        }
        let SolutionMappings {
            mappings: mut right_mappings,
            columns: mut right_columns,
            datatypes: mut right_datatypes,
        } = right_solution_mappings;

        let mut join_on: Vec<&String> = left_solution_mappings
            .columns
            .intersection(&right_columns)
            .collect();
        join_on.sort();

        let join_on_cols: Vec<Expr> = join_on.iter().map(|x| col(x)).collect();

        if join_on.is_empty() {
            left_solution_mappings.mappings = left_solution_mappings.mappings.join(
                right_mappings,
                join_on_cols.as_slice(),
                join_on_cols.as_slice(),
                JoinArgs::new(JoinType::Cross),
            )
        } else {
            for c in join_on {
                if is_string_col(right_datatypes.get(c).unwrap()) {
                    right_mappings =
                        right_mappings.with_column(col(c).cast(DataType::Categorical(None)));
                    left_solution_mappings.mappings = left_solution_mappings
                        .mappings
                        .with_column(col(c).cast(DataType::Categorical(None)));
                }
            }
            let all_false = [false].repeat(join_on_cols.len());
            right_mappings = right_mappings.sort_by_exprs(
                join_on_cols.as_slice(),
                all_false.as_slice(),
                false,
                false,
            );
            left_solution_mappings.mappings = left_solution_mappings.mappings.sort_by_exprs(
                join_on_cols.as_slice(),
                all_false.as_slice(),
                false,
                false,
            );
            left_solution_mappings.mappings = left_solution_mappings.mappings.join(
                right_mappings,
                join_on_cols.as_slice(),
                join_on_cols.as_slice(),
                JoinArgs::new(JoinType::Left),
            )
        }
        for c in right_columns.drain() {
            left_solution_mappings.columns.insert(c);
        }
        for (var, dt) in right_datatypes.drain() {
            if let Some(dt_left) = left_solution_mappings.datatypes.get(&var) {
                //TODO: handle compatibility
                // if &dt != dt_left {
                //     return Err(SparqlError::InconsistentDatatypes(var.clone(), dt_left.clone(), dt, context.clone()))
                // }
            } else {
                left_solution_mappings.datatypes.insert(var, dt);
            }
        }
        Ok(left_solution_mappings)
    }
}

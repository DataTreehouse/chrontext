//Uses code from https://github.com/magbak/maplib/blob/main/triplestore/src/sparql/lazy_graph_patterns/left_join.rs

use super::Combiner;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::virtualized_queries::split_virtualized_queries;
use crate::combiner::CombinerError;
use async_recursion::async_recursion;
use log::debug;
use polars::prelude::JoinType;
use query_processing::graph_patterns::{filter, join};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::Query;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression: &Option<Expression>,
        solution_mapping: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_virtualized_queries: Option<HashMap<Context, Vec<VirtualizedQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing left join graph pattern");
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
        let left_prepared_virtualized_queries =
            split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
        let right_prepared_virtualized_queries =
            split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
        let expression_prepared_virtualized_queries =
            split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
        let left_static_query_map = split_static_queries(&mut static_query_map, &left_context);
        let right_static_query_map = split_static_queries(&mut static_query_map, &right_context);
        let expression_static_query_map =
            split_static_queries(&mut static_query_map, &expression_context);
        assert!(static_query_map.is_empty());
        assert!(if let Some(vqs) = &prepared_virtualized_queries {
            vqs.is_empty()
        } else {
            true
        });
        let left_solution_mappings = self
            .lazy_graph_pattern(
                left,
                solution_mapping,
                left_static_query_map,
                left_prepared_virtualized_queries,
                &left_context,
            )
            .await?;

        let mut right_solution_mappings = self
            .lazy_graph_pattern(
                right,
                Some(left_solution_mappings.clone()),
                right_static_query_map,
                right_prepared_virtualized_queries,
                &right_context,
            )
            .await?;

        if let Some(expr) = expression {
            right_solution_mappings = self
                .lazy_expression(
                    expr,
                    right_solution_mappings,
                    Some(expression_static_query_map),
                    expression_prepared_virtualized_queries,
                    &expression_context,
                )
                .await?;
            right_solution_mappings = filter(right_solution_mappings, &expression_context)?;
            right_solution_mappings
                .rdf_node_types
                .remove(expression_context.as_str());
        }
        Ok(join(
            left_solution_mappings,
            right_solution_mappings,
            JoinType::Left,
        )?)
    }
}

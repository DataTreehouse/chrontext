use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::CombinerError;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use async_recursion::async_recursion;
use log::debug;
use polars::prelude::{col, Expr};
use spargebra::algebra::{GraphPattern, OrderExpression};
use spargebra::Query;
use std::collections::HashMap;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_order_by(
        &mut self,
        inner: &GraphPattern,
        expression: &Vec<OrderExpression>,
        solution_mappings: Option<SolutionMappings>,
        static_query_map: HashMap<Context, Query>,
        prepared_time_series_queries: Option<HashMap<Context, Vec<TimeSeriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing order by graph pattern");
        let mut output_solution_mappings = self
            .lazy_graph_pattern(
                inner,
                solution_mappings,
                static_query_map,
                prepared_time_series_queries,
                &context.extension_with(PathEntry::OrderByInner),
            )
            .await?;
        let order_expression_contexts: Vec<Context> = (0..expression.len())
            .map(|i| context.extension_with(PathEntry::OrderByExpression(i as u16)))
            .collect();
        let mut asc_ordering = vec![];
        let mut inner_contexts = vec![];
        for i in 0..expression.len() {
            let (ordering_solution_mappings, reverse, inner_context) = self
                .lazy_order_expression(
                    expression.get(i).unwrap(),
                    output_solution_mappings,
                    order_expression_contexts.get(i).unwrap(),
                )
                .await?;
            output_solution_mappings = ordering_solution_mappings;
            inner_contexts.push(inner_context);
            asc_ordering.push(reverse);
        }
        let SolutionMappings {
            mut mappings,
            columns,
            datatypes,
        } = output_solution_mappings;

        mappings = mappings.sort_by_exprs(
            inner_contexts
                .iter()
                .map(|c| col(c.as_str()))
                .collect::<Vec<Expr>>(),
            asc_ordering.iter().map(|asc| !asc).collect::<Vec<bool>>(),
            true,
            false,
        );
        mappings = mappings.drop_columns(
            inner_contexts
                .iter()
                .map(|x| x.as_str())
                .collect::<Vec<&str>>(),
        );
        Ok(SolutionMappings::new(mappings, columns, datatypes))
    }
}

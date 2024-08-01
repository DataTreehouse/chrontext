use super::Combiner;
use crate::combiner::CombinerError;
use async_recursion::async_recursion;
use log::debug;
use query_processing::find_query_variables::solution_mappings_has_all_order_expression_variables;
use query_processing::graph_patterns::order_by;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{GraphPattern, OrderExpression};
use spargebra::Query;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_order_by(
        &mut self,
        inner: &GraphPattern,
        expression: &Vec<OrderExpression>,
        solution_mappings: Option<SolutionMappings>,
        static_query_map: HashMap<Context, Query>,
        prepared_virtualized_queries: Option<HashMap<Context, Vec<VirtualizedQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing order by graph pattern");
        let mut output_solution_mappings = self
            .lazy_graph_pattern(
                inner,
                solution_mappings,
                static_query_map,
                prepared_virtualized_queries,
                &context.extension_with(PathEntry::OrderByInner),
            )
            .await?;
        let mut order_expressions = vec![];
        for oe in expression {
            if solution_mappings_has_all_order_expression_variables(&output_solution_mappings, oe) {
                //Todo: Avoid clone
                order_expressions.push(oe.clone());
            }
        }
        let order_expression_contexts: Vec<Context> = (0..order_expressions.len())
            .map(|i| context.extension_with(PathEntry::OrderByExpression(i as u16)))
            .collect();
        let mut asc_ordering = vec![];
        let mut inner_contexts = vec![];
        for i in 0..order_expressions.len() {
            let (ordering_solution_mappings, reverse, inner_context) = self
                .lazy_order_expression(
                    order_expressions.get(i).unwrap(),
                    output_solution_mappings,
                    order_expression_contexts.get(i).unwrap(),
                )
                .await?;
            output_solution_mappings = ordering_solution_mappings;
            inner_contexts.push(inner_context);
            asc_ordering.push(reverse);
        }
        Ok(order_by(
            output_solution_mappings,
            &inner_contexts,
            asc_ordering,
        )?)
    }
}

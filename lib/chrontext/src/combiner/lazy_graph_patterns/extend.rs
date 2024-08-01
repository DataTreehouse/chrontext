use super::Combiner;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::virtualized_queries::split_virtualized_queries;
use crate::combiner::CombinerError;
use async_recursion::async_recursion;
use log::debug;
use oxrdf::Variable;
use polars::prelude::{lit, LiteralValue};
use query_processing::find_query_variables::solution_mappings_has_all_expression_variables;
use query_processing::graph_patterns::extend;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeType};
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::Query;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_extend(
        &mut self,
        inner: &GraphPattern,
        variable: &Variable,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_virtualized_queries: Option<HashMap<Context, Vec<VirtualizedQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing extend graph pattern");
        let inner_context = context.extension_with(PathEntry::ExtendInner);
        let expression_context = context.extension_with(PathEntry::ExtendExpression);
        let inner_prepared_virtualized_queries =
            split_virtualized_queries(&mut prepared_virtualized_queries, &inner_context);
        let expression_prepared_virtualized_queries =
            split_virtualized_queries(&mut prepared_virtualized_queries, &expression_context);
        let inner_static_query_map = split_static_queries(&mut static_query_map, &inner_context);
        let expression_static_query_map =
            split_static_queries(&mut static_query_map, &expression_context);
        assert!(static_query_map.is_empty());
        assert!(if let Some(vqs) = &prepared_virtualized_queries {
            vqs.is_empty()
        } else {
            true
        });
        let mut output_solution_mappings = self
            .lazy_graph_pattern(
                inner,
                input_solution_mappings,
                inner_static_query_map,
                inner_prepared_virtualized_queries,
                &inner_context,
            )
            .await?;

        let has_all =
            solution_mappings_has_all_expression_variables(&output_solution_mappings, expression);
        if has_all {
            output_solution_mappings = self
                .lazy_expression(
                    expression,
                    output_solution_mappings,
                    Some(expression_static_query_map),
                    expression_prepared_virtualized_queries,
                    &expression_context,
                )
                .await?;
            Ok(extend(
                output_solution_mappings,
                &expression_context,
                variable,
            )?)
        } else {
            output_solution_mappings.mappings = output_solution_mappings.mappings.with_column(
                lit(LiteralValue::Null)
                    .cast(BaseRDFNodeType::None.polars_data_type())
                    .alias(variable.as_str()),
            );
            output_solution_mappings
                .rdf_node_types
                .insert(variable.as_str().to_string(), RDFNodeType::None);
            Ok(output_solution_mappings)
        }
    }
}

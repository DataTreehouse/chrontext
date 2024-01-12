use super::Combiner;
use crate::combiner::CombinerError;
use crate::constants::NEST;
use oxrdf::Variable;
use polars::prelude::{col};
use query_processing::aggregates::{AggregateReturn, avg, count_with_expression, count_without_expression, group_concat, max, min, sample, sum};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::AggregateExpression;

impl Combiner {
    pub async fn sparql_aggregate_expression_as_lazy_column_and_expression(
        &mut self,
        variable: &Variable,
        aggregate_expression: &AggregateExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
    ) -> Result<AggregateReturn, CombinerError> {
        let output_solution_mappings;
        let mut out_expr;
        let column_context;
        let out_rdf_node_type;
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                if let Some(some_expr) = expr {
                    column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                    output_solution_mappings = self
                        .lazy_expression(
                            some_expr,
                            solution_mappings,
                            None,
                            None,
                            column_context.as_ref().unwrap(),
                        )
                        .await?;
                    (out_expr, out_rdf_node_type) =
                        count_with_expression(column_context.as_ref().unwrap(), *distinct);
                } else {
                    output_solution_mappings = solution_mappings;
                    column_context = None;
                    (out_expr, out_rdf_node_type) =
                        count_without_expression(&output_solution_mappings, *distinct);
                }
            }
            AggregateExpression::Sum { expr, distinct } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self
                    .lazy_expression(
                        expr,
                        solution_mappings,
                        None,
                        None,
                        column_context.as_ref().unwrap(),
                    )
                    .await?;

                (out_expr, out_rdf_node_type) = sum(
                    &output_solution_mappings,
                    column_context.as_ref().unwrap(),
                    *distinct,
                );
            }
            AggregateExpression::Avg { expr, distinct } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                output_solution_mappings = self
                    .lazy_expression(
                        expr,
                        solution_mappings,
                        None,
                        None,
                        column_context.as_ref().unwrap(),
                    )
                    .await?;

                (out_expr, out_rdf_node_type) = avg(
                    &output_solution_mappings,
                    column_context.as_ref().unwrap(),
                    *distinct,
                );
            }
            AggregateExpression::Min { expr, distinct: _ } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self
                    .lazy_expression(
                        expr,
                        solution_mappings,
                        None,
                        None,
                        column_context.as_ref().unwrap(),
                    )
                    .await?;
                (out_expr, out_rdf_node_type) =
                    min(&output_solution_mappings, column_context.as_ref().unwrap());
            }
            AggregateExpression::Max { expr, distinct: _ } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self
                    .lazy_expression(
                        expr,
                        solution_mappings,
                        None,
                        None,
                        column_context.as_ref().unwrap(),
                    )
                    .await?;

                (out_expr, out_rdf_node_type) =
                    max(&output_solution_mappings, column_context.as_ref().unwrap());
            }
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self
                    .lazy_expression(
                        expr,
                        solution_mappings,
                        None,
                        None,
                        column_context.as_ref().unwrap(),
                    )
                    .await?;

                (out_expr, out_rdf_node_type) =
                    group_concat(column_context.as_ref().unwrap(), separator, *distinct);
            }
            AggregateExpression::Sample { expr, .. } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self
                    .lazy_expression(
                        expr,
                        solution_mappings,
                        None,
                        None,
                        column_context.as_ref().unwrap(),
                    )
                    .await?;

                (out_expr, out_rdf_node_type) =
                    sample(&output_solution_mappings, column_context.as_ref().unwrap());
            }
            AggregateExpression::Custom {
                name,
                expr,
                distinct: _,
            } => {
                let iri = name.as_str();
                if iri == NEST {
                    column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                    output_solution_mappings = self
                        .lazy_expression(
                            expr,
                            solution_mappings,
                            None,
                            None,
                            column_context.as_ref().unwrap(),
                        )
                        .await?;
                    out_expr = col(column_context.as_ref().unwrap().as_str());
                    out_rdf_node_type = output_solution_mappings
                        .rdf_node_types
                        .get(column_context.as_ref().unwrap().as_str())
                        .unwrap()
                        .clone();
                } else {
                    panic!("Custom aggregation not supported")
                }
            }
        }
        out_expr = out_expr.alias(variable.as_str());
        Ok(AggregateReturn {
            solution_mappings: output_solution_mappings,
            expr: out_expr,
            context: column_context,
            rdf_node_type: out_rdf_node_type,
        })
    }
}

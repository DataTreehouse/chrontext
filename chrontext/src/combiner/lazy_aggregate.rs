use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::CombinerError;
use crate::constants::NEST;
use crate::query_context::{Context, PathEntry};
use oxrdf::Variable;
use polars::prelude::{col, DataType, Expr, GetOutput, IntoSeries, str_concat};
use spargebra::algebra::AggregateExpression;

impl Combiner {
    pub async fn sparql_aggregate_expression_as_lazy_column_and_expression(
        &mut self,
        variable: &Variable,
        aggregate_expression: &AggregateExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
    ) -> Result<(SolutionMappings, Expr, Option<Context>), CombinerError> {
        let output_solution_mappings;
        let mut out_expr;
        let column_context;
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
                    if *distinct {
                        out_expr = col(column_context.as_ref().unwrap().as_str()).n_unique();
                    } else {
                        out_expr = col(column_context.as_ref().unwrap().as_str()).count();
                    }
                } else {
                    output_solution_mappings = solution_mappings;
                    column_context = None;
                    let all_proper_column_names: Vec<String> = output_solution_mappings
                        .columns
                        .iter()
                        .map(|x| x.clone())
                        .collect();
                    let columns_expr = Expr::Columns(all_proper_column_names);
                    if *distinct {
                        out_expr = columns_expr.n_unique();
                    } else {
                        out_expr = columns_expr.unique();
                    }
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

                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .unique()
                        .sum();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str()).sum();
                }
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

                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .unique()
                        .mean();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str()).mean();
                }
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

                out_expr = col(column_context.as_ref().unwrap().as_str()).min();
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

                out_expr = col(column_context.as_ref().unwrap().as_str()).max();
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

                let use_sep = if let Some(sep) = separator {
                    sep.to_string()
                } else {
                    "".to_string()
                };
                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .cast(DataType::String)
                        .list()
                        .0
                        .apply(
                            move |s| {
                                Ok(Some(
                                    str_concat(
                                    s.unique_stable()
                                        .expect("Unique stable error").str().unwrap(),
                                        use_sep.as_str(),
                                        false).into(),
                                ))
                            },
                            GetOutput::from_type(DataType::String),
                        )
                        .first();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .cast(DataType::String)
                        .list()
                        .0
                        .apply(
                            move |s| Ok(Some(str_concat(&s.str().unwrap(), use_sep.as_str(), false).into_series())),
                            GetOutput::from_type(DataType::String),
                        )
                        .first();
                }
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

                out_expr = col(column_context.as_ref().unwrap().as_str()).first();
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
                } else {
                    panic!("Custom aggregation not supported")
                }
            }
        }
        out_expr = out_expr.alias(variable.as_str());
        Ok((output_solution_mappings, out_expr, column_context))
    }
}

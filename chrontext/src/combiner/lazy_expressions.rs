mod exists_helper;

use super::Combiner;
use crate::combiner::lazy_expressions::exists_helper::rewrite_exists_graph_pattern;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::static_subqueries::split_static_queries_opt;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use crate::constants::{
    DATETIME_AS_NANOS, DATETIME_AS_SECONDS, MODULUS, NANOS_AS_DATETIME, SECONDS_AS_DATETIME, FLOOR_DATETIME_TO_SECONDS_INTERVAL
};
use crate::query_context::{Context, PathEntry};
use crate::sparql_result_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::lazy::dsl::is_not_null;
use polars::prelude::{
    col, lit, Expr, IntoLazy, LiteralValue, Operator, Series, TimeUnit, UniqueKeepStrategy, concat_str, is_in
};
use spargebra::algebra::{Expression, Function};
use spargebra::Query;
use std::collections::HashMap;
use std::ops::{Div, Mul};

impl Combiner {
    #[async_recursion]
    pub async fn lazy_expression(
        &mut self,
        expr: &Expression,
        mut solution_mappings: SolutionMappings,
        mut static_query_map: Option<HashMap<Context, Query>>,
        mut prepared_time_series_queries: Option<HashMap<Context, Vec<TimeseriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        let output_solution_mappings = match expr {
            Expression::NamedNode(nn) => {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    Expr::Literal(sparql_named_node_to_polars_literal_value(nn))
                        .alias(context.as_str()),
                );
                solution_mappings
            }
            Expression::Literal(lit) => {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    Expr::Literal(sparql_literal_to_polars_literal_value(lit))
                        .alias(context.as_str()),
                );
                solution_mappings
            }
            Expression::Variable(v) => {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(col(v.as_str()).alias(context.as_str()));
                solution_mappings
            }
            Expression::Or(left, right) => {
                let left_context = context.extension_with(PathEntry::OrLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::OrRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);

                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Or,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::And(left, right) => {
                let left_context = context.extension_with(PathEntry::AndLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::AndRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::And,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::Equal(left, right) => {
                let left_context = context.extension_with(PathEntry::EqualLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::EqualRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Eq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::SameTerm(_, _) => {
                todo!("Not implemented")
            }
            Expression::Greater(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::GreaterRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Gt,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterOrEqualLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::GreaterOrEqualRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::GtEq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::Less(left, right) => {
                let left_context = context.extension_with(PathEntry::LessLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::LessRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Lt,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::LessOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::LessOrEqualLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::LessOrEqualRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::LtEq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::In(left, right) => {
                let left_context = context.extension_with(PathEntry::InLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let right_contexts: Vec<Context> = (0..right.len())
                    .map(|i| context.extension_with(PathEntry::InRight(i as u16)))
                    .collect();
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                for i in 0..right.len() {
                    let expr = right.get(i).unwrap();
                    let expr_context = right_contexts.get(i).unwrap();
                    let expr_prepared_time_series_queries =
                        split_time_series_queries(&mut prepared_time_series_queries, &expr_context);
                    let expr_static_query_map =
                        split_static_queries_opt(&mut static_query_map, &expr_context);
                    output_solution_mappings = self
                        .lazy_expression(
                            expr,
                            output_solution_mappings,
                            expr_static_query_map,
                            expr_prepared_time_series_queries,
                            expr_context,
                        )
                        .await?;
                }
                let mut expr = Expr::Literal(LiteralValue::Boolean(false));

                for right_context in &right_contexts {
                    expr = Expr::BinaryExpr {
                        left: Box::new(expr),
                        op: Operator::Or,
                        right: Box::new(Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Eq,
                            right: Box::new(col(right_context.as_str())),
                        }),
                    }
                }
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(expr.alias(context.as_str()))
                    .drop_columns([left_context.as_str()])
                    .drop_columns(
                        right_contexts
                            .iter()
                            .map(|x| x.as_str())
                            .collect::<Vec<&str>>(),
                    );
                output_solution_mappings
            }
            Expression::Add(left, right) => {
                let left_context = context.extension_with(PathEntry::AddLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::AddRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Plus,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::Subtract(left, right) => {
                let left_context = context.extension_with(PathEntry::SubtractLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::SubtractRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Minus,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::Multiply(left, right) => {
                let left_context = context.extension_with(PathEntry::MultiplyLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::MultiplyRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Multiply,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::Divide(left, right) => {
                let left_context = context.extension_with(PathEntry::DivideLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::DivideRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &right_context,
                    )
                    .await?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Divide,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings
            }
            Expression::UnaryPlus(inner) => {
                let plus_context = context.extension_with(PathEntry::UnaryPlus);

                let mut output_solution_mappings = self
                    .lazy_expression(
                        inner,
                        solution_mappings,
                        static_query_map,
                        prepared_time_series_queries,
                        &plus_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Plus,
                            right: Box::new(col(&plus_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([&plus_context.as_str()]);
                output_solution_mappings
            }
            Expression::UnaryMinus(inner) => {
                let minus_context = context.extension_with(PathEntry::UnaryMinus);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        inner,
                        solution_mappings,
                        static_query_map,
                        prepared_time_series_queries,
                        &minus_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Minus,
                            right: Box::new(col(&minus_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([&minus_context.as_str()]);
                output_solution_mappings
            }
            Expression::Not(inner) => {
                let not_context = context.extension_with(PathEntry::Not);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        inner,
                        solution_mappings,
                        static_query_map,
                        prepared_time_series_queries,
                        &not_context,
                    )
                    .await?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(col(&not_context.as_str()).not().alias(context.as_str()))
                    .drop_columns([&not_context.as_str()]);
                output_solution_mappings
            }
            Expression::Exists(inner) => {
                let exists_context = context.extension_with(PathEntry::Exists);
                let mut output_solution_mappings = solution_mappings;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        Expr::Literal(LiteralValue::Int64(1)).alias(&exists_context.as_str()),
                    )
                    .with_column(col(&exists_context.as_str()).cum_sum(false).alias(&exists_context.as_str()));

                let new_inner = rewrite_exists_graph_pattern(inner, &exists_context.as_str());
                let SolutionMappings {
                    mappings: exists_lf,
                    ..
                } = self
                    .lazy_graph_pattern(
                        &new_inner,
                        Some(output_solution_mappings.clone()),
                        static_query_map.unwrap(),
                        prepared_time_series_queries,
                        &exists_context,
                    )
                    .await?;
                let SolutionMappings {
                    mappings,
                    columns,
                    datatypes,
                } = output_solution_mappings;
                let mut df = mappings.collect().unwrap();
                let exists_df = exists_lf
                    .select([col(&exists_context.as_str())])
                    .unique(None, UniqueKeepStrategy::First)
                    .collect()
                    .expect("Collect lazy exists error");
                let mut ser = Series::from(
                    is_in(df.column(&exists_context.as_str())
                        .unwrap(),
                        exists_df.column(&exists_context.as_str()).unwrap())
                        .unwrap(),
                );
                ser.rename(context.as_str());
                df.with_column(ser).unwrap();
                df = df.drop(&exists_context.as_str()).unwrap();
                SolutionMappings::new(df.lazy(), columns, datatypes)
            }
            Expression::Bound(v) => {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(col(v.as_str()).is_null().alias(context.as_str()));
                solution_mappings
            }
            Expression::If(left, middle, right) => {
                let left_context = context.extension_with(PathEntry::IfLeft);
                let left_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_time_series_queries,
                        &left_context,
                    )
                    .await?;
                let middle_context = context.extension_with(PathEntry::IfMiddle);
                let middle_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &middle_context);
                let middle_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &middle_context);
                output_solution_mappings = self
                    .lazy_expression(
                        middle,
                        output_solution_mappings,
                        middle_static_query_map,
                        middle_prepared_time_series_queries,
                        &middle_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::IfRight);
                let right_prepared_time_series_queries =
                    split_time_series_queries(&mut prepared_time_series_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_time_series_queries,
                        &context.extension_with(PathEntry::IfRight),
                    )
                    .await?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::Ternary {
                            predicate: Box::new(col(left_context.as_str())),
                            truthy: Box::new(col(middle_context.as_str())),
                            falsy: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([
                        left_context.as_str(),
                        middle_context.as_str(),
                        right_context.as_str(),
                    ]);
                output_solution_mappings
            }
            Expression::Coalesce(inner) => {
                let inner_contexts: Vec<Context> = (0..inner.len())
                    .map(|i| context.extension_with(PathEntry::Coalesce(i as u16)))
                    .collect();
                let mut output_solution_mappings = solution_mappings;
                for i in 0..inner.len() {
                    let inner_context = inner_contexts.get(i).unwrap();
                    let inner_prepared_time_series_queries = split_time_series_queries(
                        &mut prepared_time_series_queries,
                        &inner_context,
                    );
                    let inner_static_query_map =
                        split_static_queries_opt(&mut static_query_map, &inner_context);
                    output_solution_mappings = self
                        .lazy_expression(
                            inner.get(i).unwrap(),
                            output_solution_mappings,
                            inner_static_query_map,
                            inner_prepared_time_series_queries,
                            inner_context,
                        )
                        .await?;
                }

                let coalesced_context = inner_contexts.get(0).unwrap();
                let mut coalesced = col(&coalesced_context.as_str());
                for c in &inner_contexts[1..inner_contexts.len()] {
                    coalesced = Expr::Ternary {
                        predicate: Box::new(is_not_null(coalesced.clone())),
                        truthy: Box::new(coalesced.clone()),
                        falsy: Box::new(col(c.as_str())),
                    }
                }
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(coalesced.alias(context.as_str()))
                    .drop_columns(
                        inner_contexts
                            .iter()
                            .map(|c| c.as_str())
                            .collect::<Vec<&str>>(),
                    );
                output_solution_mappings
            }
            Expression::FunctionCall(func, args) => {
                let args_contexts: Vec<Context> = (0..args.len())
                    .map(|i| context.extension_with(PathEntry::FunctionCall(i as u16)))
                    .collect();
                let mut output_solution_mappings = solution_mappings;
                for i in 0..args.len() {
                    let arg_context = args_contexts.get(i).unwrap();
                    let arg_prepared_time_series_queries =
                        split_time_series_queries(&mut prepared_time_series_queries, &arg_context);
                    let arg_static_query_map =
                        split_static_queries_opt(&mut static_query_map, &arg_context);
                    output_solution_mappings = self
                        .lazy_expression(
                            args.get(i).unwrap(),
                            output_solution_mappings,
                            arg_static_query_map,
                            arg_prepared_time_series_queries,
                            arg_context,
                        )
                        .await?;
                    output_solution_mappings.mappings =
                        output_solution_mappings.mappings.collect().unwrap().lazy();
                    //TODO: workaround for stack overflow - post bug?
                }
                match func {
                    Function::Year => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str())
                                    .dt()
                                    .year()
                                    .alias(context.as_str()),
                            );
                    }
                    Function::Month => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str())
                                    .dt()
                                    .month()
                                    .alias(context.as_str()),
                            );
                    }
                    Function::Day => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str())
                                    .dt()
                                    .day()
                                    .alias(context.as_str()),
                            );
                    }
                    Function::Hours => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str())
                                    .dt()
                                    .hour()
                                    .alias(context.as_str()),
                            );
                    }
                    Function::Minutes => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str())
                                    .dt()
                                    .minute()
                                    .alias(context.as_str()),
                            );
                    }
                    Function::Seconds => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str())
                                    .dt()
                                    .second()
                                    .alias(context.as_str()),
                            );
                    }
                    Function::Abs => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str()).abs().alias(context.as_str()),
                            );
                    }
                    Function::Ceil => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str()).ceil().alias(context.as_str()),
                            );
                    }
                    Function::Floor => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str()).floor().alias(context.as_str()),
                            );
                    }
                    Function::Concat => {
                        assert!(args.len() > 1);
                        let cols: Vec<_> = args_contexts.iter().map(|x|col(x.as_str())).collect();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                concat_str(cols, "").alias(context.as_str()),
                            );
                    }
                    Function::Round => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(&first_context.as_str())
                                    .round(0)
                                    .alias(context.as_str()),
                            );
                    }
                    Function::Custom(nn) => {
                        let iri = nn.as_str();
                        if iri == xsd::INTEGER.as_str() {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(&first_context.as_str())
                                        .cast(DataType::Int64)
                                        .alias(context.as_str()),
                                );
                        } else if iri == xsd::STRING.as_str() {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(&first_context.as_str())
                                        .cast(DataType::String)
                                        .alias(context.as_str()),
                                );
                        } else if iri == DATETIME_AS_NANOS {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(&first_context.as_str())
                                        .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                                        .cast(DataType::UInt64)
                                        .alias(context.as_str()),
                                );
                        } else if iri == DATETIME_AS_SECONDS {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(&first_context.as_str())
                                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                                        .cast(DataType::UInt64)
                                        .div(lit(1000))
                                        .alias(context.as_str()),
                                );
                        } else if iri == NANOS_AS_DATETIME {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(&first_context.as_str())
                                        .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                                        .alias(context.as_str()),
                                );
                        } else if iri == SECONDS_AS_DATETIME {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(&first_context.as_str())
                                        .mul(Expr::Literal(LiteralValue::UInt64(1000)))
                                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                                        .alias(context.as_str()),
                                );
                        } else if iri == MODULUS {
                            assert_eq!(args.len(), 2);
                            let first_context = args_contexts.get(0).unwrap();
                            let second_context = args_contexts.get(1).unwrap();

                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    (col(&first_context.as_str()) % col(&second_context.as_str()))
                                        .alias(context.as_str()),
                                );
                        } else if iri == FLOOR_DATETIME_TO_SECONDS_INTERVAL {
                            assert_eq!(args.len(), 2);
                            let first_context = args_contexts.get(0).unwrap();
                            let second_context = args_contexts.get(1).unwrap();

                            let first_as_seconds = col(&first_context.as_str())
                                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                                .cast(DataType::UInt64)
                                .div(lit(1000));

                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    ((first_as_seconds.clone()
                                        - (first_as_seconds % col(&second_context.as_str())))
                                    .mul(Expr::Literal(LiteralValue::UInt64(1000)))
                                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
                                    .alias(context.as_str()),
                                );
                        } else {
                            todo!("{:?}", nn)
                        }
                    }
                    _ => {
                        todo!()
                    }
                }
                output_solution_mappings.mappings = output_solution_mappings.mappings.drop_columns(
                    args_contexts
                        .iter()
                        .map(|x| x.as_str())
                        .collect::<Vec<&str>>(),
                );
                output_solution_mappings
            }
        };
        Ok(output_solution_mappings)
    }
}

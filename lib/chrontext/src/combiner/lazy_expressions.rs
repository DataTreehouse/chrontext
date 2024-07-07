use super::Combiner;
use crate::combiner::static_subqueries::split_static_queries_opt;
use crate::combiner::virtualized_queries::split_virtualized_queries;
use crate::combiner::CombinerError;
use async_recursion::async_recursion;
use oxrdf::vocab::xsd;
use polars::prelude::{col, Expr, LiteralValue, Operator};
use query_processing::exists_helper::rewrite_exists_graph_pattern;
use query_processing::expressions::{
    binary_expression, bound, coalesce_expression, exists, func_expression, if_expression,
    in_expression, literal, named_node, not_expression, unary_minus, unary_plus, variable,
};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl Combiner {
    #[async_recursion]
    pub async fn lazy_expression(
        &mut self,
        expr: &Expression,
        solution_mappings: SolutionMappings,
        mut static_query_map: Option<HashMap<Context, Query>>,
        mut prepared_virtualized_queries: Option<HashMap<Context, Vec<VirtualizedQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        let output_solution_mappings = match expr {
            Expression::NamedNode(nn) => named_node(solution_mappings, nn, context)?,
            Expression::Literal(lit) => literal(solution_mappings, lit, context)?,
            Expression::Variable(v) => variable(solution_mappings, v, context)?,
            Expression::Or(left, right) => {
                let left_context = context.extension_with(PathEntry::OrLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::OrRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);

                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Or,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::And(left, right) => {
                let left_context = context.extension_with(PathEntry::AndLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::AndRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;
                binary_expression(
                    output_solution_mappings,
                    Operator::And,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Equal(left, right) => {
                let left_context = context.extension_with(PathEntry::EqualLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::EqualRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Eq,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::SameTerm(_, _) => {
                todo!("Not implemented")
            }
            Expression::Greater(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::GreaterRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Gt,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterOrEqualLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::GreaterOrEqualRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;

                binary_expression(
                    output_solution_mappings,
                    Operator::GtEq,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Less(left, right) => {
                let left_context = context.extension_with(PathEntry::LessLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::LessRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Lt,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::LessOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::LessOrEqualLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::LessOrEqualRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;

                binary_expression(
                    output_solution_mappings,
                    Operator::LtEq,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::In(left, right) => {
                let solution_mappings = solution_mappings.as_eager();
                let solution_mappings = solution_mappings.as_lazy();

                let left_context = context.extension_with(PathEntry::InLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
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
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                for i in 0..right.len() {
                    let expr = right.get(i).unwrap();
                    let expr_context = right_contexts.get(i).unwrap();
                    let expr_prepared_virtualized_queries =
                        split_virtualized_queries(&mut prepared_virtualized_queries, &expr_context);
                    let expr_static_query_map =
                        split_static_queries_opt(&mut static_query_map, &expr_context);
                    output_solution_mappings = self
                        .lazy_expression(
                            expr,
                            output_solution_mappings,
                            expr_static_query_map,
                            expr_prepared_virtualized_queries,
                            expr_context,
                        )
                        .await?;
                }
                in_expression(
                    output_solution_mappings,
                    &left_context,
                    &right_contexts,
                    &context,
                )?
            }
            Expression::Add(left, right) => {
                let left_context = context.extension_with(PathEntry::AddLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::AddRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Plus,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Subtract(left, right) => {
                let left_context = context.extension_with(PathEntry::SubtractLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::SubtractRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Minus,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Multiply(left, right) => {
                let left_context = context.extension_with(PathEntry::MultiplyLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::MultiplyRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;

                binary_expression(
                    output_solution_mappings,
                    Operator::Multiply,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Divide(left, right) => {
                let left_context = context.extension_with(PathEntry::DivideLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::DivideRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &right_context,
                    )
                    .await?;

                binary_expression(
                    output_solution_mappings,
                    Operator::Divide,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::UnaryPlus(inner) => {
                let plus_context = context.extension_with(PathEntry::UnaryPlus);

                let output_solution_mappings = self
                    .lazy_expression(
                        inner,
                        solution_mappings,
                        static_query_map,
                        prepared_virtualized_queries,
                        &plus_context,
                    )
                    .await?;
                unary_plus(output_solution_mappings, &plus_context, context)?
            }
            Expression::UnaryMinus(inner) => {
                let minus_context = context.extension_with(PathEntry::UnaryMinus);
                let output_solution_mappings = self
                    .lazy_expression(
                        inner,
                        solution_mappings,
                        static_query_map,
                        prepared_virtualized_queries,
                        &minus_context,
                    )
                    .await?;
                unary_minus(output_solution_mappings, &minus_context, context)?
            }
            Expression::Not(inner) => {
                let not_context = context.extension_with(PathEntry::Not);
                let output_solution_mappings = self
                    .lazy_expression(
                        inner,
                        solution_mappings,
                        static_query_map,
                        prepared_virtualized_queries,
                        &not_context,
                    )
                    .await?;
                not_expression(output_solution_mappings, &not_context, context)?
            }
            Expression::Exists(inner) => {
                let exists_context = context.extension_with(PathEntry::Exists);
                let mut output_solution_mappings = solution_mappings;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        Expr::Literal(LiteralValue::Int64(1)).alias(&exists_context.as_str()),
                    )
                    .with_column(
                        col(&exists_context.as_str())
                            .cum_sum(false)
                            .alias(&exists_context.as_str()),
                    );

                let new_inner = rewrite_exists_graph_pattern(inner, &exists_context.as_str());
                output_solution_mappings.rdf_node_types.insert(
                    exists_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                let SolutionMappings {
                    mappings: exists_lf,
                    ..
                } = self
                    .lazy_graph_pattern(
                        &new_inner,
                        Some(output_solution_mappings.clone()),
                        static_query_map.unwrap(),
                        prepared_virtualized_queries,
                        &exists_context,
                    )
                    .await?;
                exists(
                    output_solution_mappings,
                    exists_lf,
                    &exists_context,
                    context,
                )?
            }
            Expression::Bound(v) => bound(solution_mappings, v, context)?,
            Expression::If(left, middle, right) => {
                let left_context = context.extension_with(PathEntry::IfLeft);
                let left_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &left_context);
                let left_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &left_context);
                let mut output_solution_mappings = self
                    .lazy_expression(
                        left,
                        solution_mappings,
                        left_static_query_map,
                        left_prepared_virtualized_queries,
                        &left_context,
                    )
                    .await?;
                let middle_context = context.extension_with(PathEntry::IfMiddle);
                let middle_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &middle_context);
                let middle_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &middle_context);
                output_solution_mappings = self
                    .lazy_expression(
                        middle,
                        output_solution_mappings,
                        middle_static_query_map,
                        middle_prepared_virtualized_queries,
                        &middle_context,
                    )
                    .await?;
                let right_context = context.extension_with(PathEntry::IfRight);
                let right_prepared_virtualized_queries =
                    split_virtualized_queries(&mut prepared_virtualized_queries, &right_context);
                let right_static_query_map =
                    split_static_queries_opt(&mut static_query_map, &right_context);
                output_solution_mappings = self
                    .lazy_expression(
                        right,
                        output_solution_mappings,
                        right_static_query_map,
                        right_prepared_virtualized_queries,
                        &context.extension_with(PathEntry::IfRight),
                    )
                    .await?;

                if_expression(
                    output_solution_mappings,
                    &left_context,
                    &middle_context,
                    &right_context,
                    &context,
                )?
            }
            Expression::Coalesce(inner) => {
                let inner_contexts: Vec<Context> = (0..inner.len())
                    .map(|i| context.extension_with(PathEntry::Coalesce(i as u16)))
                    .collect();
                let mut output_solution_mappings = solution_mappings;
                for i in 0..inner.len() {
                    let inner_context = inner_contexts.get(i).unwrap();
                    let inner_prepared_virtualized_queries = split_virtualized_queries(
                        &mut prepared_virtualized_queries,
                        &inner_context,
                    );
                    let inner_static_query_map =
                        split_static_queries_opt(&mut static_query_map, &inner_context);
                    output_solution_mappings = self
                        .lazy_expression(
                            inner.get(i).unwrap(),
                            output_solution_mappings,
                            inner_static_query_map,
                            inner_prepared_virtualized_queries,
                            inner_context,
                        )
                        .await?;
                }

                coalesce_expression(output_solution_mappings, inner_contexts, &context)?
            }
            Expression::FunctionCall(func, args) => {
                let mut args_contexts: HashMap<usize, Context> = HashMap::new();
                let mut output_solution_mappings = solution_mappings;
                for i in 0..args.len() {
                    let arg_context = context.extension_with(PathEntry::FunctionCall(i as u16));
                    let arg_prepared_virtualized_queries =
                        split_virtualized_queries(&mut prepared_virtualized_queries, &arg_context);
                    let arg_static_query_map =
                        split_static_queries_opt(&mut static_query_map, &arg_context);
                    output_solution_mappings = self
                        .lazy_expression(
                            args.get(i).unwrap(),
                            output_solution_mappings,
                            arg_static_query_map,
                            arg_prepared_virtualized_queries,
                            &arg_context,
                        )
                        .await?;
                    args_contexts.insert(i, arg_context);
                }
                func_expression(
                    output_solution_mappings,
                    func,
                    args,
                    args_contexts,
                    &context,
                )?
            }
        };
        Ok(output_solution_mappings)
    }
}

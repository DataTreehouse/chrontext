use super::SPARQLToSQLExpressionTransformer;
use crate::timeseries_database::timeseries_sql_rewrite::TimeSeriesQueryToSQLError;
use sea_query::{Function, SimpleExpr};
use spargebra::algebra::AggregateExpression;

impl SPARQLToSQLExpressionTransformer<'_> {
    //TODO: Support distinct in aggregates.. how???
    pub(crate) fn sparql_aggregate_expression_to_sql_expression(
        &mut self,
        agg: &AggregateExpression,
    ) -> Result<SimpleExpr, TimeSeriesQueryToSQLError> {
        Ok(match agg {
            AggregateExpression::Count { expr, distinct: _ } => {
                if let Some(some_expr) = expr {
                    SimpleExpr::FunctionCall(
                        Function::Count,
                        vec![self.sparql_expression_to_sql_expression(some_expr)?],
                    )
                } else {
                    todo!("")
                }
            }
            AggregateExpression::Sum { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Sum,
                vec![self.sparql_expression_to_sql_expression(expr)?],
            ),
            AggregateExpression::Avg { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Avg,
                vec![self.sparql_expression_to_sql_expression(expr)?],
            ),
            AggregateExpression::Min { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Min,
                vec![self.sparql_expression_to_sql_expression(expr)?],
            ),
            AggregateExpression::Max { expr, distinct: _ } => SimpleExpr::FunctionCall(
                Function::Max,
                vec![self.sparql_expression_to_sql_expression(expr)?],
            ),
            AggregateExpression::GroupConcat {
                expr: _,
                distinct: _,
                separator: _,
            } => {
                todo!("")
            }
            AggregateExpression::Sample {
                expr: _,
                distinct: _,
            } => {
                todo!("")
            }
            AggregateExpression::Custom {
                expr: _,
                distinct: _,
                name: _,
            } => {
                todo!("")
            }
        })
    }
}

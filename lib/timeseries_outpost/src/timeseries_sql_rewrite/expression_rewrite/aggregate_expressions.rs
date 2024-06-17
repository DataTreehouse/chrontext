use super::SPARQLToSQLExpressionTransformer;
use crate::timeseries_sql_rewrite::TimeseriesQueryToSQLError;
use sea_query::{Func, SimpleExpr};
use spargebra::algebra::{AggregateExpression, AggregateFunction};

impl SPARQLToSQLExpressionTransformer<'_> {
    //TODO: Support distinct in aggregates.. how???
    pub(crate) fn sparql_aggregate_expression_to_sql_expression(
        &mut self,
        agg: &AggregateExpression,
    ) -> Result<SimpleExpr, TimeseriesQueryToSQLError> {
        Ok(match agg {
            AggregateExpression::CountSolutions { distinct: _ } => {
                todo!("")
            }
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct: _,
            } => match name {
                AggregateFunction::Count => SimpleExpr::FunctionCall(Func::count(
                    self.sparql_expression_to_sql_expression(expr)?,
                )),
                AggregateFunction::Sum => SimpleExpr::FunctionCall(Func::sum(
                    self.sparql_expression_to_sql_expression(expr)?,
                )),
                AggregateFunction::Avg => SimpleExpr::FunctionCall(Func::avg(
                    self.sparql_expression_to_sql_expression(expr)?,
                )),
                AggregateFunction::Min => SimpleExpr::FunctionCall(Func::min(
                    self.sparql_expression_to_sql_expression(expr)?,
                )),
                AggregateFunction::Max => SimpleExpr::FunctionCall(Func::max(
                    self.sparql_expression_to_sql_expression(expr)?,
                )),
                _ => todo!(),
            },
        })
    }
}

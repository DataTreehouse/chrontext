use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::AggregateExpression;
use std::collections::HashSet;

pub struct AEReturn {
    pub aggregate_expression: Option<AggregateExpression>,
}

impl AEReturn {
    fn new() -> AEReturn {
        AEReturn {
            aggregate_expression: None,
        }
    }

    fn with_aggregate_expression(
        &mut self,
        aggregate_expression: AggregateExpression,
    ) -> &mut AEReturn {
        self.aggregate_expression = Some(aggregate_expression);
        self
    }
}

impl StaticQueryRewriter {
    pub fn rewrite_aggregate_expression(
        &mut self,
        aggregate_expression: &AggregateExpression,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> AEReturn {
        let mut aer = AEReturn::new();
        match aggregate_expression {
            AggregateExpression::CountSolutions { distinct } => {
                aer.with_aggregate_expression(AggregateExpression::CountSolutions {
                    distinct: *distinct,
                });
            }
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct,
            } => {
                let mut expr_rewritten = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    create_subquery,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
                if expr_rewritten.is_subquery {
                    unimplemented!("Exists patterns containing time series values within aggregation is not supported")
                }
                if expr_rewritten.expression.is_some()
                    && expr_rewritten.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    aer.with_aggregate_expression(AggregateExpression::FunctionCall {
                        name: name.clone(),
                        expr: expr_rewritten.expression.take().unwrap(),
                        distinct: *distinct,
                    });
                }
            }
        }
        aer
    }
}

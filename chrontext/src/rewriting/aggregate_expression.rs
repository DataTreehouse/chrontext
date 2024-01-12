use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use representation::query_context::{Context, PathEntry};
use oxrdf::Variable;
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
            AggregateExpression::Count { expr, distinct } => {
                if let Some(expr) = expr {
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
                        aer.with_aggregate_expression(AggregateExpression::Count {
                            expr: Some(Box::new(expr_rewritten.expression.take().unwrap())),
                            distinct: *distinct,
                        });
                    }
                } else {
                    aer.with_aggregate_expression(AggregateExpression::Count {
                        expr: None,
                        distinct: *distinct,
                    });
                }
            }
            AggregateExpression::Sum { expr, distinct } => {
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
                    aer.with_aggregate_expression(AggregateExpression::Sum {
                        expr: Box::new(expr_rewritten.expression.take().unwrap()),
                        distinct: *distinct,
                    });
                }
            }
            AggregateExpression::Avg { expr, distinct } => {
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
                    aer.with_aggregate_expression(AggregateExpression::Avg {
                        expr: Box::new(expr_rewritten.expression.take().unwrap()),
                        distinct: *distinct,
                    });
                }
            }
            AggregateExpression::Min { expr, distinct } => {
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
                    aer.with_aggregate_expression(AggregateExpression::Min {
                        expr: Box::new(expr_rewritten.expression.take().unwrap()),
                        distinct: *distinct,
                    });
                }
            }
            AggregateExpression::Max { expr, distinct } => {
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
                    aer.with_aggregate_expression(AggregateExpression::Max {
                        expr: Box::new(expr_rewritten.expression.take().unwrap()),
                        distinct: *distinct,
                    });
                }
            }
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
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
                    aer.with_aggregate_expression(AggregateExpression::GroupConcat {
                        expr: Box::new(expr_rewritten.expression.take().unwrap()),
                        distinct: *distinct,
                        separator: separator.clone(),
                    });
                }
            }
            AggregateExpression::Sample { expr, distinct } => {
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
                    aer.with_aggregate_expression(AggregateExpression::Sample {
                        expr: Box::new(expr_rewritten.expression.take().unwrap()),
                        distinct: *distinct,
                    });
                }
            }
            AggregateExpression::Custom {
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
                    aer.with_aggregate_expression(AggregateExpression::Custom {
                        name: name.clone(),
                        expr: Box::new(expr_rewritten.expression.take().unwrap()),
                        distinct: *distinct,
                    });
                }
            }
        }
        aer
    }
}

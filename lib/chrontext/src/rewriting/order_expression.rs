use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::OrderExpression;
use std::collections::HashSet;

pub struct OEReturn {
    pub order_expression: Option<OrderExpression>,
}

impl OEReturn {
    fn new() -> OEReturn {
        OEReturn {
            order_expression: None,
        }
    }

    fn with_order_expression(&mut self, order_expression: OrderExpression) -> &mut OEReturn {
        self.order_expression = Some(order_expression);
        self
    }
}

impl StaticQueryRewriter {
    pub fn rewrite_order_expression(
        &mut self,
        order_expression: &OrderExpression,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> OEReturn {
        let mut oer = OEReturn::new();
        match order_expression {
            OrderExpression::Asc(e) => {
                let mut e_rewrite = self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    create_subquery,
                    &context.extension_with(PathEntry::OrderingOperation),
                );
                if e_rewrite.is_subquery {
                    unimplemented!("Exists patterns containing time series values within aggregation is not supported")
                }
                if e_rewrite.expression.is_some() {
                    oer.with_order_expression(OrderExpression::Asc(
                        e_rewrite.expression.take().unwrap(),
                    ));
                }
            }
            OrderExpression::Desc(e) => {
                let mut e_rewrite = self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    create_subquery,
                    &context.extension_with(PathEntry::OrderingOperation),
                );
                if e_rewrite.is_subquery {
                    unimplemented!("Exists patterns containing time series values within aggregation is not supported")
                }
                if e_rewrite.expression.is_some() {
                    oer.with_order_expression(OrderExpression::Desc(
                        e_rewrite.expression.take().unwrap(),
                    ));
                }
            }
        }
        oer
    }
}

use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashSet;

pub enum UnaryOrdinaryOperator {
    UnaryPlus,
    UnaryMinus,
}

impl StaticQueryRewriter {
    pub fn rewrite_unary_ordinary_expression(
        &mut self,
        wrapped: &Expression,
        operation: &UnaryOrdinaryOperator,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let (path_entry, expression): (_, fn(Box<Expression>) -> Expression) = match operation {
            UnaryOrdinaryOperator::UnaryPlus => (PathEntry::UnaryPlus, Expression::UnaryPlus),
            UnaryOrdinaryOperator::UnaryMinus => (PathEntry::UnaryMinus, Expression::UnaryMinus),
        };
        let mut wrapped_rewrite = self.rewrite_expression(
            wrapped,
            &ChangeType::NoChange,
            variables_in_scope,
            create_subquery,
            &context.extension_with(path_entry),
        );
        let mut exr = ExReturn::new();
        exr.with_is_subquery(&mut wrapped_rewrite);
        if wrapped_rewrite.expression.is_some()
            && wrapped_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
        {
            let wrapped_expression_rewrite = wrapped_rewrite.expression.take().unwrap();
            exr.with_expression(expression(Box::new(wrapped_expression_rewrite)))
                .with_change_type(ChangeType::NoChange);
            return exr;
        }
        self.project_all_static_variables(vec![&wrapped_rewrite], context);
        exr
    }
}

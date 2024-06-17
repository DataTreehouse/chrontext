use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashSet;

pub enum BinaryOrdinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    LessOrEqual,
    Less,
    Greater,
    GreaterOrEqual,
    SameTerm,
    Equal,
}

impl StaticQueryRewriter {
    pub fn rewrite_binary_ordinary_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        operation: &BinaryOrdinaryOperator,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let (left_path_entry, right_path_entry, binary_expression): (
            _,
            _,
            fn(Box<_>, Box<_>) -> Expression,
        ) = match { operation } {
            BinaryOrdinaryOperator::Add => {
                (PathEntry::AddLeft, PathEntry::AddRight, Expression::Add)
            }
            BinaryOrdinaryOperator::Subtract => (
                PathEntry::SubtractLeft,
                PathEntry::SubtractRight,
                Expression::Subtract,
            ),
            BinaryOrdinaryOperator::Multiply => (
                PathEntry::MultiplyLeft,
                PathEntry::MultiplyRight,
                Expression::Multiply,
            ),
            BinaryOrdinaryOperator::Divide => (
                PathEntry::DivideLeft,
                PathEntry::DivideRight,
                Expression::Divide,
            ),
            BinaryOrdinaryOperator::LessOrEqual => (
                PathEntry::LessOrEqualLeft,
                PathEntry::LessOrEqualRight,
                Expression::LessOrEqual,
            ),
            BinaryOrdinaryOperator::Less => {
                (PathEntry::LessLeft, PathEntry::LessRight, Expression::Less)
            }
            BinaryOrdinaryOperator::Greater => (
                PathEntry::GreaterLeft,
                PathEntry::GreaterRight,
                Expression::Greater,
            ),
            BinaryOrdinaryOperator::GreaterOrEqual => (
                PathEntry::GreaterOrEqualLeft,
                PathEntry::GreaterOrEqualRight,
                Expression::GreaterOrEqual,
            ),
            BinaryOrdinaryOperator::SameTerm => (
                PathEntry::SameTermLeft,
                PathEntry::SameTermRight,
                Expression::SameTerm,
            ),
            BinaryOrdinaryOperator::Equal => (
                PathEntry::EqualLeft,
                PathEntry::EqualRight,
                Expression::Equal,
            ),
        };

        let mut left_rewrite = self.rewrite_expression(
            left,
            &ChangeType::NoChange,
            variables_in_scope,
            create_subquery,
            &context.extension_with(left_path_entry),
        );
        let mut right_rewrite = self.rewrite_expression(
            right,
            &ChangeType::NoChange,
            variables_in_scope,
            create_subquery,
            &context.extension_with(right_path_entry),
        );
        let mut exr = ExReturn::new();
        exr.with_is_subquery(&mut left_rewrite)
            .with_is_subquery(&mut right_rewrite);
        if left_rewrite.expression.is_some()
            && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            && right_rewrite.expression.is_some()
            && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
        {
            let left_expression_rewrite = left_rewrite.expression.take().unwrap();
            let right_expression_rewrite = right_rewrite.expression.take().unwrap();
            exr.with_expression(binary_expression(
                Box::new(left_expression_rewrite),
                Box::new(right_expression_rewrite),
            ))
            .with_change_type(ChangeType::NoChange);
            return exr;
        }
        self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
        exr
    }
}

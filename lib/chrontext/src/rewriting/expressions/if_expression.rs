use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub fn rewrite_if_expression(
        &mut self,
        left: &Expression,
        mid: &Expression,
        right: &Expression,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let mut left_rewrite = self.rewrite_expression(
            left,
            &ChangeType::NoChange,
            variables_in_scope,
            create_subquery,
            &context.extension_with(PathEntry::IfLeft),
        );
        let mut mid_rewrite = self.rewrite_expression(
            mid,
            &ChangeType::NoChange,
            variables_in_scope,
            create_subquery || left_rewrite.is_subquery,
            &context.extension_with(PathEntry::IfMiddle),
        );
        let mut right_rewrite = self.rewrite_expression(
            right,
            &ChangeType::NoChange,
            variables_in_scope,
            create_subquery || left_rewrite.is_subquery || mid_rewrite.is_subquery,
            &context.extension_with(PathEntry::IfRight),
        );
        let mut exr = ExReturn::new();
        exr.with_is_subquery(&mut left_rewrite)
            .with_is_subquery(&mut mid_rewrite)
            .with_is_subquery(&mut right_rewrite);
        if left_rewrite.expression.is_some()
            && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            && mid_rewrite.expression.is_some()
            && mid_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            && right_rewrite.expression.is_some()
            && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
        {
            let left_expression_rewrite = left_rewrite.expression.take().unwrap();
            let mid_expression_rewrite = left_rewrite.expression.take().unwrap();
            let right_expression_rewrite = right_rewrite.expression.take().unwrap();
            exr.with_expression(Expression::If(
                Box::new(left_expression_rewrite),
                Box::new(mid_expression_rewrite),
                Box::new(right_expression_rewrite),
            ))
            .with_change_type(ChangeType::NoChange);
            return exr;
        }
        self.project_all_static_variables(
            vec![&left_rewrite, &mid_rewrite, &right_rewrite],
            context,
        );
        exr
    }
}

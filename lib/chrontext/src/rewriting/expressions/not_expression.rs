use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub fn rewrite_not_expression(
        &mut self,
        wrapped: &Expression,
        required_change_direction: &ChangeType,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let mut wrapped_rewrite = self.rewrite_expression(
            wrapped,
            &required_change_direction.opposite(),
            variables_in_scope,
            create_subquery,
            &context.extension_with(PathEntry::Not),
        );
        let mut exr = ExReturn::new();
        exr.with_is_subquery(&mut wrapped_rewrite);
        if wrapped_rewrite.expression.is_some() {
            let wrapped_change = wrapped_rewrite.change_type.take().unwrap();
            let use_change_type = match wrapped_change {
                ChangeType::NoChange => ChangeType::NoChange,
                ChangeType::Relaxed => ChangeType::Constrained,
                ChangeType::Constrained => ChangeType::Relaxed,
            };
            if use_change_type == ChangeType::NoChange
                || &use_change_type == required_change_direction
            {
                let wrapped_expression_rewrite = wrapped_rewrite.expression.take().unwrap();
                exr.with_expression(Expression::Not(Box::new(wrapped_expression_rewrite)))
                    .with_change_type(use_change_type);
                return exr;
            }
        }
        self.project_all_static_variables(vec![&wrapped_rewrite], context);
        exr
    }
}

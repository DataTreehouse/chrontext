use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_exists_expression(
        &mut self,
        wrapped: &GraphPattern,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let exists_context = context.extension_with(PathEntry::Exists);
        let mut wrapped_rewrite = self.rewrite_graph_pattern(wrapped, &exists_context);

        if !wrapped_rewrite.is_subquery {
            if !wrapped_rewrite.rewritten && !create_subquery {
                let mut exr = ExReturn::new();
                exr.with_expression(Expression::Exists(Box::new(
                    wrapped_rewrite.graph_pattern.take().unwrap(),
                )))
                .with_change_type(ChangeType::NoChange);
                return exr;
            } else {
                self.create_add_subquery(wrapped_rewrite, &exists_context);
                return ExReturn::subquery();
            }
        } else {
            return ExReturn::subquery();
        }
    }
}

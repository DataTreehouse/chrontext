use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub(crate) fn rewrite_extend(
        &mut self,
        inner: &GraphPattern,
        var: &Variable,
        expr: &Expression,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite =
            self.rewrite_graph_pattern(inner, &context.extension_with(PathEntry::ExtendInner));
        if inner_rewrite.is_subquery {
            return inner_rewrite;
        }

        let mut expr_rewrite = self.rewrite_expression(
            expr,
            &ChangeType::NoChange,
            &inner_rewrite.variables_in_scope,
            inner_rewrite.is_subquery,
            &context.extension_with(PathEntry::ExtendExpression),
        );

        if expr_rewrite.is_subquery {
            unimplemented!("No support for exists with time series values in extend yet")
        }

        if expr_rewrite.expression.is_some() {
            inner_rewrite.variables_in_scope.insert(var.clone());
            let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            inner_rewrite.with_graph_pattern(GraphPattern::Extend {
                inner: Box::new(inner_graph_pattern), //No need for push up since there should be no change
                variable: var.clone(),
                expression: expr_rewrite.expression.take().unwrap(),
            });
            inner_rewrite.with_rewritten(
                inner_rewrite.rewritten || expr_rewrite.change_type != Some(ChangeType::NoChange),
            );
            return inner_rewrite;
        } else {
            inner_rewrite.with_rewritten(true);
            return inner_rewrite;
        }
    }
}

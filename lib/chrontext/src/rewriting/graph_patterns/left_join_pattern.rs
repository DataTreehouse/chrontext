use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::graph_patterns::GPReturn;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression_opt: &Option<Expression>,
        context: &Context,
    ) -> GPReturn {
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let mut left_rewrite = self.rewrite_graph_pattern(left, &left_context);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let mut right_rewrite = self.rewrite_graph_pattern(right, &right_context);

        let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
        let mut expression_rewrite = None;

        if let Some(expression) = expression_opt {
            let mut use_variables_in_scope = left_rewrite.variables_in_scope.clone();
            use_variables_in_scope.extend(right_rewrite.variables_in_scope.clone());

            expression_rewrite = Some(self.rewrite_expression(
                expression,
                &ChangeType::NoChange,
                &use_variables_in_scope,
                left_rewrite.is_subquery || right_rewrite.is_subquery,
                &expression_context,
            ));
        }

        if left_rewrite.rewritten
            || right_rewrite.rewritten
            || (expression_rewrite.is_some()
                && expression_rewrite.as_ref().unwrap().expression.is_none())
        {
            if !left_rewrite.is_subquery {
                self.create_add_subquery(left_rewrite, &left_context);
            }
            if !right_rewrite.is_subquery {
                self.create_add_subquery(right_rewrite, &right_context);
            }
            return GPReturn::subquery();
        }

        let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
        let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
        let expression = if let Some(mut expression) = expression_rewrite {
            expression.expression.take()
        } else {
            None
        };

        left_rewrite
            .with_scope(&mut right_rewrite)
            .with_graph_pattern(GraphPattern::LeftJoin {
                left: Box::new(left_graph_pattern),
                right: Box::new(right_graph_pattern),
                expression,
            });
        return left_rewrite;
    }
}

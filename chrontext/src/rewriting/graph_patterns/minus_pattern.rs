use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,

        context: &Context,
    ) -> GPReturn {
        let left_context = context.extension_with(PathEntry::MinusLeftSide);
        let mut left_rewrite = self.rewrite_graph_pattern(left, &left_context);
        let right_context = context.extension_with(PathEntry::MinusRightSide);
        let mut right_rewrite = self.rewrite_graph_pattern(right, &right_context);
        if left_rewrite.is_subquery || right_rewrite.is_subquery {
            if !left_rewrite.is_subquery {
                self.create_add_subquery(left_rewrite, &left_context);
            }
            if !right_rewrite.is_subquery {
                self.create_add_subquery(right_rewrite, &right_context);
            }
            let ret = GPReturn::subquery();
            return ret;
        }

        if !left_rewrite.rewritten && !right_rewrite.rewritten {
            let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
            let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
            left_rewrite.with_graph_pattern(GraphPattern::Minus {
                left: Box::new(left_graph_pattern),
                right: Box::new(right_graph_pattern),
            });
            return left_rewrite;
        } else if left_rewrite.rewritten && !right_rewrite.rewritten {
            let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
            let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
            left_rewrite
                .with_graph_pattern(GraphPattern::Minus {
                    left: Box::new(left_graph_pattern),
                    right: Box::new(right_graph_pattern),
                })
                .with_rewritten(true);
            return left_rewrite;
        } else {
            // right rewritten
            self.create_add_subquery(left_rewrite, &left_context);
            self.create_add_subquery(right_rewrite, &right_context);

            let ret = GPReturn::subquery();
            return ret;
        }
    }
}

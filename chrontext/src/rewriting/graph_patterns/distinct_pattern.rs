use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_distinct(&mut self, inner: &GraphPattern, context: &Context) -> GPReturn {
        let mut gpr_inner =
            self.rewrite_graph_pattern(inner, &context.extension_with(PathEntry::DistinctInner));
        if !gpr_inner.is_subquery {
            let inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
            gpr_inner.with_graph_pattern(GraphPattern::Distinct {
                inner: Box::new(inner_graph_pattern),
            });
        }
        gpr_inner
    }
}

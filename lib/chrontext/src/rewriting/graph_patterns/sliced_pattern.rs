use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_slice(
        &mut self,
        inner: &GraphPattern,
        start: &usize,
        length: &Option<usize>,

        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite =
            self.rewrite_graph_pattern(inner, &context.extension_with(PathEntry::SliceInner));
        if !inner_rewrite.is_subquery {
            let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            inner_rewrite.with_graph_pattern(GraphPattern::Slice {
                inner: Box::new(inner_graph_pattern),
                start: start.clone(),
                length: length.clone(),
            });
            return inner_rewrite;
        }
        inner_rewrite
    }
}

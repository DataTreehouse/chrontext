use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;

impl StaticQueryRewriter {
    pub fn rewrite_service(
        &mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        silent: &bool,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite =
            self.rewrite_graph_pattern(inner, &context.extension_with(PathEntry::ServiceInner));
        if !inner_rewrite.rewritten {
            let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            inner_rewrite.with_graph_pattern(GraphPattern::Service {
                name: name.clone(),
                inner: Box::new(inner_graph_pattern),
                silent: silent.clone(),
            });
            return inner_rewrite;
        }
        panic!("Should never happen")
    }
}

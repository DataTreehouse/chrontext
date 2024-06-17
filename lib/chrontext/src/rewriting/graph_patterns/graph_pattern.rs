use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use representation::query_context::Context;
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;

impl StaticQueryRewriter {
    pub fn rewrite_graph(
        &mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        context: &Context,
    ) -> GPReturn {
        let mut inner_gpr = self.rewrite_graph_pattern(inner, context);
        if !inner_gpr.is_subquery {
            let inner_rewrite = inner_gpr.graph_pattern.take().unwrap();
            inner_gpr.with_graph_pattern(GraphPattern::Graph {
                name: name.clone(),
                inner: Box::new(inner_rewrite),
            });
            return inner_gpr;
        }
        unimplemented!("No support for rewritten graph graph pattern")
    }
}

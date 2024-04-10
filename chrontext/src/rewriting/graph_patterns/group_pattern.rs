use super::StaticQueryRewriter;
use crate::rewriting::aggregate_expression::AEReturn;
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::{AggregateExpression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_group(
        &mut self,
        graph_pattern: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        context: &Context,
    ) -> GPReturn {
        let inner_context = context.extension_with(PathEntry::GroupInner);

        let mut graph_pattern_rewrite = self.rewrite_graph_pattern(graph_pattern, &inner_context);
        if !graph_pattern_rewrite.is_subquery {
            if !graph_pattern_rewrite.rewritten {
                let variables_rewritten: Vec<Option<Variable>> = variables
                    .iter()
                    .map(|v| self.rewrite_variable(v, context))
                    .collect();

                let aes_rewritten: Vec<(Option<Variable>, AEReturn)> = aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, (v, a))| {
                        (
                            self.rewrite_variable(v, context),
                            self.rewrite_aggregate_expression(
                                a,
                                &graph_pattern_rewrite.variables_in_scope,
                                graph_pattern_rewrite.is_subquery,
                                &context.extension_with(PathEntry::GroupAggregation(i as u16)),
                            ),
                        )
                    })
                    .collect();

                if variables_rewritten.iter().all(|v| v.is_some())
                    && aes_rewritten
                        .iter()
                        .all(|(v, a)| v.is_some() && a.aggregate_expression.is_some())
                {
                    for v in &variables_rewritten {
                        graph_pattern_rewrite
                            .variables_in_scope
                            .insert(v.as_ref().unwrap().clone());
                    }
                    let inner_graph_pattern = graph_pattern_rewrite.graph_pattern.take().unwrap();

                    graph_pattern_rewrite.with_graph_pattern(GraphPattern::Group {
                        inner: Box::new(inner_graph_pattern),
                        variables: variables_rewritten
                            .into_iter()
                            .map(|v| v.unwrap())
                            .collect(),
                        aggregates: vec![],
                    });
                    return graph_pattern_rewrite;
                }
            } else {
                self.create_add_subquery(graph_pattern_rewrite.clone(), &inner_context);
                return GPReturn::subquery();
            }
        }
        graph_pattern_rewrite
    }
}

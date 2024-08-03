use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use oxrdf::Variable;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;
use virtualized_query::VirtualizedQuery;

impl TimeseriesQueryPrepper {
    pub(crate) fn prepare_extend(
        &mut self,
        inner: &GraphPattern,
        var: &Variable,
        expr: &Expression,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        let inner_context = context.extension_with(PathEntry::ExtendInner);
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            solution_mappings,
            &inner_context,
        );
        if try_groupby_complex_query {
            let mut expression_vars = HashSet::new();
            find_all_used_variables_in_expression(expr, &mut expression_vars, true, true);
            let mut found_i = None;
            let mut found_context = None;

            for (c, vqs) in &inner_prepare.virtualized_queries {
                for (i, vq) in vqs.iter().enumerate() {
                    let mut found_all = true;
                    let mut found_some = false;
                    for expression_var in &expression_vars {
                        if vq.has_equivalent_variable(expression_var, context) {
                            found_some = true;
                        } else {
                            found_all = false;
                            break;
                        }
                    }
                    if found_all && found_some {
                        found_i = Some(i);
                        found_context = Some(c.clone());
                    }
                }
            }
            if let (Some(i), Some(c)) = (found_i, found_context) {
                let inner_vq = inner_prepare
                    .virtualized_queries
                    .get_mut(&c)
                    .unwrap()
                    .remove(i);
                if inner_prepare
                    .virtualized_queries
                    .get(&c)
                    .unwrap()
                    .is_empty()
                {
                    inner_prepare.virtualized_queries.remove(&c);
                }
                let new_vq =
                    VirtualizedQuery::ExpressionAs(Box::new(inner_vq), var.clone(), expr.clone());
                if !inner_prepare.virtualized_queries.contains_key(context) {
                    inner_prepare
                        .virtualized_queries
                        .insert(context.clone(), vec![]);
                }
                inner_prepare
                    .virtualized_queries
                    .get_mut(context)
                    .unwrap()
                    .push(new_vq);

                inner_prepare
            } else {
                GPPrepReturn::fail_groupby_complex_query()
            }
        } else {
            inner_prepare
        }
    }
}

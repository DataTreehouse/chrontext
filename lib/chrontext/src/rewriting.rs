mod aggregate_expression;
mod expressions;
mod graph_patterns;
mod order_expression;
mod project_static;
mod subqueries;

use crate::constraints::{Constraint, VariableConstraints};
use crate::rewriting::expressions::ExReturn;
use oxrdf::NamedNode;
use representation::query_context::Context;
use spargebra::algebra::Expression;
use spargebra::term::Variable;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use virtualized_query::BasicVirtualizedQuery;

#[derive(Debug)]
pub struct StaticQueryRewriter {
    variable_counter: u16,
    variable_constraints: VariableConstraints,
    additional_projections: HashSet<Variable>,
    basic_virtualized_queries: Vec<BasicVirtualizedQuery>,
    first_level_virtualized_predicates: HashSet<NamedNode>,
    static_subqueries: HashMap<Context, Query>,
    rewritten_filters: HashMap<Context, Expression>,
    is_hybrid: bool,
}

impl StaticQueryRewriter {
    pub fn new(
        variable_constraints: VariableConstraints,
        first_level_virtualized_predicates: HashSet<NamedNode>,
    ) -> StaticQueryRewriter {
        StaticQueryRewriter {
            variable_counter: 0,
            additional_projections: Default::default(),
            variable_constraints,
            first_level_virtualized_predicates,
            basic_virtualized_queries: vec![],
            static_subqueries: HashMap::new(),
            rewritten_filters: HashMap::new(),
            is_hybrid: true, //TODO!
        }
    }

    pub fn rewrite_query(
        mut self,
        query: Query,
    ) -> (
        HashMap<Context, Query>,
        Vec<BasicVirtualizedQuery>,
        HashMap<Context, Expression>,
    ) {
        if !self.is_hybrid {
            let mut map = HashMap::new();
            map.insert(Context::new(), query);
            return (map, vec![], HashMap::new());
        }
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = query
        {
            let pattern_rewrite = self.rewrite_graph_pattern(&pattern, &Context::new());
            if let Some(p) = pattern_rewrite.graph_pattern {
                self.static_subqueries.insert(
                    Context::new(),
                    Query::Select {
                        dataset,
                        pattern: p,
                        base_iri,
                    },
                );
            }
            (
                self.static_subqueries,
                self.basic_virtualized_queries,
                self.rewritten_filters,
            )
        } else {
            panic!("Only support for select query")
        }
    }

    fn project_all_static_variables(&mut self, rewrites: Vec<&ExReturn>, context: &Context) {
        for r in rewrites {
            if let Some(expr) = &r.expression {
                self.project_all_static_variables_in_expression(expr, context);
            }
        }
    }

    fn rewrite_variable(&self, v: &Variable, context: &Context) -> Option<Variable> {
        if let Some(ctr) = self.variable_constraints.get_constraint(v, context) {
            if !(ctr == &Constraint::External || ctr == &Constraint::ExternallyDerived) {
                Some(v.clone())
            } else {
                None
            }
        } else {
            Some(v.clone())
        }
    }
}

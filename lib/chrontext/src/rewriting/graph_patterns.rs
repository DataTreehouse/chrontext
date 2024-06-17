mod bgp_pattern;
mod distinct_pattern;
mod extend_pattern;
mod filter_pattern;
mod graph_pattern;
mod group_pattern;
mod join_pattern;
mod left_join_pattern;
mod minus_pattern;
mod order_by_pattern;
mod path_pattern;
pub(crate) mod project_pattern;
mod reduced_pattern;
mod service_pattern;
mod sliced_pattern;
mod union_pattern;
mod values_pattern;

use super::StaticQueryRewriter;
use oxrdf::Variable;
use representation::query_context::Context;
use spargebra::algebra::GraphPattern;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct GPReturn {
    pub(crate) graph_pattern: Option<GraphPattern>,
    pub(crate) rewritten: bool,
    pub(crate) variables_in_scope: HashSet<Variable>,
    pub(crate) resources_in_scope: HashMap<Variable, Vec<Variable>>,
    pub(crate) external_ids_in_scope: HashMap<Variable, Vec<Variable>>,
    pub(crate) is_subquery: bool,
}

impl GPReturn {
    fn new(
        graph_pattern: GraphPattern,
        rewritten: bool,
        variables_in_scope: HashSet<Variable>,
        resources_in_scope: HashMap<Variable, Vec<Variable>>,
        external_ids_in_scope: HashMap<Variable, Vec<Variable>>,
        is_subquery: bool,
    ) -> GPReturn {
        GPReturn {
            graph_pattern: Some(graph_pattern),
            rewritten,
            variables_in_scope,
            resources_in_scope,
            external_ids_in_scope,
            is_subquery,
        }
    }

    fn subquery() -> GPReturn {
        GPReturn {
            graph_pattern: None,
            rewritten: true,
            variables_in_scope: Default::default(),
            resources_in_scope: Default::default(),
            external_ids_in_scope: Default::default(),
            is_subquery: true,
        }
    }

    pub(crate) fn with_graph_pattern(&mut self, graph_pattern: GraphPattern) -> &mut GPReturn {
        self.graph_pattern = Some(graph_pattern);
        self
    }

    fn with_rewritten(&mut self, rewritten: bool) -> &mut GPReturn {
        self.rewritten = rewritten;
        self
    }

    fn with_scope(&mut self, gpr: &mut GPReturn) -> &mut GPReturn {
        self.variables_in_scope
            .extend(&mut gpr.variables_in_scope.drain());

        for (k, v) in gpr.resources_in_scope.drain() {
            if let Some(vs) = self.resources_in_scope.get_mut(&k) {
                for vee in v {
                    vs.push(vee);
                }
            } else {
                self.resources_in_scope.insert(k, v);
            }
        }
        for (k, v) in gpr.external_ids_in_scope.drain() {
            if let Some(vs) = self.external_ids_in_scope.get_mut(&k) {
                for vee in v {
                    vs.push(vee);
                }
            } else {
                self.external_ids_in_scope.insert(k, v);
            }
        }
        self
    }
}

impl StaticQueryRewriter {
    pub fn rewrite_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        context: &Context,
    ) -> GPReturn {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => self.rewrite_bgp(patterns, context),
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.rewrite_path(subject, path, object),
            GraphPattern::Join { left, right } => self.rewrite_join(left, right, context),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.rewrite_left_join(left, right, expression, context),
            GraphPattern::Filter { expr, inner } => self.rewrite_filter(expr, inner, context),
            GraphPattern::Union { left, right } => self.rewrite_union(left, right, context),
            GraphPattern::Graph { name, inner } => self.rewrite_graph(name, inner, context),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.rewrite_extend(inner, variable, expression, context),
            GraphPattern::Minus { left, right } => self.rewrite_minus(left, right, context),
            GraphPattern::Values {
                variables,
                bindings,
            } => self.rewrite_values(variables, bindings),
            GraphPattern::OrderBy { inner, expression } => {
                self.rewrite_order_by(inner, expression, context)
            }
            GraphPattern::Project { inner, variables } => {
                self.rewrite_project(inner, variables, context)
            }
            GraphPattern::Distinct { inner } => self.rewrite_distinct(inner, context),
            GraphPattern::Reduced { inner } => self.rewrite_reduced(inner, context),
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => self.rewrite_slice(inner, start, length, context),
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => self.rewrite_group(inner, variables, aggregates, context),
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => self.rewrite_service(name, inner, silent, context),
            GraphPattern::DT { .. } => {
                panic!("Should never happen")
            }
            GraphPattern::PValues { .. } => {
                todo!("Not currently supported")
            }
        }
    }
}

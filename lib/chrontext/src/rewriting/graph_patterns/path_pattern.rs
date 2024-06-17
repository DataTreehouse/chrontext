use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::TermPattern;
use std::collections::HashSet;

impl StaticQueryRewriter {
    //We assume that all paths have been rewritten so as to not contain any datapoint, timestamp, or data value.
    //These should have been split into ordinary triples.
    pub fn rewrite_path(
        &mut self,
        subject: &TermPattern,
        path: &PropertyPathExpression,
        object: &TermPattern,
    ) -> GPReturn {
        let mut variables_in_scope = HashSet::new();
        if let TermPattern::Variable(s) = subject {
            variables_in_scope.insert(s.clone());
        }
        if let TermPattern::Variable(o) = object {
            variables_in_scope.insert(o.clone());
        }

        let gpr = GPReturn::new(
            GraphPattern::Path {
                subject: subject.clone(),
                path: path.clone(),
                object: object.clone(),
            },
            false,
            variables_in_scope,
            Default::default(),
            Default::default(),
            false,
        );
        return gpr;
    }
}

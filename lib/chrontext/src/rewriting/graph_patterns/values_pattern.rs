use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;
use std::collections::HashMap;

impl StaticQueryRewriter {
    pub fn rewrite_values(
        &mut self,
        variables: &Vec<Variable>,
        bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> GPReturn {
        return GPReturn::new(
            GraphPattern::Values {
                variables: variables.iter().map(|v| v.clone()).collect(),
                bindings: bindings.iter().map(|b| b.clone()).collect(),
            },
            false,
            variables.iter().map(|v| v.clone()).collect(),
            HashMap::new(),
            HashMap::new(),
            false,
        );
    }
}

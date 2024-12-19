use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;
use std::collections::HashMap;

impl StaticQueryRewriter {
    pub fn rewrite_values(
        &mut self,
        variables: &[Variable],
        bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> GPReturn {
        GPReturn::new(
            GraphPattern::Values {
                variables: variables.to_vec(),
                bindings: bindings.to_vec(),
            },
            false,
            variables.iter().cloned().collect(),
            HashMap::new(),
            HashMap::new(),
            false,
        )
    }
}

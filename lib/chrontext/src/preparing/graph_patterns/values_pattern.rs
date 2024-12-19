use super::TimeseriesQueryPrepper;
use std::collections::HashMap;

use crate::preparing::graph_patterns::GPPrepReturn;
use oxrdf::Variable;
use spargebra::term::GroundTerm;

impl TimeseriesQueryPrepper {
    pub fn prepare_values(
        &mut self,
        _variables: &[Variable],
        _bindings: &[Vec<Option<GroundTerm>>],
    ) -> GPPrepReturn {
        GPPrepReturn::new(HashMap::new())
    }
}

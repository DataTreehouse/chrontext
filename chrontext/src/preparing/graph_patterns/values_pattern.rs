use super::TimeSeriesQueryPrepper;
use std::collections::HashMap;

use crate::preparing::graph_patterns::GPPrepReturn;
use oxrdf::Variable;
use spargebra::term::GroundTerm;

impl TimeSeriesQueryPrepper {
    pub fn prepare_values(
        &mut self,
        _variables: &Vec<Variable>,
        _bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> GPPrepReturn {
        GPPrepReturn::new(HashMap::new())
    }
}

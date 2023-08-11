use std::collections::HashMap;
use super::TimeSeriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::TermPattern;

impl TimeSeriesQueryPrepper {
    //We assume that all paths have been prepared so as to not contain any datapoint, timestamp, or data value.
    //These should have been split into ordinary triples.
    pub fn prepare_path(
        &mut self,
        _subject: &TermPattern,
        _path: &PropertyPathExpression,
        _object: &TermPattern,
    ) -> GPPrepReturn {
        GPPrepReturn::new(HashMap::new())
    }
}

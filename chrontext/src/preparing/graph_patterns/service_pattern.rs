use super::TimeseriesQueryPrepper;
use std::collections::HashMap;

use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeseriesQueryPrepper {
    pub fn prepare_service(&mut self) -> GPPrepReturn {
        //Service pattern should not contain anything dynamic
        GPPrepReturn::new(HashMap::new())
    }
}

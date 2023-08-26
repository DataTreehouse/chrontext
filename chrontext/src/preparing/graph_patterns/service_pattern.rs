use super::TimeSeriesQueryPrepper;
use std::collections::HashMap;

use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_service(&mut self) -> GPPrepReturn {
        //Service pattern should not contain anything dynamic
        GPPrepReturn::new(HashMap::new())
    }
}

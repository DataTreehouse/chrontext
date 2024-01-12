mod expressions;
pub(crate) mod graph_patterns;
mod synchronization;

use representation::solution_mapping::SolutionMappings;
use crate::pushdown_setting::PushdownSetting;
use representation::query_context::Context;
use crate::timeseries_query::{BasicTimeseriesQuery, TimeseriesQuery};
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct TimeseriesQueryPrepper {
    pushdown_settings: HashSet<PushdownSetting>,
    pub(crate) basic_time_series_queries: Vec<BasicTimeseriesQuery>,
    grouping_counter: u16,
    rewritten_filters: HashMap<Context, Expression>,
}

impl TimeseriesQueryPrepper {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        basic_time_series_queries: Vec<BasicTimeseriesQuery>,
        rewritten_filters: HashMap<Context, Expression>,
    ) -> TimeseriesQueryPrepper {
        TimeseriesQueryPrepper {
            pushdown_settings,
            basic_time_series_queries,
            grouping_counter: 0,
            rewritten_filters,
        }
    }

    pub fn prepare(
        &mut self,
        query: &Query,
        solution_mappings: &mut SolutionMappings,
    ) -> HashMap<Context, Vec<TimeseriesQuery>> {
        if let Query::Select { pattern, .. } = query {
            let pattern_prepared =
                self.prepare_graph_pattern(pattern, false, solution_mappings, &Context::new());
            pattern_prepared.time_series_queries
        } else {
            panic!("Only support for Select");
        }
    }
}

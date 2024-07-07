mod expressions;
pub(crate) mod graph_patterns;
mod synchronization;

use polars::prelude::DataType;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use virtualized_query::pushdown_setting::PushdownSetting;
use virtualized_query::{BasicVirtualizedQuery, VirtualizedQuery};

#[derive(Debug)]
pub struct TimeseriesQueryPrepper {
    pushdown_settings: HashSet<PushdownSetting>,
    pub(crate) basic_virtualized_queries: Vec<BasicVirtualizedQuery>,
    grouping_counter: u16,
    rewritten_filters: HashMap<Context, Expression>,
}

impl TimeseriesQueryPrepper {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        basic_virtualized_queries: Vec<BasicVirtualizedQuery>,
        rewritten_filters: HashMap<Context, Expression>,
    ) -> TimeseriesQueryPrepper {
        TimeseriesQueryPrepper {
            pushdown_settings,
            basic_virtualized_queries,
            grouping_counter: 0,
            rewritten_filters,
        }
    }

    pub fn prepare(
        &mut self,
        query: &Query,
        solution_mappings: &mut SolutionMappings,
    ) -> HashMap<Context, Vec<VirtualizedQuery>> {
        if let Query::Select { pattern, .. } = query {
            let pattern_prepared =
                self.prepare_graph_pattern(pattern, false, solution_mappings, &Context::new());
            pattern_prepared.virtualized_queries
        } else {
            panic!("Only support for Select");
        }
    }
}

pub fn grouping_col_type() -> DataType {
    DataType::Int64
}

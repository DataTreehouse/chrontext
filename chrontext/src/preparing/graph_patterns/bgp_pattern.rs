use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::synchronization::create_identity_synchronized_queries;
use representation::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use std::collections::HashMap;

impl TimeseriesQueryPrepper {
    pub(crate) fn prepare_bgp(
        &mut self,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut local_tsqs = vec![];
        let bgp_context = context.extension_with(PathEntry::BGP);
        for tsq in &self.basic_time_series_queries {
            if let Some(dp_ctx) = &tsq.data_point_variable {
                if &dp_ctx.context == &bgp_context {
                    local_tsqs.push(TimeseriesQuery::Basic(tsq.clone()));
                }
            }
        }
        if try_groupby_complex_query {
            local_tsqs = create_identity_synchronized_queries(local_tsqs);
        }
        let mut tsqs_map = HashMap::new();
        if !local_tsqs.is_empty() {
            tsqs_map.insert(context.clone(), local_tsqs);
        }
        GPPrepReturn::new(tsqs_map)
    }
}

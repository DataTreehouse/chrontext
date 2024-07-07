use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::synchronization::create_identity_synchronized_queries;
use representation::query_context::{Context, PathEntry};
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl TimeseriesQueryPrepper {
    pub(crate) fn prepare_bgp(
        &mut self,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut local_vqs = vec![];
        let bgp_context = context.extension_with(PathEntry::BGP);
        for vq in &self.basic_virtualized_queries {
            if let Some(dp_ctx) = &vq.data_point_variable {
                if &dp_ctx.context == &bgp_context {
                    local_vqs.push(VirtualizedQuery::Basic(vq.clone()));
                }
            }
        }
        if try_groupby_complex_query {
            local_vqs = create_identity_synchronized_queries(local_vqs);
        }
        let mut vqs_map = HashMap::new();
        if !local_vqs.is_empty() {
            vqs_map.insert(context.clone(), local_vqs);
        }
        GPPrepReturn::new(vqs_map)
    }
}

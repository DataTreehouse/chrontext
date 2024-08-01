use super::TimeseriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::synchronization::create_identity_synchronized_queries;
use representation::query_context::{Context, PathEntry};
use spargebra::term::TriplePattern;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl TimeseriesQueryPrepper {
    pub(crate) fn prepare_bgp(
        &mut self,
        try_groupby_complex_query: bool,
        patterns: &Vec<TriplePattern>,
        context: &Context,
    ) -> GPPrepReturn {
        let mut local_vqs = vec![];
        let bgp_context = context.extension_with(PathEntry::BGP);
        for vq in &mut self.basic_virtualized_queries {
            if &vq.query_source_context == &bgp_context {
                if let Some(resource) = &vq.resource {
                    let template = self.virtualization.resources.get(resource).unwrap();
                    vq.finish_column_mapping(patterns, template);
                }
                //We create a degenerate VQ to be able to remove the columns later.
                local_vqs.push(VirtualizedQuery::Basic(vq.clone()));
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

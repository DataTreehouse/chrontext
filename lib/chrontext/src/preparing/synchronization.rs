use oxrdf::Variable;
use std::collections::HashSet;
use virtualized_query::{Synchronizer, VirtualizedQuery};

pub fn create_identity_synchronized_queries(
    mut vqs: Vec<VirtualizedQuery>,
) -> Vec<VirtualizedQuery> {
    let mut out_queries = vec![];
    while vqs.len() > 1 {
        let mut queries_to_synchronize = vec![];
        let first_query = vqs.remove(0);
        let first_query_virtualized_variables_set: HashSet<Variable> = HashSet::from_iter(
            first_query
                .get_virtualized_variables()
                .into_iter()
                .map(|x| x.variable.clone()),
        );
        let mut keep_vqs = vec![];
        for other in vqs.into_iter() {
            let other_query_virtualized_variables_set = HashSet::from_iter(
                other
                    .get_virtualized_variables()
                    .into_iter()
                    .map(|x| x.variable.clone()),
            );
            if !first_query_virtualized_variables_set
                .is_disjoint(&other_query_virtualized_variables_set)
            {
                queries_to_synchronize.push(other);
            } else {
                keep_vqs.push(other);
            }
        }
        vqs = keep_vqs;
        if !queries_to_synchronize.is_empty() {
            queries_to_synchronize.push(first_query);
            out_queries.push(VirtualizedQuery::InnerJoin(
                queries_to_synchronize,
                vec![Synchronizer::Identity(
                    first_query_virtualized_variables_set
                        .iter()
                        .next()
                        .unwrap()
                        .as_str()
                        .to_string(),
                )],
            ));
        } else {
            out_queries.push(first_query);
        }
    }
    out_queries.extend(vqs);
    out_queries
}

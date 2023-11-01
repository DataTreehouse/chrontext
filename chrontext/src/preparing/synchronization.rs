use crate::timeseries_query::{Synchronizer, TimeseriesQuery};
use oxrdf::Variable;
use std::collections::HashSet;

pub fn create_identity_synchronized_queries(
    mut tsqs: Vec<TimeseriesQuery>,
) -> Vec<TimeseriesQuery> {
    let mut out_queries = vec![];
    while tsqs.len() > 1 {
        let mut queries_to_synchronize = vec![];
        let first_query = tsqs.remove(0);
        let first_query_timestamp_variables_set: HashSet<Variable> = HashSet::from_iter(
            first_query
                .get_timestamp_variables()
                .into_iter()
                .map(|x| x.variable.clone()),
        );
        let mut keep_tsqs = vec![];
        for other in tsqs.into_iter() {
            let other_query_timestamp_variables_set = HashSet::from_iter(
                other
                    .get_timestamp_variables()
                    .into_iter()
                    .map(|x| x.variable.clone()),
            );
            if !first_query_timestamp_variables_set
                .is_disjoint(&other_query_timestamp_variables_set)
            {
                queries_to_synchronize.push(Box::new(other))
            } else {
                keep_tsqs.push(other);
            }
        }
        tsqs = keep_tsqs;
        if !queries_to_synchronize.is_empty() {
            queries_to_synchronize.push(Box::new(first_query));
            out_queries.push(TimeseriesQuery::InnerSynchronized(
                queries_to_synchronize,
                vec![Synchronizer::Identity(
                    first_query_timestamp_variables_set
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
    out_queries.extend(tsqs);
    out_queries
}

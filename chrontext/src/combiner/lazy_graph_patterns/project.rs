use super::Combiner;
use crate::combiner::lazy_graph_patterns::SolutionMappings;
use crate::combiner::CombinerError;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use log::{debug, warn};
use oxrdf::Variable;
use polars::prelude::{col, Expr};
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        solution_mappings: Option<SolutionMappings>,
        static_query_map: HashMap<Context, Query>,
        prepared_time_series_queries: Option<HashMap<Context, Vec<TimeseriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing project graph pattern");
        let SolutionMappings {
            mut mappings,
            mut datatypes,
            ..
        } = self
            .lazy_graph_pattern(
                inner,
                solution_mappings,
                static_query_map,
                prepared_time_series_queries,
                &context.extension_with(PathEntry::ProjectInner),
            )
            .await?;
        let cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
        mappings = mappings.select(cols.as_slice());
        let mut new_datatypes = HashMap::new();
        for v in variables {
            let v_str = v.as_str();
            if !datatypes.contains_key(v_str) {
                warn!("Datatypes does not contain {}", v);
            } else {
                new_datatypes.insert(v_str.to_string(), datatypes.remove(v_str).unwrap());
            }
        }
        Ok(SolutionMappings::new(
            mappings,
            variables.iter().map(|x| x.as_str().to_string()).collect(),
            new_datatypes,
        ))
    }
}

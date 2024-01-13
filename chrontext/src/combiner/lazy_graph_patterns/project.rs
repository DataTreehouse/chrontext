use super::Combiner;
use crate::combiner::lazy_graph_patterns::SolutionMappings;
use crate::combiner::CombinerError;
use representation::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use async_recursion::async_recursion;
use log::{debug};
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use query_processing::graph_patterns::project;

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
        let solution_mappings = self
            .lazy_graph_pattern(
                inner,
                solution_mappings,
                static_query_map,
                prepared_time_series_queries,
                &context.extension_with(PathEntry::ProjectInner),
            )
            .await?;
        Ok(project(solution_mappings, variables)?)
    }
}

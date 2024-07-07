use super::Combiner;
use crate::combiner::lazy_graph_patterns::SolutionMappings;
use crate::combiner::CombinerError;
use async_recursion::async_recursion;
use log::debug;
use oxrdf::Variable;
use query_processing::graph_patterns::project;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        solution_mappings: Option<SolutionMappings>,
        static_query_map: HashMap<Context, Query>,
        prepared_virtualized_queries: Option<HashMap<Context, Vec<VirtualizedQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing project graph pattern");
        let solution_mappings = self
            .lazy_graph_pattern(
                inner,
                solution_mappings,
                static_query_map,
                prepared_virtualized_queries,
                &context.extension_with(PathEntry::ProjectInner),
            )
            .await?;
        Ok(project(solution_mappings, variables)?)
    }
}

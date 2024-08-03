use super::Combiner;
use crate::combiner::CombinerError;
use async_recursion::async_recursion;
use log::debug;
use polars::prelude::col;
use query_processing::graph_patterns::distinct;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_slice(
        &mut self,
        inner: &GraphPattern,
        start: &usize,
        length: &Option<usize>,
        solution_mappings: Option<SolutionMappings>,
        static_query_map: HashMap<Context, Query>,
        prepared_virtualized_queries: Option<HashMap<Context, Vec<VirtualizedQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing slice graph pattern");
        if *start > 0 {
            todo!()
        } else {
            let mut solution_mappings = self
                .lazy_graph_pattern(
                    inner,
                    solution_mappings,
                    static_query_map,
                    prepared_virtualized_queries,
                    &context.extension_with(PathEntry::SliceInner),
                )
                .await?;
            if let Some(length) = length {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .select(vec![col("*").head(Some(*length))]);
            }
            Ok(distinct(solution_mappings)?)
        }
    }
}

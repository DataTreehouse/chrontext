pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
pub(crate) mod static_subqueries;
pub(crate) mod virtualized_queries;

use representation::query_context::Context;

use crate::preparing::TimeseriesQueryPrepper;
use crate::sparql_database::{SparqlQueryError, SparqlQueryable};
use query_processing::errors::QueryProcessingError;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use thiserror::Error;
use virtualization::errors::ChrontextError;
use virtualization::{Virtualization, VirtualizedDatabase};
use virtualized_query::pushdown_setting::PushdownSetting;
use virtualized_query::{BasicVirtualizedQuery, VirtualizedResultValidationError};

#[derive(Debug, Error)]
pub enum CombinerError {
    VirtualizedDatabaseError(ChrontextError),
    StaticQueryExecutionError(SparqlQueryError),
    QueryProcessingError(#[from] QueryProcessingError),
    InconsistentDatatype(String, String, String),
    TimeseriesValidationError(VirtualizedResultValidationError),
    ResourceIsNotString(String, String),
    InconsistentResourceName(String, String, String),
}

impl Display for CombinerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CombinerError::InconsistentDatatype(s1, s2, s3) => {
                write!(
                    f,
                    "Inconsistent datatypes {} and {} for variable {}",
                    s1, s2, s3
                )
            }
            CombinerError::VirtualizedDatabaseError(vqe) => {
                write!(f, "Virtualized query error {}", vqe)
            }
            CombinerError::StaticQueryExecutionError(sqee) => {
                write!(f, "Static query execution error {}", sqee)
            }
            CombinerError::TimeseriesValidationError(v) => {
                write!(f, "Virtualized results validation error {}", v)
            }
            CombinerError::ResourceIsNotString(value_var, actual_datatype) => {
                write!(
                    f,
                    "Resource variable for context {value_var} is not of type string, actual: {actual_datatype}"
                )
            }
            CombinerError::InconsistentResourceName(value_var, r1, r2) => {
                write!(
                    f,
                    "Resource variable for context {value_var} has conflicting variables {r1} != {r2}"
                )
            }
            CombinerError::QueryProcessingError(e) => {
                write!(f, "{}", e)
            }
        }
    }
}

pub struct Combiner {
    counter: u16,
    pub sparql_database: Arc<dyn SparqlQueryable>,
    pub virtualized_database: Arc<VirtualizedDatabase>,
    prepper: TimeseriesQueryPrepper,
    pub virtualized_contexts: Vec<Context>,
}

impl Combiner {
    pub fn new<'py>(
        sparql_database: Arc<dyn SparqlQueryable>,
        pushdown_settings: HashSet<PushdownSetting>,
        virtualized_database: Arc<VirtualizedDatabase>,
        basic_virtualized_queries: Vec<BasicVirtualizedQuery>,
        rewritten_filters: HashMap<Context, Expression>,
        virtualization: Arc<Virtualization>,
    ) -> Combiner {
        let prepper = TimeseriesQueryPrepper::new(
            pushdown_settings,
            basic_virtualized_queries,
            rewritten_filters,
            virtualization,
        );
        Combiner {
            counter: 0,
            sparql_database,
            virtualized_database,
            prepper,
            virtualized_contexts: vec![],
        }
    }

    pub async fn combine_static_and_time_series_results(
        &mut self,
        mut static_query_map: HashMap<Context, Query>,
        query: &Query,
    ) -> Result<SolutionMappings, CombinerError> {
        let context = Context::new();
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = query
        {
            let solution_mappings;
            let virtualized_queries;
            if let Some(static_query) = static_query_map.remove(&context) {
                let mut new_solution_mappings =
                    self.execute_static_query(&static_query, None).await?;
                let new_virtualized_queries =
                    self.prepper.prepare(&query, &mut new_solution_mappings);
                // Combination assumes there is something to combine!
                // If there are no time series queries, we are done.
                if new_virtualized_queries.is_empty() {
                    return Ok(new_solution_mappings);
                }
                solution_mappings = Some(new_solution_mappings);
                virtualized_queries = Some(new_virtualized_queries);
            } else {
                solution_mappings = None;
                virtualized_queries = None
            }

            Ok(self
                .lazy_graph_pattern(
                    pattern,
                    solution_mappings,
                    static_query_map,
                    virtualized_queries,
                    &context,
                )
                .await?)
        } else {
            panic!("Only select queries supported")
        }
    }
}

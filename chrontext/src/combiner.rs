pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
pub(crate) mod static_subqueries;
pub(crate) mod time_series_queries;

use representation::query_context::Context;

use representation::solution_mapping::SolutionMappings;
use crate::preparing::TimeseriesQueryPrepper;
use crate::pushdown_setting::PushdownSetting;
use crate::sparql_database::SparqlQueryable;
use crate::timeseries_database::TimeseriesQueryable;
use crate::timeseries_query::{BasicTimeseriesQuery, TimeseriesValidationError};
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use query_processing::errors::QueryProcessingError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CombinerError {
    TimeseriesQueryError(Box<dyn Error>),
    StaticQueryExecutionError(Box<dyn Error>),
    QueryProcessingError(#[from] QueryProcessingError),
    InconsistentDatatype(String, String, String),
    TimeseriesValidationError(TimeseriesValidationError),
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
            CombinerError::TimeseriesQueryError(tsqe) => {
                write!(f, "Time series query error {}", tsqe)
            }
            CombinerError::StaticQueryExecutionError(sqee) => {
                write!(f, "Static query execution error {}", sqee)
            }
            CombinerError::TimeseriesValidationError(v) => {
                write!(f, "Time series validation error {}", v)
            }
            CombinerError::ResourceIsNotString(value_var, actual_datatype) => {
                write!(
                    f,
                    "Resource variable for value variable {value_var} is not of type string, actual: {actual_datatype}"
                )
            }
            CombinerError::InconsistentResourceName(value_var, r1, r2) => {
                write!(
                    f,
                    "Resource variable for value variable {value_var} has conflicting values {r1} != {r2}"
                )
            }
            CombinerError::QueryProcessingError(e) => {
                write!(
                    f, "{}", e
                )
            }
        }
    }
}

pub struct Combiner {
    counter: u16,
    pub sparql_database: Box<dyn SparqlQueryable>,
    pub time_series_database: Box<dyn TimeseriesQueryable>,
    prepper: TimeseriesQueryPrepper,
}

impl Combiner {
    pub fn new(
        sparql_database: Box<dyn SparqlQueryable>,
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeseriesQueryable>,
        basic_time_series_queries: Vec<BasicTimeseriesQuery>,
        rewritten_filters: HashMap<Context, Expression>,
    ) -> Combiner {
        let prepper = TimeseriesQueryPrepper::new(
            pushdown_settings,
            basic_time_series_queries,
            rewritten_filters,
        );
        Combiner {
            counter: 0,
            sparql_database,
            time_series_database,
            prepper,
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
            let time_series_queries;
            if let Some(static_query) = static_query_map.remove(&context) {
                let mut new_solution_mappings =
                    self.execute_static_query(&static_query, None).await?;
                let new_time_series_queries =
                    self.prepper.prepare(&query, &mut new_solution_mappings);
                // Combination assumes there is something to combine!
                // If there are no time series queries, we are done.
                if new_time_series_queries.is_empty() {
                    return Ok(new_solution_mappings)
                }
                solution_mappings = Some(new_solution_mappings);
                time_series_queries = Some(new_time_series_queries);
            } else {
                solution_mappings = None;
                time_series_queries = None
            }

            Ok(self
                .lazy_graph_pattern(
                    pattern,
                    solution_mappings,
                    static_query_map,
                    time_series_queries,
                    &context,
                )
                .await?)
        } else {
            panic!("Only select queries supported")
        }
    }
}

pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
pub mod solution_mapping;
pub(crate) mod static_subqueries;
pub(crate) mod time_series_queries;

use crate::query_context::Context;

use crate::combiner::solution_mapping::SolutionMappings;
use crate::preparing::TimeSeriesQueryPrepper;
use crate::pushdown_setting::PushdownSetting;
use crate::static_sparql::QueryExecutionError;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesValidationError};
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum CombinerError {
    TimeSeriesQueryError(Box<dyn Error>),
    StaticQueryExecutionError(QueryExecutionError),
    InconsistentDatatype(String, String, String),
    TimeSeriesValidationError(TimeSeriesValidationError),
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
            CombinerError::TimeSeriesQueryError(tsqe) => {
                write!(f, "Time series query error {}", tsqe)
            }
            CombinerError::StaticQueryExecutionError(sqee) => {
                write!(f, "Static query execution error {}", sqee)
            }
            CombinerError::TimeSeriesValidationError(v) => {
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
        }
    }
}

impl Error for CombinerError {}

pub struct Combiner {
    counter: u16,
    endpoint: String,
    pub time_series_database: Box<dyn TimeSeriesQueryable>,
    prepper: TimeSeriesQueryPrepper,
}

impl Combiner {
    pub fn new(
        endpoint: String,
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeSeriesQueryable>,
        basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
        rewritten_filters: HashMap<Context, Expression>,
    ) -> Combiner {
        let prepper = TimeSeriesQueryPrepper::new(
            pushdown_settings,
            basic_time_series_queries,
            rewritten_filters,
        );
        Combiner {
            counter: 0,
            endpoint,
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

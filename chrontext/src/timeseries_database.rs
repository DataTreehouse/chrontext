pub mod arrow_flight_sql_database;
pub mod opcua_history_read;
pub mod simple_in_memory_timeseries;
pub mod timeseries_sql_rewrite;

use crate::timeseries_query::TimeSeriesQuery;
use async_trait::async_trait;
use polars::frame::DataFrame;
use std::error::Error;

#[async_trait]
pub trait TimeSeriesQueryable: Send {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>>;
    fn allow_compound_timeseries_queries(&self) -> bool;
}

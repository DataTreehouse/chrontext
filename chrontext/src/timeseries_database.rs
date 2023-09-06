pub mod arrow_flight_sql_database;
pub mod bigquery_database;
pub mod opcua_history_read;
pub mod simple_in_memory_timeseries;
pub mod timeseries_sql_rewrite;

use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeSeriesQueryToSQLError, TimeSeriesQueryToSQLTransformer, TimeSeriesTable,
};
use crate::timeseries_query::TimeSeriesQuery;
use async_trait::async_trait;
use log::debug;
use polars::frame::DataFrame;
use sea_query::QueryBuilder;
use std::error::Error;

#[async_trait]
pub trait TimeSeriesQueryable: Send {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>>;
    fn allow_compound_timeseries_queries(&self) -> bool;
}

pub trait TimeSeriesSQLQueryable {
    fn get_sql_string<T: QueryBuilder>(
        &self,
        tsq: &TimeSeriesQuery,
        query_builder: T,
    ) -> Result<String, TimeSeriesQueryToSQLError> {
        let query_string;
        {
            let transformer = TimeSeriesQueryToSQLTransformer::new(&self.get_time_series_tables());
            let (query, _) = transformer.create_query(tsq, false)?;
            query_string = query.to_string(query_builder);
            debug!("SQL: {}", query_string);
        }
        Ok(query_string)
    }

    fn get_time_series_tables(&self) -> &Vec<TimeSeriesTable>;
}

pub mod timeseries_bigquery_database;
pub mod timeseries_in_memory_database;
pub mod timeseries_opcua_database;
pub mod timeseries_sql_rewrite;

use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeseriesQueryToSQLError, TimeseriesQueryToSQLTransformer, TimeseriesTable,
};
use crate::timeseries_query::TimeseriesQuery;
use async_trait::async_trait;
use log::debug;
use polars::frame::DataFrame;
use sea_query::{BigQueryQueryBuilder};
use std::error::Error;
use representation::solution_mapping::SolutionMappings;

#[async_trait]
pub trait TimeseriesQueryable: Send {
    fn get_database_type(&self) -> DatabaseType;

    async fn execute(&mut self, tsq: &TimeseriesQuery) -> Result<SolutionMappings, Box<dyn Error>>;
    fn allow_compound_timeseries_queries(&self) -> bool;
}

#[derive(Clone)]
pub enum DatabaseType {
    BigQuery,
    InMemory,
    OPCUA,
}

pub trait TimeseriesSQLQueryable {
    fn get_sql_string(
        &self,
        tsq: &TimeseriesQuery,
        database_type: DatabaseType,
    ) -> Result<String, TimeseriesQueryToSQLError> {
        let query_string;
        {
            let transformer = TimeseriesQueryToSQLTransformer::new(
                &self.get_time_series_tables(),
                database_type.clone(),
            );
            let (query, _) = transformer.create_query(tsq, false)?;
            query_string = match database_type {
                DatabaseType::BigQuery => query.to_string(BigQueryQueryBuilder),
                _ => {
                    panic!("Should never happen!")
                }
            };

            debug!("SQL: {}", query_string);
        }
        Ok(query_string)
    }

    fn get_time_series_tables(&self) -> &Vec<TimeseriesTable>;
}

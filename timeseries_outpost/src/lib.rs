pub mod timeseries_bigquery_database;
pub mod timeseries_opcua_database;
pub mod timeseries_sql_rewrite;

use async_trait::async_trait;
use log::debug;
use polars::prelude::{DataFrame, DataType};
use representation::polars_to_sparql::polars_type_to_literal_type;
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;
use sea_query::BigQueryQueryBuilder;
use std::collections::HashMap;
use std::error::Error;
use timeseries_query::{TimeseriesQuery, TimeseriesTable};
use timeseries_sql_rewrite::{TimeseriesQueryToSQLError, TimeseriesQueryToSQLTransformer};

#[async_trait]
pub trait TimeseriesQueryable: Send + Sync {
    fn get_database_type(&self) -> DatabaseType;

    async fn execute(&self, tsq: &TimeseriesQuery) -> Result<SolutionMappings, Box<dyn Error>>;
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

pub fn get_datatype_map(df: &DataFrame) -> HashMap<String, RDFNodeType> {
    let mut map = HashMap::new();
    for c in df.columns(df.get_column_names()).unwrap() {
        let dtype = c.dtype();
        if let &DataType::Null = dtype {
            map.insert(c.name().to_string(), RDFNodeType::None);
        } else {
            map.insert(
                c.name().to_string(),
                polars_type_to_literal_type(dtype, None).unwrap().to_owned(),
            );
        }
    }
    map
}

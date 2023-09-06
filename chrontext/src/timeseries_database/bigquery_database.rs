use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeSeriesQueryToSQLError, TimeSeriesTable,
};
use crate::timeseries_database::{TimeSeriesQueryable, TimeSeriesSQLQueryable};
use crate::timeseries_query::TimeSeriesQuery;
use async_trait::async_trait;
use connectorx::prelude::*;
use polars::prelude::PolarsError;
use polars_core::error::ArrowError;
use polars_core::prelude::{DataFrame, Series};
use sea_query::PostgresQueryBuilder;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::thread;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;

#[derive(Error, Debug)]
pub enum BigQueryError {
    TonicStatus(#[from] Status),
    TransportError(#[from] tonic::transport::Error),
    TranslationError(#[from] TimeSeriesQueryToSQLError),
    ArrowError(#[from] ArrowError),
    PolarsError(#[from] PolarsError),
}

impl Display for BigQueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BigQueryError::TonicStatus(status) => {
                write!(f, "Error with status: {}", status)
            }
            BigQueryError::TransportError(err) => {
                write!(f, "Error during transport: {}", err)
            }
            BigQueryError::TranslationError(s) => {
                write!(f, "Error during query translation: {}", s)
            }
            BigQueryError::ArrowError(err) => {
                write!(f, "Problem deserializing arrow: {}", err)
            }
            BigQueryError::PolarsError(err) => {
                write!(f, "Problem creating dataframe from arrow: {:?}", err)
            }
        }
    }
}
pub struct BigQueryDatabase {
    gcp_sa_key: String,
    time_series_tables: Vec<TimeSeriesTable>,
}

impl BigQueryDatabase {
    pub fn new(gcp_sa_key: String, time_series_tables: Vec<TimeSeriesTable>) -> BigQueryDatabase {
        BigQueryDatabase {
            gcp_sa_key,
            time_series_tables,
        }
    }
}

#[async_trait]
impl TimeSeriesQueryable for BigQueryDatabase {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        let query_string = self.get_sql_string(tsq, PostgresQueryBuilder)?;
        let key = self.gcp_sa_key.clone();
        // Using a thread here since we do not want nested runtimes in the same thread
        let (chunks, schema) =
            thread::spawn(move || {
                let source = BigQuerySource::new(Arc::new(Runtime::new().unwrap()), &key).unwrap();
                let queries = [CXQuery::naked(query_string)];
                let mut destination = Arrow2Destination::new();
                let dispatcher = Dispatcher::<
                    BigQuerySource,
                    Arrow2Destination,
                    BigQueryArrow2Transport,
                >::new(source, &mut destination, &queries, None);
                dispatcher.run().unwrap();
                let (chunks, schema) = destination.arrow().unwrap();
                return (chunks, schema);
            })
            .join()
            .unwrap();
        let mut series_vec = vec![];
        for (ch, field) in chunks.into_iter().zip(schema.fields.iter()) {
            let mut array_refs = vec![];
            for arr in ch.into_arrays() {
                array_refs.push(arr)
            }
            let ser = Series::try_from((field.name.as_str(), array_refs)).unwrap();
            series_vec.push(ser);
        }
        let df = DataFrame::new(series_vec).unwrap();
        Ok(df)
    }

    fn allow_compound_timeseries_queries(&self) -> bool {
        true
    }
}

impl TimeSeriesSQLQueryable for BigQueryDatabase {
    fn get_time_series_tables(&self) -> &Vec<TimeSeriesTable> {
        &self.time_series_tables
    }
}

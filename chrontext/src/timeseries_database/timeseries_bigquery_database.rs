use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeseriesQueryToSQLError, TimeseriesTable,
};
use crate::timeseries_database::{DatabaseType, get_datatype_map, TimeseriesQueryable, TimeseriesSQLQueryable};
use crate::timeseries_query::TimeseriesQuery;
use async_trait::async_trait;
use bigquery_polars::{BigQueryExecutor, Client};
use polars::prelude::PolarsError;
use polars::prelude::IntoLazy;
use reqwest::Url;
use std::error::Error;
use std::fmt::{Display, Formatter};
use representation::solution_mapping::SolutionMappings;
use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum BigQueryError {
    TonicStatus(#[from] Status),
    TransportError(#[from] tonic::transport::Error),
    TranslationError(#[from] TimeseriesQueryToSQLError),
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
            BigQueryError::PolarsError(err) => {
                write!(f, "Problem creating dataframe from arrow: {:?}", err)
            }
        }
    }
}
pub struct TimeseriesBigQueryDatabase {
    gcp_sa_key: String,
    time_series_tables: Vec<TimeseriesTable>,
}

impl TimeseriesBigQueryDatabase {
    pub fn new(
        gcp_sa_key: String,
        time_series_tables: Vec<TimeseriesTable>,
    ) -> TimeseriesBigQueryDatabase {
        TimeseriesBigQueryDatabase {
            gcp_sa_key,
            time_series_tables,
        }
    }
}

#[async_trait]
impl TimeseriesQueryable for TimeseriesBigQueryDatabase {
    fn get_database_type(&self) -> DatabaseType {
        DatabaseType::BigQuery
    }
    async fn execute(&mut self, tsq: &TimeseriesQuery) -> Result<SolutionMappings, Box<dyn Error>> {
        let query_string = self.get_sql_string(tsq, DatabaseType::BigQuery)?;

        // The following code is based on https://github.com/DataTreehouse/connector-x/blob/main/connectorx/src/sources/bigquery/mod.rs
        // Last modified in commit: 8134d42
        // It has been simplified and made async
        // Connector-x has the following license:
        // MIT License
        //
        // Copyright (c) 2021 SFU Database Group
        //
        // Permission is hereby granted, free of charge, to any person obtaining a copy
        // of this software and associated documentation files (the "Software"), to deal
        // in the Software without restriction, including without limitation the rights
        // to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        // copies of the Software, and to permit persons to whom the Software is
        // furnished to do so, subject to the following conditions:
        //
        // The above copyright notice and this permission notice shall be included in all
        // copies or substantial portions of the Software.
        //
        // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        // SOFTWARE.

        let url = Url::parse(&self.gcp_sa_key)?;
        let sa_key_path = url.path();
        let client = Client::from_service_account_key_file(sa_key_path).await?;

        let auth_data = std::fs::read_to_string(sa_key_path)?;
        let auth_json: serde_json::Value = serde_json::from_str(&auth_data)?;
        let project_id = auth_json
            .get("project_id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        //End copied code.

        let ex = BigQueryExecutor::new(client, project_id, query_string);
        let lf = ex.execute_query().await?;
        let df = lf.collect().unwrap();
        let datatypes = get_datatype_map(&df);
        Ok(SolutionMappings::new(df.lazy(), datatypes))
    }

    fn allow_compound_timeseries_queries(&self) -> bool {
        true
    }
}

impl TimeseriesSQLQueryable for TimeseriesBigQueryDatabase {
    fn get_time_series_tables(&self) -> &Vec<TimeseriesTable> {
        &self.time_series_tables
    }
}

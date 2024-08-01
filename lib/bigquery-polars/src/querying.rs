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

use crate::errors::BigQueryExecutorError;
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::job::JobApi;
use gcp_bigquery_client::model::field_type::FieldType;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::get_query_results_response::GetQueryResultsResponse;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_cell::TableCell;
use gcp_bigquery_client::Client;
use polars::prelude::{concat, AnyValue, DataFrame, DataType, IntoLazy, LazyFrame, TimeUnit};
use polars::series::Series;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::time::Duration;
use tokio::time::sleep;

pub struct BigQueryExecutor {
    client: Client,
    project_id: String,
    query: String,
}

impl BigQueryExecutor {
    pub fn new(client: Client, project_id: String, query: String) -> BigQueryExecutor {
        BigQueryExecutor {
            client,
            project_id,
            query,
        }
    }

    pub async fn execute_query(&self) -> Result<LazyFrame, BigQueryExecutorError> {
        let job = self.client.job();
        let result_set = job
            .query(
                self.project_id.as_str(),
                QueryRequest::new(self.query.as_str()),
            )
            .await
            .map_err(map_bqerr)?;
        let job_info = result_set
            .query_response()
            .job_reference
            .as_ref()
            .ok_or_else(|| return BigQueryExecutorError::JobReferenceMissingError)?;

        let job_id = job_info
            .job_id
            .as_ref()
            .ok_or_else(|| return BigQueryExecutorError::JobIdNoneError)?
            .clone();
        let location = &job_info.location;

        let mut rs = loop {
            let rs = self
                .get_query_results(&job, &job_id, location.clone(), None)
                .await?;

            if let Some(complete) = &rs.job_complete {
                if *complete {
                    break rs;
                }
            } else if let Some(_) = &rs.schema {
                break rs;
            }
            sleep(Duration::from_millis(500)).await;
        };

        if rs.schema.is_none() {
            return Err(BigQueryExecutorError::SchemaMissing);
        }
        let schema = rs.schema.unwrap();

        let mut names = vec![];
        let mut types = vec![];

        if let Some(fields) = schema.fields {
            for f in fields {
                names.push(f.name.clone());
                types.push(f.r#type.clone());
            }
        } else {
            todo!();
        }
        let mut rows_processed = 0;
        let mut all_lfs = vec![];
        let some_utc = Some("UTC".to_string());
        loop {
            if let Some(rows) = &rs.rows {
                let any_value_vecs: Vec<_> = types
                    .par_iter()
                    .enumerate()
                    .map(|(i, field_type)| {
                        let mut any_values = vec![];
                        for r in rows {
                            if let Some(columns) = &r.columns {
                                any_values.push(table_cell_to_any(
                                    columns.get(i).unwrap(),
                                    field_type,
                                    &some_utc,
                                ));
                            }
                        }
                        return any_values;
                    })
                    .collect();
                rows_processed += rows.len();

                let series_vec: Vec<_> = any_value_vecs
                    .into_par_iter()
                    .zip(names.par_iter())
                    .map(|(any_value_vec, name)| {
                        Series::from_any_values(name, any_value_vec.as_slice(), false).unwrap()
                    })
                    .collect();
                all_lfs.push(DataFrame::new(series_vec).unwrap().lazy())
            }
            if let Some(total_rows) = rs.total_rows {
                let total_rows = total_rows.parse::<usize>().unwrap();
                if rows_processed == total_rows {
                    break;
                }
            }
            let page_token = rs.page_token.clone();
            rs = self
                .get_query_results(&job, &job_id, location.clone(), page_token)
                .await?;
        }
        if !all_lfs.is_empty() {
            Ok(concat(all_lfs, Default::default()).unwrap())
        } else {
            let mut series = vec![];
            for n in &names {
                series.push(Series::new_empty(n, &DataType::Null))
            }
            Ok(DataFrame::new(series).unwrap().lazy())
        }
    }

    async fn get_query_results(
        &self,
        job: &JobApi,
        job_id: &str,
        location: Option<String>,
        page_token: Option<String>,
    ) -> Result<GetQueryResultsResponse, BigQueryExecutorError> {
        let params = GetQueryResultsParameters {
            format_options: None,
            location,
            max_results: None,
            page_token,
            start_index: None,
            timeout_ms: None,
        };
        Ok(job
            .get_query_results(self.project_id.as_str(), job_id, params.clone())
            .await
            .map_err(map_bqerr)?)
    }
}

fn table_cell_to_any<'a>(
    table_cell: &'a TableCell,
    field_type: &FieldType,
    some_utc: &'a Option<String>,
) -> AnyValue<'a> {
    if table_cell.value.is_none() {
        return AnyValue::Null;
    }
    let value_as_ref = table_cell.value.as_ref().unwrap();
    match field_type {
        FieldType::String | FieldType::Bytes => AnyValue::String(value_as_ref.as_str().unwrap()),
        FieldType::Integer | FieldType::Int64 => {
            AnyValue::Int64(value_as_ref.as_str().unwrap().parse::<i64>().unwrap())
        }
        FieldType::Float | FieldType::Float64 => {
            AnyValue::Float64(value_as_ref.as_str().unwrap().parse::<f64>().unwrap())
        }
        FieldType::Numeric => {
            todo!()
        }
        FieldType::Bignumeric => {
            todo!()
        }
        FieldType::Boolean | FieldType::Bool => {
            AnyValue::Boolean(value_as_ref.as_str().unwrap().parse::<bool>().unwrap())
        }
        FieldType::Timestamp => {
            let ts_str = value_as_ref.as_str().unwrap();
            let timestamp_ns = (ts_str.parse::<f64>().unwrap() * (1e9f64)) as i64;
            AnyValue::Datetime(timestamp_ns, TimeUnit::Nanoseconds, some_utc)
        }
        FieldType::Date => {
            todo!()
        }
        FieldType::Time => {
            todo!()
        }
        FieldType::Datetime => {
            todo!()
        }
        FieldType::Record => {
            todo!()
        }
        FieldType::Struct => {
            todo!()
        }
        FieldType::Geography => {
            todo!()
        }
        FieldType::Json => {
            todo!()
        }
    }
}

fn map_bqerr(e: BQError) -> BigQueryExecutorError {
    BigQueryExecutorError::ClientError(e)
}

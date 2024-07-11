use crate::errors::VirtualizedDatabaseError;
use crate::python::translate_sql;
use crate::{get_datatype_map, Virtualization};
use bigquery_polars::{BigQueryExecutor, Client};
use pyo3::{Py, PyAny};
use representation::solution_mapping::EagerSolutionMappings;
use reqwest::Url;
use std::collections::{HashMap, HashSet};
use virtualized_query::pushdown_setting::{all_pushdowns, PushdownSetting};
use virtualized_query::VirtualizedQuery;

pub struct VirtualizedBigQueryDatabase {
    gcp_sa_key: String,
    resource_sql_map: HashMap<String, Py<PyAny>>,
}

impl VirtualizedBigQueryDatabase {
    pub fn new(
        gcp_sa_key: String,
        resource_sql_map: HashMap<String, Py<PyAny>>,
    ) -> VirtualizedBigQueryDatabase {
        VirtualizedBigQueryDatabase {
            gcp_sa_key,
            resource_sql_map,
        }
    }
}

impl VirtualizedBigQueryDatabase {
    pub fn pushdown_settings() -> HashSet<PushdownSetting> {
        all_pushdowns()
    }

    pub async fn query(
        &self,
        vq: &VirtualizedQuery,
    ) -> Result<EagerSolutionMappings, VirtualizedDatabaseError> {
        let query_string = translate_sql(vq, &self.resource_sql_map)?;
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
        Ok(EagerSolutionMappings::new(df, datatypes))
    }

    fn allow_compound_timeseries_queries(&self) -> bool {
        true
    }
}

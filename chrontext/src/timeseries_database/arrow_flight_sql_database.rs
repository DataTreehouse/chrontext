//Snippets from and inspired by: https://github.com/timvw/arrow-flight-sql-client/blob/main/src/client.rs

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::timeseries_database::{DatabaseType, TimeseriesQueryable, TimeseriesSQLQueryable};
use crate::timeseries_query::TimeseriesQuery;
use arrow2::io::flight as flight2;
use arrow_format::flight::data::{FlightDescriptor, FlightInfo, HandshakeRequest};
use async_trait::async_trait;

use polars::frame::DataFrame;
use polars_core::utils::accumulate_dataframes_vertical;

use crate::timeseries_database::timeseries_sql_rewrite::{
    TimeseriesQueryToSQLError, TimeseriesTable,
};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use arrow_format::ipc::planus::ReadAsRoot;
use arrow_format::ipc::MessageHeaderRef;
use base64::Engine;
use log::{debug, warn};
use polars_core::error::ArrowError;
use polars_core::prelude::PolarsError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Instant;
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{IntoRequest, Request, Response, Status};

#[derive(Error, Debug)]
pub enum ArrowFlightSQLError {
    TonicStatus(#[from] Status),
    TransportError(#[from] tonic::transport::Error),
    TranslationError(#[from] TimeseriesQueryToSQLError),
    ArrowError(#[from] ArrowError),
    PolarsError(#[from] PolarsError),
}

impl Display for ArrowFlightSQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowFlightSQLError::TonicStatus(status) => {
                write!(f, "Error with status: {}", status)
            }
            ArrowFlightSQLError::TransportError(err) => {
                write!(f, "Error during transport: {}", err)
            }
            ArrowFlightSQLError::TranslationError(s) => {
                write!(f, "Error during query translation: {}", s)
            }
            ArrowFlightSQLError::ArrowError(err) => {
                write!(f, "Problem deserializing arrow: {}", err)
            }
            ArrowFlightSQLError::PolarsError(err) => {
                write!(f, "Problem creating dataframe from arrow: {:?}", err)
            }
        }
    }
}

pub struct ArrowFlightSQLDatabase {
    endpoint: String,
    username: String,
    password: String,
    token: Option<String>,
    cookies: Option<Vec<String>>,
    time_series_tables: Vec<TimeseriesTable>,
}

impl ArrowFlightSQLDatabase {
    pub async fn new(
        endpoint: &str,
        username: &str,
        password: &str,
        time_series_tables: Vec<TimeseriesTable>,
    ) -> Result<ArrowFlightSQLDatabase, ArrowFlightSQLError> {
        let mut db = ArrowFlightSQLDatabase {
            endpoint: endpoint.into(),
            username: username.into(),
            password: password.into(),
            token: None,
            cookies: None,
            time_series_tables,
        };
        db.init().await?;
        Ok(db)
    }

    async fn init(&mut self) -> Result<(), ArrowFlightSQLError> {
        let token = self.get_token().await?;
        self.token = Some(token);
        Ok(())
    }

    async fn get_token(&self) -> Result<String, ArrowFlightSQLError> {
        let channel = self.get_channel().await?;
        let token = authenticate(channel, &self.username, &self.password).await?;
        Ok(token)
    }

    async fn get_channel(&self) -> Result<Channel, ArrowFlightSQLError> {
        let channel = tonic::transport::Endpoint::new(self.endpoint.clone())?
            .connect()
            .await?;
        Ok(channel)
    }

    pub async fn execute_sql_query(
        &mut self,
        query: String,
    ) -> Result<DataFrame, ArrowFlightSQLError> {
        let instant = Instant::now();
        let channel = self.get_channel().await?;
        let elapsed = instant.elapsed();
        debug!("Connecting took {} seconds", elapsed.as_secs_f32());
        let mut dfs = vec![];
        let mut request = FlightDescriptor {
            r#type: 2, //CMD
            cmd: query.into_bytes(),
            //TODO: For some reason, encoding the CommandStatementQuery-struct
            // gives me a parsing error with an extra character at the start of the decoded query.
            path: vec![], // Should be empty when CMD
        }
        .into_request();
        add_auth_header(&mut request, self.token.as_ref().unwrap());

        let mut client = FlightServiceClient::new(channel);
        let response = client.get_flight_info(request).await?;
        //We expect some new cookies here since we did not add cookies to the get flight info.
        //See: https://docs.dremio.com/software/developing-client-apps/arrow-flight/
        self.find_set_cookies(&response);
        debug!("Got flight info response");
        let mut schema_opt = None;
        let mut ipc_schema_opt = None;
        for endpoint in response.into_inner().endpoint {
            if let Some(ticket) = endpoint.ticket.clone() {
                let mut ticket = ticket.into_request();
                add_auth_header(&mut ticket, self.token.as_ref().unwrap());
                add_cookies(&mut ticket, self.cookies.as_ref().unwrap());
                let stream = client
                    .do_get(ticket)
                    .await
                    .map_err(ArrowFlightSQLError::from)?;
                let mut streaming_flight_data = stream.into_inner();
                while let Some(flight_data_result) = streaming_flight_data.next().await {
                    if let Ok(flight_data) = flight_data_result {
                        let message =
                            arrow_format::ipc::MessageRef::read_as_root(&flight_data.data_header)
                                .unwrap();
                        let header = message.header().unwrap().unwrap();
                        match header {
                            MessageHeaderRef::Schema(_) => {
                                if schema_opt.is_some() || ipc_schema_opt.is_some() {
                                    warn!("Received multiple schema messages, keeping last");
                                }
                                let (schema, ipc_schema) =
                                    flight2::deserialize_schemas(&flight_data.data_header)
                                        .expect("Schema deserialization problem");
                                schema_opt = Some(schema);
                                ipc_schema_opt = Some(ipc_schema);
                            }
                            MessageHeaderRef::DictionaryBatch(_) => {
                                unimplemented!("Dictionary batch not implemented")
                            }
                            MessageHeaderRef::RecordBatch(_) => {
                                let chunk = flight2::deserialize_batch(
                                    &flight_data,
                                    schema_opt.as_ref().unwrap().fields.as_slice(),
                                    &ipc_schema_opt.as_ref().unwrap(),
                                    &Default::default(),
                                )
                                .map_err(ArrowFlightSQLError::from)?;

                                let df = DataFrame::try_from((
                                    chunk,
                                    schema_opt.as_ref().unwrap().fields.as_slice(),
                                ))
                                .map_err(ArrowFlightSQLError::from)?;
                                dfs.push(df);
                            }
                            MessageHeaderRef::Tensor(_) => {
                                unimplemented!("Tensor message not implemented");
                            }
                            MessageHeaderRef::SparseTensor(_) => {
                                unimplemented!("Sparse tensor message not implemented");
                            }
                        }
                    }
                }
            }
        }
        Ok(accumulate_dataframes_vertical(dfs).expect("Problem stacking dataframes"))
    }
    fn find_set_cookies(&mut self, response: &Response<FlightInfo>) {
        let mut cookies: Vec<String> = response
            .metadata()
            .get_all("Set-Cookie")
            .iter()
            .map(|x| x.to_str().unwrap().to_string())
            .collect();

        cookies = cookies
            .into_iter()
            .map(|x| x.split(";").next().unwrap().to_string())
            .collect();
        self.cookies = Some(cookies);
    }
}

#[async_trait]
impl TimeseriesQueryable for ArrowFlightSQLDatabase {
    async fn execute(&mut self, tsq: &TimeseriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        let query_string = self.get_sql_string(tsq, DatabaseType::Dremio)?;
        Ok(self.execute_sql_query(query_string).await?)
    }

    fn allow_compound_timeseries_queries(&self) -> bool {
        true
    }
}

impl TimeseriesSQLQueryable for ArrowFlightSQLDatabase {
    fn get_time_series_tables(&self) -> &Vec<TimeseriesTable> {
        &self.time_series_tables
    }
}

//Adapted from: https://github.com/apache/arrow-rs/blob/master/integration-testing/src/flight_client_scenarios/auth_basic_proto.rs
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

async fn authenticate(
    conn: Channel,
    username: &str,
    password: &str,
) -> Result<String, ArrowFlightSQLError> {
    let handshake_request = HandshakeRequest {
        protocol_version: 2,
        payload: vec![],
    };
    let user_pass_string = format!("{}:{}", username, password);
    let user_pass_bytes = user_pass_string.as_bytes();
    let standard_engine = base64::prelude::BASE64_STANDARD_NO_PAD;
    let base64_bytes = standard_engine.encode(user_pass_bytes);
    let basic_auth = format!("Basic {}", base64_bytes);
    let mut client = FlightServiceClient::with_interceptor(conn, |mut req: Request<()>| {
        req.metadata_mut()
            .insert("authorization", basic_auth.parse().unwrap());
        Ok(req)
    });

    let handshake_request_streaming = tokio_stream::iter(vec![handshake_request]);

    let rx = client.handshake(handshake_request_streaming).await?;
    let bearer_token = rx
        .metadata()
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    Ok(bearer_token)
}

fn add_auth_header<T>(request: &mut Request<T>, bearer_token: &str) {
    let token_value: MetadataValue<_> = bearer_token.parse().unwrap();
    request.metadata_mut().insert("authorization", token_value);
}

fn add_cookies<T>(request: &mut Request<T>, cookies: &Vec<String>) {
    let cookies_string = cookies.join("; ");
    let cookie_value: MetadataValue<_> = cookies_string.parse().unwrap();
    debug!("Using cookies: {}", cookies_string);
    request.metadata_mut().insert("cookie", cookie_value);
}

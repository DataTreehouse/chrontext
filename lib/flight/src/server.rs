// Based on: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples/server.rs @ e7ce4bb

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

use std::net::AddrParseError;
use futures::stream::BoxStream;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Code, Request, Response, Status, Streaming};

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult,
    SchemaResult, Ticket,
};
use bincode::deserialize;
use bincode::serialize;
use chrontext::engine::Engine;
use futures::{stream, Stream};
use polars::io::SerWriter;
use polars::prelude::{IpcCompression, IpcStreamWriter};
use thiserror::*;
use tonic::transport::Server;
use log::info;

#[derive(Error, Debug)]
pub enum ChrontextFlightServerError {
    #[error(transparent)]
    TonicTransportError(tonic::transport::Error),
    #[error(transparent)]
    AddrParseError(AddrParseError),
}

#[derive(Clone)]
pub struct ChrontextFlightService {
    engine: Option<Arc<Engine>>,
}

#[tonic::async_trait]
impl FlightService for ChrontextFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        info!("Got do_get request: {:?}", request);
        let query_string: String = deserialize(request.get_ref().ticket.as_ref()).unwrap();
        let (mut df, map, _context) = self
            .engine.as_ref().unwrap()
            .clone()
            .query(&query_string)
            .await
            .map_err(|x| Status::new(Code::Internal, x.to_string()))?;
        let map_bytes = serialize(&map).unwrap();

        let mut df_bytes = vec![];
        let mut writer = IpcStreamWriter::new(&mut df_bytes)
            .with_compression(Some(IpcCompression::LZ4))
            .with_pl_flavor(true);
        writer.finish(&mut df).unwrap();

        let flight_data = FlightData::new()
            .with_app_metadata(map_bytes)
            .with_data_body(df_bytes);
        // Adapted from: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples/flight_sql_server.rs @ 7781bc2
        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(vec![Ok(flight_data)]));
        let resp = Response::new(stream);
        info!("Finished processing request {:?}", request);
        Ok(resp)
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

pub struct ChrontextFlightServer {
    chrontext_flight_service: ChrontextFlightService,
}

impl ChrontextFlightServer {
    pub fn new(engine: Option<Arc<Engine>>) -> Self {
        Self {
            chrontext_flight_service: ChrontextFlightService { engine },
        }
    }

    pub async fn serve(self, addr: &str) -> Result<(), ChrontextFlightServerError> {
        info!("Starting server on {}", addr);
        let addr = addr.parse().map_err(|x|ChrontextFlightServerError::AddrParseError(x))?;
        let svc = FlightServiceServer::new(self.chrontext_flight_service.clone());

        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .map_err(|x| ChrontextFlightServerError::TonicTransportError(x))?;
        info!("Shutdown server");
        Ok(())
    }
}
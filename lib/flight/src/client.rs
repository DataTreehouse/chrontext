use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use bincode::{deserialize, serialize};
use futures::TryStreamExt;
use log::info;
use polars::io::SerReader;
use polars::prelude::{IntoLazy, IpcStreamReader, PolarsError};
use query_processing::errors::QueryProcessingError;
use query_processing::graph_patterns::union;
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::str::FromStr;
use thiserror::*;
use tonic::metadata::MetadataKey;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

#[derive(Error, Debug)]
pub enum ChrontextFlightClientError {
    #[error("Cannot create endpoint `{0}`")]
    IpcError(String),
    #[error(transparent)]
    QueryExecutionError(Status),
    #[error(transparent)]
    TypesDeserializationError(bincode::Error),
    #[error(transparent)]
    PolarsDeserializationError(PolarsError),
    #[error(transparent)]
    UnionError(QueryProcessingError),
    #[error(transparent)]
    ConnectError(tonic::transport::Error),
}

#[derive(Clone)]
pub struct ChrontextFlightClient {
    client: Option<FlightServiceClient<Channel>>,
    uri: String,
}

impl ChrontextFlightClient {
    pub fn new(uri: &str) -> ChrontextFlightClient {
        Self {
            client: None,
            uri: uri.to_string(),
        }
    }

    pub async fn query(
        &mut self,
        query: &str,
        metadata: &HashMap<String, SecretString>,
    ) -> Result<SolutionMappings, ChrontextFlightClientError> {
        let endpoint = Endpoint::from_str(&self.uri)
            .map_err(|e| ChrontextFlightClientError::ConnectError(e))?;
        let mut client = if let Some(client) = self.client.take() {
            client
        } else {
            FlightServiceClient::connect(endpoint)
                .await
                .map_err(|x| ChrontextFlightClientError::ConnectError(x))?
        };
        info!("Building request");
        let mut request = Request::new(Ticket::new(serialize(query).unwrap()));
        for (k, v) in metadata {
            request.metadata_mut().insert(
                MetadataKey::from_str(k).unwrap().to_owned(),
                v.expose_secret().parse().unwrap(),
            );
        }
        info!("Sending request");
        let mut flight_data = client
            .do_get(request)
            .await
            .map_err(|x| ChrontextFlightClientError::QueryExecutionError(x))?;
        info!("Retrieving data");
        let batches: Vec<_> = flight_data
            .get_mut()
            .try_collect()
            .await
            .map_err(|x| ChrontextFlightClientError::QueryExecutionError(x))?;
        let mut mappings = vec![];
        for b in batches {
            let type_map: HashMap<String, RDFNodeType> = deserialize(&b.app_metadata)
                .map_err(|x| ChrontextFlightClientError::TypesDeserializationError(x))?;
            let df = IpcStreamReader::new(b.data_body.as_ref())
                .finish()
                .map_err(|x| ChrontextFlightClientError::PolarsDeserializationError(x))?;
            mappings.push(SolutionMappings::new(df.lazy(), type_map));
        }
        let solution_mappings =
            union(mappings, false).map_err(|x| ChrontextFlightClientError::UnionError(x))?;
        self.client = Some(client);
        Ok(solution_mappings)
    }
}

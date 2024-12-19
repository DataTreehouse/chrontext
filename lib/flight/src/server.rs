use chrontext::engine::Engine;
use std::net::AddrParseError;
use std::sync::Arc;
use thiserror::*;

#[derive(Error, Debug)]
pub enum ChrontextFlightServerError {
    #[error(transparent)]
    TonicTransportError(tonic::transport::Error),
    #[error(transparent)]
    AddrParseError(AddrParseError),
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct ChrontextFlightService {
    engine: Option<Arc<Engine>>,
}

#[allow(dead_code)]
pub struct ChrontextFlightServer {
    chrontext_flight_service: ChrontextFlightService,
}

impl ChrontextFlightServer {
    pub fn new(engine: Option<Arc<Engine>>) -> Self {
        Self {
            chrontext_flight_service: ChrontextFlightService { engine },
        }
    }

    pub async fn serve(self, _addr: &str) -> Result<(), ChrontextFlightServerError> {
        unimplemented!("Contact Data Treehouse to try")
    }
}

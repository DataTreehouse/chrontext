use chrontext::errors::ChrontextError;
use spargebra::ParseError;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum ChrontextPGWireError {
    #[error(transparent)]
    ChrontextError(#[from] ChrontextError),
}

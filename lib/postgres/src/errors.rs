use chrontext::errors::ChrontextError;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum ChrontextPGWireError {
    #[error(transparent)]
    ChrontextError(#[from] ChrontextError),
}

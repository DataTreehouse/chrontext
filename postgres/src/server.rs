//Based on https://github.com/sunng87/pgwire/blob/master/examples/duckdb.rs
//License found in licenses PGWIRE-LICENSE-*

use chrontext::engine::Engine;
use crate::catalog::Catalog;
use crate::config::{PGDateOrder, PGDateTimeStyle};
use crate::errors::ChrontextPGWireError;

pub async fn start_server(
    _engine: Engine,
    _config: Config,
    _catalog: Catalog,
) -> Result<(), ChrontextPGWireError> {
    unimplemented!("Contact Data Treehouse to try")
}

#[derive(Clone, Default)]
pub struct Config {
    pub(crate) pg_date_time_style: PGDateTimeStyle,
    pub(crate) pg_date_order: PGDateOrder,
}

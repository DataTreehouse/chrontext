//Based on https://github.com/sunng87/pgwire/blob/master/examples/duckdb.rs
//License found in licenses PGWIRE-LICENSE-*

use crate::catalog::Catalog;
use crate::config::{PGDateOrder, PGDateTimeStyle};
use crate::errors::ChrontextPGWireError;
use chrontext::engine::Engine;

pub async fn start_server(
    _engine: Engine,
    _config: Config,
    _catalog: Catalog,
) -> Result<(), ChrontextPGWireError> {
    unimplemented!("Contact Data Treehouse to try")
}

#[derive(Clone, Default)]
#[allow(dead_code)]
pub struct Config {
    pub(crate) pg_date_time_style: PGDateTimeStyle,
    pub(crate) pg_date_order: PGDateOrder,
}

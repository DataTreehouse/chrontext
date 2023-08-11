use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod change_types;
pub mod combiner;
pub mod constants;
pub mod constraints;
pub mod engine;
mod find_query_variables;
mod preparing;
pub mod preprocessing;
pub mod pushdown_setting;
pub mod query_context;
pub mod rewriting;
mod sparql_result_to_polars;
pub mod splitter;
pub mod static_sparql;
pub mod timeseries_database;
pub mod timeseries_query;

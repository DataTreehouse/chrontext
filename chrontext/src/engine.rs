use crate::combiner::Combiner;
use crate::errors::ChrontextError;
use crate::preprocessing::Preprocessor;
use crate::rewriting::StaticQueryRewriter;
use crate::sparql_database::sparql_embedded_oxigraph::{EmbeddedOxigraph, EmbeddedOxigraphConfig};
use crate::sparql_database::sparql_endpoint::SparqlEndpoint;
use crate::sparql_database::SparqlQueryable;
use crate::splitter::parse_sparql_select_query;
use log::debug;
use polars::enable_string_cache;
use polars::frame::DataFrame;
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::sync::Arc;
use std::thread;
use timeseries_outpost::timeseries_bigquery_database::TimeseriesBigQueryDatabase;
use timeseries_outpost::timeseries_opcua_database::TimeseriesOPCUADatabase;
use timeseries_outpost::TimeseriesQueryable;
use timeseries_query::pushdown_setting::{all_pushdowns, PushdownSetting};
use timeseries_query::TimeseriesTable;

#[derive(Debug, Serialize, Deserialize)]
pub struct EngineConfig {
    pub sparql_endpoint: Option<String>,
    pub sparql_oxigraph_config: Option<EmbeddedOxigraphConfig>,
    pub timeseries_bigquery_key_file: Option<String>,
    pub timeseries_bigquery_tables: Option<Vec<TimeseriesTable>>,
    pub timeseries_opcua_namespace: Option<u16>,
    pub timeseries_opcua_endpoint: Option<String>,
}

pub struct Engine {
    pushdown_settings: HashSet<PushdownSetting>,
    time_series_database: Arc<dyn TimeseriesQueryable>,
    pub sparql_database: Arc<dyn SparqlQueryable>,
}

impl Engine {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Arc<dyn TimeseriesQueryable>,
        sparql_database: Arc<dyn SparqlQueryable>,
    ) -> Engine {
        Engine {
            pushdown_settings,
            time_series_database: time_series_database,
            sparql_database: sparql_database,
        }
    }

    pub fn from_config(engine_config: EngineConfig) -> Result<Engine, ChrontextError> {
        let EngineConfig {
            sparql_endpoint,
            sparql_oxigraph_config,
            timeseries_bigquery_key_file,
            timeseries_bigquery_tables,
            timeseries_opcua_namespace,
            timeseries_opcua_endpoint,
        } = engine_config;

        let sparql_queryable: Arc<dyn SparqlQueryable> = if let Some(endpoint) = sparql_endpoint {
            Arc::new(SparqlEndpoint { endpoint })
        } else if let Some(config) = sparql_oxigraph_config {
            Arc::new(
                EmbeddedOxigraph::from_config(config)
                    .map_err(|x| ChrontextError::CreateSPARQLDatabaseError(x.to_string()))?,
            )
        } else {
            return Err(ChrontextError::NoSPARQLDatabaseDefined);
        };

        let (pushdown_settings, time_series_database): (
            HashSet<PushdownSetting>,
            Arc<dyn TimeseriesQueryable>,
        ) = if let (Some(timeseries_bigquery_key_file), Some(timeseries_bigquery_tables)) =
            (timeseries_bigquery_key_file, timeseries_bigquery_tables)
        {
            let key = timeseries_bigquery_key_file.clone();
            let db =
                thread::spawn(|| TimeseriesBigQueryDatabase::new(key, timeseries_bigquery_tables))
                    .join()
                    .unwrap();

            (all_pushdowns(), Arc::new(db))
        } else if let (Some(timeseries_opcua_namespace), Some(timeseries_opcua_endpoint)) =
            (timeseries_opcua_namespace, timeseries_opcua_endpoint)
        {
            (
                [PushdownSetting::GroupBy].into(),
                Arc::new(TimeseriesOPCUADatabase::new(
                    &timeseries_opcua_endpoint,
                    timeseries_opcua_namespace,
                )),
            )
        } else {
            return Err(ChrontextError::NoTimeseriesDatabaseDefined);
        };

        Ok(Engine::new(
            pushdown_settings,
            time_series_database,
            sparql_queryable,
        ))
    }

    pub fn from_json(path: &str) -> Result<Engine, ChrontextError> {
        let f = File::open(path).map_err(|x| ChrontextError::FromJSONFileError(x.to_string()))?;
        let c: EngineConfig = serde_json::from_reader(f)
            .map_err(|x| ChrontextError::DeserializeFromJSONFileError(x.to_string()))?;
        Engine::from_config(c)
    }

    pub async fn execute_hybrid_query(
        &self,
        query: &str,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
        enable_string_cache();
        let parsed_query = parse_sparql_select_query(query)?;
        debug!("Parsed query: {}", &parsed_query);
        debug!("Parsed query algebra: {:?}", &parsed_query);
        let mut preprocessor = Preprocessor::new();
        let (preprocessed_query, variable_constraints) = preprocessor.preprocess(&parsed_query);
        debug!("Constraints: {:?}", variable_constraints);
        let rewriter = StaticQueryRewriter::new(&variable_constraints);
        let (static_queries_map, basic_time_series_queries, rewritten_filters) =
            rewriter.rewrite_query(preprocessed_query);
        debug!("Produced static rewrite: {:?}", static_queries_map);
        debug!(
            "Produced basic time series queries: {:?}",
            basic_time_series_queries
        );

        let mut combiner = Combiner::new(
            self.sparql_database.clone(),
            self.pushdown_settings.clone(),
            self.time_series_database.clone(),
            basic_time_series_queries,
            rewritten_filters,
        );
        let solution_mappings = combiner
            .combine_static_and_time_series_results(static_queries_map, &parsed_query)
            .await?;
        let SolutionMappings {
            mappings,
            rdf_node_types,
        } = solution_mappings;

        Ok((mappings.collect()?, rdf_node_types))
    }
}

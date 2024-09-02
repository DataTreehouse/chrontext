use crate::combiner::Combiner;
use crate::errors::ChrontextError;
use crate::preprocessing::Preprocessor;
use crate::rename_vars::rename_query_vars;
use crate::rewriting::StaticQueryRewriter;
use crate::sparql_database::sparql_embedded_oxigraph::{EmbeddedOxigraph, EmbeddedOxigraphConfig};
use crate::sparql_database::sparql_endpoint::SparqlEndpoint;
use crate::sparql_database::SparqlQueryable;
use crate::splitter::parse_sparql_select_query;
use log::debug;
use polars::enable_string_cache;
use polars::frame::DataFrame;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use virtualization::{Virtualization, VirtualizedDatabase};
use virtualized_query::pushdown_setting::PushdownSetting;

pub struct EngineConfig {
    pub sparql_endpoint: Option<String>,
    pub sparql_oxigraph_config: Option<EmbeddedOxigraphConfig>,
    pub virtualized_database: VirtualizedDatabase,
    pub virtualization: Virtualization,
}

pub struct Engine {
    pushdown_settings: HashSet<PushdownSetting>,
    virtualized_database: Arc<VirtualizedDatabase>,
    virtualization: Arc<Virtualization>,
    pub sparql_database: Arc<dyn SparqlQueryable>,
}

impl Engine {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        virtualized_database: Arc<VirtualizedDatabase>,
        virtualization: Arc<Virtualization>,
        sparql_database: Arc<dyn SparqlQueryable>,
    ) -> Engine {
        Engine {
            pushdown_settings,
            virtualized_database: virtualized_database,
            sparql_database: sparql_database,
            virtualization,
        }
    }

    pub fn from_config(engine_config: EngineConfig) -> Result<Engine, ChrontextError> {
        let EngineConfig {
            sparql_endpoint,
            sparql_oxigraph_config,
            virtualized_database,
            virtualization,
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

        let pushdown_settings = virtualized_database.pushdown_settings();

        Ok(Engine::new(
            pushdown_settings,
            Arc::new(virtualized_database),
            Arc::new(virtualization),
            sparql_queryable,
        ))
    }

    pub async fn query<'py>(
        &self,
        query: &str,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>, Vec<Context>), ChrontextError> {
        enable_string_cache();
        let parsed_query = parse_sparql_select_query(query)?;
        debug!("Parsed query: {}", parsed_query.to_string());
        debug!("Parsed query algebra: {:?}", &parsed_query);
        let (parsed_query, rename_map) = rename_query_vars(parsed_query);
        let virtualized_iris = self.virtualization.get_virtualized_iris();
        let first_level_virtualized_iris = self.virtualization.get_first_level_virtualized_iris();

        let mut preprocessor =
            Preprocessor::new(virtualized_iris, first_level_virtualized_iris.clone());
        let (preprocessed_query, variable_constraints) = preprocessor.preprocess(&parsed_query);
        debug!("Constraints: {:?}", variable_constraints);
        let rewriter = StaticQueryRewriter::new(variable_constraints, first_level_virtualized_iris);
        let (static_queries_map, basic_virtualized_queries, rewritten_filters) =
            rewriter.rewrite_query(preprocessed_query.clone());
        debug!(
            "Produced {} static rewrites with contexts: {:?}",
            static_queries_map.len(),
            static_queries_map.keys()
        );

        debug!(
            "Produced basic time series queries: {:?}",
            basic_virtualized_queries,
        );

        let mut combiner = Combiner::new(
            self.sparql_database.clone(),
            self.pushdown_settings.clone(),
            self.virtualized_database.clone(),
            basic_virtualized_queries,
            rewritten_filters,
            self.virtualization.clone(),
        );
        let mut solution_mappings = combiner
            .combine_static_and_time_series_results(static_queries_map, &preprocessed_query)
            .await
            .map_err(|x| ChrontextError::CombinerError(x))?;
        for (original, renamed) in rename_map {
            if let Some(dt) = solution_mappings.rdf_node_types.remove(&renamed) {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .rename(&[renamed], &[original.clone()]);
                solution_mappings.rdf_node_types.insert(original, dt);
            }
        }

        let SolutionMappings {
            mappings,
            rdf_node_types,
        } = solution_mappings;

        Ok((
            mappings.collect().unwrap(),
            rdf_node_types,
            combiner.virtualized_contexts,
        ))
    }
}

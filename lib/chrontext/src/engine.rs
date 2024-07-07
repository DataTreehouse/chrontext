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
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use pyo3::Python;
use templates::ast::Template;
use virtualized_query::pushdown_setting::{PushdownSetting};
use virtualization::VirtualizedDatabase;

#[derive(Debug)]
pub struct EngineConfig {
    pub sparql_endpoint: Option<String>,
    pub sparql_oxigraph_config: Option<EmbeddedOxigraphConfig>,
    pub virtualized_database: VirtualizedDatabase,
    pub virtualization: Virtualization,
}

#[derive(Debug)]
pub struct Virtualization {
    pub resources: HashMap<String, Template>
}

pub struct QueryWithOptionalPy<'py> {
    query: &'py str,
    optional_py: Python<'py>
}

pub struct Engine {
    pushdown_settings: HashSet<PushdownSetting>,
    virtualized_database: Arc<VirtualizedDatabase>,
    pub sparql_database: Arc<dyn SparqlQueryable>,
}

impl Engine {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        virtualized_database: Arc<VirtualizedDatabase>,
        sparql_database: Arc<dyn SparqlQueryable>,
    ) -> Engine {
        Engine {
            pushdown_settings,
            virtualized_database: virtualized_database,
            sparql_database: sparql_database,
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
            sparql_queryable,
        ))
    }

    pub async fn query<'py>(
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
        let (static_queries_map, basic_virtualized_queries, rewritten_filters) =
            rewriter.rewrite_query(preprocessed_query);
        debug!("Produced static rewrite: {:?}", static_queries_map);
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

use crate::combiner::Combiner;
use crate::errors::ChrontextError;
use crate::preprocessing::Preprocessor;
use crate::rewriting::StaticQueryRewriter;
use crate::sparql_database::sparql_embedded_oxigraph::{EmbeddedOxigraph, EmbeddedOxigraphConfig};
use crate::sparql_database::sparql_endpoint::SparqlEndpoint;
use crate::sparql_database::SparqlQueryable;
use crate::splitter::parse_sparql_select_query;
use log::debug;
use oxrdf::NamedNode;
use polars::enable_string_cache;
use polars::frame::DataFrame;
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use templates::ast::{ConstantTerm, ConstantTermOrList, StottrTerm, Template};
use templates::constants::OTTR_TRIPLE;
use virtualization::VirtualizedDatabase;
use virtualized_query::ID_VARIABLE_NAME;
use virtualized_query::pushdown_setting::PushdownSetting;

#[derive(Debug)]
pub struct EngineConfig {
    pub sparql_endpoint: Option<String>,
    pub sparql_oxigraph_config: Option<EmbeddedOxigraphConfig>,
    pub virtualized_database: VirtualizedDatabase,
    pub virtualization: Virtualization,
}

#[derive(Debug)]
pub struct Virtualization {
    pub resources: HashMap<String, Template>,
}

impl Virtualization {
    pub fn get_virtualized_iris(&self) -> HashSet<NamedNode> {
        let mut nns = HashSet::new();
        for t in self.resources.values() {
            for i in &t.pattern_list {
                assert_eq!(i.template_name.as_str(), OTTR_TRIPLE);
                let a = i.argument_list.get(1).unwrap();
                if let StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                    ConstantTerm::Iri(nn),
                )) = &a.term
                {
                    nns.insert(nn.clone());
                } else {
                    todo!("Handle this error")
                }
            }
        }
        nns
    }
    pub fn get_first_level_virtualized_iris(&self) -> HashSet<NamedNode> {
        let mut nns = HashSet::new();
        for t in self.resources.values() {
            for i in &t.pattern_list {
                assert_eq!(i.template_name.as_str(), OTTR_TRIPLE);
                let subj = i.argument_list.get(0).unwrap();
                if let StottrTerm::Variable(v) = &subj.term {
                    if v.as_str() == ID_VARIABLE_NAME {
                        let a = i.argument_list.get(1).unwrap();
                        if let StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                            ConstantTerm::Iri(nn),
                        )) = &a.term
                        {
                            nns.insert(nn.clone());
                        } else {
                            todo!("Handle this error")
                        }
                    }
                }
            }
        }
        nns
    }
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
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
        enable_string_cache();
        let parsed_query = parse_sparql_select_query(query)?;
        debug!("Parsed query: {}", &parsed_query);
        debug!("Parsed query algebra: {:?}", &parsed_query);
        let virtualized_iris = self.virtualization.get_virtualized_iris();
        let first_level_virtualized_iris = self.virtualization.get_first_level_virtualized_iris();

        let mut preprocessor =
            Preprocessor::new(virtualized_iris, first_level_virtualized_iris.clone());
        let (preprocessed_query, variable_constraints) = preprocessor.preprocess(&parsed_query);
        debug!("Constraints: {:?}", variable_constraints);
        let rewriter = StaticQueryRewriter::new(variable_constraints, first_level_virtualized_iris);
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
            self.virtualization.clone(),
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

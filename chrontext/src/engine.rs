use crate::combiner::Combiner;
use crate::preprocessing::Preprocessor;
use crate::pushdown_setting::PushdownSetting;
use crate::rewriting::StaticQueryRewriter;
use crate::sparql_database::SparqlQueryable;
use crate::splitter::parse_sparql_select_query;
use crate::timeseries_database::TimeseriesQueryable;
use log::debug;
use polars::frame::DataFrame;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use polars_core::enable_string_cache;
use representation::RDFNodeType;
use representation::solution_mapping::SolutionMappings;

pub struct Engine {
    pushdown_settings: HashSet<PushdownSetting>,
    time_series_database: Option<Box<dyn TimeseriesQueryable>>,
    pub sparql_database: Option<Box<dyn SparqlQueryable>>,
}

impl Engine {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeseriesQueryable>,
        sparql_database: Box<dyn SparqlQueryable>,
    ) -> Engine {
        Engine {
            pushdown_settings,
            time_series_database: Some(time_series_database),
            sparql_database: Some(sparql_database),
        }
    }

    pub fn has_time_series_db(&self) -> bool {
        self.time_series_database.is_some()
    }

    pub fn has_sparql_db(&self) -> bool {
        self.sparql_database.is_some()
    }

    pub async fn execute_hybrid_query(&mut self, query: &str) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
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
            self.sparql_database.take().unwrap(),
            self.pushdown_settings.clone(),
            self.time_series_database.take().unwrap(),
            basic_time_series_queries,
            rewritten_filters,
        );
        let solution_mappings = match combiner
            .combine_static_and_time_series_results(static_queries_map, &parsed_query)
            .await
        {
            Ok(solution_mappings) => solution_mappings,
            Err(e) => {
                self.time_series_database = Some(combiner.time_series_database);
                self.sparql_database = Some(combiner.sparql_database);
                return Err(Box::new(e));
            }
        };
        self.time_series_database = Some(combiner.time_series_database);
        self.sparql_database = Some(combiner.sparql_database);
        let SolutionMappings { mappings, rdf_node_types } = solution_mappings;

        Ok((mappings.collect()?, rdf_node_types))
    }
}

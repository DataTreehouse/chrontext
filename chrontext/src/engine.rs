use crate::combiner::Combiner;
use crate::preprocessing::Preprocessor;
use crate::pushdown_setting::PushdownSetting;
use crate::rewriting::StaticQueryRewriter;
use crate::splitter::parse_sparql_select_query;
use crate::timeseries_database::TimeSeriesQueryable;
use log::debug;
use polars::frame::DataFrame;
use std::collections::HashSet;
use std::error::Error;

pub struct Engine {
    pushdown_settings: HashSet<PushdownSetting>,
    time_series_database: Option<Box<dyn TimeSeriesQueryable>>,
    endpoint: String,
}

impl Engine {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeSeriesQueryable>,
        endpoint: String,
    ) -> Engine {
        Engine {
            pushdown_settings,
            time_series_database: Some(time_series_database),
            endpoint,
        }
    }

    pub async fn execute_hybrid_query(&mut self, query: &str) -> Result<DataFrame, Box<dyn Error>> {
        let parsed_query = parse_sparql_select_query(query)?;
        debug!("Parsed query: {:?}", &parsed_query);
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
            self.endpoint.to_string(),
            self.pushdown_settings.clone(),
            self.time_series_database.take().unwrap(),
            basic_time_series_queries,
            rewritten_filters,
        );
        let solution_mappings = combiner
            .combine_static_and_time_series_results(static_queries_map, &parsed_query)
            .await?;
        self.time_series_database = Some(combiner.time_series_database);
        Ok(solution_mappings.mappings.collect()?)
    }
}

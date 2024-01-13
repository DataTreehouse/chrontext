use super::Combiner;
use representation::solution_mapping::SolutionMappings;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use representation::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use log::debug;
use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, GraphPattern};
use spargebra::Query;
use std::collections::HashMap;
use query_processing::aggregates::AggregateReturn;
use query_processing::graph_patterns::{group_by, prepare_group_by};

impl Combiner {
    pub(crate) async fn lazy_group(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        solution_mapping: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_time_series_queries: Option<HashMap<Context, Vec<TimeseriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing group graph pattern");
        let inner_context = context.extension_with(PathEntry::GroupInner);
        let inner_prepared_time_series_queries =
            split_time_series_queries(&mut prepared_time_series_queries, &inner_context);
        let inner_static_query_map = split_static_queries(&mut static_query_map, &inner_context);

        let output_solution_mappings = self
            .lazy_graph_pattern(
                inner,
                solution_mapping,
                inner_static_query_map,
                inner_prepared_time_series_queries,
                &inner_context,
            )
            .await?;
        let (mut output_solution_mappings, by, dummy_varname) = prepare_group_by(output_solution_mappings, variables);

        let mut aggregate_expressions = vec![];
        let mut new_rdf_node_types = HashMap::new();
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            let AggregateReturn {
                solution_mappings: aggregate_solution_mappings,
                expr,
                context: _,
                rdf_node_type,
            } = self
                .sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    a,
                    output_solution_mappings,
                    &aggregate_context,
                )
                .await?;
            output_solution_mappings = aggregate_solution_mappings;
            new_rdf_node_types.insert(v.as_str().to_string(), rdf_node_type);
            aggregate_expressions.push(expr);
        }
        Ok(group_by(output_solution_mappings, aggregate_expressions, by, dummy_varname, new_rdf_node_types)?)
    }
}

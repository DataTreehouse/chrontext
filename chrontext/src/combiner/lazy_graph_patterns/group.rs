use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::combiner::CombinerError;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeseriesQuery;
use log::debug;
use oxrdf::Variable;
use polars::prelude::{col, Expr};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use spargebra::Query;
use std::collections::HashMap;

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

        let mut output_solution_mappings = self
            .lazy_graph_pattern(
                inner,
                solution_mapping,
                inner_static_query_map,
                inner_prepared_time_series_queries,
                &inner_context,
            )
            .await?;
        let by: Vec<Expr> = variables.iter().map(|v| col(v.as_str())).collect();

        let mut aggregate_expressions = vec![];
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            let (aggregate_solution_mappings, expr, used_context) = self
                .sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    a,
                    output_solution_mappings,
                    &aggregate_context,
                )
                .await?;
            output_solution_mappings = aggregate_solution_mappings;
            aggregate_expressions.push(expr);
        }
        let SolutionMappings {
            mut mappings,
            mut columns,
            datatypes,
        } = output_solution_mappings;
        let grouped_mappings = mappings.group_by(by.as_slice());

        mappings = grouped_mappings.agg(aggregate_expressions.as_slice());
        columns.clear();
        for v in variables {
            columns.insert(v.as_str().to_string());
        }
        for (v, _) in aggregates {
            columns.insert(v.as_str().to_string());
        }
        Ok(SolutionMappings::new(mappings, columns, datatypes))
    }
}

mod bgp_pattern;
mod distinct_pattern;
mod extend_pattern;
pub(crate) mod filter_expression_rewrites;
mod filter_pattern;
mod graph_pattern;
mod group_pattern;
mod join_pattern;
mod left_join_pattern;
mod minus_pattern;
mod order_by_pattern;
mod path_pattern;
mod project_pattern;
mod reduced_pattern;
mod service_pattern;
mod sliced_pattern;
mod union_pattern;
mod values_pattern;

use super::TimeseriesQueryPrepper;
use representation::solution_mapping::SolutionMappings;
use representation::query_context::Context;
use crate::timeseries_query::TimeseriesQuery;
use log::debug;
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

#[derive(Debug)]
pub struct GPPrepReturn {
    pub fail_groupby_complex_query: bool,
    pub time_series_queries: HashMap<Context, Vec<TimeseriesQuery>>,
}

impl GPPrepReturn {
    fn new(time_series_queries: HashMap<Context, Vec<TimeseriesQuery>>) -> GPPrepReturn {
        GPPrepReturn {
            fail_groupby_complex_query: false,
            time_series_queries,
        }
    }

    pub fn fail_groupby_complex_query() -> GPPrepReturn {
        GPPrepReturn {
            fail_groupby_complex_query: true,
            time_series_queries: HashMap::new(),
        }
    }

    pub fn with_time_series_queries_from(&mut self, other: GPPrepReturn) {
        for (c, v) in other.time_series_queries {
            if let Some(myv) = self.time_series_queries.get_mut(&c) {
                myv.extend(v);
            } else {
                self.time_series_queries.insert(c, v);
            }
        }
    }
}

impl TimeseriesQueryPrepper {
    pub fn prepare_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        debug!(
            "Preparing TSQ for graph pattern at context {}, try group by complex query: {}",
            context.as_str(),
            try_groupby_complex_query
        );
        match graph_pattern {
            GraphPattern::Bgp { patterns: _ } => {
                self.prepare_bgp(try_groupby_complex_query, context)
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.prepare_path(subject, path, object),
            GraphPattern::Join { left, right } => self.prepare_join(
                left,
                right,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.prepare_left_join(
                left,
                right,
                expression,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Filter { expr, inner } => self.prepare_filter(
                expr,
                inner,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Union { left, right } => self.prepare_union(
                left,
                right,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Graph { inner, .. } => {
                self.prepare_graph(inner, try_groupby_complex_query, solution_mappings, context)
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.prepare_extend(
                inner,
                variable,
                expression,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Minus { left, right } => self.prepare_minus(
                left,
                right,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Values {
                variables,
                bindings,
            } => self.prepare_values(variables, bindings),
            GraphPattern::OrderBy { inner, expression } => self.prepare_order_by(
                inner,
                expression,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Project { inner, variables } => self.prepare_project(
                inner,
                variables,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Distinct { inner } => {
                self.prepare_distinct(inner, try_groupby_complex_query, solution_mappings, context)
            }
            GraphPattern::Reduced { inner } => {
                self.prepare_reduced(inner, try_groupby_complex_query, solution_mappings, context)
            }
            GraphPattern::Slice { inner, .. } => {
                self.prepare_slice(inner, try_groupby_complex_query, solution_mappings, context)
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => self.prepare_group(
                inner,
                variables,
                aggregates,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            GraphPattern::Service { .. } => self.prepare_service(),
            GraphPattern::DT { .. } => panic!("Should never happen"),
            GraphPattern::PValues { .. } => {
                todo!("Not currently supported")
            }
        }
    }
}

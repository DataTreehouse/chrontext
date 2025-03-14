mod distinct;
mod extend;
mod filter;
mod group;
mod join;
mod left_join;
mod minus;
mod order_by;
mod project;
mod slice;
mod union;
mod values;

use super::Combiner;
use crate::combiner::CombinerError;
use crate::preparing::graph_patterns::GPPrepReturn;
use async_recursion::async_recursion;
use log::debug;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        prepared_virtualized_queries: Option<HashMap<Context, Vec<VirtualizedQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        debug!("Processing graph pattern at context: {}", context.as_str());
        let mut updated_solution_mappings = solution_mappings;
        let mut new_prepared_virtualized_queries = prepared_virtualized_queries;

        //We have to eagerly evaluate static queries contained in the group by pattern since otherwise we are unable to push down the group by into the time series database.
        let mut found_group_by_pushdown = false;
        let mut static_query_opt = static_query_map.remove(context);
        if static_query_opt.is_none() {
            debug!("No static query found");
            let groupby_inner_context = context.extension_with(PathEntry::GroupInner);
            static_query_opt = static_query_map.remove(&groupby_inner_context);
            if static_query_opt.is_some() {
                debug!("Found static subquery generated by group by");
                found_group_by_pushdown = true;
            }
        } else {
            debug!("Found static query");
        }

        if let Some(query) = static_query_opt {
            debug!("Executing static query");
            let mut new_solution_mappings = self
                .execute_static_query(&query, updated_solution_mappings)
                .await?;
            debug!("Finished executing static query");
            debug!("Start preparing time series queries");
            let GPPrepReturn {
                virtualized_queries,
                ..
            } = self.prepper.prepare_graph_pattern(
                graph_pattern,
                false,
                &mut new_solution_mappings,
                context,
            )?;
            debug!(
                "Finshed preparing time series queries, {} were created",
                virtualized_queries.len()
            );
            updated_solution_mappings = Some(new_solution_mappings);
            new_prepared_virtualized_queries = Some(virtualized_queries);
        }

        if let Some(vqs_map) = &mut new_prepared_virtualized_queries {
            if let Some(vqs) = vqs_map.remove(context) {
                self.virtualized_contexts.push(context.clone());
                for vq in vqs {
                    debug!("Attaching time series query");
                    let new_solution_mappings = self
                        .execute_attach_virtualized_query(vq, updated_solution_mappings.unwrap())
                        .await?;
                    debug!("Finished attaching time series query");
                    updated_solution_mappings = Some(new_solution_mappings);
                }
            }
        }

        if found_group_by_pushdown
            && (new_prepared_virtualized_queries.is_none()
                || (new_prepared_virtualized_queries.is_some()
                    && new_prepared_virtualized_queries
                        .as_ref()
                        .unwrap()
                        .is_empty()))
        {
            debug!("Will not process graph pattern further due to found static group by");
            return Ok(updated_solution_mappings.unwrap());
        }

        if static_query_map.is_empty()
            && updated_solution_mappings.is_none()
            && (new_prepared_virtualized_queries.is_none()
                || (new_prepared_virtualized_queries.is_some()
                    && new_prepared_virtualized_queries
                        .as_ref()
                        .unwrap()
                        .is_empty()))
        {
            debug!("Will not process graph pattern further as there is no static or dynamic data to attach");
            return Ok(updated_solution_mappings.unwrap());
        }

        match graph_pattern {
            GraphPattern::Bgp { .. } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::Path { .. } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::Join { left, right } => {
                self.lazy_join(
                    left,
                    right,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.lazy_left_join(
                    left,
                    right,
                    expression,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Filter { expr, inner } => {
                self.lazy_filter(
                    inner,
                    expr,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Union { left, right } => {
                self.lazy_union(
                    left,
                    right,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Graph { name: _, inner: _ } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                self.lazy_extend(
                    inner,
                    variable,
                    expression,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Minus { left, right } => {
                self.lazy_minus(
                    left,
                    right,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => self.lazy_values(updated_solution_mappings, variables, bindings),
            GraphPattern::OrderBy { inner, expression } => {
                self.lazy_order_by(
                    inner,
                    expression,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Project { inner, variables } => {
                self.lazy_project(
                    inner,
                    variables,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(
                    inner,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Reduced { .. } => {
                todo!()
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                self.lazy_slice(
                    inner,
                    start,
                    length,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                self.lazy_group(
                    inner,
                    variables,
                    aggregates,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_virtualized_queries,
                    context,
                )
                .await
            }
            GraphPattern::Service { .. } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::DT { .. } => {
                panic!("Should never happen!")
            }
            GraphPattern::PValues { .. } => {
                todo!("Not currently supported")
            }
        }
    }
}

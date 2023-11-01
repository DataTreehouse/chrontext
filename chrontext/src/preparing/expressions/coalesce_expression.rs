use super::TimeseriesQueryPrepper;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashMap;

impl TimeseriesQueryPrepper {
    pub fn prepare_coalesce_expression(
        &mut self,
        wrapped: &Vec<Expression>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let mut prepared = wrapped
            .iter()
            .enumerate()
            .map(|(i, e)| {
                self.prepare_expression(
                    e,
                    try_groupby_complex_query,
                    solution_mappings,
                    &context.extension_with(PathEntry::Coalesce(i as u16)),
                )
            })
            .collect::<Vec<EXPrepReturn>>();
        if prepared.iter().any(|x| x.fail_groupby_complex_query) {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        if prepared.is_empty() {
            EXPrepReturn::new(HashMap::new())
        } else {
            let mut first_prepared = prepared.remove(0);
            for p in prepared {
                first_prepared.with_time_series_queries_from(p);
            }
            first_prepared
        }
    }
}

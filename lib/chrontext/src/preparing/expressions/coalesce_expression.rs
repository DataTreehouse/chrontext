use super::TimeseriesQueryPrepper;
use crate::combiner::CombinerError;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;
use std::collections::HashMap;

impl TimeseriesQueryPrepper {
    pub fn prepare_coalesce_expression(
        &mut self,
        wrapped: &[Expression],
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<EXPrepReturn, CombinerError> {
        let prepared = wrapped
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
            .collect::<Result<Vec<EXPrepReturn>, CombinerError>>();
        let mut prepared = prepared?;
        if prepared.iter().any(|x| x.fail_groupby_complex_query) {
            return Ok(EXPrepReturn::fail_groupby_complex_query());
        }
        if prepared.is_empty() {
            Ok(EXPrepReturn::new(HashMap::new()))
        } else {
            let mut first_prepared = prepared.remove(0);
            for p in prepared {
                first_prepared.with_virtualized_queries_from(p);
            }
            Ok(first_prepared)
        }
    }
}

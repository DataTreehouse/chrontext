use super::TimeSeriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use crate::combiner::solution_mapping::SolutionMappings;

impl TimeSeriesQueryPrepper {
    pub fn prepare_in_expression(
        &mut self,
        left: &Expression,
        expressions: &Vec<Expression>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(PathEntry::InLeft),
        );
        let prepared: Vec<EXPrepReturn> = expressions
            .iter()
            .map(|x| self.prepare_expression(x, try_groupby_complex_query, solution_mappings, context))
            .collect();
        if left_prepare.fail_groupby_complex_query
            || prepared.iter().any(|x| x.fail_groupby_complex_query)
        {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        for p in prepared {
            left_prepare.with_time_series_queries_from(p)
        }
        left_prepare
    }
}

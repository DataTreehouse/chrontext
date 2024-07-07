use super::TimeseriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

impl TimeseriesQueryPrepper {
    pub fn prepare_function_call_expression(
        &mut self,
        _fun: &Function,
        args: &Vec<Expression>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let mut args_prepared = args
            .iter()
            .enumerate()
            .map(|(i, e)| {
                self.prepare_expression(
                    e,
                    try_groupby_complex_query,
                    solution_mappings,
                    &context.extension_with(PathEntry::FunctionCall(i as u16)),
                )
            })
            .collect::<Vec<EXPrepReturn>>();
        if args_prepared.iter().any(|x| x.fail_groupby_complex_query) {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        if args_prepared.len() > 0 {
            let mut first_prepared = args_prepared.remove(0);
            for p in args_prepared {
                first_prepared.with_virtualized_queries_from(p)
            }
            first_prepared
        } else {
            EXPrepReturn::new(HashMap::new())
        }
    }
}

use super::TimeseriesQueryPrepper;
use crate::combiner::CombinerError;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;

pub enum UnaryOrdinaryOperator {
    UnaryPlus,
    UnaryMinus,
}

impl TimeseriesQueryPrepper {
    pub fn prepare_unary_ordinary_expression(
        &mut self,
        wrapped: &Expression,
        operation: &UnaryOrdinaryOperator,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> Result<EXPrepReturn, CombinerError> {
        let path_entry = match operation {
            UnaryOrdinaryOperator::UnaryPlus => PathEntry::UnaryPlus,
            UnaryOrdinaryOperator::UnaryMinus => PathEntry::UnaryMinus,
        };

        self.prepare_expression(
            wrapped,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(path_entry),
        )
    }
}

use super::TimeseriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;

pub enum BinaryOrdinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    LessOrEqual,
    Less,
    Greater,
    GreaterOrEqual,
    SameTerm,
    Equal,
}

impl TimeseriesQueryPrepper {
    pub fn prepare_binary_ordinary_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        operation: &BinaryOrdinaryOperator,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let (left_path_entry, right_path_entry) = match { operation } {
            BinaryOrdinaryOperator::Add => (PathEntry::AddLeft, PathEntry::AddRight),
            BinaryOrdinaryOperator::Subtract => (PathEntry::SubtractLeft, PathEntry::SubtractRight),
            BinaryOrdinaryOperator::Multiply => (PathEntry::MultiplyLeft, PathEntry::MultiplyRight),
            BinaryOrdinaryOperator::Divide => (PathEntry::DivideLeft, PathEntry::DivideRight),
            BinaryOrdinaryOperator::LessOrEqual => {
                (PathEntry::LessOrEqualLeft, PathEntry::LessOrEqualRight)
            }
            BinaryOrdinaryOperator::Less => (PathEntry::LessLeft, PathEntry::LessRight),
            BinaryOrdinaryOperator::Greater => (PathEntry::GreaterLeft, PathEntry::GreaterRight),
            BinaryOrdinaryOperator::GreaterOrEqual => (
                PathEntry::GreaterOrEqualLeft,
                PathEntry::GreaterOrEqualRight,
            ),
            BinaryOrdinaryOperator::SameTerm => (PathEntry::SameTermLeft, PathEntry::SameTermRight),
            BinaryOrdinaryOperator::Equal => (PathEntry::EqualLeft, PathEntry::EqualRight),
        };

        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(left_path_entry),
        );
        let right_prepare = self.prepare_expression(
            right,
            try_groupby_complex_query,
            solution_mappings,
            &context.extension_with(right_path_entry),
        );
        if left_prepare.fail_groupby_complex_query || right_prepare.fail_groupby_complex_query {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        left_prepare.with_virtualized_queries_from(right_prepare);
        left_prepare
    }
}

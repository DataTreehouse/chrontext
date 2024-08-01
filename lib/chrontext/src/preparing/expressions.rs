mod and_expression;
mod binary_ordinary_expression;
mod coalesce_expression;
mod exists_expression;
mod function_call_expression;
mod if_expression;
mod in_expression;
mod not_expression;
mod or_expression;
mod unary_ordinary_expression;

use super::TimeseriesQueryPrepper;
use crate::preparing::expressions::binary_ordinary_expression::BinaryOrdinaryOperator;
use crate::preparing::expressions::unary_ordinary_expression::UnaryOrdinaryOperator;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{Expression, OrderExpression};
use std::collections::HashMap;
use virtualized_query::VirtualizedQuery;

#[derive(Debug)]
pub struct EXPrepReturn {
    pub fail_groupby_complex_query: bool,
    pub virtualized_queries: HashMap<Context, Vec<VirtualizedQuery>>,
}

impl EXPrepReturn {
    fn new(virtualized_queries: HashMap<Context, Vec<VirtualizedQuery>>) -> EXPrepReturn {
        EXPrepReturn {
            virtualized_queries,
            fail_groupby_complex_query: false,
        }
    }

    pub fn fail_groupby_complex_query() -> EXPrepReturn {
        EXPrepReturn {
            fail_groupby_complex_query: true,
            virtualized_queries: HashMap::new(),
        }
    }

    pub fn with_virtualized_queries_from(&mut self, other: EXPrepReturn) {
        for (c, v) in other.virtualized_queries {
            if let Some(myv) = self.virtualized_queries.get_mut(&c) {
                myv.extend(v);
            } else {
                self.virtualized_queries.insert(c, v);
            }
        }
    }
}

impl TimeseriesQueryPrepper {
    pub fn prepare_order_expressions(
        &mut self,
        order_expressions: &Vec<OrderExpression>,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        let mut exprep_out: Option<EXPrepReturn> = None;

        for order_expression in order_expressions {
            let e = match order_expression {
                OrderExpression::Asc(e) => e,
                OrderExpression::Desc(e) => e,
            };
            let exprep =
                self.prepare_expression(e, try_groupby_complex_query, solution_mappings, context);
            if exprep.fail_groupby_complex_query {
                return EXPrepReturn::fail_groupby_complex_query();
            }
            exprep_out = if let Some(mut exprep_out) = exprep_out {
                exprep_out.with_virtualized_queries_from(exprep);
                Some(exprep_out)
            } else {
                Some(exprep)
            };
        }
        exprep_out.unwrap()
    }

    pub fn prepare_expression(
        &mut self,
        expression: &Expression,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> EXPrepReturn {
        match expression {
            Expression::NamedNode(..) => {
                let exr = EXPrepReturn::new(HashMap::new());
                exr
            }
            Expression::Literal(..) => {
                let exr = EXPrepReturn::new(HashMap::new());
                exr
            }
            Expression::Variable(..) => EXPrepReturn::new(HashMap::new()),
            Expression::Or(left, right) => self.prepare_or_expression(
                left,
                right,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),

            Expression::And(left, right) => self.prepare_and_expression(
                left,
                right,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Equal(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Equal,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::SameTerm(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::SameTerm,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Greater(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Greater,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::GreaterOrEqual(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::GreaterOrEqual,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Less(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Less,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::LessOrEqual(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::LessOrEqual,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::In(left, expressions) => self.prepare_in_expression(
                left,
                expressions,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Add(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Add,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Subtract(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Subtract,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Multiply(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Multiply,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Divide(left, right) => self.prepare_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Divide,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::UnaryPlus(wrapped) => self.prepare_unary_ordinary_expression(
                wrapped,
                &UnaryOrdinaryOperator::UnaryPlus,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::UnaryMinus(wrapped) => self.prepare_unary_ordinary_expression(
                wrapped,
                &UnaryOrdinaryOperator::UnaryMinus,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Not(wrapped) => self.prepare_not_expression(
                wrapped,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Exists(wrapped) => self.prepare_exists_expression(
                wrapped,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Bound(..) => EXPrepReturn::new(HashMap::new()),
            Expression::If(left, mid, right) => self.prepare_if_expression(
                left,
                mid,
                right,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::Coalesce(wrapped) => self.prepare_coalesce_expression(
                wrapped,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
            Expression::FunctionCall(fun, args) => self.prepare_function_call_expression(
                fun,
                args,
                try_groupby_complex_query,
                solution_mappings,
                context,
            ),
        }
    }
}

use super::StaticQueryRewriter;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;

impl StaticQueryRewriter {
    pub fn project_all_static_variables_in_expression(
        &mut self,
        expr: &Expression,
        context: &Context,
    ) {
        match expr {
            Expression::Variable(var) => {
                self.project_variable_if_static(var, context);
            }
            Expression::Or(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::OrLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::OrRight),
                );
            }
            Expression::And(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::AndLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::AndRight),
                );
            }
            Expression::Equal(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::EqualLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::EqualRight),
                );
            }
            Expression::SameTerm(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::SameTermLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::SameTermRight),
                );
            }
            Expression::Greater(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::GreaterLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::GreaterRight),
                );
            }
            Expression::GreaterOrEqual(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::GreaterOrEqualLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::GreaterOrEqualRight),
                );
            }
            Expression::Less(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::LessLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::LessRight),
                );
            }
            Expression::LessOrEqual(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::LessOrEqualLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::LessOrEqualRight),
                );
            }
            Expression::In(expr, expressions) => {
                self.project_all_static_variables_in_expression(
                    expr,
                    &context.extension_with(PathEntry::InLeft),
                );
                for (i, e) in expressions.iter().enumerate() {
                    self.project_all_static_variables_in_expression(
                        e,
                        &context.extension_with(PathEntry::InRight(i as u16)),
                    );
                }
            }
            Expression::Add(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::AddLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::AddRight),
                );
            }
            Expression::Subtract(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::SubtractLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::SubtractRight),
                );
            }
            Expression::Multiply(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::MultiplyLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::MultiplyRight),
                );
            }
            Expression::Divide(left, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::DivideLeft),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::DivideRight),
                );
            }
            Expression::UnaryPlus(expr) => {
                self.project_all_static_variables_in_expression(
                    expr,
                    &context.extension_with(PathEntry::UnaryPlus),
                );
            }
            Expression::UnaryMinus(expr) => {
                self.project_all_static_variables_in_expression(
                    expr,
                    &context.extension_with(PathEntry::UnaryMinus),
                );
            }
            Expression::Not(expr) => {
                self.project_all_static_variables_in_expression(
                    expr,
                    &context.extension_with(PathEntry::Not),
                );
            }
            Expression::Exists(_) => {
                todo!("Fix handling..")
            }
            Expression::Bound(var) => {
                self.project_variable_if_static(var, context);
            }
            Expression::If(left, middle, right) => {
                self.project_all_static_variables_in_expression(
                    left,
                    &context.extension_with(PathEntry::IfLeft),
                );
                self.project_all_static_variables_in_expression(
                    middle,
                    &context.extension_with(PathEntry::IfMiddle),
                );
                self.project_all_static_variables_in_expression(
                    right,
                    &context.extension_with(PathEntry::IfRight),
                );
            }
            Expression::Coalesce(expressions) => {
                for (i, e) in expressions.iter().enumerate() {
                    self.project_all_static_variables_in_expression(
                        e,
                        &context.extension_with(PathEntry::Coalesce(i as u16)),
                    );
                }
            }
            Expression::FunctionCall(_, expressions) => {
                for (i, e) in expressions.iter().enumerate() {
                    self.project_all_static_variables_in_expression(
                        e,
                        &context.extension_with(PathEntry::FunctionCall(i as u16)),
                    );
                }
            }
            _ => {}
        }
    }

    fn project_variable_if_static(&mut self, variable: &Variable, context: &Context) {
        if !self.variable_constraints.contains(variable, context) {
            self.additional_projections.insert(variable.clone());
        }
    }
}

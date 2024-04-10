use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub fn rewrite_in_expression(
        &mut self,
        left: &Expression,
        expressions: &Vec<Expression>,
        required_change_direction: &ChangeType,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let mut left_rewrite = self.rewrite_expression(
            left,
            &ChangeType::NoChange,
            variables_in_scope,
            create_subquery,
            &context.extension_with(PathEntry::InLeft),
        );
        let mut expressions_rewritten = expressions
            .iter()
            .enumerate()
            .map(|(i, e)| {
                self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    create_subquery,
                    &context.extension_with(PathEntry::InRight(i as u16)),
                )
            })
            .collect::<Vec<ExReturn>>();
        let mut exr = ExReturn::new();
        exr.with_is_subquery(&mut left_rewrite);
        for rw_exr in expressions_rewritten.iter_mut() {
            exr.with_is_subquery(rw_exr);
        }

        if left_rewrite.expression.is_some()
            && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
        {
            if expressions_rewritten.iter().all(|x| {
                x.expression.is_some() && x.change_type.as_ref().unwrap() == &ChangeType::NoChange
            }) {
                let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                let expressions_rewritten_nochange = expressions_rewritten
                    .iter_mut()
                    .filter(|x| {
                        x.expression.is_some()
                            || x.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    })
                    .map(|x| x.expression.take().unwrap())
                    .collect();
                exr.with_expression(Expression::In(
                    Box::new(left_expression_rewrite),
                    expressions_rewritten_nochange,
                ))
                .with_change_type(ChangeType::NoChange);
                return exr;
            }

            if required_change_direction == &ChangeType::Constrained
                && expressions_rewritten.iter().any(|x| {
                    x.expression.is_some()
                        && x.change_type.as_ref().unwrap() == &ChangeType::NoChange
                })
            {
                self.project_all_static_variables(
                    expressions_rewritten
                        .iter()
                        .filter(|x| {
                            x.expression.is_some()
                                && x.change_type.as_ref().unwrap() != &ChangeType::NoChange
                        })
                        .collect(),
                    context,
                );
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let expressions_rewritten_nochange = expressions_rewritten
                        .iter_mut()
                        .filter(|x| {
                            x.expression.is_some()
                                || x.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        })
                        .map(|x| x.expression.take().unwrap())
                        .collect();
                    exr.with_expression(Expression::In(
                        Box::new(left_expression_rewrite),
                        expressions_rewritten_nochange,
                    ))
                    .with_change_type(ChangeType::Constrained);
                    return exr;
                }
            }
        }
        self.project_all_static_variables(vec![&left_rewrite], context);
        self.project_all_static_variables(expressions_rewritten.iter().collect(), context);
        exr
    }
}

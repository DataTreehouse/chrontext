use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub fn rewrite_coalesce_expression(
        &mut self,
        wrapped: &Vec<Expression>,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let mut rewritten = wrapped
            .iter()
            .enumerate()
            .map(|(i, e)| {
                self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    create_subquery,
                    &context.extension_with(PathEntry::Coalesce(i as u16)),
                )
            })
            .collect::<Vec<ExReturn>>();
        let mut exr = ExReturn::new();
        for e in rewritten.iter_mut() {
            exr.with_is_subquery(e);
        }
        if rewritten.iter().all(|x| {
            x.expression.is_some() && x.change_type.as_ref().unwrap() == &ChangeType::NoChange
        }) {
            {
                exr.with_expression(Expression::Coalesce(
                    rewritten
                        .iter_mut()
                        .map(|x| x.expression.take().unwrap())
                        .collect(),
                ))
                .with_change_type(ChangeType::NoChange);
                return exr;
            }
        }
        self.project_all_static_variables(rewritten.iter().collect(), context);
        exr
    }
}

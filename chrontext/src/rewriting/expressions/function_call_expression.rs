use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::{Expression, Function};
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub fn rewrite_function_call_expression(
        &mut self,
        fun: &Function,
        args: &Vec<Expression>,
        variables_in_scope: &HashSet<Variable>,
        create_subquery: bool,
        context: &Context,
    ) -> ExReturn {
        let mut args_rewritten = args
            .iter()
            .enumerate()
            .map(|(i, e)| {
                self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    create_subquery,
                    &context.extension_with(PathEntry::FunctionCall(i as u16)),
                )
            })
            .collect::<Vec<ExReturn>>();
        let mut exr = ExReturn::new();
        for arg in args_rewritten.iter_mut() {
            exr.with_is_subquery(arg);
        }
        if args_rewritten.iter().all(|x| {
            x.expression.is_some() && x.change_type.as_ref().unwrap() == &ChangeType::NoChange
        }) {
            exr.with_expression(Expression::FunctionCall(
                fun.clone(),
                args_rewritten
                    .iter_mut()
                    .map(|x| x.expression.take().unwrap())
                    .collect(),
            ))
            .with_change_type(ChangeType::NoChange);
            return exr;
        }
        self.project_all_static_variables(args_rewritten.iter().collect(), context);
        exr
    }
}

use crate::change_types::ChangeType;
use oxrdf::Literal;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::{Expression, OrderExpression};
use std::collections::HashSet;
use virtualized_query::pushdown_setting::PushdownSetting;
use virtualized_query::VirtualizedQuery;

pub(crate) struct RecursiveRewriteReturn {
    pub expression: Option<Expression>,
    pub change_type: Option<ChangeType>,
    pub lost_value: bool,
}

impl RecursiveRewriteReturn {
    fn new(
        expression: Option<Expression>,
        change_type: Option<ChangeType>,
        lost_value: bool,
    ) -> RecursiveRewriteReturn {
        RecursiveRewriteReturn {
            expression,
            change_type,
            lost_value,
        }
    }
    fn none(lost_value: bool) -> RecursiveRewriteReturn {
        RecursiveRewriteReturn {
            expression: None,
            change_type: None,
            lost_value,
        }
    }
}

pub(crate) fn rewrite_order_expressions(
    vq: &VirtualizedQuery,
    order_expressions: &Vec<OrderExpression>,
    context: &Context,
    pushdown_settings: &HashSet<PushdownSetting>,
) -> (Option<Vec<OrderExpression>>, bool) {
    let mut rewritten_order_expressions = vec![];
    let mut lost_value = false;
    for oe in order_expressions {
        let (e, desc) = match oe {
            OrderExpression::Asc(e) => (e, false),
            OrderExpression::Desc(e) => (e, true),
        };
        let rewrite = try_recursive_rewrite_expression(
            vq,
            &None,
            e,
            &ChangeType::NoChange,
            context,
            pushdown_settings,
        );
        lost_value = lost_value || rewrite.lost_value;
        if let Some(expression) = rewrite.expression {
            rewritten_order_expressions.push(if desc {
                OrderExpression::Desc(expression)
            } else {
                OrderExpression::Asc(expression)
            });
        } else {
            lost_value = true;
        }
    }
    if rewritten_order_expressions.is_empty() {
        return (None, lost_value);
    }
    return (Some(rewritten_order_expressions), lost_value);
}

pub(crate) fn rewrite_filter_expression(
    vq: &VirtualizedQuery,
    expression: &Expression,
    required_change_direction: &ChangeType,
    context: &Context,
    static_rewrite_conjunction: &Option<Vec<&Expression>>,
    pushdown_settings: &HashSet<PushdownSetting>,
) -> (Option<Expression>, bool) {
    let mut rewrite = try_recursive_rewrite_expression(
        vq,
        static_rewrite_conjunction,
        expression,
        required_change_direction,
        context,
        pushdown_settings,
    );
    (rewrite.expression.take(), rewrite.lost_value)
}

pub(crate) fn try_recursive_rewrite_expression(
    vq: &VirtualizedQuery,
    static_rewrite_conjunction: &Option<Vec<&Expression>>,
    expression: &Expression,
    required_change_direction: &ChangeType,
    context: &Context,
    pushdown_settings: &HashSet<PushdownSetting>,
) -> RecursiveRewriteReturn {
    if static_rewrite_conjunction.is_some()
        && static_rewrite_conjunction
            .as_ref()
            .unwrap()
            .contains(&expression)
    {
        return RecursiveRewriteReturn::new(
            Some(Expression::Literal(Literal::from(true))),
            Some(ChangeType::NoChange),
            false,
        );
    }

    match &expression {
        Expression::Literal(lit) => {
            return RecursiveRewriteReturn::new(
                Some(Expression::Literal(lit.clone())),
                Some(ChangeType::NoChange),
                false,
            );
        }
        Expression::Variable(v) => {
            if vq.has_equivalent_variable(v, context) {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Variable(v.clone())),
                    Some(ChangeType::NoChange),
                    false,
                );
            } else if vq
                .get_virtualized_variables()
                .into_iter()
                .find(|x| &x.variable == v)
                .is_some()
            {
                if pushdown_settings.contains(&PushdownSetting::ValueConditions) {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Variable(v.clone())),
                        Some(ChangeType::NoChange),
                        false,
                    );
                } else {
                    return RecursiveRewriteReturn::new(None, None, true);
                }
            } else {
                return RecursiveRewriteReturn::none(false);
            }
        }
        Expression::Or(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                static_rewrite_conjunction,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::OrLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                static_rewrite_conjunction,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::OrRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);
            match required_change_direction {
                ChangeType::Relaxed => {
                    if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                        if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Or(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::NoChange),
                                use_lost_value,
                            );
                        } else if (left_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || left_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed)
                            && (right_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Relaxed)
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Or(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::Relaxed),
                                use_lost_value,
                            );
                        }
                    }
                }
                ChangeType::Constrained => {
                    if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                        if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Or(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::NoChange),
                                use_lost_value,
                            );
                        } else if (left_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Constrained)
                            && (right_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Constrained)
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Or(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::Constrained),
                                use_lost_value,
                            );
                        }
                    } else if left_rewrite.expression.is_none()
                        && right_rewrite.expression.is_some()
                    {
                        if right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            || right_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Constrained
                        {
                            return RecursiveRewriteReturn::new(
                                Some(right_rewrite.expression.as_ref().unwrap().clone()),
                                Some(ChangeType::Constrained),
                                use_lost_value,
                            );
                        }
                    } else if left_rewrite.expression.is_some()
                        && right_rewrite.expression.is_none()
                    {
                        if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            || left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Constrained
                        {
                            return RecursiveRewriteReturn::new(
                                Some(left_rewrite.expression.as_ref().unwrap().clone()),
                                Some(ChangeType::Constrained),
                                use_lost_value,
                            );
                        }
                    }
                }
                ChangeType::NoChange => {
                    if left_rewrite.expression.is_some()
                        && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        && right_rewrite.expression.is_some()
                        && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::Or(
                                Box::new(left_rewrite.expression.take().unwrap()),
                                Box::new(right_rewrite.expression.take().unwrap()),
                            )),
                            Some(ChangeType::NoChange),
                            use_lost_value,
                        );
                    }
                }
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::And(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                static_rewrite_conjunction,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::AndLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                static_rewrite_conjunction,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::AndRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            match required_change_direction {
                ChangeType::Constrained => {
                    if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                        if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::And(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::NoChange),
                                use_lost_value,
                            );
                        } else if (left_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Constrained)
                            && (right_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Constrained)
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::And(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::Constrained),
                                use_lost_value,
                            );
                        }
                    }
                }
                ChangeType::Relaxed => {
                    if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                        if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::And(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::NoChange),
                                use_lost_value,
                            );
                        } else if (left_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || left_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed)
                            && (right_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Relaxed)
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::And(
                                    Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Box::new(right_rewrite.expression.as_ref().unwrap().clone()),
                                )),
                                Some(ChangeType::Relaxed),
                                use_lost_value,
                            );
                        }
                    } else if left_rewrite.expression.is_none()
                        && right_rewrite.expression.is_some()
                    {
                        if right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            || right_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed
                        {
                            return RecursiveRewriteReturn::new(
                                Some(right_rewrite.expression.as_ref().unwrap().clone()),
                                Some(ChangeType::Relaxed),
                                use_lost_value,
                            );
                        }
                    } else if left_rewrite.expression.is_some()
                        && right_rewrite.expression.is_none()
                    {
                        if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            || left_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed
                        {
                            return RecursiveRewriteReturn::new(
                                Some(left_rewrite.expression.as_ref().unwrap().clone()),
                                Some(ChangeType::Relaxed),
                                use_lost_value,
                            );
                        }
                    }
                }
                ChangeType::NoChange => {
                    if left_rewrite.expression.is_some()
                        && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        && right_rewrite.expression.is_some()
                        && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::And(
                                Box::new(left_rewrite.expression.take().unwrap()),
                                Box::new(right_rewrite.expression.take().unwrap()),
                            )),
                            Some(ChangeType::NoChange),
                            use_lost_value,
                        );
                    }
                }
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Equal(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                static_rewrite_conjunction,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::EqualLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                static_rewrite_conjunction,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::EqualRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Equal(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Greater(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::GreaterLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::GreaterRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Greater(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::GreaterOrEqual(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::GreaterOrEqualLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::GreaterOrEqualRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::GreaterOrEqual(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Less(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::LessLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::LessRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Less(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::LessOrEqual(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::LessOrEqualLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::LessOrEqualRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::LessOrEqual(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::In(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                &ChangeType::NoChange,
                &context.extension_with(PathEntry::InLeft),
                pushdown_settings,
            );

            let mut right_rewrites = right
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    try_recursive_rewrite_expression(
                        vq,
                        &None,
                        e,
                        required_change_direction,
                        &context.extension_with(PathEntry::InRight(i as u16)),
                        pushdown_settings,
                    )
                })
                .collect::<Vec<RecursiveRewriteReturn>>();
            let use_lost_value = right_rewrites
                .iter()
                .fold(left_rewrite.lost_value, |acc, elem| acc || elem.lost_value);
            if left_rewrite.change_type.as_ref().is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                if right_rewrites.iter().all(|x| x.expression.is_some()) {
                    if right_rewrites
                        .iter()
                        .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                    {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::In(
                                Box::new(left_rewrite.expression.take().unwrap()),
                                right_rewrites
                                    .iter_mut()
                                    .map(|x| x.expression.take().unwrap())
                                    .collect(),
                            )),
                            Some(ChangeType::NoChange),
                            use_lost_value,
                        );
                    }
                } else if required_change_direction == &ChangeType::Constrained
                    && right_rewrites.iter().any(|x| x.expression.is_some())
                {
                    let right_rewrites = right_rewrites
                        .into_iter()
                        .filter(|x| x.expression.is_some())
                        .collect::<Vec<RecursiveRewriteReturn>>();
                    if right_rewrites
                        .iter()
                        .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                    {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::In(
                                Box::new(left_rewrite.expression.take().unwrap()),
                                right_rewrites
                                    .into_iter()
                                    .map(|mut x| x.expression.take().unwrap())
                                    .collect(),
                            )),
                            Some(ChangeType::Constrained),
                            use_lost_value,
                        );
                    }
                }
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Add(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::AddLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::AddRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Add(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Subtract(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::SubtractLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::SubtractRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Subtract(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Multiply(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::MultiplyLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::MultiplyRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Multiply(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Divide(left, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::DivideLeft),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::DivideRight),
                pushdown_settings,
            );
            let use_lost_value = or_lost_value(vec![&left_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Divide(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::UnaryPlus(inner) => {
            let mut inner_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                inner,
                required_change_direction,
                &context.extension_with(PathEntry::UnaryPlus),
                pushdown_settings,
            );
            if inner_rewrite.change_type.is_some()
                && inner_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::UnaryPlus(Box::new(
                        inner_rewrite.expression.take().unwrap(),
                    ))),
                    Some(ChangeType::NoChange),
                    inner_rewrite.lost_value,
                );
            }
            RecursiveRewriteReturn::none(inner_rewrite.lost_value)
        }
        Expression::UnaryMinus(inner) => {
            let mut inner_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                inner,
                required_change_direction,
                &context.extension_with(PathEntry::UnaryMinus),
                pushdown_settings,
            );
            if inner_rewrite.expression.is_some()
                && inner_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::UnaryMinus(Box::new(
                        inner_rewrite.expression.take().unwrap(),
                    ))),
                    Some(ChangeType::NoChange),
                    inner_rewrite.lost_value,
                );
            }
            RecursiveRewriteReturn::none(inner_rewrite.lost_value)
        }
        Expression::Not(inner) => {
            let use_direction = match required_change_direction {
                ChangeType::Relaxed => ChangeType::Constrained,
                ChangeType::Constrained => ChangeType::Relaxed,
                ChangeType::NoChange => ChangeType::NoChange,
            };

            let mut inner_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                inner,
                &use_direction,
                &context.extension_with(PathEntry::Not),
                pushdown_settings,
            );
            if inner_rewrite.expression.is_some() {
                match inner_rewrite.change_type.as_ref().unwrap() {
                    ChangeType::Relaxed => {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::Not(Box::new(
                                inner_rewrite.expression.take().unwrap(),
                            ))),
                            Some(ChangeType::Constrained),
                            inner_rewrite.lost_value,
                        );
                    }
                    ChangeType::Constrained => {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::Not(Box::new(
                                inner_rewrite.expression.take().unwrap(),
                            ))),
                            Some(ChangeType::Relaxed),
                            inner_rewrite.lost_value,
                        );
                    }
                    ChangeType::NoChange => {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::Not(Box::new(
                                inner_rewrite.expression.take().unwrap(),
                            ))),
                            Some(ChangeType::NoChange),
                            inner_rewrite.lost_value,
                        );
                    }
                }
            }
            RecursiveRewriteReturn::none(inner_rewrite.lost_value)
        }
        Expression::If(left, middle, right) => {
            let mut left_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                left,
                required_change_direction,
                &context.extension_with(PathEntry::IfLeft),
                pushdown_settings,
            );
            let mut middle_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                middle,
                required_change_direction,
                &context.extension_with(PathEntry::IfMiddle),
                pushdown_settings,
            );
            let mut right_rewrite = try_recursive_rewrite_expression(
                vq,
                &None,
                right,
                required_change_direction,
                &context.extension_with(PathEntry::IfRight),
                pushdown_settings,
            );
            let use_lost_value =
                or_lost_value(vec![&left_rewrite, &middle_rewrite, &right_rewrite]);

            if left_rewrite.expression.is_some()
                && middle_rewrite.expression.is_some()
                && right_rewrite.expression.is_some()
                && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && middle_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            {
                return RecursiveRewriteReturn::new(
                    Some(Expression::If(
                        Box::new(left_rewrite.expression.take().unwrap()),
                        Box::new(middle_rewrite.expression.take().unwrap()),
                        Box::new(right_rewrite.expression.take().unwrap()),
                    )),
                    Some(ChangeType::NoChange),
                    use_lost_value,
                );
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::Coalesce(inner) => {
            let inner_rewrites = inner
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    try_recursive_rewrite_expression(
                        vq,
                        &None,
                        e,
                        required_change_direction,
                        &context.extension_with(PathEntry::Coalesce(i as u16)),
                        pushdown_settings,
                    )
                })
                .collect::<Vec<RecursiveRewriteReturn>>();
            let use_lost_value = or_lost_value(inner_rewrites.iter().collect());
            if inner_rewrites.iter().all(|x| x.expression.is_some()) {
                if inner_rewrites
                    .iter()
                    .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Coalesce(
                            inner_rewrites
                                .into_iter()
                                .map(|mut x| x.expression.take().unwrap())
                                .collect(),
                        )),
                        Some(ChangeType::NoChange),
                        use_lost_value,
                    );
                }
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        Expression::FunctionCall(left, right) => {
            let right_rewrites = right
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    try_recursive_rewrite_expression(
                        vq,
                        &None,
                        e,
                        required_change_direction,
                        &context.extension_with(PathEntry::FunctionCall(i as u16)),
                        pushdown_settings,
                    )
                })
                .collect::<Vec<RecursiveRewriteReturn>>();
            let use_lost_value = or_lost_value(right_rewrites.iter().collect());
            if right_rewrites.iter().all(|x| x.expression.is_some()) {
                if right_rewrites
                    .iter()
                    .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                {
                    let use_lost_value =
                        right_rewrites.iter().fold(false, |b, x| b || x.lost_value);
                    return RecursiveRewriteReturn::new(
                        Some(Expression::FunctionCall(
                            left.clone(),
                            right_rewrites
                                .into_iter()
                                .map(|mut x| x.expression.take().unwrap())
                                .collect(),
                        )),
                        Some(ChangeType::NoChange),
                        use_lost_value,
                    );
                }
            }
            RecursiveRewriteReturn::none(use_lost_value)
        }
        _ => RecursiveRewriteReturn::none(false),
    }
}

fn or_lost_value(rewrites: Vec<&RecursiveRewriteReturn>) -> bool {
    rewrites.iter().fold(false, |a, b| a || b.lost_value)
}

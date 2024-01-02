use crate::constants::{
    DATA_POINT_SUFFIX, DATETIME_AS_SECONDS, HAS_DATA_POINT, HAS_TIMESERIES, HAS_TIMESTAMP,
    HAS_VALUE, MODULUS, SECONDS_AS_DATETIME, TIMESTAMP_VARIABLE_NAME, VALUE_SUFFIX,
};
use crate::constraints::{Constraint, VariableConstraints};
use crate::find_query_variables::{
    find_all_used_variables_in_aggregate_expression, find_all_used_variables_in_expression,
};
use crate::query_context::{Context, PathEntry};
use crate::timeseries_database::DatabaseType;
use chrono::{DateTime, Utc};
use oxrdf::vocab::{rdfs, xsd};
use oxrdf::{Literal, NamedNode};
use spargebra::algebra::{
    AggregateExpression, Expression, Function, GraphPattern, OrderExpression,
    PropertyPathExpression,
};
use spargebra::term::{BlankNode, NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::treehouse::{
    AggregationOperation, DataTreehousePattern, SimpleTimestampExpression, TimestampBinaryOperator,
    TimestampExpression,
};
use spargebra::Query;
use std::collections::{HashMap, HashSet};

pub struct SyntacticSugarRemover {
    database_type: DatabaseType,
}

impl SyntacticSugarRemover {
    pub fn new(database_type: DatabaseType) -> SyntacticSugarRemover {
        SyntacticSugarRemover { database_type }
    }

    pub fn remove_sugar(&self, select_query: Query) -> Query {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = select_query
        {
            let gp = self.remove_sugar_from_graph_pattern(pattern, &Context::new());
            let new_query = Query::Select {
                dataset,
                pattern: gp.gp.unwrap(),
                base_iri,
            };
            new_query
        } else {
            panic!("Should only be called with Select")
        }
    }

    fn remove_sugar_from_graph_pattern(
        &self,
        graph_pattern: GraphPattern,
        context: &Context,
    ) -> RemoveSugarGraphPatternReturn {
        match graph_pattern {
            GraphPattern::Bgp { .. } | GraphPattern::Path {..} => {
                RemoveSugarGraphPatternReturn::from_pattern(graph_pattern)
            }
            GraphPattern::Join { left, right } => {
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    &context.extension_with(PathEntry::JoinLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    &context.extension_with(PathEntry::JoinRightSide),
                );

                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Join {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    &context.extension_with(PathEntry::LeftJoinLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    &context.extension_with(PathEntry::LeftJoinRightSide),
                );
                let preprocessed_expression = if let Some(e) = expression {
                    Some(self.remove_sugar_from_expression(
                        e,
                        &context.extension_with(PathEntry::LeftJoinExpression),
                    ))
                } else {
                    None
                };
                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::LeftJoin {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                    expression: preprocessed_expression,
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::Filter { expr, inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::FilterInner),
                );
                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Filter {
                    inner: Box::new(inner.gp.take().unwrap()),
                    expr: self.remove_sugar_from_expression(
                        expr,
                        &context.extension_with(PathEntry::FilterExpression),
                    ),
                });
                out.projections_from(&mut inner);
                out
            }
            GraphPattern::Union { left, right } => {
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    &context.extension_with(PathEntry::UnionLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    &context.extension_with(PathEntry::UnionRightSide),
                );
                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Union {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::Graph { name, inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::GraphInner),
                );
                inner.gp = Some(GraphPattern::Graph {
                    inner: Box::new(inner.gp.unwrap()),
                    name: name.clone(),
                });
                inner
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::ExtendInner),
                );

                inner.gp = Some(GraphPattern::Extend {
                    inner: Box::new(inner.gp.unwrap()),
                    variable: variable.clone(),
                    expression: self.remove_sugar_from_expression(
                        expression,
                        &context.extension_with(PathEntry::ExtendExpression),
                    ),
                });
                inner
            }
            GraphPattern::Minus { left, right } => {
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    &context.extension_with(PathEntry::MinusLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    &context.extension_with(PathEntry::MinusRightSide),
                );

                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Minus {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Values {
                variables: variables.clone(),
                bindings: bindings.clone(),
            }),
            GraphPattern::OrderBy { inner, expression } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::OrderByInner),
                );
                inner.gp = Some(GraphPattern::OrderBy {
                    inner: Box::new(inner.gp.unwrap()),
                    expression: expression
                        .into_iter()
                        .enumerate()
                        .map(|(i, oe)| {
                            self.remove_sugar_from_order_expression(
                                oe,
                                &context.extension_with(PathEntry::OrderByExpression(i as u16)),
                            )
                        })
                        .collect(),
                });
                inner
            }
            GraphPattern::Project { inner, mut variables } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::ProjectInner),
                );
                for v in inner.vars_to_project.drain(..) {
                    if !variables.contains(&v) {
                        variables.push(v);
                    }
                }
                inner.gp = Some(GraphPattern::Project {
                    inner: Box::new(inner.gp.unwrap()),
                    variables,
                });
                inner
            }
            GraphPattern::Distinct { inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::DistinctInner),
                );
                inner.gp = Some(GraphPattern::Distinct {
                    inner: Box::new(inner.gp.unwrap()),
                });
                inner
            }
            GraphPattern::Reduced { inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::ReducedInner),
                );
                inner.gp = Some(GraphPattern::Reduced {
                    inner: Box::new(inner.gp.unwrap()),
                });
                inner
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::SliceInner),
                );
                inner.gp = Some(GraphPattern::Slice {
                    inner: Box::new(inner.gp.unwrap()),
                    start,
                    length,
                });
                inner
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::GroupInner),
                );
                let mut preprocessed_aggregates = vec![];
                for (i, (var, agg)) in aggregates.into_iter().enumerate() {
                    preprocessed_aggregates.push((
                        var.clone(),
                        self.remove_sugar_from_aggregate_expression(
                            agg,
                            &context.extension_with(PathEntry::GroupAggregation(i as u16)),
                        ),
                    ))
                }
                for (v,a) in inner.aggregations.drain(..) {
                    preprocessed_aggregates.push((v,a))
                }

                let mut variables = variables;
                for v in inner.vars_to_group_by.drain(..) {
                    if !variables.contains(&v) {
                        variables.push(v);
                    }
                }
                inner.gp = Some(GraphPattern::Group {
                    inner: Box::new(inner.gp.unwrap()),
                    variables: variables,
                    aggregates: preprocessed_aggregates,
                });
                inner
            }
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    &context.extension_with(PathEntry::ServiceInner),
                );
                inner.gp = Some(GraphPattern::Service {
                    inner: Box::new(inner.gp.unwrap()),
                    name: name,
                    silent: silent,
                });
                inner
            }
            GraphPattern::DT { dt } => {
                let mut ret = dt_to_ret(dt, &self.database_type);
                let mut ret_new = self.remove_sugar_from_graph_pattern(ret.gp.take().unwrap(), context);
                ret_new.projections_from(&mut ret);
                ret_new
            }
        }
    }

    fn remove_sugar_from_expression(&self, expression: Expression, context: &Context) -> Expression {
        match expression {
            Expression::Or(left, right) => Expression::Or(
                Box::new(
                    self.remove_sugar_from_expression(*left, &context.extension_with(PathEntry::OrLeft)),
                ),
                Box::new(
                    self.remove_sugar_from_expression(*right, &context.extension_with(PathEntry::OrRight)),
                ),
            ),
            Expression::And(left, right) => Expression::And(
                Box::new(
                    self.remove_sugar_from_expression(*left, &context.extension_with(PathEntry::AndLeft)),
                ),
                Box::new(
                    self.remove_sugar_from_expression(*right, &context.extension_with(PathEntry::AndRight)),
                ),
            ),
            Expression::Not(inner) => Expression::Not(Box::new(
                self.remove_sugar_from_expression(*inner, &context.extension_with(PathEntry::Not)),
            )),
            Expression::Exists(graph_pattern) => Expression::Exists(Box::new(
                self.remove_sugar_from_graph_pattern(
                    *graph_pattern,
                    &context.extension_with(PathEntry::Exists),
                )
                .gp
                .unwrap(),
            )),
            Expression::If(left, middle, right) => Expression::If(
                Box::new(
                    self.remove_sugar_from_expression(*left, &context.extension_with(PathEntry::IfLeft)),
                ),
                Box::new(
                    self.remove_sugar_from_expression(
                        *middle,
                        &context.extension_with(PathEntry::IfMiddle),
                    ),
                ),
                Box::new(
                    self.remove_sugar_from_expression(*right, &context.extension_with(PathEntry::IfRight)),
                ),
            ),
            _ => expression,
        }
    }

    fn remove_sugar_from_aggregate_expression(
        &self,
        aggregate_expression: AggregateExpression,
        context: &Context,
    ) -> AggregateExpression {
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                let desugared_expression = if let Some(e) = expr {
                    Some(Box::new(self.remove_sugar_from_expression(
                        *e,
                        &context.extension_with(PathEntry::AggregationOperation),
                    )))
                } else {
                    None
                };
                AggregateExpression::Count {
                    expr: desugared_expression,
                    distinct,
                }
            }
            AggregateExpression::Sum { expr, distinct } => AggregateExpression::Sum {
                expr: Box::new(self.remove_sugar_from_expression(
                    *expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                )),
                distinct,
            },
            AggregateExpression::Avg { expr, distinct } => AggregateExpression::Avg {
                expr: Box::new(self.remove_sugar_from_expression(
                    *expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                )),
                distinct,
            },
            AggregateExpression::Min { expr, distinct } => AggregateExpression::Min {
                expr: Box::new(self.remove_sugar_from_expression(
                    *expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                )),
                distinct,
            },
            AggregateExpression::Max { expr, distinct } => AggregateExpression::Max {
                expr: Box::new(self.remove_sugar_from_expression(
                    *expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                )),
                distinct,
            },
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => AggregateExpression::GroupConcat {
                expr: Box::new(self.remove_sugar_from_expression(
                    *expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                )),
                distinct,
                separator,
            },
            AggregateExpression::Sample { expr, distinct } => AggregateExpression::Sample {
                expr: Box::new(self.remove_sugar_from_expression(
                    *expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                )),
                distinct,
            },
            AggregateExpression::Custom {
                name,
                expr,
                distinct,
            } => AggregateExpression::Custom {
                name,
                expr: Box::new(self.remove_sugar_from_expression(
                    *expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                )),
                distinct,
            },
        }
    }
    fn remove_sugar_from_order_expression(
        &self,
        order_expression: OrderExpression,
        context: &Context,
    ) -> OrderExpression {
        match order_expression {
            OrderExpression::Asc(e) => {
                OrderExpression::Asc(self.remove_sugar_from_expression(
                    e,
                    &context.extension_with(PathEntry::OrderingOperation),
                ))
            }
            OrderExpression::Desc(e) => {
                OrderExpression::Desc(self.remove_sugar_from_expression(
                    e,
                    &context.extension_with(PathEntry::OrderingOperation),
                ))
            }
        }
    }
}

fn dt_to_ret(
    dt: DataTreehousePattern,
    database_type: &DatabaseType,
) -> RemoveSugarGraphPatternReturn {
    let DataTreehousePattern {
        timeseries,
        labels,
        values,
        timestamp,
        from,
        to,
        aggregation,
        filter,
        interval } = dt;

    let mut patterns = vec![];
    let timestamp = timestamp.unwrap_or(Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME));
    let mut vars_to_project = vec![timestamp.clone()];

    let values = if let Some(values) = values {
        values
    } else {
        let mut values = vec![];
        for t in timeseries.as_ref().unwrap() {
            values.push((
                t.clone(),
                Variable::new_unchecked(format!("{}_{}", t.as_str(), VALUE_SUFFIX)),
            ));
        }
        values
    };
    if let Some(labels) = labels {
        for (v, l) in labels {
            patterns.push(TriplePattern {
                subject: v.clone().into(),
                predicate: rdfs::LABEL.into_owned().into(),
                object: l.to_owned().into(),
            });
        }
    }

    for (t, v) in &values {
        vars_to_project.push(v.clone());
        let dp = Variable::new_unchecked(format!("{}_{}", t.as_str(), DATA_POINT_SUFFIX));
        patterns.push(TriplePattern {
            subject: t.to_owned().into(),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_DATA_POINT)),
            object: dp.clone().into(),
        });
        patterns.push(TriplePattern {
            subject: dp.clone().into(),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_VALUE)),
            object: v.clone().into(),
        });
        patterns.push(TriplePattern {
            subject: dp.into(),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_TIMESTAMP)),
            object: timestamp.clone().into(),
        });
    }

    let mut gp = GraphPattern::Bgp { patterns };

    let mut from_ts_datetime_opt = None;
    let mut to_ts_datetime_opt = None;

    if let Some(from) = &from {
        from_ts_datetime_opt = eval_timestamp_expression(from, None);
    }
    if let Some(to) = &to {
        to_ts_datetime_opt = eval_timestamp_expression(to, None);
    }
    let from_ts_datetime_opt = if let Some(from) = from_ts_datetime_opt {
        Some(from)
    } else {
        if let Some(from) = &from {
            if let Some(to) = &to_ts_datetime_opt {
                eval_timestamp_expression(from, Some(to))
            } else {
                panic!()
            }
        } else {
            None
        }
    };

    let to_ts_datetime_opt = if let Some(to) = to_ts_datetime_opt {
        Some(to)
    } else {
        if let Some(to) = &to {
            if let Some(from) = &from_ts_datetime_opt {
                eval_timestamp_expression(to, Some(from))
            } else {
                panic!()
            }
        } else {
            None
        }
    };
    let mut expr = None;
    if let Some(to) = to_ts_datetime_opt {
        expr = Some(Expression::LessOrEqual(
            Box::new(Expression::Variable(timestamp.clone())),
            Box::new(datetime_to_literal(&to)),
        ));
    }

    if let Some(from) = to_ts_datetime_opt {
        let new_expr = Expression::GreaterOrEqual(
            Box::new(Expression::Variable(timestamp.clone())),
            Box::new(datetime_to_literal(&from)),
        );
        expr = if let Some(expr) = expr {
            Some(Expression::And(Box::new(expr), Box::new(new_expr)))
        } else {
            Some(new_expr)
        };
    }

    if let Some(filter) = filter {
        let expr = if let Some(expr) = expr {
            Expression::And(Box::new(expr), Box::new(filter))
        } else {
            filter
        };
        gp = GraphPattern::Filter {
            expr,
            inner: Box::new(gp),
        }
    } else if let Some(expr) = expr {
        gp = GraphPattern::Filter {
            expr,
            inner: Box::new(gp),
        }
    }

    let mut vars_to_group_by = vec![];

    if let Some(i) = interval {
        match database_type {
            DatabaseType::BigQuery => {
                let seconds = i.num_seconds();
                gp = GraphPattern::Extend {
                    inner: Box::new(gp),
                    variable: timestamp.clone(),
                    expression: Expression::FunctionCall(
                        Function::Custom(NamedNode::new_unchecked(SECONDS_AS_DATETIME)),
                        vec![Expression::Subtract(
                            Box::new(Expression::FunctionCall(
                                Function::Custom(NamedNode::new_unchecked(DATETIME_AS_SECONDS)),
                                vec![Expression::Variable(timestamp.clone())],
                            )),
                            Box::new(Expression::FunctionCall(
                                Function::Custom(NamedNode::new_unchecked(MODULUS)),
                                vec![
                                    Expression::FunctionCall(
                                        Function::Custom(NamedNode::new_unchecked(
                                            DATETIME_AS_SECONDS,
                                        )),
                                        vec![Expression::Variable(timestamp.clone())],
                                    ),
                                    Expression::Literal(Literal::from(seconds)),
                                ],
                            )),
                        )],
                    ),
                };
                vars_to_group_by.push(timestamp.clone());
            }
            _ => {
                panic!("Syntatic sugar not yet supported")
            }
        }
    }

    let mut aggregations = vec![];
    if let Some(agg) = aggregation {
        for (_, v) in values {
            let a = match agg {
                AggregationOperation::Avg => AggregateExpression::Avg {
                    expr: Box::new(Expression::Variable(v.clone())),
                    distinct: false,
                },
                AggregationOperation::Min => AggregateExpression::Min {
                    expr: Box::new(Expression::Variable(v.clone())),
                    distinct: false,
                },
                AggregationOperation::Max => AggregateExpression::Max {
                    expr: Box::new(Expression::Variable(v.clone())),
                    distinct: false,
                },
            };
            aggregations.push((v, a));
        }
    }
    RemoveSugarGraphPatternReturn {
        gp: Some(gp),
        aggregations,
        vars_to_group_by,
        vars_to_project,
    }
}

fn eval_timestamp_expression(
    te: &TimestampExpression,
    from_or_to_expression: Option<&DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match te {
        TimestampExpression::Simple(s) => {
            eval_simple_timestamp_expression(s, from_or_to_expression)
        }
        TimestampExpression::Binary(s, op, d) => {
            if let Some(dt) = eval_simple_timestamp_expression(s, from_or_to_expression) {
                match op {
                    TimestampBinaryOperator::Plus => Some(dt + d.clone()),
                    TimestampBinaryOperator::Minus => Some(dt - d.clone()),
                }
            } else {
                None
            }
        }
    }
}

fn eval_simple_timestamp_expression(
    se: &SimpleTimestampExpression,
    from_or_to_expression: Option<&DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match se {
        SimpleTimestampExpression::Now => Some(Utc::now()),
        SimpleTimestampExpression::From | SimpleTimestampExpression::To => {
            if let Some(from_or_to) = from_or_to_expression {
                Some(from_or_to.clone())
            } else {
                panic!()
            }
        }
        SimpleTimestampExpression::DateTime(dt) => Some(dt.clone()),
    }
}

fn datetime_to_literal(dt: &DateTime<Utc>) -> Expression {
    Expression::Literal(Literal::new_typed_literal(
        Utc::now().to_rfc3339(),
        xsd::DATE_TIME_STAMP,
    ))
}

struct RemoveSugarGraphPatternReturn {
    pub gp: Option<GraphPattern>,
    pub aggregations: Vec<(Variable, AggregateExpression)>,
    pub vars_to_group_by: Vec<Variable>,
    pub vars_to_project: Vec<Variable>,
}

impl RemoveSugarGraphPatternReturn {
    pub fn from_pattern(gp: GraphPattern) -> Self {
        RemoveSugarGraphPatternReturn {
            gp: Some(gp),
            aggregations: vec![],
            vars_to_project: vec![],
            vars_to_group_by: vec![],
        }
    }

    pub fn projections_from(&mut self, p: &mut RemoveSugarGraphPatternReturn) {
        self.aggregations.extend(p.aggregations.drain(..));
        self.vars_to_project.extend(p.vars_to_project.drain(..));
        self.vars_to_group_by.extend(p.vars_to_group_by.drain(..));
    }
}

use crate::constraints::{Constraint, VariableConstraints};
use oxrdf::NamedNode;
use query_processing::find_query_variables::{
    find_all_used_variables_in_aggregate_expression, find_all_used_variables_in_expression,
};
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{BlankNode, NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::Query;
use std::collections::{HashMap, HashSet};

pub struct Preprocessor {
    counter: u16,
    blank_node_rename: HashMap<BlankNode, Variable>,
    variable_constraints: VariableConstraints,
    virtual_predicate_iris: HashSet<NamedNode>,
    first_level_virtual_predicate_iris: HashSet<NamedNode>,
}

impl Preprocessor {
    pub fn new(
        virtual_predicate_iris: HashSet<NamedNode>,
        first_level_virtual_predicate_iris: HashSet<NamedNode>,
    ) -> Preprocessor {
        Preprocessor {
            counter: 0,
            blank_node_rename: Default::default(),
            variable_constraints: VariableConstraints::new(),
            virtual_predicate_iris,
            first_level_virtual_predicate_iris,
        }
    }

    pub fn preprocess(&mut self, select_query: &Query) -> (Query, VariableConstraints) {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = &select_query
        {
            let gp = self.preprocess_graph_pattern(&pattern, &Context::new());
            let map = self.variable_constraints.clone();
            let new_query = Query::Select {
                dataset: dataset.clone(),
                pattern: gp,
                base_iri: base_iri.clone(),
            };
            (new_query, map)
        } else {
            panic!("Should only be called with Select")
        }
    }

    fn preprocess_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        context: &Context,
    ) -> GraphPattern {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                let bgp_context = context.extension_with(PathEntry::BGP);
                let mut new_patterns = vec![];
                for p in patterns {
                    new_patterns.push(self.preprocess_triple_pattern(p, &bgp_context));
                }
                GraphPattern::Bgp {
                    patterns: new_patterns,
                }
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.preprocess_path(subject, path, object),
            GraphPattern::Join { left, right } => {
                let left = self.preprocess_graph_pattern(
                    left,
                    &context.extension_with(PathEntry::JoinLeftSide),
                );
                let right = self.preprocess_graph_pattern(
                    right,
                    &context.extension_with(PathEntry::JoinRightSide),
                );
                GraphPattern::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let left = self.preprocess_graph_pattern(
                    left,
                    &context.extension_with(PathEntry::LeftJoinLeftSide),
                );
                let right = self.preprocess_graph_pattern(
                    right,
                    &context.extension_with(PathEntry::LeftJoinRightSide),
                );
                let preprocessed_expression = if let Some(e) = expression {
                    Some(self.preprocess_expression(
                        e,
                        &context.extension_with(PathEntry::LeftJoinExpression),
                    ))
                } else {
                    None
                };
                GraphPattern::LeftJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    expression: preprocessed_expression,
                }
            }
            GraphPattern::Filter { expr, inner } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::FilterInner),
                );
                GraphPattern::Filter {
                    inner: Box::new(inner),
                    expr: self.preprocess_expression(
                        expr,
                        &context.extension_with(PathEntry::FilterExpression),
                    ),
                }
            }
            GraphPattern::Union { left, right } => {
                let left = self.preprocess_graph_pattern(
                    left,
                    &context.extension_with(PathEntry::UnionLeftSide),
                );
                let right = self.preprocess_graph_pattern(
                    right,
                    &context.extension_with(PathEntry::UnionRightSide),
                );
                GraphPattern::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            GraphPattern::Graph { name, inner } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::GraphInner),
                );
                GraphPattern::Graph {
                    inner: Box::new(inner),
                    name: name.clone(),
                }
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::ExtendInner),
                );
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(expression, &mut used_vars, true, true);
                for v in used_vars.drain() {
                    if let Some(ctr) = self.variable_constraints.get_constraint(&v, context) {
                        if ctr == &Constraint::External || ctr == &Constraint::ExternallyDerived {
                            if !self.variable_constraints.contains(variable, context) {
                                self.variable_constraints.insert(
                                    variable.clone(),
                                    context.clone(),
                                    Constraint::ExternallyDerived,
                                );
                            }
                        }
                    }
                }

                GraphPattern::Extend {
                    inner: Box::new(inner),
                    variable: variable.clone(),
                    expression: self.preprocess_expression(
                        expression,
                        &context.extension_with(PathEntry::ExtendExpression),
                    ),
                }
            }
            GraphPattern::Minus { left, right } => {
                let left = self.preprocess_graph_pattern(
                    left,
                    &context.extension_with(PathEntry::MinusLeftSide),
                );
                let right = self.preprocess_graph_pattern(
                    right,
                    &context.extension_with(PathEntry::MinusRightSide),
                );
                GraphPattern::Minus {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => GraphPattern::Values {
                variables: variables.clone(),
                bindings: bindings.clone(),
            },
            GraphPattern::OrderBy { inner, expression } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::OrderByInner),
                );
                GraphPattern::OrderBy {
                    inner: Box::new(inner),
                    expression: expression
                        .iter()
                        .enumerate()
                        .map(|(i, oe)| {
                            self.preprocess_order_expression(
                                oe,
                                &context.extension_with(PathEntry::OrderByExpression(i as u16)),
                            )
                        })
                        .collect(),
                }
            }
            GraphPattern::Project { inner, variables } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::ProjectInner),
                );
                GraphPattern::Project {
                    inner: Box::new(inner),
                    variables: variables.clone(),
                }
            }
            GraphPattern::Distinct { inner } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::DistinctInner),
                );
                GraphPattern::Distinct {
                    inner: Box::new(inner),
                }
            }
            GraphPattern::Reduced { inner } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::ReducedInner),
                );
                GraphPattern::Reduced {
                    inner: Box::new(inner),
                }
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::SliceInner),
                );
                GraphPattern::Slice {
                    inner: Box::new(inner),
                    start: start.clone(),
                    length: length.clone(),
                }
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::GroupInner),
                );
                for (variable, agg) in aggregates {
                    let mut used_vars = HashSet::new();
                    find_all_used_variables_in_aggregate_expression(
                        agg,
                        &mut used_vars,
                        false,
                        false,
                    );
                    for v in used_vars.drain() {
                        if let Some(ctr) = self.variable_constraints.get_constraint(&v, context) {
                            if ctr == &Constraint::External || ctr == &Constraint::ExternallyDerived
                            {
                                self.variable_constraints.insert(
                                    variable.clone(),
                                    context.clone(),
                                    Constraint::ExternallyDerived,
                                );
                            }
                        }
                    }
                }
                let mut preprocessed_aggregates = vec![];
                for (i, (var, agg)) in aggregates.iter().enumerate() {
                    preprocessed_aggregates.push((
                        var.clone(),
                        self.preprocess_aggregate_expression(
                            agg,
                            &context.extension_with(PathEntry::GroupAggregation(i as u16)),
                        ),
                    ))
                }
                GraphPattern::Group {
                    inner: Box::new(inner),
                    variables: variables.clone(),
                    aggregates: preprocessed_aggregates,
                }
            }
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => {
                let inner = self.preprocess_graph_pattern(
                    inner,
                    &context.extension_with(PathEntry::ServiceInner),
                );
                GraphPattern::Service {
                    inner: Box::new(inner),
                    name: name.clone(),
                    silent: silent.clone(),
                }
            }
            GraphPattern::DT { .. } => {
                panic!()
            }
            GraphPattern::PValues { .. } => {
                todo!("Not currently supported")
            }
        }
    }

    fn preprocess_triple_pattern(
        &mut self,
        triple_pattern: &TriplePattern,
        context: &Context,
    ) -> TriplePattern {
        let new_subject = self.rename_if_blank(&triple_pattern.subject);
        let new_object = self.rename_if_blank(&triple_pattern.object);
        if let NamedNodePattern::NamedNode(named_predicate_node) = &triple_pattern.predicate {
            if let (
                TermPattern::Variable(new_subject_variable),
                TermPattern::Variable(new_object_variable),
            ) = (&new_subject, &new_object)
            {
                if self.virtual_predicate_iris.contains(named_predicate_node) {
                    self.variable_constraints.insert(
                        new_object_variable.clone(),
                        context.clone(),
                        Constraint::External,
                    );
                    if !self
                        .first_level_virtual_predicate_iris
                        .contains(named_predicate_node)
                    {
                        self.variable_constraints.insert(
                            new_subject_variable.clone(),
                            context.clone(),
                            Constraint::External,
                        );
                    }
                }
            }
        }
        return TriplePattern {
            subject: new_subject,
            predicate: triple_pattern.predicate.clone(),
            object: new_object,
        };
    }

    fn rename_if_blank(&mut self, term_pattern: &TermPattern) -> TermPattern {
        if let TermPattern::BlankNode(bn) = term_pattern {
            if let Some(var) = self.blank_node_rename.get(bn) {
                TermPattern::Variable(var.clone())
            } else {
                let var = Variable::new(
                    "blank_replacement_".to_string() + self.counter.to_string().as_str(),
                )
                .expect("Name is ok");
                self.counter += 1;
                self.blank_node_rename.insert(bn.clone(), var.clone());
                TermPattern::Variable(var)
            }
        } else {
            term_pattern.clone()
        }
    }

    fn preprocess_expression(&mut self, expression: &Expression, context: &Context) -> Expression {
        match expression {
            Expression::Or(left, right) => Expression::Or(
                Box::new(
                    self.preprocess_expression(left, &context.extension_with(PathEntry::OrLeft)),
                ),
                Box::new(
                    self.preprocess_expression(right, &context.extension_with(PathEntry::OrRight)),
                ),
            ),
            Expression::And(left, right) => Expression::And(
                Box::new(
                    self.preprocess_expression(left, &context.extension_with(PathEntry::AndLeft)),
                ),
                Box::new(
                    self.preprocess_expression(right, &context.extension_with(PathEntry::AndRight)),
                ),
            ),
            Expression::Not(inner) => Expression::Not(Box::new(
                self.preprocess_expression(inner, &context.extension_with(PathEntry::Not)),
            )),
            Expression::Exists(graph_pattern) => {
                Expression::Exists(Box::new(self.preprocess_graph_pattern(
                    graph_pattern,
                    &context.extension_with(PathEntry::Exists),
                )))
            }
            Expression::If(left, middle, right) => Expression::If(
                Box::new(
                    self.preprocess_expression(left, &context.extension_with(PathEntry::IfLeft)),
                ),
                Box::new(
                    self.preprocess_expression(
                        middle,
                        &context.extension_with(PathEntry::IfMiddle),
                    ),
                ),
                Box::new(
                    self.preprocess_expression(right, &context.extension_with(PathEntry::IfRight)),
                ),
            ),
            _ => expression.clone(),
        }
    }

    fn preprocess_path(
        &mut self,
        subject: &TermPattern,
        path: &PropertyPathExpression,
        object: &TermPattern,
    ) -> GraphPattern {
        let new_subject = self.rename_if_blank(subject);
        let new_object = self.rename_if_blank(object);
        GraphPattern::Path {
            subject: new_subject,
            path: path.clone(),
            object: new_object,
        }
    }
    fn preprocess_aggregate_expression(
        &mut self,
        aggregate_expression: &AggregateExpression,
        context: &Context,
    ) -> AggregateExpression {
        match aggregate_expression {
            AggregateExpression::CountSolutions { distinct } => {
                AggregateExpression::CountSolutions {
                    distinct: distinct.clone(),
                }
            }
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct,
            } => {
                let rewritten_expression = self.preprocess_expression(
                    expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
                AggregateExpression::FunctionCall {
                    name: name.clone(),
                    expr: rewritten_expression,
                    distinct: *distinct,
                }
            }
        }
    }
    fn preprocess_order_expression(
        &mut self,
        order_expression: &OrderExpression,
        context: &Context,
    ) -> OrderExpression {
        match order_expression {
            OrderExpression::Asc(e) => {
                OrderExpression::Asc(self.preprocess_expression(
                    e,
                    &context.extension_with(PathEntry::OrderingOperation),
                ))
            }
            OrderExpression::Desc(e) => {
                OrderExpression::Desc(self.preprocess_expression(
                    e,
                    &context.extension_with(PathEntry::OrderingOperation),
                ))
            }
        }
    }
}

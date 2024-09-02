use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashMap;

// Purpose is to rename vars so that they can be virtualized as columns in most dbs..
pub fn rename_query_vars(query: Query) -> (Query, HashMap<String, String>) {
    let mut map = HashMap::new();
    let q = match query {
        Query::Select {
            dataset,
            pattern: q,
            base_iri,
        } => Query::Select {
            pattern: rename_gp_vars(q, &mut map),
            dataset,
            base_iri,
        },
        _ => unimplemented!("Not supported by chrontext"),
    };
    (q, map)
}

fn rename_gp_vars(gp: GraphPattern, rename_map: &mut HashMap<String, String>) -> GraphPattern {
    match gp {
        GraphPattern::Bgp { patterns } => GraphPattern::Bgp {
            patterns: rename_triple_patterns_vars(patterns, rename_map),
        },
        GraphPattern::Path {
            subject,
            path,
            object,
        } => GraphPattern::Path {
            subject: rename_term_pattern(subject, rename_map),
            path,
            object: rename_term_pattern(object, rename_map),
        },
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rename_gp_vars(*left, rename_map)),
            right: Box::new(rename_gp_vars(*right, rename_map)),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rename_gp_vars(*left, rename_map)),
            right: Box::new(rename_gp_vars(*right, rename_map)),
            expression: if let Some(expression) = expression {
                Some(rename_expression_vars(expression, rename_map))
            } else {
                None
            },
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr: rename_expression_vars(expr, rename_map),
            inner: Box::new(rename_gp_vars(*inner, rename_map)),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rename_gp_vars(*left, rename_map)),
            right: Box::new(rename_gp_vars(*right, rename_map)),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph { name, inner },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rename_gp_vars(*inner, rename_map)),
            variable: maybe_rename_variable(variable, rename_map),
            expression: rename_expression_vars(expression, rename_map),
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rename_gp_vars(*left, rename_map)),
            right: Box::new(rename_gp_vars(*right, rename_map)),
        },
        GraphPattern::Values {
            variables,
            bindings,
        } => GraphPattern::Values {
            variables: variables
                .into_iter()
                .map(|x| maybe_rename_variable(x, rename_map))
                .collect(),
            bindings,
        },
        GraphPattern::PValues { .. } => unimplemented!("Not supported by chrontext"),
        GraphPattern::OrderBy { inner, expression } => {
            let mut new_expressions = Vec::with_capacity(expression.len());
            for e in expression {
                new_expressions.push(match e {
                    OrderExpression::Asc(e) => {
                        OrderExpression::Asc(rename_expression_vars(e, rename_map))
                    }
                    OrderExpression::Desc(e) => {
                        OrderExpression::Desc(rename_expression_vars(e, rename_map))
                    }
                })
            }
            GraphPattern::OrderBy {
                inner: Box::new(rename_gp_vars(*inner, rename_map)),
                expression: new_expressions,
            }
        }
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rename_gp_vars(*inner, rename_map)),
            variables: variables
                .into_iter()
                .map(|x| maybe_rename_variable(x, rename_map))
                .collect(),
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rename_gp_vars(*inner, rename_map)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rename_gp_vars(*inner, rename_map)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rename_gp_vars(*inner, rename_map)),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let mut new_aggregates = Vec::with_capacity(aggregates.len());
            for (mut v, mut a) in aggregates {
                v = maybe_rename_variable(v, rename_map);
                a = match a {
                    AggregateExpression::FunctionCall {
                        name,
                        expr,
                        distinct,
                    } => AggregateExpression::FunctionCall {
                        name,
                        expr: rename_expression_vars(expr, rename_map),
                        distinct,
                    },
                    a => a,
                };
                new_aggregates.push((v, a));
            }
            GraphPattern::Group {
                inner: Box::new(rename_gp_vars(*inner, rename_map)),
                variables: variables
                    .into_iter()
                    .map(|x| maybe_rename_variable(x, rename_map))
                    .collect(),
                aggregates: new_aggregates,
            }
        }
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => GraphPattern::Service {
            name,
            inner: Box::new(rename_gp_vars(*inner, rename_map)),
            silent,
        },
        GraphPattern::DT { .. } => unimplemented!("Should not reach chrontext"),
    }
}

fn rename_expression_vars(
    expression: Expression,
    rename_map: &mut HashMap<String, String>,
) -> Expression {
    match expression {
        Expression::NamedNode(nn) => Expression::NamedNode(nn),
        Expression::Literal(l) => Expression::Literal(l),
        Expression::Variable(v) => Expression::Variable(maybe_rename_variable(v, rename_map)),
        Expression::Or(left, right) => Expression::Or(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::And(left, right) => Expression::And(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::Equal(left, right) => Expression::Equal(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::SameTerm(left, right) => Expression::SameTerm(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::Greater(left, right) => Expression::Greater(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::GreaterOrEqual(left, right) => Expression::GreaterOrEqual(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::Less(left, right) => Expression::Less(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::LessOrEqual(left, right) => Expression::LessOrEqual(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::In(left, exprs) => Expression::In(
            Box::new(rename_expression_vars(*left, rename_map)),
            exprs
                .into_iter()
                .map(|x| rename_expression_vars(x, rename_map))
                .collect(),
        ),
        Expression::Add(left, right) => Expression::Add(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::Subtract(left, right) => Expression::Subtract(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::Multiply(left, right) => Expression::Multiply(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::Divide(left, right) => Expression::Divide(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::UnaryPlus(expr) => {
            Expression::UnaryPlus(Box::new(rename_expression_vars(*expr, rename_map)))
        }
        Expression::UnaryMinus(expr) => {
            Expression::UnaryMinus(Box::new(rename_expression_vars(*expr, rename_map)))
        }
        Expression::Not(expr) => {
            Expression::Not(Box::new(rename_expression_vars(*expr, rename_map)))
        }
        Expression::Exists(gp) => Expression::Exists(Box::new(rename_gp_vars(*gp, rename_map))),
        Expression::Bound(v) => Expression::Bound(maybe_rename_variable(v, rename_map)),
        Expression::If(left, middle, right) => Expression::If(
            Box::new(rename_expression_vars(*left, rename_map)),
            Box::new(rename_expression_vars(*middle, rename_map)),
            Box::new(rename_expression_vars(*right, rename_map)),
        ),
        Expression::Coalesce(exprs) => Expression::Coalesce(
            exprs
                .into_iter()
                .map(|x| rename_expression_vars(x, rename_map))
                .collect(),
        ),
        Expression::FunctionCall(func, args) => Expression::FunctionCall(
            func,
            args.into_iter()
                .map(|x| rename_expression_vars(x, rename_map))
                .collect(),
        ),
    }
}

fn maybe_rename_variable(v: Variable, rename_map: &mut HashMap<String, String>) -> Variable {
    if let Some(n) = rename_map.get(v.as_str()) {
        Variable::new_unchecked(n)
    } else if should_rename_var(&v) {
        let new_name = create_new_variable_name();
        rename_map.insert(v.as_str().to_string(), new_name);
        Variable::new(rename_map.get(v.as_str()).unwrap()).unwrap()
    } else {
        v
    }
}

fn rename_triple_patterns_vars(
    patterns: Vec<TriplePattern>,
    rename_map: &mut HashMap<String, String>,
) -> Vec<TriplePattern> {
    let mut out_patterns = Vec::with_capacity(patterns.len());
    for p in patterns {
        let subject = rename_term_pattern(p.subject, rename_map);
        let predicate = rename_named_node_pattern(p.predicate, rename_map);
        let object = rename_term_pattern(p.object, rename_map);
        out_patterns.push(TriplePattern {
            subject,
            predicate,
            object,
        });
    }
    out_patterns
}

fn rename_term_pattern(
    term_pattern: TermPattern,
    rename_map: &mut HashMap<String, String>,
) -> TermPattern {
    match term_pattern {
        TermPattern::Variable(v) => TermPattern::Variable(maybe_rename_variable(v, rename_map)),
        tp => tp,
    }
}

fn rename_named_node_pattern(
    named_node_pattern: NamedNodePattern,
    rename_map: &mut HashMap<String, String>,
) -> NamedNodePattern {
    match named_node_pattern {
        NamedNodePattern::Variable(v) => {
            NamedNodePattern::Variable(maybe_rename_variable(v, rename_map))
        }
        nnp => nnp,
    }
}

fn create_new_variable_name() -> String {
    let mut vn = uuid::Uuid::new_v4().to_string();
    vn.replace_range(0..1, "c");
    vn = vn.replace("-", "_");
    vn
}

fn should_rename_var(v: &Variable) -> bool {
    !v.as_str().starts_with(|x: char| x.is_alphabetic())
}

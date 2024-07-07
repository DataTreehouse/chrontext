use oxrdf::Variable;
use representation::query_context::{Context, VariableInContext};

#[derive(PartialEq, Debug, Clone)]
pub enum Constraint {
    External,
    ExternallyDerived,
}

#[derive(Clone, Debug)]
pub struct VariableConstraints {
    variable_constraints: Vec<(VariableInContext, Constraint)>,
}

impl VariableConstraints {
    pub fn get_constraint(&self, variable: &Variable, context: &Context) -> Option<&Constraint> {
        let mut constraint = None;
        for (v, c) in &self.variable_constraints {
            if v.same_name(variable) && v.in_scope(context, true) {
                if constraint.is_none() {
                    constraint = Some(c);
                } else if constraint.unwrap() != c {
                    panic!(
                        "There should be only one type of constraint per variable {:?}, {:?}, {:?}",
                        v, constraint, c
                    );
                }
            }
        }
        constraint
    }

    pub fn contains(&self, variable: &Variable, context: &Context) -> bool {
        self.get_constraint(variable, context).is_some()
    }

    pub fn insert(&mut self, variable: Variable, context: Context, constraint: Constraint) {
        self.variable_constraints
            .push((VariableInContext::new(variable, context), constraint));
    }

    pub fn new() -> VariableConstraints {
        return VariableConstraints {
            variable_constraints: vec![],
        };
    }
}

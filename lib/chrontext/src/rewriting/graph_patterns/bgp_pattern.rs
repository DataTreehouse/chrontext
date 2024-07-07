use super::StaticQueryRewriter;
use crate::constants::{HAS_EXTERNAL_ID, HAS_RESOURCE};
use crate::constraints::{Constraint, VariableConstraints};
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::{NamedNode, Variable};
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use virtualized_query::BasicVirtualizedQuery;

impl StaticQueryRewriter {
    pub(crate) fn rewrite_bgp(
        &mut self,
        patterns: &Vec<TriplePattern>,
        context: &Context,
    ) -> GPReturn {
        let context = context.extension_with(PathEntry::BGP);
        let mut new_triples = vec![];
        let mut dynamic_triples = vec![];
        let mut resources_in_scope = HashMap::new();
        let mut external_ids_in_scope = HashMap::new();
        let mut new_basic_vqs = vec![];
        for t in patterns {
            if let NamedNodePattern::NamedNode(nn) = &t.predicate {
                if self.first_level_virtualized_predicates.contains(nn) {
                    if let TermPattern::Variable(subj_var) = &t.subject {
                        if !external_ids_in_scope.contains_key(subj_var) {
                            let external_id_var = Variable::new(
                                "ts_external_id_".to_string()
                                    + self.variable_counter.to_string().as_str(),
                            )
                            .unwrap();

                            let resource_var = Variable::new_unchecked(format!(
                                "ts_resource_{}",
                                self.variable_counter
                            ));
                            self.variable_counter += 1;

                            let bvq = BasicVirtualizedQuery::new(
                                context.clone(),
                                subj_var.clone(),
                                external_id_var.clone(),
                                resource_var.clone(),
                            );
                            new_basic_vqs.push(bvq);
                            let new_external_id_triple = TriplePattern {
                                subject: TermPattern::Variable(subj_var.clone()),
                                predicate: NamedNodePattern::NamedNode(
                                    NamedNode::new(HAS_EXTERNAL_ID).unwrap(),
                                ),
                                object: TermPattern::Variable(external_id_var.clone()),
                            };
                            let new_resource_triple = TriplePattern {
                                subject: TermPattern::Variable(subj_var.clone()),
                                predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                                    HAS_RESOURCE,
                                )),
                                object: TermPattern::Variable(resource_var.clone()),
                            };
                            new_triples.push(new_external_id_triple);
                            new_triples.push(new_resource_triple);
                            external_ids_in_scope
                                .insert(subj_var.clone(), vec![external_id_var.clone()]);
                            resources_in_scope.insert(subj_var.clone(), vec![resource_var.clone()]);
                        }
                    }
                }
            }

            if !is_external_variable(&t.subject, &context, &self.variable_constraints)
                && !is_external_variable(&t.object, &context, &self.variable_constraints)
            {
                if !new_triples.contains(t) {
                    new_triples.push(t.clone());
                }
            } else {
                dynamic_triples.push(t)
            }
        }

        let rewritten;
        if dynamic_triples.len() > 0 {
            rewritten = true;
        } else {
            rewritten = false;
        }
        self.basic_virtualized_queries.extend(new_basic_vqs);

        if new_triples.is_empty() {
            GPReturn::new(
                GraphPattern::Bgp { patterns: vec![] },
                rewritten,
                Default::default(),
                Default::default(),
                Default::default(),
                false,
            )
        } else {
            let mut variables_in_scope = HashSet::new();
            for t in &new_triples {
                if let TermPattern::Variable(v) = &t.subject {
                    variables_in_scope.insert(v.clone());
                }
                if let TermPattern::Variable(v) = &t.object {
                    variables_in_scope.insert(v.clone());
                }
            }

            let gpr = GPReturn::new(
                GraphPattern::Bgp {
                    patterns: new_triples,
                },
                rewritten,
                variables_in_scope,
                resources_in_scope,
                external_ids_in_scope,
                false,
            );
            gpr
        }
    }
}

fn is_external_variable(
    term_pattern: &TermPattern,
    context: &Context,
    variable_constraints: &VariableConstraints,
) -> bool {
    if let TermPattern::Variable(var) = term_pattern {
        if let Some(ctr) = variable_constraints.get_constraint(var, context) {
            if ctr == &Constraint::External || ctr == &Constraint::ExternallyDerived {
                return true;
            }
        }
    }
    false
}

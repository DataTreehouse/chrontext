use super::StaticQueryRewriter;
use crate::constants::{HAS_DATATYPE, HAS_DATA_POINT, HAS_EXTERNAL_ID, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::{Constraint, VariableConstraints};
use crate::query_context::{Context, PathEntry, VariableInContext};
use crate::rewriting::graph_patterns::GPReturn;
use crate::timeseries_query::BasicTimeSeriesQuery;
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

impl StaticQueryRewriter {
    pub(crate) fn rewrite_bgp(
        &mut self,
        patterns: &Vec<TriplePattern>,
        context: &Context,
    ) -> GPReturn {
        let context = context.extension_with(PathEntry::BGP);
        let mut new_triples = vec![];
        let mut dynamic_triples = vec![];
        let mut datatypes_in_scope = HashMap::new();
        let mut external_ids_in_scope = HashMap::new();
        let mut new_basic_tsqs = vec![];
        for t in patterns {
            //If the object is an external timeseries, we need to do get the external id
            if let TermPattern::Variable(object_var) = &t.object {
                let obj_constr_opt = self
                    .variable_constraints
                    .get_constraint(object_var, &context)
                    .cloned();
                if let Some(obj_constr) = &obj_constr_opt {
                    if obj_constr == &Constraint::ExternalTimeseries {
                        if !external_ids_in_scope.contains_key(object_var) {
                            let external_id_var = Variable::new(
                                "ts_external_id_".to_string()
                                    + self.variable_counter.to_string().as_str(),
                            )
                            .unwrap();

                            let datatype_var = Variable::new(
                                "ts_datatype_".to_string()
                                    + self.variable_counter.to_string().as_str(),
                            )
                            .unwrap();
                            self.variable_counter += 1;
                            let btsq = self.create_basic_time_series_query(
                                &object_var,
                                &external_id_var,
                                &datatype_var,
                                &context,
                            );
                            new_basic_tsqs.push(btsq);
                            let new_external_id_triple = TriplePattern {
                                subject: t.object.clone(),
                                predicate: NamedNodePattern::NamedNode(
                                    NamedNode::new(HAS_EXTERNAL_ID).unwrap(),
                                ),
                                object: TermPattern::Variable(external_id_var.clone()),
                            };
                            let new_datatype_triple = TriplePattern {
                                subject: t.object.clone(),
                                predicate: NamedNodePattern::NamedNode(
                                    NamedNode::new(HAS_DATATYPE).unwrap(),
                                ),
                                object: TermPattern::Variable(datatype_var.clone()),
                            };
                            new_triples.push(new_external_id_triple);
                            new_triples.push(new_datatype_triple);
                            external_ids_in_scope
                                .insert(object_var.clone(), vec![external_id_var.clone()]);
                            datatypes_in_scope
                                .insert(object_var.clone(), vec![datatype_var.clone()]);
                        }
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
                        if ctr == &Constraint::ExternalDataPoint
                            || ctr == &Constraint::ExternalTimestamp
                            || ctr == &Constraint::ExternalDataValue
                        {
                            return true;
                        }
                    }
                }
                false
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

        //We wait until last to process the dynamic triples, making sure all relationships are known first.
        process_dynamic_triples(&mut new_basic_tsqs, dynamic_triples, &context);
        self.basic_time_series_queries.extend(new_basic_tsqs);

        if new_triples.is_empty() {
            GPReturn::new(
                GraphPattern::Bgp { patterns: vec![] },
                rewritten,
                Default::default(),
                Default::default(),
                Default::default(),
                false
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
                datatypes_in_scope,
                external_ids_in_scope,
                false,
            );
            gpr
        }
    }

    fn create_basic_time_series_query(
        &mut self,
        time_series_variable: &Variable,
        time_series_id_variable: &Variable,
        datatype_variable: &Variable,
        context: &Context,
    ) -> BasicTimeSeriesQuery {
        let mut ts_query = BasicTimeSeriesQuery::new_empty();
        ts_query.identifier_variable = Some(time_series_id_variable.clone());
        ts_query.datatype_variable = Some(datatype_variable.clone());
        ts_query.timeseries_variable = Some(VariableInContext::new(
            time_series_variable.clone(),
            context.clone(),
        ));
        ts_query
    }
}

fn process_dynamic_triples(
    local_basic_tsqs: &mut Vec<BasicTimeSeriesQuery>,
    dynamic_triples: Vec<&TriplePattern>,
    context: &Context,
) {
    for t in &dynamic_triples {
        if let NamedNodePattern::NamedNode(named_predicate_node) = &t.predicate {
            if named_predicate_node == HAS_DATA_POINT {
                for q in local_basic_tsqs.iter_mut() {
                    if let (Some(q_timeseries_variable), TermPattern::Variable(subject_variable)) =
                        (&q.timeseries_variable, &t.subject)
                    {
                        if q_timeseries_variable.partial(subject_variable, context) {
                            if let TermPattern::Variable(ts_var) = &t.object {
                                q.data_point_variable =
                                    Some(VariableInContext::new(ts_var.clone(), context.clone()));
                            }
                        }
                    }
                }
            }
        }
    }

    for t in &dynamic_triples {
        if let NamedNodePattern::NamedNode(named_predicate_node) = &t.predicate {
            if named_predicate_node == HAS_VALUE {
                for q in local_basic_tsqs.iter_mut() {
                    if q.value_variable.is_none() {
                        if let (
                            Some(q_data_point_variable),
                            TermPattern::Variable(subject_variable),
                        ) = (&q.data_point_variable, &t.subject)
                        {
                            if q_data_point_variable.partial(subject_variable, context) {
                                if let TermPattern::Variable(value_var) = &t.object {
                                    q.value_variable = Some(VariableInContext::new(
                                        value_var.clone(),
                                        context.clone(),
                                    ));
                                }
                            }
                        }
                    }
                }
            } else if named_predicate_node == HAS_TIMESTAMP {
                for q in local_basic_tsqs.iter_mut() {
                    if q.timestamp_variable.is_none() {
                        if let (
                            Some(q_data_point_variable),
                            TermPattern::Variable(subject_variable),
                        ) = (&q.data_point_variable, &t.subject)
                        {
                            if q_data_point_variable.partial(subject_variable, context) {
                                if let TermPattern::Variable(timestamp_var) = &t.object {
                                    q.timestamp_variable = Some(VariableInContext::new(
                                        timestamp_var.clone(),
                                        context.clone(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

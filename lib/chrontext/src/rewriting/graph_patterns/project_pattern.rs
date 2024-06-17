use super::StaticQueryRewriter;
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::Variable;
use representation::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite =
            self.rewrite_graph_pattern(inner, &context.extension_with(PathEntry::ProjectInner));
        if !inner_rewrite.is_subquery {
            let project_pattern =
                self.create_projection_graph_pattern(&inner_rewrite, context, variables);
            inner_rewrite.with_graph_pattern(project_pattern);
            return inner_rewrite;
        }
        inner_rewrite
    }

    pub(crate) fn create_projection_graph_pattern(
        &self,
        gpreturn: &GPReturn,
        context: &Context,
        variables: &Vec<Variable>,
    ) -> GraphPattern {
        let mut variables_rewrite = variables
            .iter()
            .map(|v| self.rewrite_variable(v, context))
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect::<Vec<Variable>>();

        let mut resource_keys_sorted = gpreturn
            .resources_in_scope
            .keys()
            .collect::<Vec<&Variable>>();
        resource_keys_sorted.sort_by_key(|v| v.to_string());

        for k in resource_keys_sorted {
            let vs = gpreturn.resources_in_scope.get(k).unwrap();
            let mut vars = vs.iter().collect::<Vec<&Variable>>();
            //Sort to make rewrites deterministic
            vars.sort_by_key(|v| v.to_string());
            for v in vars {
                if !variables_rewrite.contains(v) {
                    variables_rewrite.push(v.clone());
                }
            }
        }

        let mut id_keys_sorted = gpreturn
            .external_ids_in_scope
            .keys()
            .collect::<Vec<&Variable>>();
        id_keys_sorted.sort_by_key(|v| v.to_string());
        for k in id_keys_sorted {
            let vs = gpreturn.external_ids_in_scope.get(k).unwrap();
            let mut vars = vs.iter().collect::<Vec<&Variable>>();
            //Sort to make rewrites deterministic
            vars.sort_by_key(|v| v.to_string());
            for v in vars {
                if !variables_rewrite.contains(v) {
                    variables_rewrite.push(v.clone());
                }
            }
        }
        let mut additional_projections_sorted = self
            .additional_projections
            .iter()
            .collect::<Vec<&Variable>>();
        additional_projections_sorted.sort_by_key(|x| x.to_string());
        for v in additional_projections_sorted {
            if !variables_rewrite.contains(v) {
                variables_rewrite.push(v.clone());
            }
        }
        GraphPattern::Project {
            inner: Box::new(gpreturn.graph_pattern.as_ref().unwrap().clone()),
            variables: variables_rewrite,
        }
    }
}

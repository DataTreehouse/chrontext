use super::Combiner;
use crate::combiner::CombinerError;
use oxrdf::Variable;
use polars::prelude::JoinType;
use query_processing::graph_patterns::{join, values_pattern};
use representation::solution_mapping::SolutionMappings;
use spargebra::term::GroundTerm;

impl Combiner {
    pub(crate) fn lazy_values(
        &mut self,
        solution_mappings: Option<SolutionMappings>,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
    ) -> Result<SolutionMappings, CombinerError> {
        let sm = values_pattern(variables, bindings);
        if let Some(mut mappings) = solution_mappings {
            mappings = join(mappings, sm, JoinType::Inner)?;
            Ok(mappings)
        } else {
            Ok(sm)
        }
    }
}

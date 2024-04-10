use super::Combiner;
use crate::combiner::CombinerError;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::OrderExpression;

impl Combiner {
    pub async fn lazy_order_expression(
        &mut self,
        oexpr: &OrderExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
    ) -> Result<(SolutionMappings, bool, Context), CombinerError> {
        match oexpr {
            OrderExpression::Asc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                Ok((
                    self.lazy_expression(expr, solution_mappings, None, None, &inner_context)
                        .await?,
                    true,
                    inner_context,
                ))
            }
            OrderExpression::Desc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                Ok((
                    self.lazy_expression(expr, solution_mappings, None, None, &inner_context)
                        .await?,
                    false,
                    inner_context,
                ))
            }
        }
    }
}

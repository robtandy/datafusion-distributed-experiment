use datafusion::common::plan_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed_experiment::ArrowFlightReadExec;
use std::sync::Arc;

pub fn distribute_aggregate(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let mut aggregate_partial_found = false;
    Ok(plan
        .transform_up(|node| {
            let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };

            match agg.mode() {
                AggregateMode::Partial => {
                    if aggregate_partial_found {
                        return plan_err!("Two consecutive partial aggregations found");
                    }
                    aggregate_partial_found = true;
                    let expr = agg
                        .group_expr()
                        .expr()
                        .iter()
                        .map(|(v, _)| Arc::clone(v))
                        .collect::<Vec<_>>();
                    let node = ArrowFlightReadExec::new(node, Partitioning::Hash(expr, 1));
                    Ok(Transformed::yes(Arc::new(node)))
                }
                AggregateMode::Final
                | AggregateMode::FinalPartitioned
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned => {
                    if !aggregate_partial_found {
                        return plan_err!("No partial aggregate found before the final one");
                    }

                    let node = ArrowFlightReadExec::new(node, Partitioning::RoundRobinBatch(8));
                    Ok(Transformed::yes(Arc::new(node)))
                }
            }
        })?
        .data)
}

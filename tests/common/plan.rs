use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_distributed_experiment::ArrowFlightReadExec;

pub fn distribute_hash_repartition_exec(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    Ok(plan
        .transform_down(|node| {
            let Some(coalesce) = node.as_any().downcast_ref::<CoalesceBatchesExec>() else {
                return Ok(Transformed::no(node));
            };

            let Some(child) = coalesce.children().first().cloned() else {
                return Ok(Transformed::no(node));
            };

            let Some(repartition) = child.as_any().downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(node));
            };

            if !matches!(repartition.partitioning(), Partitioning::Hash(_, _)) {
                return Ok(Transformed::no(node));
            }

            let children = repartition.children().into_iter().cloned().collect();
            let partitioning = repartition.partitioning().clone();
            
            let node = ArrowFlightReadExec::new(
                node.with_new_children(children)?,
                partitioning,
            );
            Ok(Transformed::yes(Arc::new(node)))
        })?
        .data)
}

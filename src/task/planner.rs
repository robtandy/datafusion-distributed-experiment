use std::sync::Arc;

use datafusion::{
    common::{
        internal_datafusion_err, internal_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor},
    },
    error::Result,
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::{
    remote::{DefaultWorkerAssignment, WorkerAssignment},
    ExecutionStage,
};

use super::{ExecutionTask, WorkerAddr};

/// TaskPlanner creates a new `ExecutionTask` for the given stage and worker addresses.
///
/// This is accomplished by walking down the [`ExecutionStage`] tree, and for each stage
/// we encounter, create a number of tasks equal to the number of partitions divied by
/// `partitions_per_task`.
///
/// [`ExecutionTask::execute`] can be invoked on the resulting task object to return a SendableRecordBatchStream.
pub struct TaskPlanner {
    partitions_per_task: usize,
    /// The physical extension codec used by tasks to encode custom ExecutionPlans
    codec: Option<Arc<dyn PhysicalExtensionCodec>>,
}

impl TaskPlanner {
    /// Creates a new `TaskPlanner` with the given `partitions_per_task` and optional codec.
    pub fn new(partitions_per_task: usize, codec: Option<Arc<dyn PhysicalExtensionCodec>>) -> Self {
        Self {
            partitions_per_task,
            codec,
        }
    }

    /// Createa an [`ExecutionTask`] for the given `ExecutionStage`
    pub fn create_task(&mut self, stage: Arc<ExecutionStage>) -> Result<Arc<dyn ExecutionPlan>> {
        let num_partitions = stage.plan.output_partitioning().partition_count();
        let partition_groups = (0..num_partitions)
            .collect::<Vec<_>>()
            .chunks(self.partitions_per_task)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();

        let top_level_tasks = partition_groups
            .iter()
            .map(|pg| self.down(stage.clone(), pg.clone()))
            .collect::<Result<Vec<_>>>()?;

        // if there is one top level task, return it, otherwise create a task to consume them
        let task = if top_level_tasks.len() == 1 {
            top_level_tasks[0].clone()
        } else {
            let mut task = ExecutionTask::new(
                "EntryPoint Task",
                stage.plan.clone(),
                &(0..num_partitions).collect::<Vec<_>>(),
                top_level_tasks
                    .iter()
                    .map(|t| t.clone() as Arc<dyn ExecutionPlan>)
                    .collect(),
                self.codec.clone(),
            );
            task.name = format!("TaskPlanner: {}", stage.name());
            Arc::new(task)
        };
        Ok(task)
    }

    fn down(
        &mut self,
        stage: Arc<ExecutionStage>,
        desired_partition_group: Vec<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let num_partitions = stage.plan.output_partitioning().partition_count();
        let partition_groups = (0..num_partitions)
            .collect::<Vec<_>>()
            .chunks(self.partitions_per_task)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();

        println!(
            "TaskPlanner: Stage {} partition groups: {:?} desired partition group: {:?}",
            stage.name(),
            partition_groups,
            desired_partition_group,
        );

        // find the partition group that contains at least our partitions
        let partition_group = partition_groups
            .iter()
            .find(|pg| desired_partition_group.iter().all(|dp| pg.contains(dp)))
            .or(
                // if we can't find a partition group that contains all desired partitions,
                // TODO: Can we always assume it is one?
                partition_groups.first(),
            )
            .ok_or(internal_datafusion_err!(
                "No partition group found for stage {} with desired partitions {:?}",
                stage.name(),
                desired_partition_group
            ))?;

        Ok(Arc::new(ExecutionTask::new(
            &make_task_name(stage.num, partition_group),
            stage.plan.clone(),
            partition_group,
            stage
                .inputs
                .iter()
                .map(|input| self.down(input.clone(), partition_group.clone()))
                .collect::<Result<Vec<_>>>()?,
            self.codec.clone(),
        )))
    }
}

fn make_task_name(stage_num: usize, partition_group: &[usize]) -> String {
    format!(
        "S{}-T:[{}]",
        stage_num,
        partition_group
            .iter()
            .map(|pg| format!("{pg}"))
            .collect::<Vec<_>>()
            .join(",")
    )
}

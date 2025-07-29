use std::sync::Arc;

use datafusion::{
    common::{
        internal_datafusion_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor},
    },
    config::ConfigOptions,
    error::Result,
    physical_plan::{execution_plan::need_data_exchange, ExecutionPlan, ExecutionPlanProperties},
};

use crate::ArrowFlightReadExec;

use super::task::ExecutionTask;

pub fn create_task(
    plan: Arc<dyn ExecutionPlan>,
    partitions_per_task: usize,
) -> Result<ExecutionTask> {
    let mut vis = Planner::new(partitions_per_task);

    plan.rewrite(&mut vis)?;

    vis.finish()
}

struct Planner {
    partitions_per_task: usize,
    plan_head: Option<Arc<dyn ExecutionPlan>>,
    input_tasks: Vec<Arc<ExecutionTask>>,
}

impl Planner {
    pub fn new(partitions_per_task: usize) -> Self {
        Planner {
            partitions_per_task,
            plan_head: None,
            input_tasks: Vec::new(),
        }
    }

    pub fn finish(mut self) -> Result<ExecutionTask> {
        if self.input_tasks.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "No input tasks created".to_string(),
            ));
        }
        // Create a new ExecutionTask with the input tasks collected
        Ok(ExecutionTask::new(
            self.plan_head
                .take()
                .ok_or_else(|| internal_datafusion_err!("No plan head set"))?,
            self.input_tasks.clone(),
        ))
    }
}

impl TreeNodeRewriter for Planner {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        self.plan_head = Some(plan.clone());
        if need_data_exchange(plan.clone()) {
            // At this point we need to put the plan that we have seen so far into
            // one or more ExecutionTasks.   We will examine the number of partitions
            // and chunk them into at most partitiions_per_task partitions.
            // Then we'll clone the plan, and reparitition it to our new amount
            // and create ExecutionTasks
            let partitions = plan.output_partitioning().partition_count();
            if partitions == 0 {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Plan has no partitions".to_string(),
                ));
            }
            let num_tasks = (partitions / self.partitions_per_task).max(1); // Ensure at least one task is created

            let modified_plan = plan
                // TODO: what ConfigOptions should we use here?
                .repartitioned(self.partitions_per_task, &ConfigOptions::default())?
                .unwrap_or(plan.clone());

            let et = Arc::new(ExecutionTask::new(
                modified_plan.clone(),
                self.input_tasks.clone(),
            ));

            let tasks = (0..num_tasks).map(|_| et.clone()).collect::<Vec<_>>();
            self.input_tasks = tasks;

            // replace this part of the plan with an ArrowFlightReadExec that can consume from
            // the ArrowFlightEndpoints that will host these tasks
            let read = Arc::new(ArrowFlightReadExec::new(plan.clone()));
            Ok(Transformed::yes(read as Self::Node))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

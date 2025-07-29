use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::{execution::SendableRecordBatchStream, physical_plan::ExecutionPlan};

use datafusion::error::Result;
use datafusion_proto::protobuf::PhysicalPlanNode;

/// A task that can be executed by a worker in the distributed execution framework.
/// It produces a SendableRecordBatchStream when executed.
#[derive(Debug, Clone)]
pub enum ExecutionTask {
    Assigned {
        /// The address of the worker to which this task is assigned.
        worker_addr: String,
        /// The task to be executed.
        task: Task,
        /// The inputs to this task, which are other execution tasks that this task depends on.
        inputs: Vec<Arc<ExecutionTask>>,
    },
    Unassigned {
        /// The task to be executed, which is not yet assigned to any worker.
        task: Task,
        /// The inputs to this task, which are other execution tasks that this task depends on.
        inputs: Vec<Arc<ExecutionTask>>,
    },
}

#[derive(Debug, Clone)]
pub struct Task {
    pub(super) plan: Arc<dyn ExecutionPlan>,
}

impl ExecutionTask {
    pub fn new(plan: Arc<dyn ExecutionPlan>, inputs: Vec<Arc<ExecutionTask>>) -> Self {
        ExecutionTask::Unassigned {
            task: Task { plan },
            inputs,
        }
    }

    pub fn is_assigned(&self) -> bool {
        matches!(self, ExecutionTask::Assigned { .. })
    }

    pub fn assign(self, worker_addr: String) -> Result<ExecutionTask> {
        match self {
            ExecutionTask::Assigned { .. } => {
                internal_err!("Cannot assign a task that is already assigned")
            }
            ExecutionTask::Unassigned { task, inputs } => Ok(ExecutionTask::Assigned {
                worker_addr,
                task,
                inputs,
            }),
        }
    }

    pub fn execute(self, task_context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let plan = match self {
            ExecutionTask::Assigned { task, .. } => task.plan,
            ExecutionTask::Unassigned { task, .. } => task.plan,
        };
        plan.execute(0, task_context)
    }
}

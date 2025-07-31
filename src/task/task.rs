use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::{execution::SendableRecordBatchStream, physical_plan::ExecutionPlan};

use datafusion::error::Result;
use datafusion_proto::protobuf::PhysicalPlanNode;

#[derive(Debug, Clone)]
pub struct ExecutionStage {
    plan: Arc<dyn ExecutionPlan>,
    input: Arc<ExecutionStage>,
}

impl ExecutionStage {
    pub fn new(plan: Arc<dyn ExecutionPlan>, inputs: Vec<Arc<ExecutionStage>>) -> Self {
        ExecutionStage::Unassigned {
            task: Task { plan },
            inputs,
        }
    }

    pub fn is_assigned(&self) -> bool {
        matches!(self, ExecutionStage::Assigned { .. })
    }

    pub fn assign(self, worker_addr: String) -> Result<ExecutionStage> {
        match self {
            ExecutionStage::Assigned { .. } => {
                internal_err!("Cannot assign a task that is already assigned")
            }
            ExecutionStage::Unassigned { task, inputs } => Ok(ExecutionStage::Assigned {
                worker_addr,
                task,
                inputs,
            }),
        }
    }

    pub fn execute(self, task_context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let plan = match self {
            ExecutionStage::Assigned { task, .. } => task.plan,
            ExecutionStage::Unassigned { task, .. } => task.plan,
        };
        plan.execute(0, task_context)
    }
}

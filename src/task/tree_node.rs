use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion::error::Result;

use super::task::ExecutionTask;

impl<'a> TreeNodeContainer<'a, Self> for ExecutionTask {
    fn apply_elements<F: FnMut(&'a Self) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        f(self)
    }

    fn map_elements<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        f(self)
    }
}

impl TreeNode for ExecutionTask {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        let inputs = match self {
            ExecutionTask::Assigned { inputs, .. } => inputs,
            ExecutionTask::Unassigned { inputs, .. } => inputs,
        };

        inputs.apply_elements(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        match self {
            ExecutionTask::Assigned {
                inputs,
                task,
                worker_addr,
            } => inputs.map_elements(f)?.map_data(|data| {
                Ok(ExecutionTask::Assigned {
                    inputs: data,
                    task,
                    worker_addr,
                })
            }),
            ExecutionTask::Unassigned { inputs, task } => inputs
                .map_elements(f)?
                .map_data(|data| Ok(ExecutionTask::Unassigned { inputs: data, task })),
        }
    }
}

mod tests {
    use std::sync::Arc;

    use crate::task::task::Task;

    use super::*;
    use datafusion::{
        arrow::datatypes::Schema, common::tree_node::TreeNodeVisitor,
        physical_plan::empty::EmptyExec,
    };

    fn build_task() -> ExecutionTask {
        let schema = Arc::new(Schema::empty());
        let empty = Arc::new(EmptyExec::new(schema.clone()));
        let task1 = Arc::new(ExecutionTask::new(empty.clone(), vec![]));
        let task2 = Arc::new(ExecutionTask::new(empty.clone(), vec![]));
        let task3 = Arc::new(ExecutionTask::new(empty.clone(), vec![task1, task2]));
        ExecutionTask::new(empty, vec![task3])
    }

    #[test]
    fn test_traversal() {
        let task = build_task();
        let mut visited = vec![];
        task.apply(|node| {
            visited.push(node.clone());
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();

        assert_eq!(visited.len(), 4);
        assert!(matches!(visited[0], ExecutionTask::Unassigned { .. }));
        assert!(matches!(visited[1], ExecutionTask::Unassigned { .. }));
        assert!(matches!(visited[2], ExecutionTask::Unassigned { .. }));
        assert!(matches!(visited[3], ExecutionTask::Unassigned { .. }));
    }
}

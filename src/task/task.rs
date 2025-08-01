use std::collections::{self, HashMap};
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, internal_err, HashMap};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::{execution::SendableRecordBatchStream, physical_plan::ExecutionPlan};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use prost::Message;

use datafusion::error::Result;
use datafusion_proto::protobuf::PhysicalPlanNode;

use crate::remote::WorkerAssignment;

/// An ExecutionTask is a finer grained unit of work compared to an ExecutionStage.
/// One ExecutionStage will create one or more ExecutionTasks
///
/// When a task is execute()'d if will execute its plan and return a stream of record batches.
///
/// If the task has input tasks, then it those input tasks will be executed on remote resources
/// and will be provided the remainder of the task tree.
///
/// For example if our task tree looks like this:
///
/// ```text
///                       ┌────────┐
///                       │ Task 1 │
///                       └───┬────┘
///                           │
///                    ┌──────┴───────┐
///               ┌────┴───┐     ┌────┴───┐
///               │ Task 2 │     │ Task 3 │
///               └────┬───┘     └────────┘
///                    │
///             ┌──────┴───────┐
///        ┌────┴───┐     ┌────┴───┐
///        │ Task 4 │     │ Task 5 │
///        └────────┘     └────────┘                    
///
/// ```
///  
/// Then executing Task 1 will run its plan locally.  Task 1 has two inputs, Task 2 and Task 3.  We
/// know these will execute on remote resources.   As such the plan for Task 1 must contain an
/// [`ArrowFlightReadExec`] node that will read the results of Task 2 and Task 3 and coalese the
/// results.
///
/// When Task 1's [`ArrowFlightReadExec`] node is executed, it makes an ArrowFlightRequest to the
/// host assigned in the Task.  It provides the following Task tree serialilzed in the body of the
/// Arrow Flight Ticket:
///
/// ```text
///               ┌────────┐     
///               │ Task 2 │    
///               └────┬───┘   
///                    │
///             ┌──────┴───────┐
///        ┌────┴───┐     ┌────┴───┐
///        │ Task 4 │     │ Task 5 │
///        └────────┘     └────────┘                    
///
/// ```
///
/// The receiving ArrowFlightEndpoint will then execute Task 2 and will repeated this process.
///
/// When Task 4 is executed, it has no input tasks, so it is assumed that the plan included in that
/// Task can complete on its own; its likely holding a leaf node in the overall phyysical plan and
/// producing data from a [`DataSourceExec`].
#[derive(Debug, Clone)]
pub struct ExecutionTask {
    /// The name of this task, used for debugging and logging
    pub name: String,
    /// The plan that will be executed by this task.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The address of the worker that will execute this task.  A None value is interpreted as
    /// unassinged.
    pub worker_addr: Option<WorkerAddr>,
    /// The partitions that we can execute from this plan
    pub partition_group: Vec<usize>,
    /// The input tasks that this task depends on.  These tasks will be executed on remote wrokers
    /// Note that they are stored as dyn ExecutionPlan, but will always be ExecutionTask
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Stored as a map of partition -> ExecutionTask
    pub task_map: HashMap<usize, Arc<dyn ExecutionPlan>>,
    /// The physical execution codec used to serialize subtasks
    pub codec: Arc<dyn PhysicalExtensionCodec>,
}

/// The host and port of a worker that will execute the task.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WorkerAddr {
    /// The host name or IP address of the worker.
    #[prost(string, tag = "1")]
    pub host: String,
    /// The port number of the worker, a u32 vs u16 as prost doesn't like u16
    #[prost(uint32, tag = "2")]
    pub port: u32,
}

/// A [`ExecutionTask`] Tree that can be sent over the wire
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionTaskMessage {
    /// our name
    #[prost(string, optional, tag = "1")]
    pub name: Option<String>,
    /// The plan proto that will be executed by this task.  Boxed to avoid inf recursion per prost
    /// docs
    #[prost(message, optional, boxed, tag = "2")]
    pub plan: Option<Box<PhysicalPlanNode>>,
    /// The address of the worker that will execute this task.
    #[prost(message, optional, tag = "3")]
    pub worker_addr: Option<WorkerAddr>,
    /// The partitions that we can execute from this plan
    #[prost(uint32, repeated, tag = "4")]
    pub partition_group: Vec<u32>,
    /// The input tasks, proto encoded, that this task depends on. Boxed to avoid infinite recursion
    /// per prost docs
    #[prost(map = "uint32, message", tag = "5")]
    pub inputs: HashMap<u32, ExecutionTaskMessage>,
}

impl ExecutionTask {
    /// Creates a new ExecutionTask with the given plan and inputs, and optionally a PhysicalExtensionCodec
    /// which will be needed if we are exchanging any custom [`ExecutionPlan`]s over the network
    pub fn new(
        name: &str,
        plan: Arc<dyn ExecutionPlan>,
        partition_group: &[usize],
        inputs: HashMap<usize, Arc<dyn ExecutionPlan>>,
        codec: Option<Arc<dyn PhysicalExtensionCodec>>,
    ) -> Self {
        ExecutionTask {
            name: name.to_string(),
            worker_addr: None,
            plan,
            partition_group: partition_group.to_vec(),
            inputs,
            codec: codec.unwrap_or_else(|| Arc::new(DefaultPhysicalExtensionCodec {})),
        }
    }

    /// Assign this task to a worker address.  Recursively do so for all input tasks.
    ///
    /// Defer to the worker assignment logic to determine if this task can be assigned to the worker.
    ///
    /// Return an assigned ExecutionTask with the worker address set, or an error if the assignment fails.
    pub fn assign(
        &self,
        worker_addrs: &[WorkerAddr],
        worker_assignment: &dyn WorkerAssignment,
    ) -> Result<ExecutionTask> {
        if worker_addrs.is_empty() {
            return internal_err!("No worker addresses provided for task assignment");
        }

        // Find the first worker address that can accept this task
        let worker_addr = worker_addrs
            .iter()
            .find(|addr| {
                worker_assignment.try_assign(addr, self.plan.clone(), self.partition_group.clone())
            })
            .cloned()
            .ok_or(internal_datafusion_err!(
                "No suitable worker found for task {} with partition group {:?}",
                self.name,
                self.partition_group
            ))?;

        // Recursively assign inputs
        let assigned_inputs = self.inputs.clone();

        assigned_inputs
            .iter_mut()
            .map(|mut entry| {
                let v = entry.value_mut();
                *v = v
                    .as_any()
                    .downcast_ref::<ExecutionTask>()
                    .ok_or_else(|| internal_datafusion_err!("Input is not an ExecutionTask"))
                    .and_then(|task| task.assign(worker_addrs, worker_assignment))
                    .map(Arc::new)
                    .map(|a| a as Arc<dyn ExecutionPlan>)?;
                Ok(())
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ExecutionTask::new(
            &self.name,
            self.plan.clone(),
            &self.partition_group,
            assigned_inputs,
            Some(self.codec.clone()),
        ))
    }
}

impl DisplayAs for ExecutionTask {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "Task {}", self.name,)
            }
            DisplayFormatType::TreeRender => write!(
                f,
                "Worker: {:?}\nPartitions: {:?}",
                self.worker_addr, self.partition_group,
            ),
        }
    }
}

impl ExecutionPlan for ExecutionTask {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != self.inputs.len() {
            return internal_err!(
                "ExecutionTask: Expected {} children, got {}",
                self.inputs.len(),
                children.len()
            );
        }

        // sanity check
        children
            .iter()
            .map(|c| c.as_any().downcast_ref::<ExecutionTask>().cloned())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                internal_datafusion_err!("Failed to downcast children to ExecutionTask")
            })?;

        Ok(Arc::new(ExecutionTask {
            name: self.name.clone(),
            worker_addr: self.worker_addr.clone(),
            plan: self.plan.clone(),
            partition_group: self.partition_group.clone(),
            inputs: children,
            codec: self.codec.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if !self.partition_group.contains(&partition) {
            return internal_err!(
                "ExecutionTask: Partition {} not in partition group {:?}",
                partition,
                self.partition_group
            );
        }
        self.plan.execute(partition, context)
    }
}

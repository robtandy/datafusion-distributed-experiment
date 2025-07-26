use crate::plan::arrow_flight_read::ArrowFlightReadExec;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::{proto_error, PhysicalPlanNode};
use prost::bytes::BufMut;
use prost::Message;
use std::sync::Arc;

/// DataFusion [PhysicalExtensionCodec] implementation that allows sending and receiving
/// [ArrowFlightReadExecProto] over the wire.
#[derive(Debug)]
pub struct ArrowFlightReadExecProtoCodec {
    runtime: Arc<RuntimeEnv>,
}

impl ArrowFlightReadExecProtoCodec {
    pub fn new(runtime: &Arc<RuntimeEnv>) -> Self {
        Self {
            runtime: Arc::clone(runtime),
        }
    }
}

impl PhysicalExtensionCodec for ArrowFlightReadExecProtoCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>], // TODO: why would I want this here
        registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        ArrowFlightReadExecProto::try_decode(buf)?.try_into_physical_plan(
            registry,
            &self.runtime,
            self,
        )
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        ArrowFlightReadExecProto::try_from_physical_plan(node, self)?.try_encode(buf)
    }
}

/// Protobuf representation of the [ArrowFlightReadExec] physical node. It serves as
/// an intermediate format for serializing/deserializing [ArrowFlightReadExec] nodes
/// to send them over the wire.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowFlightReadExecProto {
    #[prost(message, optional, boxed, tag = "1")]
    child: Option<Box<PhysicalPlanNode>>,
    #[prost(uint64, tag = "2")]
    partitions: u64,
}

impl AsExecutionPlan for ArrowFlightReadExecProto {
    fn try_decode(buf: &[u8]) -> datafusion::common::Result<Self>
    where
        Self: Sized,
    {
        ArrowFlightReadExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))
    }

    fn try_encode<B>(&self, buf: &mut B) -> datafusion::common::Result<()>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf)
            .map_err(|err| proto_error(format!("{err}")))
    }

    fn try_into_physical_plan(
        &self,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let Some(child) = &self.child else {
            return Err(proto_error("ArrowFlightReadExecProto must have a child"));
        };

        Ok(Arc::new(ArrowFlightReadExec::new(
            child.try_into_physical_plan(registry, runtime, extension_codec)?,
            self.partitions as usize,
        )))
    }

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> datafusion::common::Result<Self>
    where
        Self: Sized,
    {
        let Some(node) = plan.as_any().downcast_ref::<ArrowFlightReadExec>() else {
            return Err(proto_error("Only ArrowFlightReadExec is supported"));
        };
        if node.children().len() != 1 {
            return Err(proto_error(format!(
                "Expected ArrowFlightReadExec to have exactly 1 child, got {}",
                node.children().len()
            )));
        }
        let child = node.children()[0];

        Ok(Self {
            partitions: node.properties().partitioning.partition_count() as u64,
            child: Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
                Arc::clone(child),
                extension_codec,
            )?)),
        })
    }
}

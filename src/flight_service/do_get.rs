use crate::flight_service::service::ArrowFlightEndpoint;
use crate::plan::ArrowFlightReadExecProtoCodec;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::task::{ExecutionTask, ExecutionTaskMessage};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    #[prost(message, optional, tag = "1")]
    pub task: Option<ExecutionTaskMessage>,
}

impl DoGet {
    pub fn new_remote_plan_exec_ticket(
        plan: Arc<dyn ExecutionPlan>,
        stage_context: StageContext,
        actor_context: ActorContext,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Ticket, DataFusionError> {
        let node = PhysicalPlanNode::try_from_physical_plan(plan, extension_codec)?;
        let do_get = Self {
            inner: Some(DoGetInner::RemotePlanExec(RemotePlanExec {
                plan: Some(Box::new(node)),
                stage_context: Some(stage_context),
                actor_context: Some(actor_context),
            })),
        };
        Ok(Ticket::new(do_get.encode_to_vec()))
    }
}

impl ArrowFlightEndpoint {
    pub(super) async fn get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<<ArrowFlightEndpoint as FlightService>::DoGetStream>, Status> {
        let Ticket { ticket } = request.into_inner();
        let action = DoGet::decode(ticket).map_err(|err| {
            Status::invalid_argument(format!("Cannot decode DoGet message: {err}"))
        })?;

        let Some(action) = action.inner else {
            return invalid_argument("DoGet message is empty");
        };

        let DoGetInner::RemotePlanExec(action) = action;

        let mut state = SessionStateBuilder::new()
            .with_runtime_env(Arc::clone(&self.runtime))
            .with_default_features()
            .build();

        let Some(function_registry) = state.function_registry() else {
            return invalid_argument("FunctionRegistry not present in newly built SessionState");
        };

        let Some(plan_proto) = action.plan else {
            return invalid_argument("RemotePlanExec is missing the plan");
        };

        let Some(stage_context) = action.stage_context else {
            return invalid_argument("RemotePlanExec is missing the stage context");
        };

        let Some(actor_context) = action.actor_context else {
            return invalid_argument("RemotePlanExec is missing the actor context");
        };

        let plan = plan_proto
            .try_into_physical_plan(
                function_registry,
                &self.runtime,
                // TODO: The user should be able to pass its own extension decoder.
                &ArrowFlightReadExecProtoCodec::new(&self.runtime),
            )
            .map_err(|err| Status::internal(format!("Cannot deserialize plan: {err}")))?;

        let stage_id = stage_context.id.clone();
        let caller_actor_idx = actor_context.caller_actor_idx as usize;
        let actor_idx = actor_context.actor_idx as usize;
        let prev_n = stage_context.prev_actors as usize;
        let partitioning = match parse_protobuf_partitioning(
            stage_context.partitioning.as_ref(),
            function_registry,
            &plan.schema(),
            // TODO: The user should be able to pass its own extension decoder.
            &ArrowFlightReadExecProtoCodec::new(&self.runtime),
        ) {
            // We need to replace the partition count in the provided Partitioning scheme with
            // the number of actors in the previous stage. ArrowFlightReadExec might be declaring
            // N partitions, but each ArrowFlightReadExec::execute(n) call will go to a different
            // actor in the next stage.
            //
            // Each actor in that next stage (us here) needs to expose as many partitioned streams
            // as actors exist on its previous stage.
            Ok(Some(partitioning)) => match partitioning {
                Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(prev_n),
                Partitioning::Hash(expr, _) => Partitioning::Hash(expr, prev_n),
                Partitioning::UnknownPartitioning(_) => Partitioning::UnknownPartitioning(prev_n),
            },
            Ok(None) => return invalid_argument("Missing partitioning"),
            Err(err) => return invalid_argument(format!("Cannot parse partitioning {err}")),
        };
        let config = state.config_mut();
        config.set_extension(Arc::clone(&self.stage_delegation));
        config.set_extension(Arc::clone(&self.channel_manager));
        config.set_extension(Arc::new(stage_context));
        config.set_extension(Arc::new(actor_context));

        let stream_partitioner = self
            .partitioner_registry
            .get_or_create_stream_partitioner(
                stage_id,
                actor_idx,
                plan,
                partitioning,
                state.task_ctx(),
            )
            .map_err(|err| {
                Status::internal(format!("Could not create stream partitioner: {err}"))
            })?;

        let stream = stream_partitioner
            .stream_partition(caller_actor_idx)
            .map_err(|err| Status::internal(format!("Cannot get stream partition: {err}")))?;

        // TODO: error propagation
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(stream_partitioner.schema())
            .build(stream.map_err(|err| FlightError::ExternalError(Box::new(err))));

        Ok(Response::new(Box::pin(flight_data_stream.map_err(|err| {
            Status::internal(format!("Error during flight stream: {err}"))
        }))))
    }
}

fn invalid_argument<T>(msg: impl Into<String>) -> Result<T, Status> {
    Err(Status::invalid_argument(msg))
}

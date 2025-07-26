use crate::channel_manager::{ArrowFlightChannel, ChannelManager};
use crate::flight_service::DoGet;
use crate::plan::arrow_flight_read_proto::ArrowFlightReadExecProtoCodec;
use crate::stage_delegation::{StageContext, StageDelegation};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::common::{exec_datafusion_err, exec_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{TryFutureExt, TryStreamExt};
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::IntoRequest;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ArrowFlightReadExec {
    properties: PlanProperties,
    child: Arc<dyn ExecutionPlan>,
    next_stage_context: Arc<OnceCell<(StageContext, Vec<ArrowFlightChannel>)>>,
}

impl ArrowFlightReadExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, partitions: usize) -> Self {
        Self {
            properties: PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            child,
            next_stage_context: Arc::new(OnceCell::new()),
        }
    }
}

impl DisplayAs for ArrowFlightReadExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        // TODO
        write!(f, "ArrowFlightReadExec")
    }
}

impl ExecutionPlan for ArrowFlightReadExec {
    fn name(&self) -> &str {
        "ArrowFlightReadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "ArrowFlightReadExec: wrong number of children, expected 1, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self {
            properties: self.properties.clone(),
            child: Arc::clone(&children[0]),
            next_stage_context: Arc::new(OnceCell::new()),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let runtime = context.runtime_env();
        let output_partitions = self.properties.partitioning.partition_count();

        let current_stage = context.session_config().get_extension::<StageContext>();
        let stage_delegation = context.session_config().get_extension::<StageDelegation>();
        if current_stage.is_some() && stage_delegation.is_none() {
            return internal_err!("No StageDelegation extension found in the SessionConfig even though a StageContext was present.");
        }

        let plan = Arc::clone(&self.child);
        let next_stage_context = Arc::clone(&self.next_stage_context);

        let stream = async move {
            let channel_manager = ChannelManager::try_from_session(context.session_config())?;

            let previous_stage = current_stage.as_ref().map(|v| v.to_previous());
            let (next_stage_context, channels) = next_stage_context.get_or_try_init(|| async move {
                if let Some(current_stage) = current_stage {
                    if current_stage.current == current_stage.delegate {
                        // We are inside a stage, and we are the delegate, so need to
                        // build the channels and communicate them.
                        let channels = channel_manager.get_n_channels(output_partitions).await?;

                        let next_stage_context = StageContext {
                            id: Uuid::new_v4().to_string(),
                            previous_stage,
                            current: partition as u64,
                            delegate: 0,
                            actors: channels.iter().map(|t| t.url.to_string()).collect(),
                        };

                        // TODO: communicate to the rest of the actors what are the next Urls.
                        Ok::<_, DataFusionError>((next_stage_context, channels))
                    } else {
                        // We are inside a stage, but we are not the delegate, so we need to
                        // wait for the delegate to tell us what the new channels are.
                        let Some(stage_delegation) = stage_delegation else {
                            return internal_err!("No StageDelegation extension found in the SessionConfig even though a StageContext was present.")
                        };
                        let next_stage_context = stage_delegation.wait_for_delegate_info(current_stage.id.clone()).await?;
                        let urls = next_stage_context.actors.iter().map(|a| Url::parse(a.as_str())).collect::<Result<Vec<_>, _>>().map_err(|err| {
                            exec_datafusion_err!("Invalid actor Urls in next stage context: {err}")
                        })?;
                        let channels = channel_manager.get_channels_for_urls(&urls).await?;
                        Ok((next_stage_context, channels))
                    }
                } else {
                    // There was no previous stage; this means we need to start the whole
                    // thing here.
                    let channels = channel_manager.get_n_channels(output_partitions).await?;

                    let next_stage_context = StageContext {
                        id: Uuid::new_v4().to_string(),
                        previous_stage,
                        current: partition as u64,
                        delegate: 0,
                        actors: channels.iter().map(|t| t.url.to_string()).collect(),
                    };
                    Ok((next_stage_context, channels))
                }
            })
                .await?;

            if partition >= channels.len() {
                return exec_err!("ChannelManager resolved an incorrect number of partitions. Expected {output_partitions}, got {}", channels.len());
            }

            let ticket = Ticket::new(DoGet::new_remote_plan_exec(
                plan,
                next_stage_context.clone(),
                // TODO: The user should be able to pass its own extension decoder.
                &ArrowFlightReadExecProtoCodec::new(&runtime),
            )?.encode_to_vec());

            let mut client = FlightServiceClient::new(channels[partition].channel.clone());
            let res = client
                .do_get(ticket.into_request())
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let stream = res.into_inner();

            Ok(FlightRecordBatchStream::new_from_flight_data(
                stream.map_err(|err| FlightError::Tonic(Box::new(err))),
            )
                .map_err(|err| DataFusionError::External(Box::new(err))))
        }
            .try_flatten_stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

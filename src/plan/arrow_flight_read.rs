use crate::channel_manager::{ArrowFlightChannel, ChannelManager};
use crate::flight_service::{DoGet, DoPut};
use crate::plan::arrow_flight_read_proto::ArrowFlightReadExecProtoCodec;
use crate::stage_delegation::{PreviousStage, StageContext, StageDelegation};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{exec_datafusion_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use futures::{TryFutureExt, TryStreamExt};
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
    next_stage_context_cell: Arc<OnceCell<(StageContext, Vec<ArrowFlightChannel>)>>,
}

impl ArrowFlightReadExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        Self {
            properties: PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            child,
            next_stage_context_cell: Arc::new(OnceCell::new()),
        }
    }
}

impl DisplayAs for ArrowFlightReadExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ArrowFlightReadExec: partitioning={}", self.properties.partitioning)
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
            next_stage_context_cell: Arc::new(OnceCell::new()),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let runtime = context.runtime_env();
        let partitioning = self.properties.partitioning.clone();

        let channel_manager = ChannelManager::try_from_session(context.session_config())?;
        let current_stage_opt = context.session_config().get_extension::<StageContext>();
        let stage_delegation_opt = context.session_config().get_extension::<StageDelegation>();
        if current_stage_opt.is_some() && stage_delegation_opt.is_none() {
            return internal_err!("No StageDelegation extension found in the SessionConfig even though a StageContext was present.");
        }

        let plan = Arc::clone(&self.child);
        let next_stage_context = Arc::clone(&self.next_stage_context_cell);

        let stream = async move {
            let (next_stage_context, channels) = next_stage_context.get_or_try_init(|| async {
                if let Some(ref current_stage) = current_stage_opt {
                    if current_stage.current == current_stage.delegate {
                        // We are inside a stage, and we are the delegate, so need to
                        // build the channels and communicate them.
                        build_next_stage(
                            &channel_manager,
                            Some(&current_stage),
                            partition,
                            partitioning,
                        ).await
                    } else {
                        // We are inside a stage, but we are not the delegate, so we need to
                        // wait for the delegate to tell us what the new channels are.
                        let Some(stage_delegation) = stage_delegation_opt else {
                            return internal_err!("No StageDelegation extension found in the SessionConfig even though a StageContext was present.");
                        };
                        listen_to_next_stage(
                            &channel_manager,
                            &stage_delegation,
                            current_stage.id.clone()
                        ).await
                    }
                } else {
                    // We are not in a stage, the whole thing starts here.
                    build_next_stage(
                        &channel_manager,
                        current_stage_opt.as_deref(),
                        partition,
                        partitioning,
                    ).await
                }
            }).await?;

            if let Some(current_stage) = current_stage_opt {
                if current_stage.current == current_stage.delegate {
                    // We are the delegate, and it's our duty to communicate the next stage context
                    // to the other actors that are not us. They will be waiting for us to send
                    // them this info.
                    communicate_next_stage(
                        &channel_manager,
                        current_stage.as_ref().clone(),
                        next_stage_context.clone()
                    ).await?;
                }
            }

            if partition >= channels.len() {
                return internal_err!("Invalid channel index {partition} with a total number of {} channels", channels.len());
            }

            let mut next_stage_context = next_stage_context.clone();
            next_stage_context.current = partition as u64;
            let ticket = DoGet::new_remote_plan_exec_ticket(
                plan,
                next_stage_context,
                // TODO: The user should be able to pass its own extension decoder.
                &ArrowFlightReadExecProtoCodec::new(&runtime),
            )?;

            let mut client = FlightServiceClient::new(channels[partition].channel.clone());
            let stream = client
                .do_get(ticket.into_request())
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?
                .into_inner()
                .map_err(|err| FlightError::Tonic(Box::new(err)));

            Ok(FlightRecordBatchStream::new_from_flight_data(stream)
                // TODO: propagate the error from the service to here, probably serializing it
                //  somehow.
                .map_err(|err| DataFusionError::External(Box::new(err))))
        }.try_flatten_stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Builds the next stage context. This should be done by either the delegate in case we are already
/// inside a stage context, or unconditionally if we are not in a stage context.
async fn build_next_stage(
    channel_manager: &ChannelManager,
    current_stage: Option<&StageContext>,
    partition: usize,
    partitioning: Partitioning,
) -> Result<(StageContext, Vec<ArrowFlightChannel>), DataFusionError> {
    let output_partitions = partitioning.partition_count();
    let channels = channel_manager.get_n_channels(output_partitions).await?;

    let next_stage_context = StageContext {
        id: Uuid::new_v4().to_string(),
        previous_stage: current_stage.map(|v| PreviousStage {
            id: v.id.clone(),
            caller: v.current,
            actors: v.actors.clone(),
        }),
        partitioning: Some(serialize_partitioning(
            &partitioning,
            // TODO: this should be set by the user
            &DefaultPhysicalExtensionCodec {},
        )?),
        current: partition as u64,
        delegate: 0,
        actors: channels.iter().map(|t| t.url.to_string()).collect(),
    };
    Ok((next_stage_context, channels))
}

/// Communicates the next stage context to all the actors that are not us. This should be
/// done by the delegate in a stage, as it's the one responsible for ensuring every actor in
/// a stage knows how the next stage looks like.
async fn communicate_next_stage(
    channel_manager: &ChannelManager,
    current_stage: StageContext,
    next_stage: StageContext,
) -> Result<(), DataFusionError> {
    let urls = current_stage
        .actors
        .iter()
        .enumerate()
        // Do not communicate to self.
        .filter(|(i, _)| *i != current_stage.current as usize)
        .map(|(_, url)| Url::parse(url.as_str()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| exec_datafusion_err!("Invalid actor Urls in next stage context: {err}"))?;
    let current_actors = channel_manager.get_channels_for_urls(&urls).await?;

    let mut join_set = JoinSet::new();
    for channel in current_actors {
        let next_stage = next_stage.clone();
        join_set.spawn(async move {
            let flight_data = DoPut::new_stage_context_flight_data(next_stage);

            let mut client = FlightServiceClient::new(channel.channel.clone());
            client
                .do_put(futures::stream::once(async move { flight_data }))
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))
        });
    }
    for res in join_set.join_all().await {
        res?;
    }
    Ok(())
}

/// Waits until the delegate in the current stage communicates us the next stage context. It's
/// the responsibility of the delegate to choose the next stage context, and other actors in the
/// stage must wait for that info to be communicated. This function does just that.
async fn listen_to_next_stage(
    channel_manager: &ChannelManager,
    stage_delegation: &StageDelegation,
    id: String,
) -> Result<(StageContext, Vec<ArrowFlightChannel>), DataFusionError> {
    let next_stage_context = stage_delegation.wait_for_delegate_info(id).await?;
    let urls = next_stage_context
        .actors
        .iter()
        .map(|a| Url::parse(a.as_str()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| exec_datafusion_err!("Invalid actor Urls in next stage context: {err}"))?;
    let channels = channel_manager.get_channels_for_urls(&urls).await?;
    Ok((next_stage_context, channels))
}

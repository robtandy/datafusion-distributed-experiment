use crate::flight_service::service::ArrowFlightEndpoint;
use crate::stage_delegation::StageContext;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::FlightData;
use futures::StreamExt;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoPut {
    #[prost(oneof = "DoPutInner", tags = "1")]
    pub inner: Option<DoPutInner>,
}

#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum DoPutInner {
    #[prost(message, tag = "1")]
    StageContext(StageContext),
}

impl ArrowFlightEndpoint {
    pub(super) async fn put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<ArrowFlightEndpoint as FlightService>::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        while let Some(msg) = stream.message().await? {
            let action = DoPut::decode(msg.data_body).map_err(|err| {
                Status::invalid_argument(format!("Cannot decode DoPut message: {err}"))
            })?;
            let Some(action) = action.inner else {
                return Err(Status::invalid_argument("DoPut message is empty"));
            };
            match action {
                DoPutInner::StageContext(stage_context) => {
                    self.stage_delegation
                        .add_delegate_info(stage_context)
                        .map_err(|err| {
                            Status::internal(format!(
                                "Cannot add delegate to stage_delegation: {err}"
                            ))
                        })?;
                }
            }
        }
        Ok(Response::new(futures::stream::empty().boxed()))
    }
}

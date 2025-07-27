use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::common::runtime::JoinSet;
use datafusion::common::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_distributed_experiment::{
    ArrowFlightChannel, ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelManager, ChannelResolver,
};
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Server};
use url::Url;

pub async fn start_localhost_context<N: TryInto<u16>, I: IntoIterator<Item = N>>(
    ports: I,
) -> (SessionContext, JoinSet<()>)
where
    N::Error: std::fmt::Debug,
{
    let ports: Vec<u16> = ports.into_iter().map(|x| x.try_into().unwrap()).collect();
    let channel_resolver = LocalHostChannelResolver::new(ports.clone());
    let mut join_set = JoinSet::new();
    for port in ports {
        let channel_resolver = channel_resolver.clone();
        join_set.spawn(async move {
            spawn_flight_service(channel_resolver, port).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let ctx = SessionContext::new();

    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(ChannelManager::new(channel_resolver)));

    (ctx, join_set)
}

#[derive(Clone)]
pub struct LocalHostChannelResolver {
    ports: Vec<u16>,
    i: Arc<AtomicUsize>,
}

impl LocalHostChannelResolver {
    pub fn new<N: TryInto<u16>, I: IntoIterator<Item = N>>(ports: I) -> Self
    where
        N::Error: std::fmt::Debug,
    {
        Self {
            i: Arc::new(AtomicUsize::new(0)),
            ports: ports.into_iter().map(|v| v.try_into().unwrap()).collect(),
        }
    }
}

#[async_trait]
impl ChannelResolver for LocalHostChannelResolver {
    async fn get_n_channels(&self, n: usize) -> Result<Vec<ArrowFlightChannel>, DataFusionError> {
        let mut result = vec![];
        for _ in 0..n {
            let i = self.i.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % self.ports.len();
            let port = self.ports[i];
            let url = format!("http://localhost:{port}");
            let endpoint = Channel::from_shared(url.clone()).map_err(external_err)?;
            let channel = endpoint.connect().await.map_err(external_err)?;
            result.push(ArrowFlightChannel {
                url: Url::parse(&url).map_err(external_err)?,
                channel: BoxCloneSyncChannel::new(channel),
            })
        }
        Ok(result)
    }

    async fn get_channels_for_urls(
        &self,
        urls: &[Url],
    ) -> Result<Vec<ArrowFlightChannel>, DataFusionError> {
        let mut result = vec![];
        for url in urls {
            let endpoint = Channel::from_shared(url.to_string()).map_err(external_err)?;
            let channel = endpoint.connect().await.map_err(external_err)?;
            result.push(ArrowFlightChannel {
                url: url.clone(),
                channel: BoxCloneSyncChannel::new(channel),
            })
        }
        Ok(result)
    }
}

pub async fn spawn_flight_service(
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
    port: u16,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let endpoint = ArrowFlightEndpoint::new(channel_resolver);
    Ok(Server::builder()
        .add_service(FlightServiceServer::new(endpoint))
        .serve(format!("127.0.0.1:{port}").parse()?)
        .await?)
}

fn external_err(err: impl Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

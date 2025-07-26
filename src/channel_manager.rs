use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;
use delegate::delegate;
use std::sync::Arc;
use tonic::body::BoxBody;
use url::Url;

pub struct ChannelManager(Arc<dyn ChannelResolver + Send + Sync>);

impl ChannelManager {
    pub fn new(resolver: impl ChannelResolver + Send + Sync + 'static) -> Self {
        Self(Arc::new(resolver))
    }
}

#[derive(Clone, Debug)]
pub struct ArrowFlightChannel {
    pub url: Url,
    pub channel: tower::util::BoxCloneSyncService<
        http::Request<BoxBody>,
        http::Response<BoxBody>,
        tonic::transport::Error,
    >,
}

#[async_trait]
pub trait ChannelResolver {
    async fn get_n_channels(&self, n: usize) -> Result<Vec<ArrowFlightChannel>, DataFusionError>;
    async fn get_channels_for_urls(
        &self,
        urls: &[Url],
    ) -> Result<Vec<ArrowFlightChannel>, DataFusionError>;
}

impl ChannelManager {
    pub fn try_from_session(session: &SessionConfig) -> Result<Arc<Self>, DataFusionError> {
        Ok(session
            .get_extension::<ChannelManager>()
            .ok_or_else(|| internal_datafusion_err!("No extension ChannelManager"))?)
    }

    delegate! {
        to self.0 {
            pub async fn get_n_channels(&self, n: usize) -> Result<Vec<ArrowFlightChannel>, DataFusionError>;
            pub async fn get_channels_for_urls(&self, urls: &[Url]) -> Result<Vec<ArrowFlightChannel>, DataFusionError>;
        }
    }
}

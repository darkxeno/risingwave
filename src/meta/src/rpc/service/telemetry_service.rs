use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::config::MetaBackend;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_pb::meta::telemetry_info_service_server::TelemetryInfoService;
use risingwave_pb::meta::{TelemetryInfoRequest, TelemetryInfoResponse};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::storage::MetaStore;
use crate::telemetry::report::{TELEMETRY_CF, TELEMETRY_KEY};

pub struct TelemetryInfoServiceImpl<S: MetaStore> {
    meta_store: Arc<S>,
}

impl<S: MetaStore> TelemetryInfoServiceImpl<S> {
    pub fn new(meta_store: Arc<S>) -> Self {
        Self { meta_store }
    }

    async fn get_tracking_id(&self) -> Option<String> {
        match self.meta_store.meta_store_type() {
            MetaBackend::Etcd => match self.meta_store.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await {
                Ok(id) => Uuid::from_slice_le(&id)
                    .map_err(|e| anyhow!("failed to parse uuid, {}", e))
                    .ok()
                    .map(|uuid| uuid.to_string()),
                Err(_) => None,
            },
            MetaBackend::Mem => None,
        }
    }

    fn should_kill_telemetry(&self) -> bool {
        match self.meta_store.meta_store_type() {
            MetaBackend::Mem => true,
            MetaBackend::Etcd => false,
        }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> TelemetryInfoService for TelemetryInfoServiceImpl<S> {
    async fn get_telemetry_info(
        &self,
        _request: Request<TelemetryInfoRequest>,
    ) -> Result<Response<TelemetryInfoResponse>, Status> {
        match self.get_tracking_id().await {
            Some(tracking_id) => Ok(Response::new(TelemetryInfoResponse {
                tracking_id,
                telemetry_enabled: telemetry_env_enabled(),
                should_kill_telemetry: self.should_kill_telemetry(),
            })),
            None => Ok(Response::new(TelemetryInfoResponse {
                tracking_id: String::default(),
                telemetry_enabled: false,
                // It's possible that telemetry is disabled but reporting is alive
                should_kill_telemetry: self.should_kill_telemetry(),
            })),
        }
    }
}

use std::{fmt, time::Duration};

use serde::{Deserialize, Serialize};

use crate::domain::{HttpMethod, LeaseId, RouteHost, RoutePath, TargetUrl};

pub const CONTROL_PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Serialize, Deserialize)]
pub struct ControlDescriptor {
    pub protocol_version: u16,
    pub address: String,
    pub token: String,
    pub process_id: u32,
}

impl fmt::Debug for ControlDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlDescriptor")
            .field("protocol_version", &self.protocol_version)
            .field("address", &self.address)
            .field("token", &"<redacted>")
            .field("process_id", &self.process_id)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateLeaseRequest {
    pub method: HttpMethod,
    pub path: RoutePath,
    pub host: Option<RouteHost>,
    pub target: TargetUrl,
    pub preserve_host: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseResponse {
    pub lease_id: LeaseId,
    pub lease_ttl_ms: u64,
    pub renew_after_ms: u64,
}

impl LeaseResponse {
    #[must_use]
    pub fn renew_after(&self) -> Duration {
        Duration::from_millis(self.renew_after_ms)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseView {
    pub lease_id: LeaseId,
    pub method: HttpMethod,
    pub path: RoutePath,
    pub host: Option<RouteHost>,
    pub target: TargetUrl,
    pub preserve_host: bool,
    pub expires_in_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseListResponse {
    pub leases: Vec<LeaseView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// Converts a duration to the saturating milliseconds used by the `*_ms`
/// protocol fields.
pub(crate) fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

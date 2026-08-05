use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use reqwest::Url;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::LeaseId,
    protocol::{
        CONTROL_PROTOCOL_VERSION, ControlDescriptor, CreateLeaseRequest, ErrorResponse,
        LeaseListResponse, LeaseResponse, duration_millis,
    },
    registry::{Registry, RegistryError},
    runtime::{RuntimeError, RuntimePaths},
};

const CONTROL_PATH: &str = "/v1/leases";
const STATUS_PATH: &str = "/v1/status";
const SHUTDOWN_PATH: &str = "/v1/shutdown";
const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_MAX_BODY_BYTES: usize = 16 * 1024;
/// Clients renew every third of the lease TTL, a documented contract in the
/// README, so a lease survives two missed renewals before it expires.
const RENEW_INTERVAL_DIVISOR: u32 = 3;
const MIN_TOKEN_LENGTH: usize = 32;

#[derive(Clone)]
struct ControlState {
    registry: Arc<Registry>,
    authorization: Arc<str>,
    shutdown: CancellationToken,
}

pub fn router(registry: Arc<Registry>, token: &str, shutdown: CancellationToken) -> Router {
    let state = ControlState {
        registry,
        authorization: Arc::from(format!("Bearer {token}")),
        shutdown,
    };
    Router::new()
        .route(CONTROL_PATH, post(create_lease).get(list_leases))
        .route("/v1/leases/{lease_id}", delete(remove_lease))
        .route("/v1/leases/{lease_id}/renew", put(renew_lease))
        .route(STATUS_PATH, get(status))
        .route(SHUTDOWN_PATH, post(shutdown_server))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(DefaultBodyLimit::max(CONTROL_MAX_BODY_BYTES))
        .with_state(state)
}

async fn authenticate(
    State(state): State<ControlState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let provided = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let authorized = provided.len() == state.authorization.len()
        && bool::from(provided.as_bytes().ct_eq(state.authorization.as_bytes()));
    if authorized {
        next.run(request).await
    } else {
        ControlApiError::Unauthorized.into_response()
    }
}

async fn status() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

/// Requests the same graceful shutdown as a termination signal; the server
/// finishes in-flight work, removes the descriptor, and exits.
async fn shutdown_server(State(state): State<ControlState>) -> Response {
    state.shutdown.cancel();
    StatusCode::ACCEPTED.into_response()
}

async fn create_lease(
    State(state): State<ControlState>,
    Json(request): Json<CreateLeaseRequest>,
) -> Response {
    match state.registry.create(request).await {
        Ok(lease_id) => (
            StatusCode::CREATED,
            Json(lease_response(&state.registry, lease_id)),
        )
            .into_response(),
        Err(error) => ControlApiError::from(error).into_response(),
    }
}

async fn renew_lease(State(state): State<ControlState>, Path(lease_id): Path<String>) -> Response {
    let Ok(lease_id) = LeaseId::from_str(&lease_id) else {
        return ControlApiError::InvalidLeaseId.into_response();
    };
    match state.registry.renew(lease_id).await {
        Ok(()) => Json(lease_response(&state.registry, lease_id)).into_response(),
        Err(error) => ControlApiError::from(error).into_response(),
    }
}

async fn remove_lease(State(state): State<ControlState>, Path(lease_id): Path<String>) -> Response {
    let Ok(lease_id) = LeaseId::from_str(&lease_id) else {
        return ControlApiError::InvalidLeaseId.into_response();
    };
    match state.registry.remove(lease_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => ControlApiError::from(error).into_response(),
    }
}

async fn list_leases(State(state): State<ControlState>) -> Response {
    Json(LeaseListResponse {
        leases: state.registry.list().await,
    })
    .into_response()
}

fn lease_response(registry: &Registry, lease_id: LeaseId) -> LeaseResponse {
    let lease_ttl = registry.lease_ttl();
    LeaseResponse {
        lease_id,
        lease_ttl_ms: duration_millis(lease_ttl),
        renew_after_ms: duration_millis(lease_ttl / RENEW_INTERVAL_DIVISOR),
    }
}

#[derive(Clone, Copy, Debug)]
enum ControlApiError {
    Unauthorized,
    InvalidLeaseId,
    NotFound,
    NonLoopbackTarget,
    Capacity(usize),
}

impl From<RegistryError> for ControlApiError {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::NotFound => Self::NotFound,
            RegistryError::NonLoopbackTarget => Self::NonLoopbackTarget,
            RegistryError::Capacity(limit) => Self::Capacity(limit),
        }
    }
}

impl IntoResponse for ControlApiError {
    fn into_response(self) -> Response {
        let (status, error, message) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "valid control authorization is required".to_owned(),
            ),
            Self::InvalidLeaseId => (
                StatusCode::BAD_REQUEST,
                "invalid-lease-id",
                "lease ID must be a UUID".to_owned(),
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "lease-not-found",
                "lease not found or expired".to_owned(),
            ),
            Self::NonLoopbackTarget => (
                StatusCode::BAD_REQUEST,
                "non-loopback-target",
                "target URL must use localhost or a loopback IP address".to_owned(),
            ),
            Self::Capacity(limit) => (
                StatusCode::CONFLICT,
                "target-capacity",
                format!("target capacity of {limit} has been reached"),
            ),
        };
        (
            status,
            Json(ErrorResponse {
                error: error.to_owned(),
                message,
            }),
        )
            .into_response()
    }
}

#[derive(Clone)]
pub struct ControlClient {
    client: reqwest::Client,
    base_url: Url,
    authorization: String,
}

impl ControlClient {
    pub fn connect(paths: &RuntimePaths) -> Result<Self, ControlClientError> {
        let descriptor = paths
            .read_descriptor()
            .map_err(ControlClientError::Descriptor)?;
        Self::from_descriptor(&descriptor)
    }

    pub fn from_descriptor(descriptor: &ControlDescriptor) -> Result<Self, ControlClientError> {
        if descriptor.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(ControlClientError::ProtocolVersion {
                expected: CONTROL_PROTOCOL_VERSION,
                actual: descriptor.protocol_version,
            });
        }
        let address: SocketAddr = descriptor
            .address
            .parse()
            .map_err(ControlClientError::Address)?;
        if !address.ip().is_loopback() {
            return Err(ControlClientError::NonLoopbackControlAddress(address));
        }
        if descriptor.token.len() < MIN_TOKEN_LENGTH
            || descriptor
                .token
                .bytes()
                .any(|byte| byte.is_ascii_whitespace())
        {
            return Err(ControlClientError::InvalidToken);
        }
        let base_url =
            Url::parse(&format!("http://{address}")).map_err(ControlClientError::BaseUrl)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CONTROL_REQUEST_TIMEOUT)
            .build()
            .map_err(ControlClientError::BuildClient)?;
        Ok(Self {
            client,
            base_url,
            authorization: format!("Bearer {}", descriptor.token),
        })
    }

    pub async fn create(
        &self,
        request: &CreateLeaseRequest,
    ) -> Result<LeaseResponse, ControlClientError> {
        let request = self.client.post(self.endpoint(CONTROL_PATH)?).json(request);
        decode(self.send(request).await?).await
    }

    pub async fn renew(&self, lease_id: LeaseId) -> Result<LeaseResponse, ControlClientError> {
        let endpoint = self.endpoint(&format!("{CONTROL_PATH}/{lease_id}/renew"))?;
        decode(self.send(self.client.put(endpoint)).await?).await
    }

    pub async fn remove(&self, lease_id: LeaseId) -> Result<(), ControlClientError> {
        let endpoint = self.endpoint(&format!("{CONTROL_PATH}/{lease_id}"))?;
        self.send(self.client.delete(endpoint)).await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<LeaseListResponse, ControlClientError> {
        let request = self.client.get(self.endpoint(CONTROL_PATH)?);
        decode(self.send(request).await?).await
    }

    pub async fn status(&self) -> Result<(), ControlClientError> {
        self.send(self.client.get(self.endpoint(STATUS_PATH)?))
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), ControlClientError> {
        self.send(self.client.post(self.endpoint(SHUTDOWN_PATH)?))
            .await?;
        Ok(())
    }

    /// Sends an authorized request and returns the response only when its
    /// status reports success.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, ControlClientError> {
        let response = request
            .header(AUTHORIZATION, &self.authorization)
            .send()
            .await
            .map_err(ControlClientError::Request)?;
        let status = response.status();
        if status.is_success() {
            Ok(response)
        } else {
            Err(rejection(status, response).await)
        }
    }

    fn endpoint(&self, path: &str) -> Result<Url, ControlClientError> {
        self.base_url
            .join(path)
            .map_err(ControlClientError::BaseUrl)
    }
}

async fn decode<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, ControlClientError> {
    response
        .json()
        .await
        .map_err(ControlClientError::DecodeResponse)
}

/// A rejection always reports the HTTP status, even when the responder is not
/// this control server and the body is not the expected JSON error shape.
async fn rejection(status: reqwest::StatusCode, response: reqwest::Response) -> ControlClientError {
    let message = match response.json::<ErrorResponse>().await {
        Ok(error) => error.message,
        Err(_) => "the response body was not a control protocol error".to_owned(),
    };
    ControlClientError::Rejected { status, message }
}

#[derive(Debug, Error)]
pub enum ControlClientError {
    #[error("failed to load the local control descriptor: {0}")]
    Descriptor(RuntimeError),
    #[error("control protocol version {actual} is incompatible; expected {expected}")]
    ProtocolVersion { expected: u16, actual: u16 },
    #[error("invalid control address: {0}")]
    Address(std::net::AddrParseError),
    #[error("control address must be loopback, got {0}")]
    NonLoopbackControlAddress(SocketAddr),
    #[error("control descriptor contains an invalid authorization token")]
    InvalidToken,
    #[error("invalid control base URL: {0}")]
    BaseUrl(url::ParseError),
    #[error("failed to build the control client: {0}")]
    BuildClient(reqwest::Error),
    #[error("control request failed: {0}")]
    Request(reqwest::Error),
    #[error("failed to decode the control response: {0}")]
    DecodeResponse(reqwest::Error),
    #[error("control request was rejected with HTTP {status}: {message}")]
    Rejected {
        status: reqwest::StatusCode,
        message: String,
    },
}

use std::{collections::HashSet, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{
        HeaderMap, HeaderName, HeaderValue, Request, StatusCode,
        header::{CONNECTION, CONTENT_LENGTH, HOST, TRANSFER_ENCODING},
    },
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use http::Method;
use serde::Serialize;
use tokio::{sync::Semaphore, task::JoinSet, time::timeout};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    domain::{LeaseId, ResponsePolicy},
    registry::{DispatchTarget, Registry},
};

const STANDARD_HOP_BY_HOP_HEADERS: [&str; 8] = [
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

#[derive(Clone, Debug)]
pub struct IngressConfig {
    pub response_policy: ResponsePolicy,
    pub accept_when_empty: bool,
    pub max_body_bytes: usize,
    pub target_timeout: Duration,
    pub max_inflight_requests: usize,
    pub max_concurrent_deliveries: usize,
}

#[derive(Clone)]
struct IngressState {
    registry: Arc<Registry>,
    client: reqwest::Client,
    config: IngressConfig,
    inflight_requests: Arc<Semaphore>,
    concurrent_deliveries: Arc<Semaphore>,
}

pub fn router(registry: Arc<Registry>, config: IngressConfig) -> Result<Router, reqwest::Error> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let state = IngressState {
        registry,
        client,
        inflight_requests: Arc::new(Semaphore::new(config.max_inflight_requests)),
        concurrent_deliveries: Arc::new(Semaphore::new(config.max_concurrent_deliveries)),
        config,
    };
    Ok(Router::new().fallback(forward).with_state(state))
}

async fn forward(State(state): State<IngressState>, request: Request<Body>) -> Response {
    let request_id = Uuid::new_v4();
    let Ok(inflight_permit) = Arc::clone(&state.inflight_requests).try_acquire_owned() else {
        return short_circuit_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            0,
            "ingress-capacity",
        );
    };

    let (parts, body) = request.into_parts();
    let host = effective_host(&parts).map(str::to_owned);
    let targets = state
        .registry
        .matching(&parts.method, parts.uri.path(), host.as_deref())
        .await;

    if targets.is_empty() {
        drop(inflight_permit);
        let status = if state.config.accept_when_empty {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        return short_circuit_response(status, request_id, 0, "no-targets");
    }

    let body = match to_bytes(body, state.config.max_body_bytes).await {
        Ok(body) => body,
        Err(error) => {
            drop(inflight_permit);
            let (status, reason) = classify_body_read_error(&error);
            return short_circuit_response(status, request_id, targets.len(), reason);
        }
    };

    let context = Arc::new(ForwardContext {
        method: parts.method,
        query: parts.uri.query().map(str::to_owned),
        headers: parts.headers,
        host,
        body,
    });
    let summary = dispatch_all(&state, request_id, context, targets).await;
    drop(inflight_permit);
    aggregate_response(&state.config, request_id, summary)
}

async fn dispatch_all(
    state: &IngressState,
    request_id: Uuid,
    context: Arc<ForwardContext>,
    targets: Vec<DispatchTarget>,
) -> DeliverySummary {
    let total = targets.len();
    let mut deliveries = JoinSet::new();
    for target in targets {
        let context = Arc::clone(&context);
        let client = state.client.clone();
        let permits = Arc::clone(&state.concurrent_deliveries);
        let target_timeout = state.config.target_timeout;
        deliveries.spawn(async move {
            let lease_id = target.lease_id;
            let result = timeout(target_timeout, async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| DeliveryError::ShuttingDown)?;
                deliver(&client, &target, &context)
                    .await
                    .map_err(DeliveryError::Request)
            })
            .await;
            match result {
                Ok(Ok(status)) => DeliveryResult::Response { lease_id, status },
                Ok(Err(error)) => DeliveryResult::Error { lease_id, error },
                Err(_) => DeliveryResult::Timeout { lease_id },
            }
        });
    }

    let mut succeeded = 0;
    let mut timed_out = false;
    while let Some(result) = deliveries.join_next().await {
        match result {
            Ok(DeliveryResult::Response { lease_id, status }) if status.is_success() => {
                succeeded += 1;
                info!(%request_id, %lease_id, %status, "webhook target accepted request");
            }
            Ok(DeliveryResult::Response { lease_id, status }) => {
                warn!(%request_id, %lease_id, %status, "webhook target rejected request");
            }
            Ok(DeliveryResult::Error { lease_id, error }) => {
                warn!(%request_id, %lease_id, %error, "webhook target request failed");
            }
            Ok(DeliveryResult::Timeout { lease_id }) => {
                timed_out = true;
                warn!(%request_id, %lease_id, "webhook target timed out");
            }
            Err(error) => {
                warn!(%request_id, %error, "webhook delivery task failed");
            }
        }
    }
    DeliverySummary {
        total,
        succeeded,
        timed_out,
    }
}

fn aggregate_response(
    config: &IngressConfig,
    request_id: Uuid,
    summary: DeliverySummary,
) -> Response {
    let failed = summary.total - summary.succeeded;
    let accepted = config
        .response_policy
        .accepts(summary.succeeded, summary.total);
    let status = if accepted {
        StatusCode::OK
    } else if summary.timed_out {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::BAD_GATEWAY
    };
    info!(
        %request_id,
        matched = summary.total,
        succeeded = summary.succeeded,
        failed,
        %status,
        "webhook fan-out completed"
    );
    (
        status,
        Json(IngressResponse {
            request_id,
            matched: summary.total,
            succeeded: summary.succeeded,
            failed,
            reason: None,
        }),
    )
        .into_response()
}

/// The authority of the request target takes precedence over the `Host`
/// header, both for HTTP/2 requests, which normally carry the authority as a
/// pseudo-header and no `Host` header at all, and for HTTP/1.1 absolute-form
/// request targets.
fn effective_host(parts: &http::request::Parts) -> Option<&str> {
    parts
        .uri
        .authority()
        .map(http::uri::Authority::as_str)
        .or_else(|| {
            parts
                .headers
                .get(HOST)
                .and_then(|value| value.to_str().ok())
        })
}

struct ForwardContext {
    method: Method,
    query: Option<String>,
    headers: HeaderMap,
    host: Option<String>,
    body: Bytes,
}

async fn deliver(
    client: &reqwest::Client,
    target: &DispatchTarget,
    context: &ForwardContext,
) -> Result<StatusCode, reqwest::Error> {
    let url = target.target.with_query(context.query.as_deref());
    let headers = forwarded_headers(
        &context.headers,
        target.preserve_host,
        context.host.as_deref(),
    );
    client
        .request(context.method.clone(), url)
        .headers(headers)
        .body(context.body.clone())
        .send()
        .await
        .map(|response| response.status())
}

fn forwarded_headers(
    incoming: &HeaderMap,
    preserve_host: bool,
    effective_host: Option<&str>,
) -> HeaderMap {
    let connection_headers = connection_header_names(incoming);
    let mut headers = incoming.clone();
    headers.remove(CONNECTION);
    headers.remove(CONTENT_LENGTH);
    headers.remove(HOST);
    headers.remove(TRANSFER_ENCODING);
    for name in STANDARD_HOP_BY_HOP_HEADERS {
        headers.remove(name);
    }
    for name in connection_headers {
        headers.remove(name);
    }
    if preserve_host
        && let Some(host) = effective_host
        && let Ok(value) = HeaderValue::from_str(host)
    {
        headers.insert(HOST, value);
    }
    headers
}

fn connection_header_names(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect()
}

#[derive(Debug)]
enum DeliveryResult {
    Response {
        lease_id: LeaseId,
        status: StatusCode,
    },
    Error {
        lease_id: LeaseId,
        error: DeliveryError,
    },
    Timeout {
        lease_id: LeaseId,
    },
}

#[derive(Clone, Copy)]
struct DeliverySummary {
    total: usize,
    succeeded: usize,
    timed_out: bool,
}

#[derive(Debug, thiserror::Error)]
enum DeliveryError {
    #[error("delivery concurrency controller is shutting down")]
    ShuttingDown,
    #[error("HTTP request failed: {0}")]
    Request(reqwest::Error),
}

#[derive(Debug, Serialize)]
struct IngressResponse {
    request_id: Uuid,
    matched: usize,
    succeeded: usize,
    failed: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

fn classify_body_read_error(error: &axum::Error) -> (StatusCode, &'static str) {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        if current.is::<http_body_util::LengthLimitError>() {
            return (StatusCode::PAYLOAD_TOO_LARGE, "body-too-large");
        }
        source = current.source();
    }
    (StatusCode::BAD_REQUEST, "body-read-failed")
}

/// Reports a request that ended before any delivery was attempted.
fn short_circuit_response(
    status: StatusCode,
    request_id: Uuid,
    matched: usize,
    reason: &'static str,
) -> Response {
    (
        status,
        Json(IngressResponse {
            request_id,
            matched,
            succeeded: 0,
            failed: 0,
            reason: Some(reason),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use http::{HeaderMap, HeaderValue, Request, header::CONNECTION, header::HOST};

    use super::{effective_host, forwarded_headers};

    #[test]
    fn forwarding_removes_standard_and_connection_named_hop_headers() {
        let mut incoming = HeaderMap::new();
        incoming.insert(CONNECTION, HeaderValue::from_static("keep-alive, x-local"));
        incoming.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        incoming.insert("x-local", HeaderValue::from_static("private"));
        incoming.insert("x-signature", HeaderValue::from_static("signed"));

        let forwarded = forwarded_headers(&incoming, false, None);

        assert!(!forwarded.contains_key(CONNECTION));
        assert!(!forwarded.contains_key("keep-alive"));
        assert!(!forwarded.contains_key("x-local"));
        assert_eq!(forwarded.get("x-signature"), incoming.get("x-signature"));
    }

    #[test]
    fn forwarding_preserves_the_effective_host_only_on_request() {
        let mut incoming = HeaderMap::new();
        incoming.insert(HOST, HeaderValue::from_static("stale.example.test"));

        let preserved = forwarded_headers(&incoming, true, Some("public.example.test"));
        let replaced = forwarded_headers(&incoming, false, Some("public.example.test"));

        assert_eq!(
            preserved.get(HOST),
            Some(&HeaderValue::from_static("public.example.test"))
        );
        assert!(!replaced.contains_key(HOST));
    }

    #[test]
    fn effective_host_prefers_the_request_target_authority() {
        let (with_authority, ()) = Request::builder()
            .uri("http://authority.example.test/hook")
            .header(HOST, "header.example.test")
            .body(())
            .expect("valid request")
            .into_parts();
        let (header_only, ()) = Request::builder()
            .uri("/hook")
            .header(HOST, "header.example.test")
            .body(())
            .expect("valid request")
            .into_parts();

        assert_eq!(
            effective_host(&with_authority),
            Some("authority.example.test")
        );
        assert_eq!(effective_host(&header_only), Some("header.example.test"));
    }
}

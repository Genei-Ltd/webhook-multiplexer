#![allow(clippy::expect_used)]

use std::{str::FromStr, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::Response,
};
use http::{
    Method,
    header::{HOST, LOCATION},
};
use tokio::{
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use webhook_multiplexer::{
    domain::{HttpMethod, ResponsePolicy, RouteHost, RoutePath, TargetUrl},
    ingress::{IngressConfig, router as ingress_router},
    protocol::CreateLeaseRequest,
    registry::Registry,
};

#[derive(Debug)]
struct CapturedRequest {
    method: Method,
    path_and_query: String,
    body: Vec<u8>,
    signature: Option<String>,
    host: Option<String>,
}

#[derive(Clone)]
struct TargetState {
    sender: mpsc::Sender<CapturedRequest>,
    status: StatusCode,
    delay: Duration,
}

struct RunningServer {
    address: std::net::SocketAddr,
    cancellation: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}

impl RunningServer {
    async fn stop(self) {
        self.cancellation.cancel();
        self.task
            .await
            .expect("server task")
            .expect("server result");
    }
}

async fn target_handler(
    State(state): State<TargetState>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 1024 * 1024)
        .await
        .expect("capture request body");
    if !state.delay.is_zero() {
        sleep(state.delay).await;
    }
    state
        .sender
        .send(CapturedRequest {
            method: parts.method,
            path_and_query: parts
                .uri
                .path_and_query()
                .map_or_else(|| parts.uri.path().to_owned(), ToString::to_string),
            body: body.to_vec(),
            signature: parts
                .headers
                .get("x-webhook-signature")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            host: parts
                .headers
                .get(HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        })
        .await
        .expect("capture receiver remains available");
    Response::builder()
        .status(state.status)
        .body(Body::empty())
        .expect("valid target response")
}

async fn spawn_target(
    status: StatusCode,
    delay: Duration,
) -> (RunningServer, mpsc::Receiver<CapturedRequest>) {
    let (sender, receiver) = mpsc::channel(8);
    let router = Router::new()
        .fallback(target_handler)
        .with_state(TargetState {
            sender,
            status,
            delay,
        });
    (spawn_server(router).await, receiver)
}

async fn redirect_handler(State(location): State<String>) -> Response<Body> {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(LOCATION, location)
        .body(Body::empty())
        .expect("valid redirect response")
}

async fn spawn_redirect_target(location: String) -> RunningServer {
    let router = Router::new()
        .fallback(redirect_handler)
        .with_state(location);
    spawn_server(router).await
}

async fn spawn_server(router: Router) -> RunningServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("test listener address");
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.child_token();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
    });
    RunningServer {
        address,
        cancellation,
        task,
    }
}

fn config(response_policy: ResponsePolicy) -> IngressConfig {
    IngressConfig {
        response_policy,
        accept_when_empty: false,
        max_body_bytes: 1024 * 1024,
        target_timeout: Duration::from_secs(1),
        max_inflight_requests: 8,
        max_concurrent_deliveries: 8,
    }
}

async fn register_scoped(
    registry: &Registry,
    address: std::net::SocketAddr,
    host: Option<&str>,
    preserve_host: bool,
) {
    registry
        .create(CreateLeaseRequest {
            method: HttpMethod::from_str("POST").expect("valid method"),
            path: RoutePath::from_str("/hooks").expect("valid path"),
            host: host.map(|value| RouteHost::from_str(value).expect("valid host")),
            target: TargetUrl::from_str(&format!("http://{address}/target")).expect("valid target"),
            preserve_host,
        })
        .await
        .expect("register target");
}

async fn register(registry: &Registry, address: std::net::SocketAddr) {
    register_scoped(registry, address, None, false).await;
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build test client")
}

#[tokio::test]
async fn one_webhook_reaches_every_matching_target_without_body_changes() {
    let (first, mut first_requests) = spawn_target(StatusCode::OK, Duration::ZERO).await;
    let (second, mut second_requests) = spawn_target(StatusCode::NO_CONTENT, Duration::ZERO).await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    register(&registry, first.address).await;
    register(&registry, second.address).await;
    let ingress = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::All)).expect("build ingress"),
    )
    .await;
    let payload = b"{\"event\":\"email.delivered\",\"bytes\":\"\\u0000\"}\n";

    let response = client()
        .post(format!("http://{}/hooks?attempt=1", ingress.address))
        .header("content-type", "application/json")
        .header("x-webhook-id", "msg_test")
        .header("x-webhook-timestamp", "1700000000")
        .header("x-webhook-signature", "test-signature")
        .body(payload.as_slice())
        .send()
        .await
        .expect("send webhook");

    assert_eq!(response.status(), StatusCode::OK);
    for captured in [
        timeout(Duration::from_secs(1), first_requests.recv())
            .await
            .expect("first target timeout")
            .expect("first request"),
        timeout(Duration::from_secs(1), second_requests.recv())
            .await
            .expect("second target timeout")
            .expect("second request"),
    ] {
        assert_eq!(captured.method, Method::POST);
        assert_eq!(captured.path_and_query, "/target?attempt=1");
        assert_eq!(captured.body, payload);
        assert_eq!(captured.signature.as_deref(), Some("test-signature"));
    }

    ingress.stop().await;
    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn response_policy_controls_partial_delivery_failure() {
    let (successful, _successful_requests) =
        spawn_target(StatusCode::NO_CONTENT, Duration::ZERO).await;
    let (failing, _failing_requests) =
        spawn_target(StatusCode::INTERNAL_SERVER_ERROR, Duration::ZERO).await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    register(&registry, successful.address).await;
    register(&registry, failing.address).await;
    let strict = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::All))
            .expect("build strict ingress"),
    )
    .await;
    let best_effort = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::Always))
            .expect("build best-effort ingress"),
    )
    .await;

    let strict_response = client()
        .post(format!("http://{}/hooks", strict.address))
        .body("event")
        .send()
        .await
        .expect("strict webhook");
    let best_effort_response = client()
        .post(format!("http://{}/hooks", best_effort.address))
        .body("event")
        .send()
        .await
        .expect("best-effort webhook");

    assert_eq!(strict_response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(best_effort_response.status(), StatusCode::OK);

    strict.stop().await;
    best_effort.stop().await;
    successful.stop().await;
    failing.stop().await;
}

#[tokio::test]
async fn target_timeout_is_bounded_and_reported() {
    let (slow, _requests) = spawn_target(StatusCode::OK, Duration::from_secs(1)).await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    register(&registry, slow.address).await;
    let mut ingress_config = config(ResponsePolicy::All);
    ingress_config.target_timeout = Duration::from_millis(25);
    let ingress =
        spawn_server(ingress_router(Arc::clone(&registry), ingress_config).expect("build ingress"))
            .await;

    let response = client()
        .post(format!("http://{}/hooks", ingress.address))
        .body("event")
        .send()
        .await
        .expect("send webhook");

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

    ingress.stop().await;
    slow.stop().await;
}

#[tokio::test]
async fn empty_routes_are_rejected_unless_explicitly_accepted() {
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    let rejecting = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::All))
            .expect("build rejecting ingress"),
    )
    .await;
    let mut accepting_config = config(ResponsePolicy::All);
    accepting_config.accept_when_empty = true;
    let accepting =
        spawn_server(ingress_router(registry, accepting_config).expect("build accepting ingress"))
            .await;

    let rejecting_response = client()
        .post(format!("http://{}/unregistered", rejecting.address))
        .send()
        .await
        .expect("send rejected webhook");
    let accepting_response = client()
        .post(format!("http://{}/unregistered", accepting.address))
        .send()
        .await
        .expect("send accepted webhook");

    assert_eq!(rejecting_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(accepting_response.status(), StatusCode::OK);
    rejecting.stop().await;
    accepting.stop().await;
}

#[tokio::test]
async fn request_bodies_over_the_configured_limit_are_rejected() {
    let (target, mut target_requests) = spawn_target(StatusCode::OK, Duration::ZERO).await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    register(&registry, target.address).await;
    let mut ingress_config = config(ResponsePolicy::All);
    ingress_config.max_body_bytes = 3;
    let ingress =
        spawn_server(ingress_router(registry, ingress_config).expect("build limited ingress"))
            .await;

    let response = client()
        .post(format!("http://{}/hooks", ingress.address))
        .body("four")
        .send()
        .await
        .expect("send oversized webhook");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        timeout(Duration::from_millis(250), target_requests.recv())
            .await
            .is_err()
    );
    ingress.stop().await;
    target.stop().await;
}

#[tokio::test]
async fn host_scoped_leases_route_by_the_incoming_host() {
    let (target, mut target_requests) = spawn_target(StatusCode::OK, Duration::ZERO).await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    register_scoped(&registry, target.address, Some("hooks.example.test"), false).await;
    let ingress = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::All)).expect("build ingress"),
    )
    .await;

    let matched = client()
        .post(format!("http://{}/hooks", ingress.address))
        .header(HOST, "HOOKS.Example.Test")
        .body("event")
        .send()
        .await
        .expect("send matching webhook");
    let unmatched = client()
        .post(format!("http://{}/hooks", ingress.address))
        .header(HOST, "other.example.test")
        .body("event")
        .send()
        .await
        .expect("send unmatched webhook");

    assert_eq!(matched.status(), StatusCode::OK);
    assert_eq!(unmatched.status(), StatusCode::SERVICE_UNAVAILABLE);
    let captured = timeout(Duration::from_secs(1), target_requests.recv())
        .await
        .expect("target timeout")
        .expect("captured request");
    assert_eq!(captured.body, b"event");
    assert!(
        timeout(Duration::from_millis(250), target_requests.recv())
            .await
            .is_err()
    );

    ingress.stop().await;
    target.stop().await;
}

#[tokio::test]
async fn preserve_host_forwards_the_incoming_host_only_when_requested() {
    let (preserving, mut preserving_requests) = spawn_target(StatusCode::OK, Duration::ZERO).await;
    let (replacing, mut replacing_requests) = spawn_target(StatusCode::OK, Duration::ZERO).await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    register_scoped(&registry, preserving.address, None, true).await;
    register_scoped(&registry, replacing.address, None, false).await;
    let ingress = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::All)).expect("build ingress"),
    )
    .await;

    let response = client()
        .post(format!("http://{}/hooks", ingress.address))
        .header(HOST, "public.example.test")
        .body("event")
        .send()
        .await
        .expect("send webhook");

    assert_eq!(response.status(), StatusCode::OK);
    let preserved = timeout(Duration::from_secs(1), preserving_requests.recv())
        .await
        .expect("preserving target timeout")
        .expect("preserved request");
    assert_eq!(preserved.host.as_deref(), Some("public.example.test"));
    let replaced = timeout(Duration::from_secs(1), replacing_requests.recv())
        .await
        .expect("replacing target timeout")
        .expect("replaced request");
    assert_eq!(replaced.host, Some(replacing.address.to_string()));

    ingress.stop().await;
    preserving.stop().await;
    replacing.stop().await;
}

#[tokio::test]
async fn http2_requests_route_by_the_request_authority() {
    let (target, mut target_requests) = spawn_target(StatusCode::OK, Duration::ZERO).await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    let ingress = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::All)).expect("build ingress"),
    )
    .await;
    register_scoped(
        &registry,
        target.address,
        Some(&ingress.address.to_string()),
        true,
    )
    .await;

    let response = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .build()
        .expect("build h2 client")
        .post(format!("http://{}/hooks", ingress.address))
        .body("event")
        .send()
        .await
        .expect("send h2 webhook");

    assert_eq!(response.status(), StatusCode::OK);
    let captured = timeout(Duration::from_secs(1), target_requests.recv())
        .await
        .expect("target timeout")
        .expect("captured request");
    assert_eq!(captured.body, b"event");
    assert_eq!(captured.host, Some(ingress.address.to_string()));

    ingress.stop().await;
    target.stop().await;
}

#[tokio::test]
async fn target_redirects_are_rejected_without_forwarding_again() {
    let (redirect_destination, mut redirected_requests) =
        spawn_target(StatusCode::OK, Duration::ZERO).await;
    let redirect_target = spawn_redirect_target(format!(
        "http://{}/redirected",
        redirect_destination.address
    ))
    .await;
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    register(&registry, redirect_target.address).await;
    let ingress = spawn_server(
        ingress_router(Arc::clone(&registry), config(ResponsePolicy::All)).expect("build ingress"),
    )
    .await;

    let response = client()
        .post(format!("http://{}/hooks", ingress.address))
        .body("event")
        .send()
        .await
        .expect("send webhook");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(
        timeout(Duration::from_millis(250), redirected_requests.recv())
            .await
            .is_err()
    );
    ingress.stop().await;
    redirect_target.stop().await;
    redirect_destination.stop().await;
}

#![allow(clippy::expect_used)]

use std::{str::FromStr, sync::Arc, time::Duration};

use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use webhook_multiplexer::{
    control::{ControlClient, router},
    domain::{HttpMethod, InstanceName, RoutePath, TargetUrl},
    protocol::{CONTROL_PROTOCOL_VERSION, ControlDescriptor, CreateLeaseRequest},
    registration::{RegistrationOptions, register_until_shutdown},
    registry::Registry,
    runtime::RuntimePaths,
};

const CONTROL_TOKEN: &str = "control-secret-control-secret-1234";
const WRONG_CONTROL_TOKEN: &str = "wrong-control-secret-wrong-secret-1";
const RESTARTED_CONTROL_TOKEN: &str = "restarted-control-secret-secret-123";

struct RunningControlServer {
    address: std::net::SocketAddr,
    cancellation: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}

impl RunningControlServer {
    async fn stop(self) {
        self.cancellation.cancel();
        self.task
            .await
            .expect("server task")
            .expect("server result");
    }
}

async fn spawn_control(registry: Arc<Registry>, token: &str) -> RunningControlServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind control listener");
    let address = listener.local_addr().expect("control listener address");
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.child_token();
    let router = router(registry, token, CancellationToken::new());
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
    });
    RunningControlServer {
        address,
        cancellation,
        task,
    }
}

fn descriptor(address: std::net::SocketAddr, token: &str) -> ControlDescriptor {
    ControlDescriptor {
        protocol_version: CONTROL_PROTOCOL_VERSION,
        address: address.to_string(),
        token: token.to_owned(),
        process_id: 123,
    }
}

fn registration() -> CreateLeaseRequest {
    CreateLeaseRequest {
        method: HttpMethod::from_str("POST").expect("valid method"),
        path: RoutePath::from_str("/webhook").expect("valid path"),
        host: None,
        target: TargetUrl::from_str("http://127.0.0.1:9001/target").expect("valid target"),
        preserve_host: false,
    }
}

#[tokio::test]
async fn authenticated_control_client_manages_the_full_lease_lifecycle() {
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    let server = spawn_control(Arc::clone(&registry), CONTROL_TOKEN).await;
    let client = ControlClient::from_descriptor(&descriptor(server.address, CONTROL_TOKEN))
        .expect("build control client");

    let created = client.create(&registration()).await.expect("create lease");
    assert_eq!(client.list().await.expect("list leases").leases.len(), 1);
    let renewed = client.renew(created.lease_id).await.expect("renew lease");
    assert_eq!(renewed.lease_id, created.lease_id);
    client.remove(created.lease_id).await.expect("remove lease");
    assert!(client.list().await.expect("list leases").leases.is_empty());

    server.stop().await;
}

#[tokio::test]
async fn control_server_rejects_an_invalid_token() {
    let registry = Arc::new(Registry::new(Duration::from_secs(20), 10, false));
    let server = spawn_control(registry, CONTROL_TOKEN).await;
    let client = ControlClient::from_descriptor(&descriptor(server.address, WRONG_CONTROL_TOKEN))
        .expect("build control client");

    let error = client
        .create(&registration())
        .await
        .expect_err("invalid token must fail");

    assert!(error.to_string().contains("HTTP 401"));
    server.stop().await;
}

#[tokio::test]
async fn registration_renews_reconnects_and_cleans_up_its_lease() {
    let temporary = TempDir::new().expect("temporary state directory");
    let instance = InstanceName::from_str("lifecycle").expect("valid instance");
    let paths = RuntimePaths::new(Some(temporary.path()), &instance);
    let lease_ttl = Duration::from_millis(240);

    let first_registry = Arc::new(Registry::new(lease_ttl, 10, false));
    let first_server = spawn_control(Arc::clone(&first_registry), CONTROL_TOKEN).await;
    paths
        .write_descriptor(&descriptor(first_server.address, CONTROL_TOKEN))
        .expect("write first descriptor");

    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
    let shutdown = CancellationToken::new();
    let registration_shutdown = shutdown.child_token();
    let registration_task = tokio::spawn(register_until_shutdown(
        RegistrationOptions {
            instance,
            state_directory: Some(temporary.path().to_path_buf()),
            method: HttpMethod::from_str("POST").expect("valid method"),
            path: RoutePath::from_str("/webhook").expect("valid path"),
            host: None,
            target: TargetUrl::from_str("http://127.0.0.1:9001/target").expect("valid target"),
            preserve_host: false,
        },
        registration_shutdown.cancelled_owned(),
        move |output| {
            output_sender
                .send(output)
                .map_err(|_| anyhow::anyhow!("registration output receiver closed"))
        },
    ));

    let first_output = timeout(Duration::from_secs(2), output_receiver.recv())
        .await
        .expect("first registration timeout")
        .expect("first registration output");
    sleep(Duration::from_millis(400)).await;
    assert_eq!(first_registry.list().await.len(), 1);

    first_server.stop().await;
    let second_registry = Arc::new(Registry::new(lease_ttl, 10, false));
    let second_server = spawn_control(Arc::clone(&second_registry), RESTARTED_CONTROL_TOKEN).await;
    paths
        .write_descriptor(&descriptor(second_server.address, RESTARTED_CONTROL_TOKEN))
        .expect("write restarted descriptor");

    let second_output = timeout(Duration::from_secs(3), output_receiver.recv())
        .await
        .expect("re-registration timeout")
        .expect("re-registration output");
    assert_ne!(second_output.lease_id, first_output.lease_id);
    assert_eq!(second_registry.list().await.len(), 1);

    shutdown.cancel();
    registration_task
        .await
        .expect("registration task")
        .expect("registration result");
    assert!(second_registry.list().await.is_empty());
    second_server.stop().await;
}

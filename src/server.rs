use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    control,
    domain::{InstanceName, ResponsePolicy},
    ingress::{self, IngressConfig},
    protocol::{CONTROL_PROTOCOL_VERSION, ControlDescriptor},
    registry::Registry,
    runtime::RuntimePaths,
};

const MIN_LEASE_TTL: Duration = Duration::from_secs(3);

#[derive(Clone, Debug)]
pub struct ServeOptions {
    pub instance: InstanceName,
    pub state_directory: Option<PathBuf>,
    pub listen: SocketAddr,
    pub response_policy: ResponsePolicy,
    pub accept_when_empty: bool,
    pub lease_ttl: Duration,
    pub target_timeout: Duration,
    pub max_body_bytes: NonZeroUsize,
    pub max_targets: NonZeroUsize,
    pub max_inflight_requests: NonZeroUsize,
    pub max_concurrent_deliveries: NonZeroUsize,
    pub allow_non_loopback_targets: bool,
}

pub async fn serve(options: ServeOptions) -> Result<()> {
    validate_options(&options)?;
    let paths = RuntimePaths::new(options.state_directory.as_deref(), &options.instance);
    let _instance_lock = paths
        .acquire_instance_lock()
        .context("failed to acquire the server instance lock")?;

    let ingress_listener = TcpListener::bind(options.listen)
        .await
        .with_context(|| format!("failed to bind ingress listener at {}", options.listen))?;
    let ingress_address = ingress_listener
        .local_addr()
        .context("failed to read the ingress listener address")?;
    let control_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind the private control listener")?;
    let control_address = control_listener
        .local_addr()
        .context("failed to read the control listener address")?;

    let registry = Arc::new(Registry::new(
        options.lease_ttl,
        options.max_targets.get(),
        options.allow_non_loopback_targets,
    ));
    let ingress_router = ingress::router(
        Arc::clone(&registry),
        IngressConfig {
            response_policy: options.response_policy,
            accept_when_empty: options.accept_when_empty,
            max_body_bytes: options.max_body_bytes.get(),
            target_timeout: options.target_timeout,
            max_inflight_requests: options.max_inflight_requests.get(),
            max_concurrent_deliveries: options.max_concurrent_deliveries.get(),
        },
    )
    .context("failed to build the webhook forwarding client")?;
    let cancellation = CancellationToken::new();
    let token = format!("{}.{}", Uuid::new_v4(), Uuid::new_v4());
    let control_router = control::router(Arc::clone(&registry), &token, cancellation.clone());
    paths
        .write_descriptor(&ControlDescriptor {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            address: control_address.to_string(),
            token,
            process_id: std::process::id(),
        })
        .context("failed to publish the local control descriptor")?;

    info!(
        instance = %options.instance,
        ingress = %ingress_address,
        control = %control_address,
        descriptor = %paths.descriptor_path().display(),
        "webhook multiplexer started"
    );

    let mut ingress_task =
        spawn_http_server(ingress_listener, ingress_router, cancellation.child_token());
    let mut control_task =
        spawn_http_server(control_listener, control_router, cancellation.child_token());

    let primary_result = tokio::select! {
        signal = shutdown_signal() => {
            signal.context("failed to listen for a shutdown signal")
        }
        () = cancellation.cancelled() => {
            info!(instance = %options.instance, "shutdown requested through the control API");
            Ok(())
        }
        result = &mut ingress_task => {
            task_stopped("ingress", result)
        }
        result = &mut control_task => {
            task_stopped("control", result)
        }
    };
    cancellation.cancel();

    let ingress_result = finish_task("ingress", &mut ingress_task).await;
    let control_result = finish_task("control", &mut control_task).await;
    if let Err(error) = paths.remove_descriptor() {
        warn!(%error, "failed to remove the control descriptor");
    }
    primary_result?;
    ingress_result?;
    control_result?;
    info!(instance = %options.instance, "webhook multiplexer stopped");
    Ok(())
}

fn spawn_http_server(
    listener: TcpListener,
    router: axum::Router,
    cancellation: CancellationToken,
) -> JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(cancellation.cancelled_owned())
            .await
    })
}

fn task_stopped(
    name: &'static str,
    result: Result<std::io::Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    task_result(name, result)?;
    Err(anyhow!("{name} server stopped unexpectedly"))
}

async fn finish_task(name: &'static str, task: &mut JoinHandle<std::io::Result<()>>) -> Result<()> {
    if task.is_finished() {
        return Ok(());
    }
    task_result(name, task.await)
}

fn task_result(
    name: &'static str,
    result: Result<std::io::Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error).with_context(|| format!("{name} server failed")),
        Err(error) => Err(error).with_context(|| format!("{name} server task failed")),
    }
}

fn validate_options(options: &ServeOptions) -> Result<()> {
    if !options.listen.ip().is_loopback() {
        return Err(anyhow!(
            "ingress listener must use a loopback address; expose it through a tunnel"
        ));
    }
    if options.lease_ttl < MIN_LEASE_TTL {
        return Err(anyhow!(
            "lease TTL must be at least {} seconds",
            MIN_LEASE_TTL.as_secs()
        ));
    }
    if options.target_timeout.is_zero() {
        return Err(anyhow!("target timeout must be greater than zero"));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
pub(crate) async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

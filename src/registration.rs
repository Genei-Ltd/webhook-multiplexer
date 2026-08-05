use std::{future::Future, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::{
    control::ControlClient,
    domain::{HttpMethod, InstanceName, LeaseId, RouteHost, RoutePath, TargetUrl},
    protocol::{CreateLeaseRequest, LeaseResponse},
    runtime::RuntimePaths,
};

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct RegistrationOptions {
    pub instance: InstanceName,
    pub state_directory: Option<PathBuf>,
    pub method: HttpMethod,
    pub path: RoutePath,
    pub host: Option<RouteHost>,
    pub target: TargetUrl,
    pub preserve_host: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct RegistrationOutput {
    pub lease_id: LeaseId,
    pub lease_ttl_ms: u64,
}

pub async fn register_until_shutdown<F, C>(
    options: RegistrationOptions,
    shutdown: F,
    mut on_registered: C,
) -> Result<()>
where
    F: Future<Output = ()> + Send,
    C: FnMut(RegistrationOutput) -> Result<()>,
{
    tokio::pin!(shutdown);
    let paths = RuntimePaths::new(options.state_directory.as_deref(), &options.instance);
    let request = CreateLeaseRequest {
        method: options.method,
        path: options.path,
        host: options.host,
        target: options.target,
        preserve_host: options.preserve_host,
    };
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

    loop {
        let registration = tokio::select! {
            () = &mut shutdown => return Ok(()),
            registration = connect_and_register(&paths, &request) => registration,
        };
        let (client, mut lease) = match registration {
            Ok(registration) => {
                reconnect_delay = INITIAL_RECONNECT_DELAY;
                registration
            }
            Err(error) => {
                warn!(%error, instance = %options.instance, "waiting for the multiplexer server");
                tokio::select! {
                    () = &mut shutdown => return Ok(()),
                    () = sleep(reconnect_delay) => {}
                }
                reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
                continue;
            }
        };

        info!(
            lease_id = %lease.lease_id,
            instance = %options.instance,
            "webhook target registered"
        );
        if let Err(callback_error) = on_registered(RegistrationOutput {
            lease_id: lease.lease_id,
            lease_ttl_ms: lease.lease_ttl_ms,
        }) {
            remove_lease_best_effort(&client, lease.lease_id).await;
            return Err(callback_error);
        }
        loop {
            tokio::select! {
                () = &mut shutdown => {
                    remove_lease_best_effort(&client, lease.lease_id).await;
                    return Ok(());
                }
                () = sleep(lease.renew_after()) => {
                    match client.renew(lease.lease_id).await {
                        Ok(renewed) => lease = renewed,
                        Err(error) => {
                            warn!(%error, lease_id = %lease.lease_id, "lease renewal failed; reconnecting");
                            remove_lease_best_effort(&client, lease.lease_id).await;
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// Removal is best effort; a lease on an unreachable server expires by TTL.
async fn remove_lease_best_effort(client: &ControlClient, lease_id: LeaseId) {
    if let Err(error) = client.remove(lease_id).await {
        warn!(%error, %lease_id, "failed to remove lease");
    }
}

async fn connect_and_register(
    paths: &RuntimePaths,
    request: &CreateLeaseRequest,
) -> Result<(ControlClient, LeaseResponse)> {
    let client =
        ControlClient::connect(paths).context("failed to connect to the control server")?;
    let lease = client
        .create(request)
        .await
        .context("failed to register the webhook target")?;
    Ok((client, lease))
}

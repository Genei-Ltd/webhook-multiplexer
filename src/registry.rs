use std::{collections::HashMap, time::Duration};

use http::Method;
use thiserror::Error;
use tokio::{sync::RwLock, time::Instant};

use crate::{
    domain::{LeaseId, RouteHost, TargetUrl},
    protocol::{CreateLeaseRequest, LeaseView, duration_millis},
};

#[derive(Clone, Debug)]
pub struct DispatchTarget {
    pub lease_id: LeaseId,
    pub target: TargetUrl,
    pub preserve_host: bool,
}

#[derive(Debug)]
struct Lease {
    request: CreateLeaseRequest,
    expires_at: Instant,
}

#[derive(Debug)]
pub struct Registry {
    leases: RwLock<HashMap<LeaseId, Lease>>,
    lease_ttl: Duration,
    max_targets: usize,
    allow_non_loopback_targets: bool,
}

impl Registry {
    #[must_use]
    pub fn new(lease_ttl: Duration, max_targets: usize, allow_non_loopback_targets: bool) -> Self {
        Self {
            leases: RwLock::new(HashMap::new()),
            lease_ttl,
            max_targets,
            allow_non_loopback_targets,
        }
    }

    pub async fn create(&self, request: CreateLeaseRequest) -> Result<LeaseId, RegistryError> {
        if !self.allow_non_loopback_targets && !request.target.is_loopback() {
            return Err(RegistryError::NonLoopbackTarget);
        }

        let now = Instant::now();
        let mut leases = self.leases.write().await;
        leases.retain(|_, lease| lease.expires_at > now);
        if leases.len() >= self.max_targets {
            return Err(RegistryError::Capacity(self.max_targets));
        }

        let lease_id = LeaseId::new();
        leases.insert(
            lease_id,
            Lease {
                request,
                expires_at: now + self.lease_ttl,
            },
        );
        Ok(lease_id)
    }

    pub async fn renew(&self, lease_id: LeaseId) -> Result<(), RegistryError> {
        let now = Instant::now();
        let mut leases = self.leases.write().await;
        leases.retain(|_, lease| lease.expires_at > now);
        let lease = leases.get_mut(&lease_id).ok_or(RegistryError::NotFound)?;
        lease.expires_at = now + self.lease_ttl;
        Ok(())
    }

    pub async fn remove(&self, lease_id: LeaseId) -> Result<(), RegistryError> {
        let removed = self.leases.write().await.remove(&lease_id);
        if removed.is_some() {
            Ok(())
        } else {
            Err(RegistryError::NotFound)
        }
    }

    /// Reads filter expired leases in place instead of pruning them, so the
    /// hot ingress path never contends on the write lock. Expired entries are
    /// removed by the next `create` or `renew`, and the map never exceeds
    /// `max_targets` entries either way.
    pub async fn matching(
        &self,
        method: &Method,
        path: &str,
        host: Option<&str>,
    ) -> Vec<DispatchTarget> {
        let now = Instant::now();
        let leases = self.leases.read().await;
        leases
            .iter()
            .filter(|(_, lease)| {
                lease.expires_at > now
                    && lease.request.method.matches(method)
                    && lease.request.path.as_str() == path
                    && host_matches(lease.request.host.as_ref(), host)
            })
            .map(|(lease_id, lease)| DispatchTarget {
                lease_id: *lease_id,
                target: lease.request.target.clone(),
                preserve_host: lease.request.preserve_host,
            })
            .collect()
    }

    pub async fn list(&self) -> Vec<LeaseView> {
        let now = Instant::now();
        let leases = self.leases.read().await;
        let mut views: Vec<_> = leases
            .iter()
            .filter(|(_, lease)| lease.expires_at > now)
            .map(|(lease_id, lease)| LeaseView {
                lease_id: *lease_id,
                method: lease.request.method.clone(),
                path: lease.request.path.clone(),
                host: lease.request.host.clone(),
                target: lease.request.target.clone(),
                preserve_host: lease.request.preserve_host,
                expires_in_ms: duration_millis(lease.expires_at.saturating_duration_since(now)),
            })
            .collect();
        views.sort_by_key(|view| view.lease_id);
        views
    }

    #[must_use]
    pub fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }
}

fn host_matches(expected: Option<&RouteHost>, actual: Option<&str>) -> bool {
    expected.is_none_or(|host| host.matches(actual))
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RegistryError {
    #[error("lease not found or expired")]
    NotFound,
    #[error("target URL must use localhost or a loopback IP address")]
    NonLoopbackTarget,
    #[error("target capacity of {0} has been reached")]
    Capacity(usize),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::{str::FromStr, time::Duration};

    use http::Method;

    use super::Registry;
    use crate::{
        domain::{HttpMethod, RouteHost, RoutePath, TargetUrl},
        protocol::CreateLeaseRequest,
    };

    fn request(target: &str) -> CreateLeaseRequest {
        CreateLeaseRequest {
            method: HttpMethod::from_str("POST").expect("valid test method"),
            path: RoutePath::from_str("/webhook").expect("valid test path"),
            host: None,
            target: TargetUrl::from_str(target).expect("valid test URL"),
            preserve_host: false,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn leases_expire_without_clean_shutdown() {
        let registry = Registry::new(Duration::from_secs(20), 10, false);
        registry
            .create(request("http://127.0.0.1:9001/hook"))
            .await
            .expect("create lease");
        assert_eq!(
            registry
                .matching(&Method::POST, "/webhook", None)
                .await
                .len(),
            1
        );

        tokio::time::advance(Duration::from_secs(21)).await;

        assert!(
            registry
                .matching(&Method::POST, "/webhook", None)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn non_loopback_targets_require_explicit_permission() {
        let registry = Registry::new(Duration::from_secs(20), 10, false);
        let result = registry.create(request("https://example.com/hook")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn routing_matches_method_path_and_optional_host_exactly() {
        let registry = Registry::new(Duration::from_secs(20), 10, false);
        let mut registration = request("http://127.0.0.1:9001/hook");
        registration.host =
            Some(RouteHost::from_str("webhooks.example.test:443").expect("valid test host"));
        registry.create(registration).await.expect("create lease");

        assert_eq!(
            registry
                .matching(&Method::POST, "/webhook", Some("WEBHOOKS.EXAMPLE.TEST:443"),)
                .await
                .len(),
            1
        );
        assert!(
            registry
                .matching(&Method::GET, "/webhook", Some("webhooks.example.test:443"),)
                .await
                .is_empty()
        );
        assert!(
            registry
                .matching(
                    &Method::POST,
                    "/webhook/",
                    Some("webhooks.example.test:443"),
                )
                .await
                .is_empty()
        );
        assert!(
            registry
                .matching(&Method::POST, "/webhook", Some("other.example.test:443"))
                .await
                .is_empty()
        );
    }
}

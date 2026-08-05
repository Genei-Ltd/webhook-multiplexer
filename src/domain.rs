use std::{fmt, str::FromStr};

use clap::ValueEnum;
use http::{
    Method,
    uri::{Authority, PathAndQuery},
};
use nutype::nutype;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::{Host, Url};
use uuid::Uuid;

const MAX_INSTANCE_NAME_LENGTH: usize = 64;

#[nutype(
    validate(with = validate_instance_name, error = InstanceNameError),
    derive(Clone, Debug, Eq, Hash, PartialEq, Display, FromStr, AsRef, Serialize, Deserialize, Default),
    default = "default"
)]
pub struct InstanceName(String);

impl InstanceName {
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
}

fn validate_instance_name(value: &str) -> Result<(), InstanceNameError> {
    let valid_length = !value.is_empty() && value.len() <= MAX_INSTANCE_NAME_LENGTH;
    let valid_characters = value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    let valid_edges = value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);

    if valid_length && valid_characters && valid_edges {
        Ok(())
    } else {
        Err(InstanceNameError)
    }
}

#[nutype(
    sanitize(uppercase),
    validate(with = validate_http_method, error = MethodError),
    derive(Clone, Debug, Eq, Hash, PartialEq, Display, FromStr, AsRef, Serialize, Deserialize, Default),
    default = "POST"
)]
pub struct HttpMethod(String);

impl HttpMethod {
    #[must_use]
    pub fn matches(&self, method: &Method) -> bool {
        let value: &str = self.as_ref();
        value == method.as_str()
    }
}

fn validate_http_method(value: &str) -> Result<(), MethodError> {
    Method::from_bytes(value.as_bytes())
        .map(|_| ())
        .map_err(|_| MethodError)
}

#[nutype(
    validate(with = validate_route_path, error = RoutePathError),
    derive(Clone, Debug, Eq, Hash, PartialEq, Display, FromStr, AsRef, Serialize, Deserialize)
)]
pub struct RoutePath(String);

impl RoutePath {
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
}

fn validate_route_path(value: &str) -> Result<(), RoutePathError> {
    let valid = value.starts_with('/')
        && !value.contains(['?', '#'])
        && PathAndQuery::from_str(value).is_ok_and(|path| path.as_str() == value);
    if valid { Ok(()) } else { Err(RoutePathError) }
}

#[nutype(
    sanitize(lowercase),
    validate(with = validate_route_host, error = RouteHostError),
    derive(Clone, Debug, Eq, Hash, PartialEq, Display, FromStr, AsRef, Serialize, Deserialize)
)]
pub struct RouteHost(String);

impl RouteHost {
    #[must_use]
    pub fn matches(&self, host: Option<&str>) -> bool {
        let value: &str = self.as_ref();
        host.is_some_and(|candidate| value.eq_ignore_ascii_case(candidate))
    }
}

fn validate_route_host(value: &str) -> Result<(), RouteHostError> {
    let authority = Authority::from_str(value).map_err(|_| RouteHostError)?;
    let has_invalid_user_info = authority.as_str().contains('@');
    let has_explicit_port = authority.as_str().len() > authority.host().len();
    let has_invalid_port = has_explicit_port && authority.port_u16().is_none();
    if has_invalid_user_info || has_invalid_port {
        Err(RouteHostError)
    } else {
        Ok(())
    }
}

#[nutype(
    validate(with = validate_target_url, error = TargetUrlError),
    derive(Clone, Debug, Eq, PartialEq, Display, FromStr, AsRef, Serialize, Deserialize)
)]
pub struct TargetUrl(Url);

impl TargetUrl {
    #[must_use]
    pub fn as_url(&self) -> &Url {
        self.as_ref()
    }

    #[must_use]
    pub fn is_loopback(&self) -> bool {
        match self.as_url().host() {
            Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            Some(Host::Ipv4(address)) => address.is_loopback(),
            Some(Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        }
    }

    #[must_use]
    pub fn with_query(&self, query: Option<&str>) -> Url {
        let mut url = self.as_url().clone();
        url.set_query(query);
        url
    }
}

fn validate_target_url(url: &Url) -> Result<(), TargetUrlError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(TargetUrlError::UnsupportedScheme);
    }
    if url.host().is_none() {
        return Err(TargetUrlError::MissingHost);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(TargetUrlError::Credentials);
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(TargetUrlError::QueryOrFragment);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseId(Uuid);

impl LeaseId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for LeaseId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for LeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for LeaseId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum ResponsePolicy {
    /// Accept only when every matching target returns 2xx.
    #[default]
    All,
    /// Accept when at least one matching target returns 2xx.
    Any,
    /// Accept after attempting every matching target, regardless of result.
    Always,
}

impl ResponsePolicy {
    #[must_use]
    pub fn accepts(self, succeeded: usize, total: usize) -> bool {
        match self {
            Self::All => total > 0 && succeeded == total,
            Self::Any => succeeded > 0,
            Self::Always => total > 0,
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error(
    "instance names must be 1-64 characters, start and end with an alphanumeric character, and contain only alphanumeric characters, '.', '_', or '-'"
)]
pub struct InstanceNameError;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("invalid HTTP method")]
pub struct MethodError;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error(
    "route paths must begin with '/' and must not contain a query, fragment, or control character"
)]
pub struct RoutePathError;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("invalid HTTP host or authority")]
pub struct RouteHostError;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TargetUrlError {
    #[error("target URL scheme must be http or https")]
    UnsupportedScheme,
    #[error("target URL must include a host")]
    MissingHost,
    #[error("target URL must not include credentials")]
    Credentials,
    #[error("target URL must not include a query string or fragment")]
    QueryOrFragment,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use http::Method;

    use super::{HttpMethod, InstanceName, ResponsePolicy, RouteHost, RoutePath, TargetUrl};

    #[test]
    fn instance_names_cannot_escape_the_state_directory() {
        assert!("panel-dev".parse::<InstanceName>().is_ok());
        assert!("../panel".parse::<InstanceName>().is_err());
        assert!("-panel".parse::<InstanceName>().is_err());
    }

    #[test]
    fn methods_normalise_to_uppercase() {
        let method = "post"
            .parse::<HttpMethod>()
            .expect("valid lowercase method");
        assert!(method.matches(&Method::POST));
        assert!("p@st".parse::<HttpMethod>().is_err());
    }

    #[test]
    fn route_paths_exclude_query_and_fragment_components() {
        assert!("/api/webhooks/payments".parse::<RoutePath>().is_ok());
        assert!("api/webhooks/payments".parse::<RoutePath>().is_err());
        assert!("/api/webhooks?source=test".parse::<RoutePath>().is_err());
        assert!("/api/webhooks with-space".parse::<RoutePath>().is_err());
    }

    #[test]
    fn route_hosts_exclude_user_info_and_invalid_ports() {
        assert!("example.test:443".parse::<RouteHost>().is_ok());
        assert!("[::1]:8080".parse::<RouteHost>().is_ok());
        assert!("user@example.test".parse::<RouteHost>().is_err());
        assert!("example.test:99999".parse::<RouteHost>().is_err());
    }

    #[test]
    fn target_urls_have_unambiguous_forwarding_components() {
        let loopback = "http://127.0.0.1:3230/hooks"
            .parse::<TargetUrl>()
            .expect("valid test target");
        assert!(loopback.is_loopback());
        assert!("https://example.com/hooks".parse::<TargetUrl>().is_ok());
        assert!("ftp://127.0.0.1/hooks".parse::<TargetUrl>().is_err());
        assert!(
            "http://user:secret@127.0.0.1/hooks"
                .parse::<TargetUrl>()
                .is_err()
        );
        assert!(
            "http://127.0.0.1/hooks?secret=value"
                .parse::<TargetUrl>()
                .is_err()
        );
    }

    #[test]
    fn serialized_domain_values_are_revalidated_at_input_boundaries() {
        assert!(
            serde_json::from_str::<TargetUrl>("\"http://127.0.0.1/hook?source=test\"").is_err()
        );
        assert!(serde_json::from_str::<RoutePath>("\"path-without-slash\"").is_err());
        assert!(serde_json::from_str::<RouteHost>("\"user@example.test\"").is_err());
    }

    #[test]
    fn response_policies_have_distinct_success_rules() {
        assert!(ResponsePolicy::All.accepts(2, 2));
        assert!(!ResponsePolicy::All.accepts(1, 2));
        assert!(ResponsePolicy::Any.accepts(1, 2));
        assert!(!ResponsePolicy::Any.accepts(0, 2));
        assert!(ResponsePolicy::Always.accepts(0, 2));
        assert!(!ResponsePolicy::Always.accepts(0, 0));
    }
}

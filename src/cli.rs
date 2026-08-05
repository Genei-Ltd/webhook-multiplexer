use std::{
    io::{self, Write},
    net::SocketAddr,
    num::NonZeroUsize,
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use tracing_subscriber::{EnvFilter, layer::Layer, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    control::{ControlClient, ControlClientError},
    domain::{HttpMethod, InstanceName, LeaseId, ResponsePolicy, RouteHost, RoutePath, TargetUrl},
    protocol::LeaseListResponse,
    registration::{RegistrationOptions, RegistrationOutput, register_until_shutdown},
    runtime::{RuntimeError, RuntimePaths},
    server::{ServeOptions, serve, shutdown_signal},
};

const DEFAULT_LISTEN: &str = "127.0.0.1:8080";
const DEFAULT_LEASE_TTL: &str = "20s";
const DEFAULT_TARGET_TIMEOUT: &str = "10s";
/// Covers a full drain of in-flight deliveries at the default target timeout.
const STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Parser)]
#[command(
    name = "hmux",
    version,
    about = "Fan out incoming HTTP webhooks to dynamically registered targets"
)]
pub struct Cli {
    /// Format for diagnostic logs written to stderr.
    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = LogFormat::Text,
        env = "WEBHOOK_MULTIPLEXER_LOG_FORMAT"
    )]
    log_format: LogFormat,

    /// Tracing filter for diagnostic logs written to stderr.
    #[arg(
        long,
        global = true,
        default_value = "info",
        env = "WEBHOOK_MULTIPLEXER_LOG"
    )]
    log_filter: String,

    #[command(subcommand)]
    command: Command,
}

impl Cli {
    pub fn initialize_logging(&self) -> Result<()> {
        let filter = EnvFilter::try_new(&self.log_filter)
            .with_context(|| format!("invalid log filter: {}", self.log_filter))?;
        let stderr_layer = match self.log_format {
            LogFormat::Text => tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .boxed(),
            LogFormat::Json => tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr)
                .boxed(),
        };
        tracing_subscriber::registry()
            .with(filter)
            .with(stderr_layer)
            .try_init()
            .context("failed to initialize logging")
    }

    pub async fn run(self) -> Result<ExitCode> {
        match self.command {
            Command::Serve(arguments) => serve(arguments.into_options()).await?,
            Command::Register(arguments) => run_register(arguments).await?,
            Command::List(arguments) => run_list(arguments).await?,
            Command::Unregister(arguments) => run_unregister(arguments).await?,
            Command::Stop(arguments) => run_stop(arguments).await?,
            Command::Status(arguments) => return run_status(arguments).await,
        }
        Ok(ExitCode::SUCCESS)
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the ingress and private control servers.
    Serve(ServeArguments),
    /// Register a target and keep its lease alive until shutdown.
    Register(RegisterArguments),
    /// List active target leases.
    List(ListArguments),
    /// Remove an active target lease.
    Unregister(UnregisterArguments),
    /// Request a graceful server shutdown and wait for it to finish.
    Stop(StopArguments),
    /// Report whether a server is running. Exits non-zero when it is not.
    Status(StatusArguments),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum LogFormat {
    /// Human-readable logs.
    #[default]
    Text,
    /// Newline-delimited JSON logs.
    Json,
}

#[derive(Clone, Debug, Args)]
struct InstanceArguments {
    /// Name of the independent server and control namespace.
    #[arg(long, default_value = "default", env = "WEBHOOK_MULTIPLEXER_INSTANCE")]
    instance: InstanceName,

    /// Root directory for local control state.
    #[arg(long, env = "WEBHOOK_MULTIPLEXER_STATE_DIR")]
    state_directory: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ServeArguments {
    #[command(flatten)]
    common: InstanceArguments,

    /// Loopback address for incoming webhooks. Port 0 selects a free port.
    #[arg(long, default_value = DEFAULT_LISTEN)]
    listen: SocketAddr,

    /// Rule used to accept or reject the aggregate delivery result.
    #[arg(long, value_enum, default_value_t = ResponsePolicy::All)]
    response_policy: ResponsePolicy,

    /// Return success when no target matches instead of HTTP 503.
    #[arg(long)]
    accept_when_empty: bool,

    /// Time before a registration expires without renewal. Minimum: 3s.
    #[arg(long, default_value = DEFAULT_LEASE_TTL, value_parser = parse_duration)]
    lease_ttl: Duration,

    /// Maximum time for one delivery, including capacity wait time.
    #[arg(long, default_value = DEFAULT_TARGET_TIMEOUT, value_parser = parse_duration)]
    target_timeout: Duration,

    /// Maximum accepted webhook body size in bytes.
    #[arg(long, default_value = "10485760")]
    max_body_bytes: NonZeroUsize,

    /// Maximum number of active target registrations.
    #[arg(long, default_value = "128")]
    max_targets: NonZeroUsize,

    /// Maximum number of webhook requests processed at once.
    #[arg(long, default_value = "64")]
    max_inflight_requests: NonZeroUsize,

    /// Maximum number of target deliveries in progress at once.
    #[arg(long, default_value = "128")]
    max_concurrent_deliveries: NonZeroUsize,

    /// Permit targets outside localhost and loopback IP ranges.
    #[arg(long)]
    allow_non_loopback_targets: bool,
}

impl ServeArguments {
    fn into_options(self) -> ServeOptions {
        ServeOptions {
            instance: self.common.instance,
            state_directory: self.common.state_directory,
            listen: self.listen,
            response_policy: self.response_policy,
            accept_when_empty: self.accept_when_empty,
            lease_ttl: self.lease_ttl,
            target_timeout: self.target_timeout,
            max_body_bytes: self.max_body_bytes,
            max_targets: self.max_targets,
            max_inflight_requests: self.max_inflight_requests,
            max_concurrent_deliveries: self.max_concurrent_deliveries,
            allow_non_loopback_targets: self.allow_non_loopback_targets,
        }
    }
}

#[derive(Debug, Args)]
struct RegisterArguments {
    #[command(flatten)]
    common: InstanceArguments,

    /// HTTP method to match exactly.
    #[arg(long, default_value = "POST")]
    method: HttpMethod,

    /// Incoming URL path to match exactly, beginning with '/'.
    #[arg(long)]
    path: RoutePath,

    /// Optional incoming Host authority to match exactly.
    #[arg(long)]
    host: Option<RouteHost>,

    /// HTTP or HTTPS URL that receives matching webhooks.
    #[arg(long)]
    target: TargetUrl,

    /// Forward the incoming Host header instead of the target host.
    #[arg(long)]
    preserve_host: bool,

    /// Write each registration and reconnection as one JSON line.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ListArguments {
    #[command(flatten)]
    common: InstanceArguments,

    /// Write the active lease list as JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct UnregisterArguments {
    #[command(flatten)]
    common: InstanceArguments,

    /// Lease ID returned by register or list.
    lease_id: LeaseId,
}

#[derive(Debug, Args)]
struct StopArguments {
    #[command(flatten)]
    common: InstanceArguments,
}

#[derive(Debug, Args)]
struct StatusArguments {
    #[command(flatten)]
    common: InstanceArguments,

    /// Write the status as JSON.
    #[arg(long)]
    json: bool,
}

async fn run_register(arguments: RegisterArguments) -> Result<()> {
    let options = RegistrationOptions {
        instance: arguments.common.instance,
        state_directory: arguments.common.state_directory,
        method: arguments.method,
        path: arguments.path,
        host: arguments.host,
        target: arguments.target,
        preserve_host: arguments.preserve_host,
    };
    let result = register_until_shutdown(options, wait_for_shutdown(), move |output| {
        print_registration(output, arguments.json)
    })
    .await;
    ignore_broken_pipe(result)
}

async fn run_list(arguments: ListArguments) -> Result<()> {
    let response = connect_control(&arguments.common)?.list().await?;
    ignore_broken_pipe(write_lease_list(&response, arguments.json))
}

fn write_lease_list(response: &LeaseListResponse, json: bool) -> Result<()> {
    let mut stdout = io::stdout().lock();
    if json {
        let encoded =
            serde_json::to_string_pretty(response).context("failed to encode lease list")?;
        writeln!(stdout, "{encoded}")?;
        return Ok(());
    }

    writeln!(stdout, "LEASE ID\tMETHOD\tHOST\tPATH\tTARGET\tEXPIRES IN")?;
    for lease in &response.leases {
        writeln!(
            stdout,
            "{}\t{}\t{}\t{}\t{}\t{}ms",
            lease.lease_id,
            lease.method,
            lease
                .host
                .as_ref()
                .map_or_else(|| "*".to_owned(), ToString::to_string),
            lease.path,
            lease.target,
            lease.expires_in_ms
        )?;
    }
    Ok(())
}

async fn run_unregister(arguments: UnregisterArguments) -> Result<()> {
    connect_control(&arguments.common)?
        .remove(arguments.lease_id)
        .await?;
    Ok(())
}

async fn run_stop(arguments: StopArguments) -> Result<()> {
    let paths = RuntimePaths::new(
        arguments.common.state_directory.as_deref(),
        &arguments.common.instance,
    );
    ControlClient::connect(&paths)?.shutdown().await?;

    let deadline = tokio::time::Instant::now() + STOP_WAIT_TIMEOUT;
    while paths.descriptor_path().exists() {
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!("shutdown was accepted, but the server is still draining requests");
            return Ok(());
        }
        tokio::time::sleep(STOP_POLL_INTERVAL).await;
    }
    Ok(())
}

async fn run_status(arguments: StatusArguments) -> Result<ExitCode> {
    let paths = RuntimePaths::new(
        arguments.common.state_directory.as_deref(),
        &arguments.common.instance,
    );
    let report = status_report(&paths, arguments.common.instance).await?;
    let code = if report.running {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    };
    ignore_broken_pipe(write_status(&report, arguments.json))?;
    Ok(code)
}

async fn status_report(paths: &RuntimePaths, instance: InstanceName) -> Result<StatusReport> {
    let descriptor = match paths.read_descriptor() {
        Ok(descriptor) => descriptor,
        Err(RuntimeError::ReadDescriptor(error)) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(StatusReport::not_running(instance, false));
        }
        Err(error) => return Err(error.into()),
    };
    let client = ControlClient::from_descriptor(&descriptor)?;
    match client.status().await {
        Ok(()) => {
            let active_leases = client.list().await?.leases.len();
            Ok(StatusReport {
                instance,
                running: true,
                stale_descriptor: false,
                server: Some(RunningServerStatus {
                    process_id: descriptor.process_id,
                    control_address: descriptor.address,
                    active_leases,
                }),
            })
        }
        Err(ControlClientError::Request(_)) => Ok(StatusReport::not_running(instance, true)),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Serialize)]
struct StatusReport {
    instance: InstanceName,
    running: bool,
    stale_descriptor: bool,
    #[serde(flatten)]
    server: Option<RunningServerStatus>,
}

impl StatusReport {
    fn not_running(instance: InstanceName, stale_descriptor: bool) -> Self {
        Self {
            instance,
            running: false,
            stale_descriptor,
            server: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct RunningServerStatus {
    process_id: u32,
    control_address: String,
    active_leases: usize,
}

fn write_status(report: &StatusReport, json: bool) -> Result<()> {
    let mut stdout = io::stdout().lock();
    if json {
        let line = serde_json::to_string(report).context("failed to encode status")?;
        writeln!(stdout, "{line}")?;
        return Ok(());
    }
    match &report.server {
        Some(server) => writeln!(
            stdout,
            "{}: running (process {}, control {}, {} active leases)",
            report.instance, server.process_id, server.control_address, server.active_leases
        )?,
        None if report.stale_descriptor => writeln!(
            stdout,
            "{}: not running (stale control descriptor)",
            report.instance
        )?,
        None => writeln!(stdout, "{}: not running", report.instance)?,
    }
    Ok(())
}

fn connect_control(common: &InstanceArguments) -> Result<ControlClient> {
    let paths = RuntimePaths::new(common.state_directory.as_deref(), &common.instance);
    Ok(ControlClient::connect(&paths)?)
}

/// A closed stdout means the consumer of this command's output has gone away,
/// which ends the command cleanly rather than as an error.
fn ignore_broken_pipe(result: Result<()>) -> Result<()> {
    match result {
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe) =>
        {
            Ok(())
        }
        result => result,
    }
}

fn print_registration(output: RegistrationOutput, json: bool) -> Result<()> {
    let mut stdout = io::stdout().lock();
    if json {
        let line =
            serde_json::to_string(&output).context("failed to encode registration output")?;
        writeln!(stdout, "{line}")?;
    } else {
        writeln!(
            stdout,
            "registered lease {} (TTL {}ms)",
            output.lease_id, output.lease_ttl_ms
        )?;
    }
    Ok(())
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
}

async fn wait_for_shutdown() {
    if shutdown_signal().await.is_err() {
        // Signal registration failed; fall back to waiting for Ctrl-C alone.
        let _result = tokio::signal::ctrl_c().await;
    }
}

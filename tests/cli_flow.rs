#![allow(clippy::expect_used)]

use std::{net::TcpListener as StdTcpListener, path::Path, time::Duration};

use axum::{Router, body::Bytes, extract::State, http::StatusCode};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use webhook_multiplexer::registration::RegistrationOutput;

const BINARY: &str = env!("CARGO_BIN_EXE_hmux");

struct TargetServer {
    address: std::net::SocketAddr,
    receiver: mpsc::Receiver<Bytes>,
    cancellation: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}

impl TargetServer {
    async fn stop(self) {
        self.cancellation.cancel();
        self.task
            .await
            .expect("target task")
            .expect("target result");
    }
}

async fn capture_body(State(sender): State<mpsc::Sender<Bytes>>, body: Bytes) -> StatusCode {
    sender.send(body).await.expect("capture receiver");
    StatusCode::NO_CONTENT
}

async fn spawn_target() -> TargetServer {
    let (sender, receiver) = mpsc::channel(4);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind target");
    let address = listener.local_addr().expect("target address");
    let cancellation = CancellationToken::new();
    let target_cancellation = cancellation.child_token();
    let router = Router::new().fallback(capture_body).with_state(sender);
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(target_cancellation.cancelled_owned())
            .await
    });
    TargetServer {
        address,
        receiver,
        cancellation,
        task,
    }
}

fn unused_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("reserve test port");
    listener.local_addr().expect("test port").port()
}

async fn wait_for_file(path: &Path) {
    timeout(Duration::from_secs(10), async {
        while !path.is_file() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("control descriptor timeout");
}

async fn stop_child(child: &mut Child) {
    if child.id().is_some() {
        child.kill().await.expect("stop child process");
    }
    let _status = child.wait().await;
}

fn base_command() -> Command {
    let mut command = Command::new(BINARY);
    command.arg("--log-filter").arg("warn");
    command
}

fn command(state_directory: &Path, subcommand: &str) -> Command {
    let mut command = base_command();
    command
        .arg(subcommand)
        .arg("--state-directory")
        .arg(state_directory);
    command
}

#[tokio::test]
async fn cli_serves_registers_lists_forwards_and_unregisters() {
    let temporary = TempDir::new().expect("temporary state directory");
    let ingress_port = unused_port();
    let mut server = command(temporary.path(), "serve");
    server
        .arg("--listen")
        .arg(format!("127.0.0.1:{ingress_port}"))
        .arg("--lease-ttl")
        .arg("3s")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut server = server.spawn().expect("start multiplexer server");
    wait_for_file(&temporary.path().join("default/control.json")).await;

    let mut target = spawn_target().await;
    let mut registration = command(temporary.path(), "register");
    registration
        .arg("--path")
        .arg("/hooks")
        .arg("--target")
        .arg(format!("http://{}/receive", target.address))
        .arg("--json")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let mut registration = registration.spawn().expect("start registration client");
    let stdout = registration.stdout.take().expect("registration stdout");
    let mut lines = BufReader::new(stdout).lines();
    let output = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("registration output timeout")
        .expect("read registration output")
        .expect("registration output line");
    let registered: RegistrationOutput = serde_json::from_str(&output).expect("registration JSON");

    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build webhook client")
        .post(format!("http://127.0.0.1:{ingress_port}/hooks"))
        .body("webhook-body")
        .send()
        .await
        .expect("send webhook");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        timeout(Duration::from_secs(1), target.receiver.recv())
            .await
            .expect("target receive timeout")
            .expect("target body"),
        "webhook-body"
    );

    let list = command(temporary.path(), "list")
        .arg("--json")
        .output()
        .await
        .expect("list leases");
    assert!(list.status.success());
    let list: serde_json::Value = serde_json::from_slice(&list.stdout).expect("lease list JSON");
    assert_eq!(list["leases"].as_array().expect("leases array").len(), 1);

    let status = command(temporary.path(), "status")
        .arg("--json")
        .output()
        .await
        .expect("status while running");
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).expect("status JSON");
    assert_eq!(status["running"], true);
    assert_eq!(status["active_leases"], 1);

    stop_child(&mut registration).await;
    let unregister = command(temporary.path(), "unregister")
        .arg(registered.lease_id.to_string())
        .output()
        .await
        .expect("unregister lease");
    assert!(unregister.status.success());
    let list = command(temporary.path(), "list")
        .arg("--json")
        .output()
        .await
        .expect("list empty leases");
    let list: serde_json::Value =
        serde_json::from_slice(&list.stdout).expect("empty lease list JSON");
    assert!(list["leases"].as_array().expect("leases array").is_empty());

    let stop = command(temporary.path(), "stop")
        .output()
        .await
        .expect("stop server");
    assert!(stop.status.success());
    let server_status = timeout(Duration::from_secs(10), server.wait())
        .await
        .expect("server exit timeout")
        .expect("server exit status");
    assert!(server_status.success());
    assert!(!temporary.path().join("default/control.json").exists());

    let stopped_status = command(temporary.path(), "status")
        .output()
        .await
        .expect("status after stop");
    assert!(!stopped_status.status.success());

    target.stop().await;
}

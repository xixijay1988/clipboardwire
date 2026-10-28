// SPDX-License-Identifier: GPL-3.0-or-later

//! End-to-end log-redaction check (task 1.8).
//!
//! Drives a real eager transfer through two real supervisors — real hub, real
//! WebSocket transports — with a tracing subscriber that captures everything at
//! INFO and above, then asserts the sent file's name appears nowhere in the
//! captured output.
//!
//! This is the only test that proves redaction rather than merely calling the
//! helper: `logging.rs` has unit tests for the identifier itself, but a call
//! site that formatted a raw path would still leak without this.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clipboardwire_core::client::transport::{spawn_with_options, SpawnOptions};
use clipboardwire_core::client::{
    run_supervisor_with_options, ClientConfig, ClipChange, Clipboard,
};
use clipboardwire_core::server::{build_app, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// A tracing writer that keeps every byte in memory so the test can search it.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn server_cfg(addr: std::net::SocketAddr) -> ServerConfig {
    ServerConfig {
        bind: addr,
        user: "alice".into(),
        password: "hunter2".into(),
        max_conns: 8,
        max_frame_bytes: 16 * 1024 * 1024,
        tls_cert_file: None,
        tls_key_file: None,
        tls_disabled: true,
        state_dir: None,
        stats: None,
    }
}

/// `log_file_names = false` is the default under test: redaction must be on
/// unless the user explicitly opts out.
fn client_cfg(addr: std::net::SocketAddr) -> ClientConfig {
    ClientConfig {
        server: format!("ws://{addr}/sync"),
        user: "alice".into(),
        password: "hunter2".into(),
        poll_ms: 1000,
        tls_ca_file: None,
        tls_insecure: false,
        hub: None,
        log_file_names: false,
        send_mode: Default::default(),
        receive_mode: Default::default(),
        pickup_auto: false,
        offer_ttl_minutes: 30,
        pull_window_chunks: 3,
        pull_idle_timeout_secs: 30,
    }
}

fn unique_dir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("cw-redact-{label}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "current_thread")]
async fn info_logs_carry_no_raw_file_name() {
    let capture = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(capture.clone())
        .finish();
    // `set_default` is thread-local, so this test deliberately uses the
    // single-threaded runtime: every task below (hub, both supervisors) then
    // runs on this thread and inherits the capture. A global default would
    // leak across the other tests in this binary.
    let _guard = tracing::subscriber::set_default(subscriber);

    const SECRET_NAME: &str = "customer-passwords-q3.txt";

    // A real transfer writes into the receiver's save dir. Point it at a temp
    // dir: the default is the developer's actual `~/Downloads/clipboardwire/`,
    // and a suite run must not leave copies of `customer-passwords-q3.txt`
    // there (it had accumulated 31 of them before this was caught).
    let save_dir = unique_dir("recv");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (app, _hub_join) = build_app(server_cfg(addr));
    let _server = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    // -- Receiver: a full supervisor, so the "saved received file" INFO line
    //    (the one that names the path) actually runs. --
    let (_recv_events_tx, recv_events_rx) = mpsc::channel::<ClipChange>(8);
    let (recv_apply_tx, _recv_apply_rx) = std::sync::mpsc::channel::<ClipChange>();
    let recv_clipboard = Clipboard {
        events_rx: recv_events_rx,
        apply_tx: recv_apply_tx,
    };
    // `receive_files: true` — `transport::spawn` defaults it to false, and a
    // receiver that drops inbound files never runs the log line under test.
    let (recv_transport, _recv_join) = spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: None,
            receive_files: true,
            one_shot: false,
            ..SpawnOptions::default()
        },
    );
    let save_dir_for_recv = save_dir.clone();
    tokio::spawn(async move {
        run_supervisor_with_options(
            recv_clipboard,
            recv_transport,
            None,
            false,
            Some(save_dir_for_recv),
        )
        .await
    });

    // -- Sender: also a supervisor, driven by an injected clipboard event so
    //    the whole eager send path (including its own log lines) runs. --
    let (send_events_tx, send_events_rx) = mpsc::channel::<ClipChange>(8);
    let (send_apply_tx, _send_apply_rx) = std::sync::mpsc::channel::<ClipChange>();
    let send_clipboard = Clipboard {
        events_rx: send_events_rx,
        apply_tx: send_apply_tx,
    };
    let (send_transport, _send_join) = spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: None,
            receive_files: false,
            one_shot: false,
            ..SpawnOptions::default()
        },
    );
    tokio::spawn(async move {
        run_supervisor_with_options(send_clipboard, send_transport, None, false, None).await
    });

    // Both peers need a completed handshake before the publish.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let send_dir = unique_dir("send");
    let src = send_dir.join(SECRET_NAME);
    fs::write(&src, b"contents that do not matter for this test").unwrap();

    send_events_tx
        .send(ClipChange::Files(vec![src]))
        .await
        .unwrap();

    // Wait for the receiver to publish the file to its clipboard: that is what
    // proves the "saved received file" INFO line was emitted.
    // Wait for the receiver's INFO line rather than for a clipboard event: the
    // line is exactly what this test is about, and it is emitted before the
    // batch is applied. Polling the capture also keeps the wait off the
    // blocking `apply_tx` receiver, which would stall the single-threaded
    // runtime and with it the transfer.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if capture.contents().contains("saved received file") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let logs = capture.contents();
    assert!(
        logs.contains("saved received file"),
        "the receiver's INFO line should have run:\n{logs}"
    );
    assert!(
        !logs.contains(SECRET_NAME),
        "INFO logs leaked the raw file name:\n{logs}"
    );
    assert!(
        !logs.contains("customer-passwords"),
        "INFO logs leaked part of the file name:\n{logs}"
    );
    // A pseudonym is actually present, so we know the redacted line ran rather
    // than the line being absent.
    assert!(
        logs.contains("file#"),
        "expected at least one redacted identifier in:\n{logs}"
    );

    for path in [&send_dir, &save_dir] {
        let _ = fs::remove_dir_all(path);
    }
}

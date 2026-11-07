// SPDX-License-Identifier: GPL-3.0-or-later

//! Received files must land without anyone naming a directory.
//!
//! The bug this covers was total: with `send_mode = "lazy"` and
//! `receive_mode = "auto"` (which resolves to `pickup` on macOS and Windows),
//! **every** completed transfer was discarded with
//!
//! ```text
//! WARN clipboardwire_core::client: digest arrived but no save dir is configured
//! ```
//!
//! `ReceivePolicy::from_config` set `save_dir: None`, the supervisor turned
//! `None` into `assembler: None`, and the digest arm threw the session away.
//! `FileReceiver` (the eager path) meanwhile called `FileReceiver::new()`, which
//! resolves the platform default itself — so eager worked and lazy never could.
//!
//! ## Why this file runs the child-process dance
//!
//! The default directory is `$HOME/Downloads/clipboardwire` on macOS and Linux,
//! read from the environment on **every** call (`dirs_sys::home_dir`). Pointing
//! it at a sandbox therefore means changing `$HOME` — process-global, hence
//! unsafe to do inside a test binary that runs other tests in parallel.
//!
//! So the test re-executes itself: the parent spawns `current_exe()` with
//! `HOME` set to a temp dir and a marker variable set, and the child (same
//! binary, same test name) takes the branch that actually runs the transfer.
//! This is the standard Rust idiom for "a test that needs its own environment"
//! and it keeps `PARALLELISM` intact.
//!
//! **What this test does not pass**: a `save_dir`. The whole point is that the
//! production call path — `run_with_status`, which passes `None` — has to work.
//! `lazy_transfer.rs::a_lazy_transfer_crosses_a_real_hub_byte_exactly` passes
//! `Some(recv_dir)` and therefore could never have caught this.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clipboardwire_core::client::{
    run_supervisor_with_policy, ClientConfig, ClientStatus, ClipChange, Clipboard, ReceiveMode,
    ReceivePolicy, SendMode, SendPolicy,
};
use clipboardwire_core::server::{build_app, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::watch;
use uuid::Uuid;

/// Set in the child so it knows to run the transfer rather than spawn itself.
const CHILD_MARKER: &str = "CW_LAZY_DEFAULT_DIR_CHILD";

fn unique_dir(label: &str) -> PathBuf {
    static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "cw-lazy-default-{label}-{}-{id}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
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
        send_mode: SendMode::Lazy,
        receive_mode: ReceiveMode::Pickup,
        pickup_auto: false,
        offer_ttl_minutes: 30,
        pull_window_chunks: 2,
        pull_idle_timeout_secs: 30,
    }
}

async fn wait_connected(rx: &mut watch::Receiver<ClientStatus>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if matches!(*rx.borrow_and_update(), ClientStatus::Connected) {
            return;
        }
        if Instant::now() > deadline {
            panic!("transport never reported Connected");
        }
        if rx.changed().await.is_err() {
            panic!("status channel closed before Connected");
        }
    }
}

/// Poll `dir` for a landed (non-`.partial`) file.
async fn wait_for_landed_file(dir: &Path, timeout: Duration) -> PathBuf {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().ends_with(".partial") {
                    continue;
                }
                if entry.path().is_file() {
                    return entry.path();
                }
            }
        }
        if Instant::now() > deadline {
            panic!("no file landed in {} within {timeout:?}", dir.display());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
fn a_lazy_transfer_lands_with_no_save_dir_configured() {
    if std::env::var_os(CHILD_MARKER).is_some() {
        // The child: `HOME` already points at the sandbox.
        child_body();
        return;
    }

    let sandbox_home = unique_dir("home");
    let status = std::process::Command::new(std::env::current_exe().expect("current_exe"))
        .args([
            "--exact",
            "a_lazy_transfer_lands_with_no_save_dir_configured",
            "--nocapture",
        ])
        .env(CHILD_MARKER, "1")
        .env("HOME", &sandbox_home)
        // On Linux the downloads dir comes from XDG, not HOME; point it at the
        // same sandbox so the test means the same thing there.
        .env("XDG_DOWNLOAD_DIR", sandbox_home.join("Downloads"))
        .status()
        .expect("re-running the test binary");

    assert!(
        status.success(),
        "the child run failed; see its output above. Expected a file under \
         {}/Downloads/clipboardwire",
        sandbox_home.display()
    );
    let _ = fs::remove_dir_all(&sandbox_home);
}

/// Runs inside the child process, where `HOME` is the sandbox.
fn child_body() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let landing = clipboardwire_core::client::file::default_save_dir()
            .expect("the sandbox HOME must yield a downloads dir");
        // Guard the premise: the sandbox really is where the default points,
        // so a passing assertion below cannot be about some other directory.
        assert!(
            landing.starts_with(std::env::var("HOME").unwrap()),
            "default_save_dir resolved outside the sandbox: {}",
            landing.display()
        );
        assert!(
            !landing.exists(),
            "the assembler must create the directory, not require it: {}",
            landing.display()
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (app, _hub_join) = build_app(server_cfg(addr));
        tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await;
        });

        // -- Receiver: **no save_dir at all** — the production shape. --
        let (recv_status_tx, mut recv_status_rx) = watch::channel(ClientStatus::Connecting);
        let (recv_transport, _recv_join) =
            clipboardwire_core::client::transport::spawn_with_options(
                client_cfg(addr),
                clipboardwire_core::client::transport::SpawnOptions {
                    status_tx: Some(recv_status_tx),
                    receive_files: true,
                    one_shot: false,
                    device: Some(Uuid::new_v4()),
                    caps: clipboardwire_core::protocol::Caps::LAZY_FILE,
                },
            );
        let (recv_events_tx, recv_events_rx) = tokio::sync::mpsc::channel::<ClipChange>(8);
        let (recv_apply_tx, _recv_apply_rx) = std::sync::mpsc::channel::<ClipChange>();
        // `save_dir: None` in **both** places: the argument and the policy.
        // This is exactly what `run_with_status` passes.
        let recv_supervisor = tokio::spawn(run_supervisor_with_policy(
            Clipboard {
                events_rx: recv_events_rx,
                apply_tx: recv_apply_tx,
            },
            recv_transport,
            None,
            false,
            None,
            SendPolicy {
                mode: SendMode::Eager,
                ..SendPolicy::default()
            },
            ReceivePolicy {
                mode: ReceiveMode::Pickup,
                pull_window_chunks: 2,
                save_dir: None,
            },
            None,
        ));
        wait_connected(&mut recv_status_rx).await;

        // -- Sender: lazy, an ordinary client. --
        let (send_status_tx, mut send_status_rx) = watch::channel(ClientStatus::Connecting);
        let (send_transport, _send_join) =
            clipboardwire_core::client::transport::spawn_with_options(
                client_cfg(addr),
                clipboardwire_core::client::transport::SpawnOptions {
                    status_tx: Some(send_status_tx),
                    receive_files: true,
                    one_shot: false,
                    device: Some(Uuid::new_v4()),
                    caps: clipboardwire_core::protocol::Caps::LAZY_FILE,
                },
            );
        let (send_events_tx, send_events_rx) = tokio::sync::mpsc::channel::<ClipChange>(8);
        let (send_apply_tx, _send_apply_rx) = std::sync::mpsc::channel::<ClipChange>();
        let send_supervisor = tokio::spawn(run_supervisor_with_policy(
            Clipboard {
                events_rx: send_events_rx,
                apply_tx: send_apply_tx,
            },
            send_transport,
            None,
            false,
            None,
            SendPolicy {
                mode: SendMode::Lazy,
                offer_ttl_ms: 60_000,
                ..SendPolicy::default()
            },
            ReceivePolicy {
                mode: ReceiveMode::Eager,
                ..ReceivePolicy::default()
            },
            None,
        ));
        wait_connected(&mut send_status_rx).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let src_dir = unique_dir("src");
        let src = src_dir.join("defaultland.bin");
        let expected: Vec<u8> = (0..(4 * 1024 * 1024 + 512u64))
            .map(|i| ((i.wrapping_mul(131)).wrapping_add(17) & 0xff) as u8)
            .collect();
        fs::write(&src, &expected).unwrap();

        send_events_tx
            .send(ClipChange::Files(vec![src.clone()]))
            .await
            .unwrap();

        // The only assertion that matters: a file appears in the **default**
        // directory, with the byte content the sender read.
        let landed = wait_for_landed_file(&landing, Duration::from_secs(20)).await;
        let received = fs::read(&landed).expect("the landed file is readable");
        assert_eq!(
            received.len(),
            expected.len(),
            "size mismatch in {}",
            landed.display()
        );
        assert_eq!(
            received,
            expected,
            "content mismatch in {}",
            landed.display()
        );
        assert_eq!(
            landed.file_name().and_then(|n| n.to_str()),
            Some("defaultland.bin"),
            "the original name must survive; got {}",
            landed.display()
        );

        send_supervisor.abort();
        recv_supervisor.abort();
        drop(send_events_tx);
        drop(recv_events_tx);
        let _ = fs::remove_dir_all(&src_dir);
    });
}

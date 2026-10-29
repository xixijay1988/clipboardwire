// SPDX-License-Identifier: GPL-3.0-or-later

//! `clipboardwire send` must exit once it has sent the file.
//!
//! This is the regression test for a release blocker: `send` delivered the file
//! and then hung forever. The cause was `let _ = transport.outbound_tx;` in
//! `run_send_once` — `let _ = expr` discards the *place*, not the value, so the
//! `mpsc::Sender` stayed alive inside `Transport`, the transport's `select!`
//! never observed its channel closing, and the one-shot task never returned.
//!
//! It runs the real binary against a real hub rather than calling a helper,
//! because the defect lived in the call site, not in any function the call site
//! used: `send_file_through` returned `Ok(())`, the transport was healthy, and
//! the bytes arrived. Only the exit was missing.

use std::io::Read;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cw-send-exit-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn send_exits_after_delivering_the_file() {
    // The binary under test is this crate's own; Cargo builds it for us and
    // exposes the path.
    let bin = env!("CARGO_BIN_EXE_clipboardwire");

    // A hub in-process on a loopback port picked by the OS.
    //
    // The std listener is bound here (so the port is known before the thread
    // starts), but it is converted to a tokio listener **inside** the hub
    // thread's runtime. `TcpListener::from_std` registers with the reactor of
    // the runtime it is called under; doing it under a temporary runtime that
    // is then dropped leaves a listener whose driver no longer exists.
    let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    std_listener.set_nonblocking(true).unwrap();

    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
    let hub = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            let cfg = clipboardwire_core::server::ServerConfig {
                bind: addr,
                user: "alice".into(),
                password: "hunter2".into(),
                max_conns: 8,
                max_frame_bytes: 8 * 1024 * 1024,
                tls_cert_file: None,
                tls_key_file: None,
                tls_disabled: true,
                state_dir: None,
                stats: None,
            };
            // The test ends by signalling this channel rather than by asking
            // the server to stop: `serve` only needs a future that resolves.
            let stop = async move {
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = shutdown_rx.recv();
                })
                .await;
            };
            // Tell the test the listener is live and owned by this runtime
            // before `serve` takes it.
            let _ = ready_tx.send(());
            let _ = clipboardwire_core::server::serve(listener, cfg, stop).await;
        });
    });
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the test hub's runtime failed to start");

    // Wait for the hub to accept a connection.
    wait_for_port(addr, Duration::from_secs(10));

    let dir = unique_dir("run");
    let cfg_path = dir.join("config.toml");
    std::fs::write(
        &cfg_path,
        format!("server = \"ws://{addr}/sync\"\nuser = \"alice\"\npassword = \"hunter2\"\n"),
    )
    .unwrap();
    // The loader refuses a world-readable config with credentials in it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cfg_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let file = dir.join("payload.bin");
    // Big enough to be one frame, small enough that the test is about exiting
    // rather than throughput.
    std::fs::write(&file, vec![0xA5u8; 64 * 1024]).unwrap();

    let mut child = Command::new(bin)
        .arg("--config")
        .arg(&cfg_path)
        .arg("send")
        .arg(&file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawning the send subcommand");

    // **The assertion that would have caught the bug.** Without it the process
    // stays alive indefinitely and this loop times out.
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        match child.try_wait().expect("polling the child") {
            Some(status) => break status,
            None => {
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let mut err = String::new();
                    if let Some(mut e) = child.stderr.take() {
                        let _ = e.read_to_string(&mut err);
                    }
                    panic!(
                        "`send` did not exit within 15s; the one-shot transport never \
                         observed its outbound channels closing. stderr: {err}"
                    );
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };

    let mut err = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut err);
    }
    assert!(
        status.success(),
        "`send` exited with {status}; stderr: {err}"
    );

    let _ = shutdown_tx.send(());
    drop(hub);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Wait until something accepts a TCP connection on `addr`.
fn wait_for_port(addr: std::net::SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(250)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("the test hub never started listening on {addr}");
}

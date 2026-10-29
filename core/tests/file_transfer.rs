// SPDX-License-Identifier: GPL-3.0-or-later

//! End-to-end file-transfer test.
//!
//! Spins up a real hub, two real WebSocket transports, and pushes a
//! file through the sender's `outbound_files_tx`. A small relay task
//! pumps the receiver's `inbound_files_rx` into a `FileReceiver` with
//! a sandboxed save dir. Assert: the assembled file matches the
//! original bytes.
//!
//! Exercises the v0.3.0 protocol additions (`file_chunk` frame +
//! per-client file fan-out channels in the hub).

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use clipboardwire_core::client::file::{send_file_through, FileReceiver};
use clipboardwire_core::client::transport::{spawn_with_options, SpawnOptions};
use clipboardwire_core::client::{ClientConfig, ClientStatus};
use clipboardwire_core::server::{build_app, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::watch;

fn server_cfg(addr: std::net::SocketAddr) -> ServerConfig {
    ServerConfig {
        bind: addr,
        user: "alice".into(),
        password: "hunter2".into(),
        max_conns: 8,
        // Big enough for the test files plus a 4 MiB chunk + base64
        // inflation; matches the receiver's expectations.
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
        send_mode: Default::default(),
        receive_mode: Default::default(),
        pickup_auto: false,
        offer_ttl_minutes: 30,
        pull_window_chunks: 3,
        pull_idle_timeout_secs: 30,
    }
}

async fn wait_for_connected(rx: &mut watch::Receiver<ClientStatus>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if matches!(*rx.borrow(), ClientStatus::Connected) {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("transport never reached Connected within 5s");
        }
        if rx.changed().await.is_err() {
            panic!("status channel dropped before Connected");
        }
    }
}

fn clip_frame(id: uuid::Uuid, text: &str) -> clipboardwire_core::protocol::ClipFrame {
    clipboardwire_core::protocol::ClipFrame {
        id,
        ts: 0,
        content_type: clipboardwire_core::protocol::TEXT_CONTENT_TYPE.to_string(),
        content: Some(text.to_string()),
        content_b64: None,
        from: None,
    }
}

fn unique_tmp_dir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "cw-file-e2e-{label}-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_sent_by_one_client_arrives_intact_at_the_other() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (app, _hub_join) = build_app(server_cfg(addr));
    let _server_task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    // -- Receiver: subscribes to inbound files and pipes them into a
    //    FileReceiver whose save_dir is a sandboxed tempdir. --
    let receiver_save_dir = unique_tmp_dir("recv");
    let (recv_status_tx, mut recv_status_rx) = watch::channel(ClientStatus::Connecting);
    let (mut receiver_transport, _r_join) = spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: Some(recv_status_tx),
            receive_files: true,
            one_shot: false,
            ..SpawnOptions::default()
        },
    );
    let mut inbound_files_rx = receiver_transport
        .inbound_files_rx
        .take()
        .expect("we asked for receive_files=true");

    let receiver_save_dir_for_task = receiver_save_dir.clone();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let receiver_task = tokio::spawn(async move {
        let mut receiver = FileReceiver::with_save_dir(receiver_save_dir_for_task).unwrap();
        let mut done_tx = Some(done_tx);
        while let Some(chunk) = inbound_files_rx.recv().await {
            match receiver.receive_chunk(chunk) {
                Ok(Some(path)) => {
                    if let Some(tx) = done_tx.take() {
                        let _ = tx.send(path);
                    }
                }
                Ok(None) => {}
                Err(e) => panic!("receiver rejected a chunk: {e:#}"),
            }
        }
    });

    // -- Sender: one-shot, no inbound channels --
    let (send_status_tx, mut send_status_rx) = watch::channel(ClientStatus::Connecting);
    let (sender_transport, sender_join) = spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: Some(send_status_tx),
            receive_files: false,
            one_shot: true,
            ..SpawnOptions::default()
        },
    );

    // Wait until BOTH transports report Connected before publishing.
    wait_for_connected(&mut recv_status_rx).await;
    wait_for_connected(&mut send_status_rx).await;
    // ClientStatus::Connected fires inside connect_and_serve *before*
    // it consumes the welcome frame and enters its inner select loop.
    // Give both peers a couple of poll ticks so they're actually
    // listening on their outbound channels by the time we publish.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Small payload first to keep this test fast — a separate test
    // below exercises the multi-chunk path.
    let payload: Vec<u8> = b"hello clipboardwire file transfer".to_vec();
    let send_dir = unique_tmp_dir("send");
    let src_path = send_dir.join("payload.bin");
    fs::write(&src_path, &payload).unwrap();

    send_file_through(&src_path, &sender_transport.outbound_files_tx)
        .await
        .expect("send_file_through");

    // Close the sender's channels so the one-shot run_loop exits.
    drop(sender_transport);
    let _ = tokio::time::timeout(Duration::from_secs(5), sender_join).await;

    // Wait for the receiver to assemble the file (timeout ⇒ test fail).
    let final_path = tokio::time::timeout(Duration::from_secs(10), done_rx)
        .await
        .expect("file should be assembled within 10s")
        .expect("oneshot channel");

    // The receiver is allowed to keep running until the test scope ends.
    drop(receiver_task);

    let received = fs::read(&final_path).expect("assembled file readable");
    assert_eq!(received.len(), payload.len(), "size mismatch");
    assert_eq!(received, payload, "content mismatch");

    let _ = fs::remove_dir_all(&send_dir);
    let _ = fs::remove_dir_all(&receiver_save_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_chunk_file_round_trips_intact() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // The 9-MiB payload crosses FILE_CHUNK_BYTES (4 MiB), so we need a
    // bigger max_frame_bytes on the hub than the default test config.
    let mut cfg = server_cfg(addr);
    cfg.max_frame_bytes = 16 * 1024 * 1024;
    let (app, _hub_join) = build_app(cfg);
    let _server_task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    let receiver_save_dir = unique_tmp_dir("multi-recv");
    let (recv_status_tx, mut recv_status_rx) = watch::channel(ClientStatus::Connecting);
    let (mut receiver_transport, _r_join) = spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: Some(recv_status_tx),
            receive_files: true,
            one_shot: false,
            ..SpawnOptions::default()
        },
    );
    let mut inbound_files_rx = receiver_transport.inbound_files_rx.take().unwrap();
    let receiver_save_dir_for_task = receiver_save_dir.clone();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let _receiver_task = tokio::spawn(async move {
        let mut receiver = FileReceiver::with_save_dir(receiver_save_dir_for_task).unwrap();
        let mut done_tx = Some(done_tx);
        while let Some(chunk) = inbound_files_rx.recv().await {
            if let Ok(Some(path)) = receiver.receive_chunk(chunk) {
                if let Some(tx) = done_tx.take() {
                    let _ = tx.send(path);
                }
            }
        }
    });

    let (send_status_tx, mut send_status_rx) = watch::channel(ClientStatus::Connecting);
    let (sender_transport, sender_join) = spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: Some(send_status_tx),
            receive_files: false,
            one_shot: true,
            ..SpawnOptions::default()
        },
    );

    wait_for_connected(&mut recv_status_rx).await;
    wait_for_connected(&mut send_status_rx).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 9 MiB ⇒ 3 chunks at 4 MiB each.
    let mut payload = vec![0u8; 9 * 1024 * 1024];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = ((i.wrapping_mul(31)).wrapping_add(7) & 0xff) as u8;
    }
    let send_dir = unique_tmp_dir("multi-send");
    let src_path = send_dir.join("blob.bin");
    fs::write(&src_path, &payload).unwrap();

    send_file_through(&src_path, &sender_transport.outbound_files_tx)
        .await
        .expect("send_file_through");
    drop(sender_transport);
    let _ = tokio::time::timeout(Duration::from_secs(10), sender_join).await;

    let final_path = tokio::time::timeout(Duration::from_secs(15), done_rx)
        .await
        .expect("multi-chunk file should assemble within 15s")
        .expect("oneshot channel");
    let received = fs::read(&final_path).unwrap();
    assert_eq!(received.len(), payload.len());
    assert_eq!(received, payload);

    let _ = fs::remove_dir_all(&send_dir);
    let _ = fs::remove_dir_all(&receiver_save_dir);
}

/// Repeatedly drive the exact shutdown shape that lost chunks.
///
/// `drop(sender_transport)` closes all three outbound channels at once, and
/// `select!` picks among ready arms at random. Before the shared drain, an arm
/// observing a *closed but empty* control channel returned immediately while
/// the file channel still held most of the transfer — the receiver then waited
/// for a chunk that never came.
///
/// One pass proves nothing: the bug reproduced roughly one run in three. Each
/// iteration here is a full connect → send → drop → assemble cycle against a
/// fresh payload, so nineteen passes and one failure is still a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_transport_flushes_queued_chunks_whichever_arm_wins() {
    const ITERATIONS: usize = 20;
    /// Enough that the transport cannot plausibly drain them all before the
    /// drop lands on a 4-worker runtime. The outbound clip buffer holds 8.
    const CLIPS_PER_ROUND: usize = 6;
    // Two chunks' worth: one chunk can be handed to the socket before the race
    // is decided, so a single-chunk payload would often pass by luck.
    const PAYLOAD_BYTES: usize = 9 * 1024 * 1024;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = server_cfg(addr);
    cfg.max_frame_bytes = 16 * 1024 * 1024;
    let (app, _hub_join) = build_app(cfg);
    let _server_task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    let save_dir = unique_tmp_dir("flush-rec");
    let (mut receiver_transport, _r_join) = spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: None,
            receive_files: true,
            one_shot: false,
            ..SpawnOptions::default()
        },
    );
    let mut inbound_files_rx = receiver_transport.inbound_files_rx.take().unwrap();
    let mut inbound_rx = receiver_transport.inbound_rx;

    // One receiver loop for all iterations: it reports the assembled path per
    // completed file, so a lost chunk shows up as a missing report rather than
    // as a stuck receiver that has to be restarted.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let save_dir_for_task = save_dir.clone();
    let _receiver_task = tokio::spawn(async move {
        let mut receiver = FileReceiver::with_save_dir(save_dir_for_task).unwrap();
        while let Some(chunk) = inbound_files_rx.recv().await {
            match receiver.receive_chunk(chunk) {
                Ok(Some(path)) => {
                    let _ = done_tx.send(path);
                }
                Ok(None) => {}
                Err(e) => panic!("receiver rejected a chunk: {e:#}"),
            }
        }
    });

    // Give the receiver a moment to finish its handshake; it has no status
    // channel here, and the sender's connect is what matters below.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let send_dir = unique_tmp_dir("flush-send");

    for iteration in 0..ITERATIONS {
        let mut payload = vec![0u8; PAYLOAD_BYTES];
        for (i, b) in payload.iter_mut().enumerate() {
            // Distinct per iteration so a stale file from an earlier round can
            // never satisfy this round's assertion.
            *b = ((i.wrapping_mul(31)).wrapping_add(iteration.wrapping_mul(97)) & 0xff) as u8;
        }
        let src_path = send_dir.join(format!("iter-{iteration}.bin"));
        fs::write(&src_path, &payload).unwrap();

        let (send_status_tx, mut send_status_rx) = watch::channel(ClientStatus::Connecting);
        let (sender_transport, sender_join) = spawn_with_options(
            client_cfg(addr),
            SpawnOptions {
                status_tx: Some(send_status_tx),
                receive_files: false,
                one_shot: true,
                ..SpawnOptions::default()
            },
        );
        wait_for_connected(&mut send_status_rx).await;
        // `Connected` is emitted before the welcome is consumed and the inner
        // `select!` starts listening.
        tokio::time::sleep(Duration::from_millis(20)).await;

        send_file_through(&src_path, &sender_transport.outbound_files_tx)
            .await
            .expect("send_file_through");

        // Several clips in the queue too, so all three outbound channels carry
        // work when they close. One is not enough: the transport can drain a
        // single clip before the drop lands, and then a bare `return` on the
        // file arm would look harmless while in reality it drops whatever is
        // still queued.
        let clip_ids: Vec<uuid::Uuid> =
            (0..CLIPS_PER_ROUND).map(|_| uuid::Uuid::new_v4()).collect();
        for (index, id) in clip_ids.iter().enumerate() {
            sender_transport
                .outbound_tx
                .send(clip_frame(*id, &format!("iteration-{iteration}-{index}")))
                .await
                .expect("queue a clip");
        }

        // The shutdown under test: all three outbound channels close at once.
        drop(sender_transport);
        let _ = tokio::time::timeout(Duration::from_secs(5), sender_join).await;

        let final_path = tokio::time::timeout(Duration::from_secs(10), done_rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("iteration {iteration}: file never assembled; queued chunks were lost")
            })
            .unwrap_or_else(|| panic!("iteration {iteration}: receiver task ended"));

        let received = fs::read(&final_path).expect("assembled file readable");
        assert_eq!(
            received.len(),
            payload.len(),
            "iteration {iteration}: size mismatch"
        );
        assert!(
            received == payload,
            "iteration {iteration}: content mismatch"
        );
        let _ = fs::remove_file(&final_path);

        // The clips were queued behind the chunks, so they are the ones most
        // likely to still be sitting in their channel when the shutdown race is
        // decided. All of them must arrive: a bare `return` on any arm loses
        // however many remained.
        let mut seen: Vec<uuid::Uuid> = Vec::new();
        for slot in 0..CLIPS_PER_ROUND {
            let clip = tokio::time::timeout(Duration::from_secs(5), inbound_rx.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "iteration {iteration}: only {slot} of {CLIPS_PER_ROUND} queued clips \
                         arrived; the rest were dropped on shutdown"
                    )
                })
                .expect("inbound clip channel stayed open");
            seen.push(clip.id);
        }
        // Hub relay preserves nothing about ordering across channels, so
        // compare as a set.
        seen.sort_unstable();
        let mut expected = clip_ids.clone();
        expected.sort_unstable();
        assert_eq!(seen, expected, "iteration {iteration}: a clip was lost");
    }

    let _ = fs::remove_dir_all(&send_dir);
    let _ = fs::remove_dir_all(&save_dir);
}

// ---- Phase 8.9: the v0.3 fixture must survive the production path ----------

/// `protocol.rs` pins the v0.3 eager wire shape with `V03_EAGER_CHUNK_JSON`, but
/// that fixture is built from a hand-written struct — it proves the *type*
/// serialises correctly, not that `send_file_through` produces that type.
///
/// `phase2_fixture_gap`: a field added to `FileChunkFrame` and populated by
/// `send_file_through` (or a reordering of the struct) would pass the
/// protocol test and still break v0.3 readers on the wire. This test drives the
/// real sender and inspects the bytes it actually emits.
///
/// Not byte-for-byte against the whole fixture, and the reason is documented
/// rather than worked around: `send_file_through` mints a fresh `Uuid::new_v4()`
/// per call (deliberately — see `send_file_with_progress`), so `file_id` cannot
/// be fixed without changing production code. Everything else is compared
/// literally, **including field order**, because the assertion is on the
/// serialised substring list rather than on a struct.
#[tokio::test]
async fn send_file_through_produces_the_v03_fixture_bytes() {
    use clipboardwire_core::protocol::Frame;

    let dir = unique_tmp_dir("fixture-prod");
    let path = dir.join("fixture.txt");
    // The fixture's payload is base64 "aGk=" = b"hi", file_size 3 in the
    // original fixture is "aGk=" decoded... the fixture is a shape test, so the
    // bytes only have to be reproducible here, not identical to it.
    fs::write(&path, b"hi").unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    send_file_through(&path, &tx)
        .await
        .expect("send_file_through");
    let chunk = rx
        .try_recv()
        .expect("the sender must emit at least one chunk");

    // Every new-in-0.4 field must be absent, or a v0.3 reader sees a lazy chunk.
    assert_eq!(chunk.transfer_id, None, "eager chunks carry no session");
    assert_eq!(chunk.to, None, "eager chunks are broadcast, never directed");
    assert_eq!(
        chunk.from_device, None,
        "only the hub stamps `from_device`, and only on relay"
    );
    assert!(
        chunk.file_sha256.is_some(),
        "eager chunks carry the whole-file hash; a lazy one would not"
    );

    let json = serde_json::to_string(&Frame::FileChunk(chunk.clone())).unwrap();

    // Field order is part of the contract: v0.3 readers tolerate extra
    // trailing fields but the shipped v0.3 writer produced this exact order,
    // and `eager_chunk_serialises_byte_identically_to_v03` pins the type
    // against it. Re-assert it here on a production-produced value.
    let keys = [
        r#""type":"file_chunk""#,
        r#""file_id":""#,
        r#""name":"fixture.txt""#,
        r#""content_type":"text/plain; charset=utf-8""#,
        r#""chunk_index":0"#,
        r#""total_chunks":1"#,
        r#""file_sha256":"#,
        r#""file_size":2"#,
        r#""payload_b64":"aGk=""#,
    ];
    let mut cursor = 0usize;
    for key in keys {
        let found = json[cursor..]
            .find(key)
            .unwrap_or_else(|| panic!("expected {key:?} after byte {cursor} in {json}"));
        cursor += found + key.len();
    }
    assert!(json.ends_with('}'), "the frame must close cleanly: {json}");

    // Nothing from a later revision leaked in.
    for forbidden in ["\"transfer_id\"", "\"to\"", "\"from_device\""] {
        assert!(
            !json.contains(forbidden),
            "an eager chunk must not serialise {forbidden}: {json}"
        );
    }

    let _ = fs::remove_dir_all(&dir);
}

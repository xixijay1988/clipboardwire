//! Reconnect and late-join coverage for lazy transfers.
//!
//! A broadcast `file_offer` only reaches the peers connected at that instant;
//! everyone else learns about it from the `welcome` replay. These tests pin
//! the three behaviours that replay depends on: the transport forwarding it,
//! the supervisor de-duplicating offers it already answered, and an aborted
//! pull resuming — not restarting — so a dropped window does not re-download
//! the whole file.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use clipboardwire_core::client::transport::SpawnOptions;
use clipboardwire_core::client::{
    run_supervisor_with_policy, ClientConfig, ClientStatus, ClipChange, Clipboard, ReceiveMode,
    ReceivePolicy, SendMode, SendPolicy,
};
use clipboardwire_core::protocol::Caps;
use clipboardwire_core::server::{build_app, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::watch;
use uuid::Uuid;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("cwrepro-{label}-{}", Uuid::new_v4()));
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
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if matches!(*rx.borrow_and_update(), ClientStatus::Connected) {
            return;
        }
        assert!(std::time::Instant::now() <= deadline, "never Connected");
        rx.changed().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offer_published_before_receiver_joins_still_lands() {
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

    // -- Sender first, alone. --
    let (send_status_tx, mut send_status_rx) = watch::channel(ClientStatus::Connecting);
    let (send_transport, send_join) = clipboardwire_core::client::transport::spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: Some(send_status_tx),
            receive_files: true,
            one_shot: false,
            device: Some(Uuid::new_v4()),
            caps: Caps::LAZY_FILE,
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
    ));
    wait_connected(&mut send_status_rx).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // -- Sender copies a file; the hub now holds a live offer with no peers. --
    let src_dir = unique_dir("late-join-src");
    let src = src_dir.join("late.bin");
    fs::write(&src, vec![0xABu8; 700_000]).unwrap();
    let expected = fs::read(&src).unwrap();
    send_events_tx
        .send(ClipChange::Files(vec![src.clone()]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await; // let the offer reach the hub

    // -- Receiver joins AFTER the offer exists. Welcome must replay it. --
    let recv_dir = unique_dir("late-join-recv");
    let (recv_status_tx, mut recv_status_rx) = watch::channel(ClientStatus::Connecting);
    let (recv_transport, recv_join) = clipboardwire_core::client::transport::spawn_with_options(
        client_cfg(addr),
        SpawnOptions {
            status_tx: Some(recv_status_tx),
            receive_files: true,
            one_shot: false,
            device: Some(Uuid::new_v4()),
            caps: Caps::LAZY_FILE,
        },
    );
    let (recv_events_tx, recv_events_rx) = tokio::sync::mpsc::channel::<ClipChange>(8);
    let (recv_apply_tx, _recv_apply_rx) = std::sync::mpsc::channel::<ClipChange>();
    let recv_supervisor = tokio::spawn(run_supervisor_with_policy(
        Clipboard {
            events_rx: recv_events_rx,
            apply_tx: recv_apply_tx,
        },
        recv_transport,
        None,
        false,
        Some(recv_dir.clone()),
        SendPolicy {
            mode: SendMode::Eager,
            ..SendPolicy::default()
        },
        ReceivePolicy {
            mode: ReceiveMode::Pickup,
            pull_window_chunks: 2,
            save_dir: Some(recv_dir.clone()),
        },
    ));
    wait_connected(&mut recv_status_rx).await;

    // -- The file must land even though the receiver was not online for the offer. --
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let landed = loop {
        if let Ok(entries) = fs::read_dir(&recv_dir) {
            if let Some(e) = entries.flatten().find(|e| {
                e.path().is_file() && !e.file_name().to_string_lossy().ends_with(".partial")
            }) {
                break e.path();
            }
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "no file landed within 15s: the offer in `welcome` never became a pull"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(fs::read(&landed).unwrap(), expected);

    drop(send_events_tx);
    drop(recv_events_tx);
    send_supervisor.abort();
    recv_supervisor.abort();
    let _ = tokio::time::timeout(Duration::from_secs(2), send_join).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), recv_join).await;
    let _ = fs::remove_dir_all(&src_dir);
    let _ = fs::remove_dir_all(&recv_dir);
}

// ---------------------------------------------------------------------------
// Restart-on-abort and replay dedup tests.
//
// The supervisor needs no real hub for these: a mock `Transport` built from
// channels lets the test feed `file_offer`/`file_chunk`/`file_abort` frames in
// and read the `file_pull`s coming out — the exact boundary where the bugs
// lived.
// ---------------------------------------------------------------------------

use clipboardwire_core::client::transport::Transport;
use clipboardwire_core::protocol::ClipFrame;
use clipboardwire_core::protocol::{
    FileAbortFrame, FileChunkFrame, FileDigestFrame, FileOfferFrame, Frame, FILE_CHUNK_BYTES,
};
use sha2::Digest as _;
use tokio::sync::mpsc;

fn colon_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

struct MockWire {
    // Kept alive so the supervisor's `recv` arms never see a closed channel —
    // a closed `events`/`inbound` arm is an immediate `return`, not a skip.
    _inbound_tx: mpsc::Sender<ClipFrame>,
    _outbound_rx: mpsc::Receiver<ClipFrame>,
    _outbound_files_rx: mpsc::Receiver<FileChunkFrame>,
    _hub_caps_tx: watch::Sender<Caps>,
    inbound_control_tx: mpsc::Sender<Frame>,
    inbound_files_tx: mpsc::Sender<FileChunkFrame>,
    outbound_control_rx: mpsc::Receiver<Frame>,
    events_tx: mpsc::Sender<ClipChange>,
}

fn mock_supervisor(recv_dir: PathBuf) -> (MockWire, tokio::task::JoinHandle<()>) {
    let (events_tx, events_rx) = mpsc::channel::<ClipChange>(8);
    let (apply_tx, _apply_rx) = std::sync::mpsc::channel::<ClipChange>();
    let (outbound_tx, outbound_rx) = mpsc::channel::<ClipFrame>(8);
    let (outbound_files_tx, outbound_files_rx) = mpsc::channel::<FileChunkFrame>(8);
    let (outbound_control_tx, outbound_control_rx) = mpsc::channel::<Frame>(8);
    let (inbound_tx, inbound_rx) = mpsc::channel::<ClipFrame>(8);
    let (inbound_control_tx, inbound_control_rx) = mpsc::channel::<Frame>(8);
    let (inbound_files_tx, inbound_files_rx) = mpsc::channel::<FileChunkFrame>(8);
    let (hub_caps_tx, hub_caps) = watch::channel(Caps::LAZY_FILE);

    let transport = Transport {
        inbound_rx,
        inbound_files_rx: Some(inbound_files_rx),
        outbound_tx,
        outbound_files_tx,
        inbound_control_rx: Some(inbound_control_rx),
        outbound_control_tx,
        hub_caps,
    };
    let save_dir = recv_dir.clone();
    let join = tokio::spawn(run_supervisor_with_policy(
        Clipboard {
            events_rx,
            apply_tx,
        },
        transport,
        None,
        false,
        Some(recv_dir.clone()),
        SendPolicy {
            mode: SendMode::Eager,
            ..SendPolicy::default()
        },
        ReceivePolicy {
            mode: ReceiveMode::Pickup,
            pull_window_chunks: 1,
            save_dir: Some(save_dir),
        },
    ));
    (
        MockWire {
            _inbound_tx: inbound_tx,
            _outbound_rx: outbound_rx,
            _outbound_files_rx: outbound_files_rx,
            _hub_caps_tx: hub_caps_tx,
            inbound_control_tx,
            inbound_files_tx,
            outbound_control_rx,
            events_tx,
        },
        join,
    )
}

async fn next_pull(
    rx: &mut mpsc::Receiver<Frame>,
    what: &str,
) -> clipboardwire_core::protocol::FilePullFrame {
    match tokio::time::timeout(Duration::from_secs(6), rx.recv()).await {
        Ok(Some(Frame::FilePull(p))) => p,
        Ok(Some(other)) => panic!("expected file_pull for {what}, got {other:?}"),
        Ok(None) => panic!("control channel closed waiting for {what}"),
        Err(_) => panic!("no file_pull for {what} within 6s"),
    }
}

fn offer(file_id: Uuid, device: Uuid, size: u64) -> Frame {
    Frame::FileOffer(FileOfferFrame {
        file_id,
        batch_id: Uuid::new_v4(),
        ts: 1_700_000_000_000,
        name: "blob.bin".into(),
        content_type: "application/octet-stream".into(),
        file_size: size,
        expires_at_ms: 1_700_000_000_000 + 60_000,
        from_device: Some(device),
    })
}

fn chunk(
    file_id: Uuid,
    transfer_id: Uuid,
    device: Uuid,
    index: u32,
    total: u32,
    file_size: u64,
    bytes: &[u8],
) -> FileChunkFrame {
    use base64::Engine;
    FileChunkFrame {
        file_id,
        name: "blob.bin".into(),
        content_type: "application/octet-stream".into(),
        chunk_index: index,
        total_chunks: total,
        file_sha256: None,
        file_size,
        payload_b64: base64::engine::general_purpose::STANDARD.encode(bytes),
        from: None,
        to: None,
        from_device: Some(device),
        transfer_id: Some(transfer_id),
    }
}

/// A session-scoped `file_abort` used to delete the transfer outright: one
/// dropped window (a `busy` answer, a refused pull) ended the whole file,
/// silently. The aborted session must restart — *and* resume from where it
/// stopped, not re-download from index 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_pull_resumes_from_where_it_stopped() {
    let dir = unique_dir("abort-resume");
    let (mut wire, join) = mock_supervisor(dir.clone());
    let device = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    let size = FILE_CHUNK_BYTES as u64 * 2; // two chunks

    wire.inbound_control_tx
        .send(offer(file_id, device, size))
        .await
        .unwrap();
    let p0 = next_pull(&mut wire.outbound_control_rx, "initial window").await;
    assert_eq!((p0.chunk_index, p0.window), (0, 1));

    // The first chunk lands; the next window is requested.
    wire.inbound_files_tx
        .send(chunk(file_id, p0.transfer_id, device, 0, 2, size, b"first"))
        .await
        .unwrap();
    let p1 = next_pull(&mut wire.outbound_control_rx, "second window").await;
    assert_eq!((p1.chunk_index, p1.window), (1, 1));
    assert_eq!(p1.transfer_id, p0.transfer_id);

    // The hub says `busy` — that attempt is dead. Restart must (a) happen at
    // all and (b) resume at chunk 1, not restart from 0.
    wire.inbound_control_tx
        .send(Frame::FileAbort(FileAbortFrame {
            file_id,
            transfer_id: Some(p1.transfer_id),
            to: None,
            from_device: Some(device),
            reason: "busy".into(),
        }))
        .await
        .unwrap();

    let p2 = next_pull(&mut wire.outbound_control_rx, "resume pull").await;
    assert_ne!(
        p2.transfer_id, p1.transfer_id,
        "a restart must mint a new transfer_id"
    );
    assert_eq!(
        (p2.chunk_index, p2.window),
        (1, 1),
        "the resume must continue from contiguous=1, not re-download from 0"
    );

    drop(wire.events_tx);
    drop(wire.inbound_control_tx);
    join.abort();
    let _ = fs::remove_dir_all(&dir);
}

/// Once `welcome.offers` reaches the supervisor, a replayed offer for a file
/// already pulled must not pull again — otherwise every reconnect
/// re-downloads every offer still inside its TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replayed_offer_for_a_finished_pull_is_ignored() {
    let dir = unique_dir("replay-dedup");
    let (mut wire, join) = mock_supervisor(dir.clone());
    let device = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    let payload = b"done in one chunk".to_vec();
    let size = payload.len() as u64;

    wire.inbound_control_tx
        .send(offer(file_id, device, size))
        .await
        .unwrap();
    let p0 = next_pull(&mut wire.outbound_control_rx, "only window").await;
    wire.inbound_files_tx
        .send(chunk(file_id, p0.transfer_id, device, 0, 1, size, &payload))
        .await
        .unwrap();

    // Finish: real digest so the file verifies and `finish` runs.
    let sha = colon_hex(&sha2::Sha256::digest(&payload));
    wire.inbound_control_tx
        .send(Frame::FileDigest(FileDigestFrame {
            file_id,
            transfer_id: p0.transfer_id,
            to: device,
            from_device: Some(device),
            file_sha256: sha,
        }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The same offer arrives again — the shape a `welcome` replay takes.
    wire.inbound_control_tx
        .send(offer(file_id, device, size))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1500), wire.outbound_control_rx.recv())
            .await
            .is_err(),
        "a replayed offer for a finished pull must not start a new session"
    );

    drop(wire.events_tx);
    drop(wire.inbound_control_tx);
    join.abort();
    let _ = fs::remove_dir_all(&dir);
}

/// The wire does not preserve order between chunk and control queues, so the
/// digest legitimately overtakes the tail chunks on a receiver that falls
/// behind. Treating that as corruption used to re-download the whole file on
/// every reorder — on a busy receiver, on most large transfers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_digest_that_overtakes_the_last_chunk_still_completes() {
    let dir = unique_dir("early-digest");
    let (mut wire, join) = mock_supervisor(dir.clone());
    let device = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    // Chunk 0 must carry a full FILE_CHUNK_BYTES payload: the assembler
    // writes each chunk at index*FILE_CHUNK_BYTES, so a short middle chunk
    // would leave a hole of zeros and the file would not verify.
    let payload = vec![0xABu8; FILE_CHUNK_BYTES];
    let payload2 = b"second half".to_vec();
    let mut whole = payload.clone();
    whole.extend_from_slice(&payload2);
    let size = (FILE_CHUNK_BYTES + payload2.len()) as u64;

    wire.inbound_control_tx
        .send(offer(file_id, device, size))
        .await
        .unwrap();
    let p0 = next_pull(&mut wire.outbound_control_rx, "window 0").await;
    wire.inbound_files_tx
        .send(chunk(file_id, p0.transfer_id, device, 0, 2, size, &payload))
        .await
        .unwrap();
    let p1 = next_pull(&mut wire.outbound_control_rx, "window 1").await;

    // The digest arrives BEFORE the final chunk — the wire reorder.
    let sha = colon_hex(&sha2::Sha256::digest(&whole));
    wire.inbound_control_tx
        .send(Frame::FileDigest(FileDigestFrame {
            file_id,
            transfer_id: p1.transfer_id,
            to: device,
            from_device: Some(device),
            file_sha256: sha,
        }))
        .await
        .unwrap();

    // No restart must be attempted — no pull frames for the old session.
    assert!(
        tokio::time::timeout(Duration::from_millis(800), wire.outbound_control_rx.recv())
            .await
            .is_err(),
        "an early digest must not trigger a restart pull"
    );

    // The last chunk lands: file must verify against the stored digest and be saved.
    wire.inbound_files_tx
        .send(chunk(
            file_id,
            p1.transfer_id,
            device,
            1,
            2,
            size,
            &payload2,
        ))
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(entries) = fs::read_dir(&dir) {
            if let Some(e) = entries.flatten().find(|e| {
                e.path().is_file() && !e.file_name().to_string_lossy().ends_with(".partial")
            }) {
                assert_eq!(fs::read(e.path()).unwrap(), whole);
                break;
            }
        }
        assert!(std::time::Instant::now() <= deadline, "file never landed");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    drop(wire.events_tx);
    drop(wire.inbound_control_tx);
    join.abort();
    let _ = fs::remove_dir_all(&dir);
}

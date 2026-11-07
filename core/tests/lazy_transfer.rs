// SPDX-License-Identifier: GPL-3.0-or-later

//! Lazy-transfer integration tests.
//!
//! Every piece of the lazy path has unit tests of its own: the offer registry
//! decides what to advertise, `PullServer` decides what to serve, and
//! `ReceiveEngine` decides what to accept. None of those prove that the
//! *pieces fit*: that a pull request the source answers can be fed straight
//! into the receiver, that the resulting bytes match the file on disk, and
//! that the digest the source computes is the one the receiver verifies
//! against.
//!
//! These tests wire the real `PullServer` to the real `ReceiveEngine` and
//! `LazyAssembler` in-process. That deliberately skips the hub and the
//! WebSocket layer: the interfaces under test are the ones the supervisor
//! calls, and driving them directly keeps the test deterministic instead of
//! racing a socket. The transport end of the chain is covered separately by
//! `file_transfer.rs` (eager) and `client_e2e.rs` (supervisor round trip).

use std::fs;
use std::path::{Path, PathBuf};

use clipboardwire_core::client::file::finish_lazy_transfer;
use clipboardwire_core::client::offer::OfferRegistry;
use clipboardwire_core::client::pull::{PullOutcome, PullServer, MAX_ACTIVE_PULLS};
use clipboardwire_core::client::receive::{
    ChunkOutcome, DigestOutcome, LazyAssembler, ReceiveEngine, StaleReason,
};
use clipboardwire_core::client::{
    run_supervisor_with_policy, ClientConfig, ClientStatus, ClipChange, Clipboard, ReceiveMode,
    ReceivePolicy, SendMode, SendPolicy,
};
use clipboardwire_core::protocol::{FilePullFrame, Frame, FILE_CHUNK_BYTES, MAX_PULL_WINDOW};
use clipboardwire_core::server::{build_app, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::watch;
use uuid::Uuid;

const NOW: i64 = 1_700_000_000_000;
const TTL: i64 = 30 * 60_000;
const IDLE: i64 = 30_000;

fn unique_dir(label: &str) -> PathBuf {
    static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("cw-lazy-{label}-{}-{id}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// A file of exactly `size` bytes whose content depends on position, so a
/// chunk read from the wrong offset differs in value, not just in length.
fn write_source(dir: &Path, name: &str, size: usize) -> PathBuf {
    let path = dir.join(name);
    let body: Vec<u8> = (0..size)
        .map(|i| ((i.wrapping_mul(131)).wrapping_add(17) & 0xff) as u8)
        .collect();
    fs::write(&path, &body).unwrap();
    path
}

/// Two full chunks plus a short tail: enough to need more than one pull window
/// for any window below `MAX_PULL_WINDOW`.
fn multi_chunk_size() -> usize {
    FILE_CHUNK_BYTES * 2 + 1_000
}

fn advertise(
    registry: &mut OfferRegistry,
    path: &Path,
) -> clipboardwire_core::protocol::FileOfferFrame {
    let outcome = registry.advertise(&[path.to_path_buf()], Uuid::new_v4(), NOW, TTL);
    assert_eq!(
        outcome.offers.len(),
        1,
        "the fixture must produce one offer"
    );
    outcome.offers.into_iter().next().unwrap()
}

fn pull_of(
    file_id: Uuid,
    source: Uuid,
    puller: Uuid,
    transfer_id: Uuid,
    index: u32,
    window: u32,
) -> FilePullFrame {
    FilePullFrame {
        file_id,
        transfer_id,
        to: source,
        chunk_index: index,
        window,
        from_device: Some(puller),
    }
}

/// Drive one whole transfer through the real source and the real receiver,
/// and return the landed path.
///
/// This is the contract the supervisor relies on: `next_pull` says what to
/// ask for, `handle_pull` answers it, every chunk the answer carries is
/// accepted and written, and the trailer ends the transfer. Any disagreement
/// between the two sides shows up here as a failed assertion rather than as a
/// flaky timeout.
#[allow(clippy::too_many_arguments)]
fn drive_to_completion(
    server: &mut PullServer,
    registry: &OfferRegistry,
    engine: &mut ReceiveEngine,
    assembler: &mut LazyAssembler,
    file_id: Uuid,
    source: Uuid,
    puller: Uuid,
    transfer_id: Uuid,
    start_now: i64,
) -> String {
    let mut now = start_now;
    for round in 0..64 {
        let Some((index, window)) = engine.next_pull(&transfer_id) else {
            panic!("round {round}: the engine wants neither a window nor a digest");
        };
        let pull = pull_of(file_id, source, puller, transfer_id, index, window);
        let mut response = server.handle_pull(registry, &pull, now);
        assert_eq!(
            response.outcome,
            PullOutcome::Serving,
            "round {round}: the source refused window ({index}, {window})"
        );
        assert!(!response.chunks.is_empty(), "round {round}: empty window");

        engine.mark_pulled(&transfer_id, index, window);
        for chunk in &mut response.chunks {
            // The hub is the only writer of `from_device` on relay (design
            // §4.3); the source leaves it `None` because a client-supplied
            // value would be unauthenticated. Stamp it here so the receiver
            // sees what it sees in production.
            chunk.from_device = Some(source);
            let outcome = engine.handle_chunk(chunk, now);
            assert_eq!(
                outcome,
                ChunkOutcome::Accept {
                    chunk_index: chunk.chunk_index
                },
                "round {round}: chunk {} was not accepted",
                chunk.chunk_index
            );
            assembler.write(chunk).expect("writing a received chunk");
        }

        match response.trailer {
            None => {
                now += 10;
            }
            Some(Frame::FileDigest(digest)) => {
                let outcome = engine.handle_digest(&digest, now);
                match outcome {
                    DigestOutcome::Ready { sha256 } => {
                        assert_eq!(
                            sha256, digest.file_sha256,
                            "the digest the source computed must be the one the receiver verifies"
                        );
                        return sha256;
                    }
                    other => panic!("round {round}: digest was not ready: {other:?}"),
                }
            }
            Some(other) => panic!("round {round}: unexpected trailer {other:?}"),
        }
    }
    panic!("the transfer did not converge in 64 windows");
}

fn land(
    assembler: &mut LazyAssembler,
    file_id: Uuid,
    save_dir: &Path,
    name: &str,
    sha256: &str,
) -> PathBuf {
    let partial = assembler
        .take(&file_id)
        .expect("the receiver must have a partial file once the digest arrives");
    finish_lazy_transfer(&partial, save_dir, name, sha256).expect("verifying and landing the file")
}

#[test]
fn a_slow_chunk_is_not_treated_as_a_gap() {
    // Spec, §Error handling: "Slow chunk is not a gap" — when a single chunk
    // takes several seconds to arrive, the receiver must not restart.
    //
    // The unit test for this only covers the *ordering* half (`contiguous`
    // advancing on in-order arrival). What is not covered anywhere is the
    // interaction with the stall watchdog: `stale` runs on a timer, and a
    // watchdog that fires on "a chunk took a while" would restart a transfer
    // that is merely slow. That is the failure mode this test pins: the gap
    // rule must key on *index*, and the stall rule must give a whole window
    // longer than "several seconds".
    let dir = unique_dir("slow-src");
    let size = FILE_CHUNK_BYTES + 512;
    let path = write_source(&dir, "slow.bin", size);

    let mut registry = OfferRegistry::default();
    let offer = advertise(&mut registry, &path);
    let file_id = offer.file_id;
    let source = Uuid::new_v4();
    let shape = ReceiveEngine::shape_from_offer(&offer, source);
    assert_eq!(shape.total_chunks, 2);

    let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
    let mut engine = ReceiveEngine::new(MAX_PULL_WINDOW);
    let transfer_id = engine.open(file_id, shape, NOW);
    let puller = Uuid::new_v4();

    // Ask for both chunks in one window and receive them in order, five
    // seconds apart. No clock in the engine may declare a gap or a stall
    // between them, because nothing is missing and nothing is overdue.
    let pull = pull_of(file_id, source, puller, transfer_id, 0, MAX_PULL_WINDOW);
    let mut response = server.handle_pull(&registry, &pull, NOW);
    assert_eq!(response.chunks.len(), 2);
    engine.mark_pulled(&transfer_id, 0, MAX_PULL_WINDOW);

    let mut arrived_at = Vec::new();
    let mut when = NOW;
    for chunk in &mut response.chunks {
        chunk.from_device = Some(source);
        let outcome = engine.handle_chunk(chunk, when);
        assert_eq!(
            outcome,
            ChunkOutcome::Accept {
                chunk_index: chunk.chunk_index
            },
            "an in-order chunk is accepted no matter how late it is"
        );
        arrived_at.push(when);

        // Five seconds of silence before the next one. Nothing may be
        // reported stale in that gap: the transfer is in flight, in order.
        let probe = when + 5_000;
        assert!(
            engine.stale(probe).is_empty(),
            "5 s of silence is not a stall, and chunk {} is not a gap",
            chunk.chunk_index
        );
        when = probe;
    }
    assert_eq!(arrived_at, vec![NOW, NOW + 5_000]);

    // The run is contiguous: the gap rule never fired.
    let session = engine.session(&transfer_id).expect("session");
    assert_eq!(session.contiguous, 2);
    assert_eq!(session.chunks_seen, 2);

    // Only now does the tail timer legitimately apply: everything arrived and
    // the digest is overdue. Even then it is `DigestWait`, never a restart.
    assert!(
        engine.stale(NOW + 5_000 + 1).is_empty(),
        "the digest is not yet overdue"
    );
    let stale = engine.stale(NOW + 5_000 + 20_000);
    assert_eq!(
        stale,
        vec![(transfer_id, StaleReason::DigestWait)],
        "a missing digest is a re-request, not a restart"
    );
    assert_eq!(
        engine.restarts_used(&file_id),
        0,
        "nothing here may have consumed a restart"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn ttl_gates_the_start_but_not_an_in_flight_transfer() {
    // Spec, §Offer lifecycle: "Expiry SHALL gate only the *start* of a pull;
    // an in-flight transfer SHALL continue until it finishes or hits the idle
    // timeout." And the scenario: "TTL expires during a transfer -> the
    // transfer continues and completes."
    //
    // The TTL check lives in the hub's `authorize_pull`, which runs per
    // *frame*; `PullServer::handle_pull` never consults `expires_at_ms` at
    // all. That is the right split by design — a source that re-checked
    // expiry per window would kill any transfer that outlived the offer, which
    // for a 5 GB file on a slow link is the normal case, not an edge case.
    // This test holds the source to it: start inside the TTL, let the deadline
    // pass mid-file, and require the rest of the windows to be served.
    let dir = unique_dir("ttl-src");
    let save_dir = unique_dir("ttl-dst");
    let size = multi_chunk_size();
    let path = write_source(&dir, "long.bin", size);

    let mut registry = OfferRegistry::default();
    let offer = advertise(&mut registry, &path);
    let file_id = offer.file_id;
    let source = Uuid::new_v4();
    let shape = ReceiveEngine::shape_from_offer(&offer, source);
    let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
    let mut engine = ReceiveEngine::new(1);
    let transfer_id = engine.open(file_id, shape, NOW);
    let mut assembler = LazyAssembler::new(save_dir.clone());
    let puller = Uuid::new_v4();

    // A clock that starts inside the offer's lifetime and then jumps well past
    // its expiry. `advertise` set `expires_at_ms = NOW + TTL`.
    let expiry = offer.expires_at_ms;
    assert!(expiry > NOW);
    let mut now = NOW;

    assert_eq!(
        server
            .handle_pull(
                &registry,
                &pull_of(file_id, source, puller, transfer_id, 0, 1),
                now
            )
            .outcome,
        PullOutcome::Serving,
        "the first window is inside the TTL and must be served"
    );

    // Half an hour of TTL is spent mid-file. From here on every window is
    // past the offer's deadline.
    now = expiry + 60_000;

    let mut served_after_expiry = 0usize;
    for round in 0..64 {
        let Some((index, window)) = engine.next_pull(&transfer_id) else {
            break;
        };
        let mut response = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, index, window),
            now,
        );
        assert_eq!(
            response.outcome,
            PullOutcome::Serving,
            "round {round}: window ({index}, {window}) was refused after the TTL;              an in-flight transfer must outlive its offer"
        );
        engine.mark_pulled(&transfer_id, index, window);
        for chunk in &mut response.chunks {
            chunk.from_device = Some(source);
            assert_eq!(
                engine.handle_chunk(chunk, now),
                ChunkOutcome::Accept {
                    chunk_index: chunk.chunk_index
                }
            );
            assembler.write(chunk).unwrap();
        }
        served_after_expiry += response.chunks.len();
        if let Some(Frame::FileDigest(digest)) = response.trailer {
            assert_eq!(
                engine.handle_digest(&digest, now),
                DigestOutcome::Ready {
                    sha256: digest.file_sha256.clone()
                }
            );
            let partial = assembler.take(&file_id).expect("partial file");
            let landed =
                finish_lazy_transfer(&partial, &save_dir, &offer.name, &digest.file_sha256)
                    .expect("the file must verify");
            assert_eq!(
                fs::read(&landed).unwrap(),
                fs::read(&path).unwrap(),
                "a transfer that outlives the TTL still lands intact"
            );
            assert!(
                served_after_expiry > 0,
                "the point of the test is that chunks were served past the deadline"
            );
            let _ = fs::remove_dir_all(&dir);
            let _ = fs::remove_dir_all(&save_dir);
            return;
        }
        now += 10;
    }
    panic!("the transfer did not complete after the TTL elapsed");
}

#[test]
fn two_concurrent_pulls_of_one_file_are_independent() {
    // The same offer, pulled twice at once under two `transfer_id`s. This is
    // the scenario that broke the tray (5.14) and the one the digest-retention
    // work had to keep working: two sessions of one `file_id` must not share
    // a hasher, must not share an address, and must both complete.
    let dir = unique_dir("concurrent-src");
    let size = multi_chunk_size();
    let path = write_source(&dir, "shared.bin", size);

    let mut registry = OfferRegistry::default();
    let offer = advertise(&mut registry, &path);
    let file_id = offer.file_id;
    let source = Uuid::new_v4();
    let shape = ReceiveEngine::shape_from_offer(&offer, source);

    let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

    // Two receivers, as two devices pulling the same offer.
    let mut engine_a = ReceiveEngine::new(2);
    let mut engine_b = ReceiveEngine::new(1);
    let a = engine_a.open(file_id, shape.clone(), NOW);
    let b = engine_b.open(file_id, shape, NOW);
    assert_ne!(a, b, "two pulls get two sessions");

    let puller_a = Uuid::new_v4();
    let puller_b = Uuid::new_v4();
    let mut done_a: Option<String> = None;
    let mut done_b: Option<String> = None;
    let mut now = NOW;

    for _ in 0..64 {
        if done_a.is_none() {
            if let Some(digest) = step(
                &mut server,
                &registry,
                &mut engine_a,
                file_id,
                source,
                puller_a,
                a,
                now,
            ) {
                done_a = Some(digest);
            }
        }
        if done_b.is_none() {
            if let Some(digest) = step(
                &mut server,
                &registry,
                &mut engine_b,
                file_id,
                source,
                puller_b,
                b,
                now,
            ) {
                done_b = Some(digest);
            }
        }
        if done_a.is_some() && done_b.is_some() {
            break;
        }
        now += 10;
    }

    let (done_a, done_b) = (done_a.expect("A completed"), done_b.expect("B completed"));
    assert_eq!(done_a, done_b, "one file, one digest, whoever asks");
    assert_eq!(done_a, sha256_of(&path));

    // Each session addresses its own puller: the source must not have sent
    // A's chunks to B. The engine rejects a mismatched `from_device`, so a
    // crossed address would have surfaced as a missing chunk, but assert the
    // recorded session state too.
    assert_eq!(engine_a.session(&a).unwrap().contiguous, 3);
    assert_eq!(engine_b.session(&b).unwrap().contiguous, 3);

    let _ = fs::remove_dir_all(&dir);
}

/// Advance one session by a single pull. Returns the digest when it finishes.
#[allow(clippy::too_many_arguments)]
fn step(
    server: &mut PullServer,
    registry: &OfferRegistry,
    engine: &mut ReceiveEngine,
    file_id: Uuid,
    source: Uuid,
    puller: Uuid,
    transfer_id: Uuid,
    now: i64,
) -> Option<String> {
    let (index, window) = engine.next_pull(&transfer_id)?;
    let mut response = server.handle_pull(
        registry,
        &pull_of(file_id, source, puller, transfer_id, index, window),
        now,
    );
    assert_eq!(response.outcome, PullOutcome::Serving);
    engine.mark_pulled(&transfer_id, index, window);
    for chunk in &mut response.chunks {
        chunk.from_device = Some(source);
        assert_eq!(
            engine.handle_chunk(chunk, now),
            ChunkOutcome::Accept {
                chunk_index: chunk.chunk_index
            }
        );
    }
    match response.trailer {
        Some(Frame::FileDigest(digest)) => {
            assert_eq!(
                engine.handle_digest(&digest, now),
                DigestOutcome::Ready {
                    sha256: digest.file_sha256.clone()
                }
            );
            Some(digest.file_sha256)
        }
        _ => None,
    }
}

fn sha256_of(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(fs::read(path).unwrap())
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

#[test]
fn a_full_lazy_transfer_delivers_bytes_exactly() {
    // The one test that says the whole lazy path works end to end: a copy
    // produces an offer, a puller walks the offer window by window, and the
    // bytes that land are the bytes that were advertised.
    let source_dir = unique_dir("full-src");
    let save_dir = unique_dir("full-dst");
    let size = multi_chunk_size();
    let path = write_source(&source_dir, "payload.bin", size);

    let mut registry = OfferRegistry::default();
    let offer = advertise(&mut registry, &path);
    let file_id = offer.file_id;
    assert_eq!(
        offer.file_size, size as u64,
        "the offer carries the stat size"
    );

    let source = Uuid::new_v4();
    let puller = Uuid::new_v4();
    let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
    // A window of two means the fixture needs two pulls: (0, 2) then (2, 1).
    let mut engine = ReceiveEngine::new(2);
    // The engine builds its shape from the offer, exactly as the supervisor
    // does on `Frame::FileOffer`.
    let shape = ReceiveEngine::shape_from_offer(&offer, source);
    assert_eq!(
        shape.total_chunks, 3,
        "three chunks for two full plus a tail"
    );
    // The engine mints the session id; `open` is what the supervisor calls.
    let transfer_id = engine.open(file_id, shape, NOW);

    let mut assembler = LazyAssembler::new(save_dir.clone());
    let sha256 = drive_to_completion(
        &mut server,
        &registry,
        &mut engine,
        &mut assembler,
        file_id,
        source,
        puller,
        transfer_id,
        NOW,
    );

    let landed = land(&mut assembler, file_id, &save_dir, &offer.name, &sha256);
    let received = fs::read(&landed).unwrap();
    let original = fs::read(&path).unwrap();
    assert_eq!(
        received, original,
        "the receiver must land the advertised file byte for byte"
    );
    assert_eq!(received.len(), size);

    // Every byte was seen, in order, with nothing missing.
    let session = engine.session(&transfer_id).expect("session until finish");
    assert_eq!(session.contiguous, 3);
    assert_eq!(session.chunks_seen, 3);

    let _ = fs::remove_dir_all(&source_dir);
    let _ = fs::remove_dir_all(&save_dir);
}

// ---- Phase 8.8: the whole chain, through a real hub over real WebSockets ----

/// A hub config shaped like the one `file_transfer.rs` uses.
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

/// Wait until a transport reports `Connected`.
async fn wait_connected(rx: &mut watch::Receiver<ClientStatus>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if matches!(*rx.borrow_and_update(), ClientStatus::Connected) {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("transport never reported Connected");
        }
        if rx.changed().await.is_err() {
            panic!("status channel closed before Connected");
        }
    }
}

/// Poll for a file to appear in `dir`, up to `timeout`.
///
/// Polling rather than a fixed sleep: the transfer spans two supervisors, a hub,
/// four websockets and a pull round trip per window, so any fixed delay is
/// either flaky or needlessly slow.
async fn wait_for_landed_file(dir: &Path, timeout: std::time::Duration) -> PathBuf {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = entry.file_name();
                // The assembler writes `<file_id>.partial` while in flight.
                if name.to_string_lossy().ends_with(".partial") {
                    continue;
                }
                if path.is_file() {
                    return path;
                }
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("no file landed in {} within {timeout:?}", dir.display());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// The test the other lazy tests cannot be: **everything, over the wire**.
///
/// `a_full_lazy_transfer_delivers_bytes_exactly` deliberately wires
/// `PullServer` straight to `ReceiveEngine`, which is what makes it
/// deterministic — and what let the missing offer fan-out survive 433 tests,
/// because no hub was on the path. This one starts a real hub, connects two
/// real supervisors over real WebSockets, and only passes if the bytes arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lazy_transfer_crosses_a_real_hub_byte_exactly() {
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

    // -- Receiver: pickup mode, sandboxed save dir. --
    let recv_dir = unique_dir("hub-e2e-recv");
    let (recv_status_tx, mut recv_status_rx) = watch::channel(ClientStatus::Connecting);
    let (recv_transport, recv_join) = clipboardwire_core::client::transport::spawn_with_options(
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
        None,
    ));
    wait_connected(&mut recv_status_rx).await;

    // -- Sender: lazy mode, with its own control channel observed. --
    let (send_status_tx, mut send_status_rx) = watch::channel(ClientStatus::Connecting);
    let (send_transport, send_join) = clipboardwire_core::client::transport::spawn_with_options(
        client_cfg(addr),
        clipboardwire_core::client::transport::SpawnOptions {
            status_tx: Some(send_status_tx),
            // **Must be true.** A lazy source still has to *receive*
            // `file_pull` frames to serve them, and those arrive on the
            // control channel this flag creates. `run_with_status` passes
            // `true` for the same reason; a real sender that set `false`
            // would advertise `lazy-file` and then answer nothing.
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

    // Both sides are connected and their supervisors are looping. `Connected`
    // is emitted inside `connect_and_serve` *before* it consumes the welcome and
    // enters its select loop, so give the loops a couple of poll ticks.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // -- Copy a multi-chunk file on the sender. --
    let src_dir = unique_dir("hub-e2e-src");
    let src = write_source(&src_dir, "crosshub.bin", multi_chunk_size());
    let expected = fs::read(&src).unwrap();
    send_events_tx
        .send(ClipChange::Files(vec![src.clone()]))
        .await
        .unwrap();

    // -- The receiver must land identical bytes. --
    let landed = wait_for_landed_file(&recv_dir, std::time::Duration::from_secs(20)).await;
    let received = fs::read(&landed).expect("the landed file is readable");

    assert_eq!(
        received.len(),
        expected.len(),
        "size mismatch: got {} bytes, expected {}",
        received.len(),
        expected.len()
    );
    assert_eq!(
        received, expected,
        "the lazy transfer must land byte-for-byte through a real hub"
    );

    // Cleanup: drop the senders so the supervisors observe closed channels and
    // return, then stop the tasks.
    drop(send_events_tx);
    drop(recv_events_tx);
    send_supervisor.abort();
    recv_supervisor.abort();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), send_join).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), recv_join).await;
    let _ = fs::remove_dir_all(&src_dir);
    let _ = fs::remove_dir_all(&recv_dir);
}

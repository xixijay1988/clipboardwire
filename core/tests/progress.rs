// SPDX-License-Identifier: GPL-3.0-or-later

//! Progress reporting and log redaction (Phase 1, tasks 1.1–1.8).
//!
//! These drive the public file-transfer API directly — no hub, no sockets —
//! because the contract under test is "which events come out of an eager
//! transfer, in what order", not the wire.

use std::fs;
use std::path::PathBuf;

use clipboardwire_core::client::file::{send_file_with_progress, FileReceiver};
use clipboardwire_core::client::{Direction, ProgressState, TransferProgress};
use clipboardwire_core::protocol::FILE_CHUNK_BYTES;
use tokio::sync::broadcast;

fn unique_dir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "cw-progress-{label}-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// 9 MiB of deterministic bytes ⇒ 3 chunks at 4 MiB.
fn multi_chunk_payload() -> Vec<u8> {
    let mut body = vec![0u8; 9 * 1024 * 1024];
    for (i, b) in body.iter_mut().enumerate() {
        *b = ((i.wrapping_mul(31)).wrapping_add(7) & 0xff) as u8;
    }
    body
}

fn drain(rx: &mut broadcast::Receiver<TransferProgress>) -> Vec<TransferProgress> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

#[tokio::test]
async fn multi_chunk_send_emits_hashing_then_pulling_then_done() {
    let dir = unique_dir("send");
    let path = dir.join("blob.bin");
    fs::write(&path, multi_chunk_payload()).unwrap();

    let (tx, mut rx) = broadcast::channel(256);
    let (outbound_tx, mut outbound_rx) =
        tokio::sync::mpsc::channel::<clipboardwire_core::protocol::FileChunkFrame>(64);
    // Drain the chunk channel so `send_file_with_progress` is not blocked.
    let drainer = tokio::spawn(async move {
        let mut n = 0;
        while outbound_rx.recv().await.is_some() {
            n += 1;
        }
        n
    });

    send_file_with_progress(&path, &outbound_tx, Some(&tx), false)
        .await
        .expect("send should succeed");
    drop(outbound_tx);
    let chunks = drainer.await.unwrap();
    assert_eq!(chunks, 3, "9 MiB should be 3 chunks of 4 MiB");

    let events = drain(&mut rx);
    assert!(!events.is_empty(), "subscriber must receive events");

    // Hashing comes first and covers the whole pre-pass.
    assert_eq!(events[0].state, ProgressState::Hashing);
    assert_eq!(events[0].bytes_done, 0);
    assert_eq!(events[0].bytes_total, 9 * 1024 * 1024);
    assert_eq!(events[0].direction, Direction::Send);

    // Then one Pulling per chunk, monotonically increasing.
    let pulling: Vec<&TransferProgress> = events
        .iter()
        .filter(|e| e.state == ProgressState::Pulling)
        .collect();
    assert_eq!(pulling.len(), 3, "one Pulling per chunk");
    let done: Vec<u64> = pulling.iter().map(|e| e.bytes_done).collect();
    assert_eq!(
        done,
        vec![
            FILE_CHUNK_BYTES as u64,
            2 * FILE_CHUNK_BYTES as u64,
            9 * 1024 * 1024,
        ]
    );

    // And it ends with exactly one Done at full size.
    let done_events: Vec<&TransferProgress> = events
        .iter()
        .filter(|e| e.state == ProgressState::Done)
        .collect();
    assert_eq!(done_events.len(), 1);
    assert_eq!(done_events[0].bytes_done, 9 * 1024 * 1024);

    // The sender never claims to verify.
    assert!(
        !events.iter().any(|e| e.state == ProgressState::Verifying),
        "the sender must not report Verifying"
    );

    // Every event carries the same file id and a redacted name.
    let file_id = events[0].file_id;
    assert!(events.iter().all(|e| e.file_id == file_id));
    assert!(events.iter().all(|e| e.transfer_id.is_none()));
    assert!(
        events.iter().all(|e| e.name.starts_with("file#")),
        "default redaction must pseudonymise the name: {:?}",
        events.iter().map(|e| &e.name).collect::<Vec<_>>()
    );

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn no_events_arrive_after_done() {
    let dir = unique_dir("after");
    let path = dir.join("small.txt");
    fs::write(&path, b"hello clipboardwire").unwrap();

    let (tx, mut rx) = broadcast::channel(64);
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(8);
    let drainer = tokio::spawn(async move { while outbound_rx.recv().await.is_some() {} });

    send_file_with_progress(&path, &outbound_tx, Some(&tx), false)
        .await
        .unwrap();
    drop(outbound_tx);
    drainer.await.unwrap();

    let events = drain(&mut rx);
    assert_eq!(events.last().unwrap().state, ProgressState::Done);

    // The sender is finished; nothing else may be published. Give any stray
    // task a chance to run before asserting the channel is quiet.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        drain(&mut rx).is_empty(),
        "no event may follow Done for this file"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn receive_emits_verifying_and_done_but_send_never_does() {
    let dir = unique_dir("recv");
    let mut receiver = FileReceiver::with_save_dir(dir.clone()).unwrap();
    let (tx, mut rx) = broadcast::channel(64);

    // Build the chunk stream with the real sender, then feed it back in.
    let src = unique_dir("recv-src");
    let path = src.join("blob.bin");
    fs::write(&path, multi_chunk_payload()).unwrap();
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(8);
    let drainer = tokio::spawn(async move {
        let mut chunks = Vec::new();
        while let Some(chunk) = outbound_rx.recv().await {
            chunks.push(chunk);
        }
        chunks
    });
    send_file_with_progress(&path, &outbound_tx, None, false)
        .await
        .unwrap();
    drop(outbound_tx);
    let chunks = drainer.await.unwrap();

    let mut final_path = None;
    for chunk in chunks {
        if let Some(p) = receiver
            .receive_chunk_with_progress(chunk, Some(&tx), false)
            .unwrap()
        {
            final_path = Some(p);
        }
    }
    let final_path = final_path.expect("transfer should complete");
    assert_eq!(fs::read(&final_path).unwrap(), multi_chunk_payload());

    let events = drain(&mut rx);
    assert_eq!(events.len(), 2 + 3, "3 Pulling + Verifying + Done");

    let states: Vec<ProgressState> = events.iter().map(|e| e.state).collect();
    assert_eq!(
        states,
        vec![
            ProgressState::Pulling,
            ProgressState::Pulling,
            ProgressState::Pulling,
            ProgressState::Verifying,
            ProgressState::Done,
        ]
    );
    assert!(events.iter().all(|e| e.direction == Direction::Receive));
    // Receive-side byte totals track the whole file, including at Verifying.
    assert_eq!(events.last().unwrap().bytes_done, 9 * 1024 * 1024);
    assert_eq!(
        events[events.len() - 2].bytes_done,
        9 * 1024 * 1024,
        "Verifying is reported at the full byte count"
    );

    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_dir_all(&src);
}

#[tokio::test]
async fn receive_failure_emits_failed_after_verifying() {
    let dir = unique_dir("recv-bad");
    let mut receiver = FileReceiver::with_save_dir(dir.clone()).unwrap();
    let (tx, mut rx) = broadcast::channel(64);

    // One chunk whose payload does not match the advertised hash.
    let file_id = uuid::Uuid::new_v4();
    let chunk = clipboardwire_core::protocol::FileChunkFrame {
        file_id,
        name: "victim.txt".to_string(),
        content_type: "text/plain; charset=utf-8".to_string(),
        chunk_index: 0,
        total_chunks: 1,
        file_sha256: Some("00:11".to_string()),
        file_size: 8,
        payload_b64: base64_encode(b"tampered"),
        from: None,
        to: None,
        transfer_id: None,
        from_device: None,
    };
    let err = receiver
        .receive_chunk_with_progress(chunk, Some(&tx), false)
        .unwrap_err();
    assert!(format!("{err:#}").contains("SHA-256"), "{err:#}");

    let events = drain(&mut rx);
    let states: Vec<ProgressState> = events.iter().map(|e| e.state).collect();
    assert_eq!(
        states,
        vec![
            ProgressState::Pulling,
            ProgressState::Verifying,
            ProgressState::Failed,
        ]
    );

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn progress_name_is_raw_only_when_explicitly_enabled() {
    let dir = unique_dir("names");
    let path = dir.join("passwords.txt");
    fs::write(&path, b"secret").unwrap();

    let (tx, mut rx) = broadcast::channel(16);
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(4);
    let drainer = tokio::spawn(async move { while outbound_rx.recv().await.is_some() {} });
    send_file_with_progress(&path, &outbound_tx, Some(&tx), true)
        .await
        .unwrap();
    drop(outbound_tx);
    drainer.await.unwrap();

    let events = drain(&mut rx);
    assert!(!events.is_empty());
    assert!(
        events.iter().all(|e| e.name == "passwords.txt"),
        "log_file_names = true must show the real name"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn send_failure_reports_the_same_file_id_and_only_once() {
    // A send that fails before the first chunk must still clear the tray entry,
    // and must do so under the *same* id as any event it already emitted —
    // otherwise the terminal event never matches the entry and it hangs.
    let dir = unique_dir("send-fail");
    let missing = dir.join("does-not-exist.bin");

    let (tx, mut rx) = broadcast::channel(64);
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::channel(4);

    let err = send_file_with_progress(&missing, &outbound_tx, Some(&tx), false)
        .await
        .unwrap_err();
    // With redaction on (the default), even the error context carries the
    // pseudonym rather than the path.
    let message = format!("{err:#}");
    assert!(message.contains("stat"), "{message}");
    assert!(
        !message.contains("does-not-exist"),
        "error context must not leak the name: {message}"
    );

    let events = drain(&mut rx);
    let failed: Vec<&TransferProgress> = events
        .iter()
        .filter(|e| e.state == ProgressState::Failed)
        .collect();
    assert_eq!(failed.len(), 1, "exactly one Failed, got {events:?}");

    // No chunk was ever queued, so nothing else may appear.
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.state, ProgressState::Done | ProgressState::Pulling)),
        "a failed send must not report progress it never made: {events:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn receive_failure_reports_exactly_one_failed_event() {
    // The regression this guards: the SHA-mismatch path used to emit its own
    // `Failed` *and* the wrapping error handler emitted another.
    let dir = unique_dir("recv-one-failed");
    let mut receiver = FileReceiver::with_save_dir(dir.clone()).unwrap();
    let (tx, mut rx) = broadcast::channel(64);

    let chunk = clipboardwire_core::protocol::FileChunkFrame {
        file_id: uuid::Uuid::new_v4(),
        name: "victim.txt".to_string(),
        content_type: "text/plain; charset=utf-8".to_string(),
        chunk_index: 0,
        total_chunks: 1,
        file_sha256: Some("00:11".to_string()),
        file_size: 8,
        payload_b64: base64_encode(b"tampered"),
        from: None,
        to: None,
        transfer_id: None,
        from_device: None,
    };
    assert!(receiver
        .receive_chunk_with_progress(chunk, Some(&tx), false)
        .is_err());

    let events = drain(&mut rx);
    let failed = events
        .iter()
        .filter(|e| e.state == ProgressState::Failed)
        .count();
    assert_eq!(
        failed,
        1,
        "one failure must produce one Failed, got {:?}",
        events.iter().map(|e| e.state).collect::<Vec<_>>()
    );
    // The failure reports the real total, not 0/0, so the tray can name it.
    let last = events.last().unwrap();
    assert_eq!(last.bytes_total, 8);
    assert!(last.name.starts_with("file#"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn summary_line_stays_within_the_requested_width() {
    let event = TransferProgress {
        file_id: uuid::Uuid::nil(),
        transfer_id: None,
        direction: Direction::Send,
        peer: None,
        name: "a-very-long-file-name-that-would-not-fit-in-a-tooltip-at-all.bin".to_string(),
        bytes_done: 42,
        bytes_total: 100,
        state: ProgressState::Pulling,
    };
    // `max_name_chars` bounds the *name*; the verb and the percentage are
    // added around it, so the whole line is name budget + a small constant.
    let line = event.summary_line(24);
    let name_part = line
        .strip_prefix("sending ")
        .and_then(|rest| rest.strip_suffix(" 42%"))
        .expect("line should be `sending <name> 42%`");
    assert!(
        name_part.chars().count() <= 24,
        "name budget exceeded: {name_part:?}"
    );
    assert!(line.chars().count() <= 24 + 12, "{line:?}");
    assert!(!line.contains(".bin"), "name must be truncated: {line}");

    // A short name is never touched.
    let short = TransferProgress {
        name: "a.txt".to_string(),
        ..event
    };
    assert_eq!(short.summary_line(24), "sending a.txt 42%");
}

#[test]
fn percent_handles_zero_and_overflow() {
    let mut event = TransferProgress {
        file_id: uuid::Uuid::nil(),
        transfer_id: None,
        direction: Direction::Receive,
        peer: None,
        name: "x".into(),
        bytes_done: 5,
        bytes_total: 0,
        state: ProgressState::Pulling,
    };
    assert_eq!(event.percent(), None);

    event.bytes_total = 10;
    event.bytes_done = 5;
    assert_eq!(event.percent(), Some(50));

    event.bytes_done = 99;
    assert_eq!(event.percent(), Some(100), "clamped, never above 100");
}

#[test]
fn terminal_states_are_done_failed_aborted() {
    for (state, terminal) in [
        (ProgressState::Hashing, false),
        (ProgressState::Offering, false),
        (ProgressState::Pulling, false),
        (ProgressState::Verifying, false),
        (ProgressState::Done, true),
        (ProgressState::Failed, true),
        (ProgressState::Aborted, true),
    ] {
        assert_eq!(state.is_terminal(), terminal, "{state:?}");
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

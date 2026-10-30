// SPDX-License-Identifier: GPL-3.0-or-later

//! Receiver side of a lazy transfer.
//!
//! A pull session is identified by `transfer_id` alone. `file_id` is looked at
//! only to confirm the sender is the device that advertised the offer — all
//! routing uses `transfer_id`, because two pastes of the same file are two
//! independent sessions.
//!
//! # Why a restart always begins at index 0
//!
//! One `file_digest` covers the whole file. A session that resumed at index
//! *k* would produce a digest over `[k, N)`, which can never validate `[0, N)`
//! — so a resume is *not* a continuation. Any abort discards the session and
//! starts over under a new `transfer_id`, overwriting the partial file in
//! place. That costs bandwidth and buys the only hash shape that means
//! anything.
//!
//! # Why restarts are counted per file, not per session
//!
//! The spec caps attempts "per file". Each restart mints a new `transfer_id`,
//! so a counter living on the session would reset to zero every time and the
//! cap would never be reached. [`ReceiveEngine::restarts`] is therefore keyed
//! by `file_id` and outlives the sessions it counts.

use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::protocol::{FileChunkFrame, FileDigestFrame, FILE_CHUNK_BYTES};

/// How long a window may go unanswered before the transfer is considered
/// stalled. Only the *tail* needs a timer: a hole in the middle is visible the
/// moment a higher index arrives.
pub const TAIL_STALL_TIMEOUT_MS: i64 = 15_000;

/// How long to wait for the digest once every chunk is in.
///
/// Deliberately shorter than [`TAIL_STALL_TIMEOUT_MS`]: waiting for a digest
/// counts as progress, so this timer must fire in the window where the stall
/// timer has not.
pub const DIGEST_WAIT_MS: i64 = 10_000;

/// Full restarts allowed for one file before failing visibly.
pub const MAX_RESTARTS: u32 = 3;

/// What happened to one inbound chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// Accept and write at this index.
    Accept { chunk_index: u32 },
    /// Not our session, or from the wrong device: drop silently.
    Ignored,
    /// A gap or duplicate-from-another-session: the caller must abort and
    /// restart from zero under a new `transfer_id`.
    Restart,
    /// The session is unusable and retries are exhausted.
    Failed(&'static str),
}

/// What happened to one inbound digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DigestOutcome {
    /// Verify these bytes against this hash, then publish.
    Ready { sha256: String },
    /// Digest for a session we do not have: drop.
    Unknown,
    /// Digest arrived but not every chunk did: the sender is confused.
    Incomplete,
}

/// Why a session has gone quiet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleReason {
    /// No chunk for [`TAIL_STALL_TIMEOUT_MS`] while a window is outstanding.
    TailStall,
    /// The last chunk is in but no digest for [`DIGEST_WAIT_MS`].
    DigestWait,
}

/// One in-flight receive session.
pub struct ReceiveSession {
    pub transfer_id: Uuid,
    pub file_id: Uuid,
    /// Device that offered the file. Chunks from anyone else are rejected.
    pub from_device: Uuid,
    pub name: String,
    pub content_type: String,
    pub total_chunks: u32,
    pub file_size: u64,
    /// Which indices have landed. Sized from `total_chunks` on the first chunk.
    pub received: Vec<bool>,
    pub chunks_seen: u32,
    /// Highest contiguous index + 1: everything below is present.
    pub contiguous: u32,
    pub last_progress_ms: i64,
    /// Set once the digest arrives, so the caller can verify and publish.
    pub digest: Option<String>,
    /// The window currently asked for, `(start, count)`. Cleared when its last
    /// chunk lands, which is what lets `next_pull` ask for the next one.
    in_flight: Option<(u32, u32)>,
}

/// What a restart needs to rebuild a session after its predecessor is dropped.
///
/// Kept per `file_id` and independent of sessions: the broken session is gone
/// by the time the restart is built, and the offer it came from is not.
#[derive(Debug, Clone)]
pub struct OfferShape {
    pub from_device: Uuid,
    pub name: String,
    pub content_type: String,
    pub file_size: u64,
    pub total_chunks: u32,
}

pub struct ReceiveEngine {
    sessions: HashMap<Uuid, ReceiveSession>,
    /// Restart counts by `file_id`, not by session — see the module docs.
    restarts: HashMap<Uuid, u32>,
    /// The offer each `file_id` was pulled from, so a restart can be built
    /// after the failed session has been dropped.
    shapes: HashMap<Uuid, OfferShape>,
    window: u32,
}

impl ReceiveEngine {
    pub fn new(window: u32) -> Self {
        Self {
            sessions: HashMap::new(),
            restarts: HashMap::new(),
            shapes: HashMap::new(),
            window: window.clamp(1, crate::protocol::MAX_PULL_WINDOW),
        }
    }

    pub fn session(&self, transfer_id: &Uuid) -> Option<&ReceiveSession> {
        self.sessions.get(transfer_id)
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Whether this device already has a live session for `file_id`.
    ///
    /// Used to refuse a second concurrent paste of the same file: the two
    /// would write the same temporary path.
    pub fn has_session_for(&self, file_id: &Uuid) -> bool {
        self.sessions.values().any(|s| s.file_id == *file_id)
    }

    /// Open a session for `file_id` under a fresh `transfer_id`.
    ///
    /// Returns the id to pull with. The shape comes from the offer, not from a
    /// chunk: the first pull has to be asked for before any chunk exists.
    pub fn open(&mut self, file_id: Uuid, shape: OfferShape, now_ms: i64) -> Uuid {
        let transfer_id = Uuid::new_v4();
        self.shapes.insert(file_id, shape.clone());
        self.sessions.insert(
            transfer_id,
            ReceiveSession {
                transfer_id,
                file_id,
                from_device: shape.from_device,
                name: shape.name,
                content_type: shape.content_type,
                total_chunks: shape.total_chunks,
                file_size: shape.file_size,
                received: vec![false; shape.total_chunks as usize],
                chunks_seen: 0,
                contiguous: 0,
                last_progress_ms: now_ms,
                digest: None,
                in_flight: None,
            },
        );
        transfer_id
    }

    /// Build an [`OfferShape`] from a wire offer.
    ///
    /// `total_chunks` is derived from `file_size` because an offer carries no
    /// chunk count; it is at least 1 so a zero-byte file (which the offer
    /// registry never publishes, but a hostile hub could) still sizes its
    /// bitmap.
    pub fn shape_from_offer(
        offer: &crate::protocol::FileOfferFrame,
        from_device: Uuid,
    ) -> OfferShape {
        let total_chunks = u32::try_from(
            offer
                .file_size
                .div_ceil(crate::protocol::FILE_CHUNK_BYTES as u64),
        )
        .unwrap_or(u32::MAX)
        .max(1);
        OfferShape {
            from_device,
            name: offer.name.clone(),
            content_type: offer.content_type.clone(),
            file_size: offer.file_size,
            total_chunks,
        }
    }

    /// Record that a pull for `(start, count)` has been sent.
    ///
    /// Kept separate from [`Self::next_pull`] so a caller that fails to send
    /// the request does not leave the engine believing one is in flight.
    pub fn mark_pulled(&mut self, transfer_id: &Uuid, start: u32, count: u32) {
        if let Some(session) = self.sessions.get_mut(transfer_id) {
            session.in_flight = Some((start, count));
        }
    }

    /// Which pull to send next, if any.
    pub fn next_pull(&self, transfer_id: &Uuid) -> Option<(u32, u32)> {
        let session = self.sessions.get(transfer_id)?;
        if session.digest.is_some() || session.in_flight.is_some() {
            return None;
        }
        if session.contiguous >= session.total_chunks {
            return None;
        }
        let remaining = session.total_chunks - session.contiguous;
        Some((session.contiguous, self.window.min(remaining).max(1)))
    }

    /// Re-open `old` under a fresh `transfer_id`, keeping its bitmap.
    ///
    /// A fresh [`Self::open`] zeroes the bitmap, which is what turns every
    /// restart into a from-scratch re-download; a resumed session only
    /// re-asks for the window that died (`in_flight` cleared) and carries on
    /// from `contiguous`. The partial on disk stays aligned because the
    /// assembler writes each chunk at its own offset.
    pub fn resume(&mut self, old: ReceiveSession, now_ms: i64) -> Uuid {
        let transfer_id = Uuid::new_v4();
        self.sessions.insert(
            transfer_id,
            ReceiveSession {
                transfer_id,
                in_flight: None,
                last_progress_ms: now_ms,
                ..old
            },
        );
        transfer_id
    }

    /// Handle one inbound chunk.
    ///
    /// The order below is the whole design: a chunk that names an unknown
    /// session and a non-zero index is somebody else's stream, not an error;
    /// a chunk from the wrong device is likewise not ours; only a chunk inside
    /// our own session with a hole in front of it is a genuine gap.
    pub fn handle_chunk(&mut self, chunk: &FileChunkFrame, now_ms: i64) -> ChunkOutcome {
        let (Some(transfer_id), Some(from_device)) = (chunk.transfer_id, chunk.from_device) else {
            // An eager chunk (no `transfer_id`) or one the hub never stamped.
            // Neither belongs to a lazy session this engine owns — and a
            // session is only ever opened by [`Self::open`] from an offer, never
            // by an unsolicited chunk.
            return ChunkOutcome::Ignored;
        };

        let Some(session) = self.sessions.get_mut(&transfer_id) else {
            return ChunkOutcome::Ignored;
        };

        // Read what we need before any branch that has to touch the map
        // wholesale: `restart_or_fail` drops the session, so the borrow above
        // cannot survive into it.
        let file_id = session.file_id;
        let total_chunks = session.total_chunks;

        if from_device != session.from_device {
            // Another device pulling the same file is not our problem, and
            // certainly not an error worth restarting over.
            return ChunkOutcome::Ignored;
        }

        // Clear the in-flight window as soon as its last chunk lands, whether
        // or not the run is contiguous: the point is that the reply arrived.
        if let Some((start, count)) = session.in_flight {
            let end = start.saturating_add(count);
            if chunk.chunk_index >= start && chunk.chunk_index < end && chunk.chunk_index + 1 == end
            {
                session.in_flight = None;
            }
        }

        if chunk.chunk_index > session.contiguous {
            // A hole. Chunks arrive FIFO from one source, so this index proves
            // that `contiguous` and everything up to it will never arrive in
            // this session.
            return self.restart_or_fail(file_id);
        }

        if chunk.chunk_index < session.contiguous {
            // Already have it: a retransmitted window, which is normal after a
            // lost reply. Idempotent, and nothing new was learned.
            return ChunkOutcome::Ignored;
        }

        let index = chunk.chunk_index as usize;
        if index >= session.received.len() {
            // `total_chunks` disagreed with the chunk's own index. Metadata
            // this inconsistent cannot be assembled.
            return self.restart_or_fail(file_id);
        }
        debug_assert!(chunk.chunk_index < total_chunks);

        if !session.received[index] {
            session.received[index] = true;
            session.chunks_seen += 1;
        }
        while (session.contiguous as usize) < session.received.len()
            && session.received[session.contiguous as usize]
        {
            session.contiguous += 1;
        }
        session.last_progress_ms = now_ms;
        ChunkOutcome::Accept {
            chunk_index: chunk.chunk_index,
        }
    }

    /// Handle one inbound digest.
    pub fn handle_digest(&mut self, digest: &FileDigestFrame, now_ms: i64) -> DigestOutcome {
        let Some(session) = self.sessions.get_mut(&digest.transfer_id) else {
            return DigestOutcome::Unknown;
        };
        if digest.file_id != session.file_id {
            // The digest names a different file than the session is assembling.
            return DigestOutcome::Unknown;
        }
        session.last_progress_ms = now_ms;

        if session.contiguous < session.total_chunks {
            // The sender thinks it is done; we are missing bytes. Discard and
            // start over rather than verifying a partial file.
            let file_id = session.file_id;
            self.sessions.remove(&digest.transfer_id);
            // The restart counter advances either way; whether the budget is
            // spent is the caller's next decision (it sees `restarts_used`).
            let _ = self.bump_restart(file_id);
            return DigestOutcome::Incomplete;
        }

        session.digest = Some(digest.file_sha256.clone());
        DigestOutcome::Ready {
            sha256: digest.file_sha256.clone(),
        }
    }

    /// Which sessions have stalled past their deadline.
    ///
    /// The two timers cannot fire for the same session: `DigestWait` requires
    /// `contiguous == total_chunks`, and in that state `in_flight` has been
    /// cleared by the last chunk, so `TailStall`'s "a window is outstanding"
    /// precondition is false. `DIGEST_WAIT_MS < TAIL_STALL_TIMEOUT_MS` keeps
    /// them from racing even if that invariant were ever broken.
    pub fn stale(&self, now_ms: i64) -> Vec<(Uuid, StaleReason)> {
        let mut out = Vec::new();
        for (transfer_id, session) in &self.sessions {
            if session.digest.is_some() {
                continue;
            }
            let idle = now_ms.saturating_sub(session.last_progress_ms);
            if session.contiguous >= session.total_chunks {
                // Everything arrived; only the digest is outstanding.
                if idle >= DIGEST_WAIT_MS {
                    out.push((*transfer_id, StaleReason::DigestWait));
                }
            } else if session.in_flight.is_some() && idle >= TAIL_STALL_TIMEOUT_MS {
                out.push((*transfer_id, StaleReason::TailStall));
            }
        }
        out
    }

    /// Drop a session, e.g. after an abort from the source.
    pub fn drop_session(&mut self, transfer_id: &Uuid) -> Option<ReceiveSession> {
        self.sessions.remove(transfer_id)
    }

    /// Drop every session for `file_id` (an offer-scoped abort).
    pub fn drop_sessions_for_file(&mut self, file_id: &Uuid) -> usize {
        let doomed: Vec<Uuid> = self
            .sessions
            .values()
            .filter(|s| s.file_id == *file_id)
            .map(|s| s.transfer_id)
            .collect();
        for transfer_id in &doomed {
            self.sessions.remove(transfer_id);
        }
        doomed.len()
    }

    /// The offer shape for `file_id`, if this engine has ever pulled it.
    pub fn shape(&self, file_id: &Uuid) -> Option<&OfferShape> {
        self.shapes.get(file_id)
    }

    /// Forget everything about `file_id`: session, restart budget and shape.
    ///
    /// Called when an offer-scoped abort arrives, so a later `superseded` copy
    /// of the same path starts with a fresh budget.
    pub fn forget_file(&mut self, file_id: &Uuid) {
        self.drop_sessions_for_file(file_id);
        self.restarts.remove(file_id);
        self.shapes.remove(file_id);
    }

    /// How many restarts `file_id` has used.
    pub fn restarts_used(&self, file_id: &Uuid) -> u32 {
        self.restarts.get(file_id).copied().unwrap_or(0)
    }

    /// End the session for `file_id` after a successful transfer, so the same
    /// file can be pulled again later.
    pub fn finish(&mut self, transfer_id: &Uuid) {
        self.sessions.remove(transfer_id);
    }

    fn restart_or_fail(&mut self, file_id: Uuid) -> ChunkOutcome {
        // The session is unusable: a hole means the rest of this run can never
        // be assembled, and the digest could never validate. Drop it before
        // deciding whether another attempt is allowed.
        let doomed: Vec<Uuid> = self
            .sessions
            .values()
            .filter(|s| s.file_id == file_id)
            .map(|s| s.transfer_id)
            .collect();
        for transfer_id in doomed {
            self.sessions.remove(&transfer_id);
        }
        match self.bump_restart(file_id) {
            Some(_) => ChunkOutcome::Restart,
            None => ChunkOutcome::Failed("too many restarts"),
        }
    }

    /// Count one restart against `file_id`. `None` when the budget is spent.
    ///
    /// Also used by the supervisor's stall/abort restart path: counting at
    /// call time (rather than comparing `restarts_used` first) is what makes
    /// the budget real — a check that never increments is not a cap.
    pub fn bump_restart(&mut self, file_id: Uuid) -> Option<u32> {
        let count = self.restarts.entry(file_id).or_insert(0);
        *count += 1;
        if *count > MAX_RESTARTS {
            None
        } else {
            Some(*count)
        }
    }
}

/// Writes lazy chunks to `<save_dir>/<file_id>.partial`.
///
/// Kept apart from [`ReceiveEngine`], which is pure: the engine decides *what*
/// to accept, this decides *where those bytes go*. That split is what lets the
/// engine's tests run without a filesystem.
pub struct LazyAssembler {
    save_dir: PathBuf,
    /// One open partial file per `file_id`. A restart reuses it, truncating in
    /// place — the spec's "overwriting bytes already written".
    open: HashMap<Uuid, PartialFile>,
}

struct PartialFile {
    temp_path: PathBuf,
    file: std::fs::File,
}

impl LazyAssembler {
    pub fn new(save_dir: PathBuf) -> Self {
        Self {
            save_dir,
            open: HashMap::new(),
        }
    }

    /// Write one chunk at its index.
    ///
    /// `chunk_index` is used for the offset, not the arrival order, so a
    /// retransmitted or out-of-order window still lands in the right place.
    pub fn write(&mut self, chunk: &FileChunkFrame) -> std::io::Result<()> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine as _;
        let bytes = STANDARD
            .decode(&chunk.payload_b64)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let partial = match self.open.get_mut(&chunk.file_id) {
            Some(partial) => partial,
            None => {
                std::fs::create_dir_all(&self.save_dir)?;
                let temp_path = self.save_dir.join(format!("{}.partial", chunk.file_id));
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(&temp_path)?;
                file.set_len(chunk.file_size)?;
                self.open
                    .insert(chunk.file_id, PartialFile { temp_path, file });
                self.open.get_mut(&chunk.file_id).expect("just inserted")
            }
        };

        let offset = u64::from(chunk.chunk_index) * FILE_CHUNK_BYTES as u64;
        partial.file.seek(SeekFrom::Start(offset))?;
        partial.file.write_all(&bytes)?;
        Ok(())
    }

    /// Close `file_id`'s partial file and hand back its path for verification.
    pub fn take(&mut self, file_id: &Uuid) -> Option<PathBuf> {
        self.open.remove(file_id).map(|partial| {
            let _ = partial.file.sync_all();
            partial.temp_path
        })
    }

    /// Forget `file_id` and delete whatever was written.
    pub fn discard(&mut self, file_id: &Uuid) {
        if let Some(partial) = self.open.remove(file_id) {
            let _ = std::fs::remove_file(&partial.temp_path);
        }
    }

    /// Whether a partial file is open for `file_id`.
    pub fn is_open(&self, file_id: &Uuid) -> bool {
        self.open.contains_key(file_id)
    }

    /// Path of the partial file for `file_id`, if any.
    pub fn partial_path(&self, file_id: &Uuid) -> Option<&Path> {
        self.open.get(file_id).map(|p| p.temp_path.as_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_700_000_000_000;

    /// A `total_chunks`-chunk offer shape from `device`.
    fn shape(device: Uuid, total_chunks: u32) -> OfferShape {
        OfferShape {
            from_device: device,
            name: "a.bin".to_string(),
            content_type: "application/octet-stream".to_string(),
            file_size: u64::from(total_chunks) * FILE_CHUNK_BYTES as u64,
            total_chunks,
        }
    }

    fn chunk(
        transfer_id: Uuid,
        file_id: Uuid,
        from_device: Uuid,
        index: u32,
        total: u32,
    ) -> FileChunkFrame {
        FileChunkFrame {
            file_id,
            name: "a.bin".to_string(),
            content_type: "application/octet-stream".to_string(),
            chunk_index: index,
            total_chunks: total,
            file_sha256: None,
            file_size: u64::from(total) * FILE_CHUNK_BYTES as u64,
            payload_b64: "AAEC".to_string(),
            from: None,
            to: Some(from_device),
            transfer_id: Some(transfer_id),
            from_device: Some(from_device),
        }
    }

    /// An engine with one session open for 5 chunks.
    fn engine_with_session(window: u32) -> (ReceiveEngine, Uuid, Uuid, Uuid, Uuid) {
        let mut engine = ReceiveEngine::new(window);
        let file_id = Uuid::new_v4();
        let device = Uuid::new_v4();
        let transfer_id = engine.open(file_id, shape(device, 5), NOW);
        (engine, transfer_id, file_id, device, Uuid::new_v4())
    }

    #[test]
    fn the_first_chunk_opens_a_session() {
        let mut engine = ReceiveEngine::new(3);
        let file_id = Uuid::new_v4();
        let device = Uuid::new_v4();
        let transfer_id = engine.open(file_id, shape(device, 3), NOW);

        let outcome = engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 3), NOW);
        assert_eq!(outcome, ChunkOutcome::Accept { chunk_index: 0 });
        assert_eq!(engine.session(&transfer_id).unwrap().contiguous, 1);

        // A chunk for a transfer_id the engine has never seen, at index 0,
        // cannot open a session: only `open` does that, from an offer.
        let stranger = engine.handle_chunk(&chunk(Uuid::new_v4(), file_id, device, 0, 3), NOW);
        assert_eq!(stranger, ChunkOutcome::Ignored);
    }

    #[test]
    fn a_chunk_beyond_the_contiguous_run_is_a_gap() {
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(3);
        engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 5), NOW);

        // 2 arrives while 1 is missing. FIFO from one source means 1 will
        // never come in this session.
        let outcome = engine.handle_chunk(&chunk(transfer_id, file_id, device, 2, 5), NOW);
        assert_eq!(outcome, ChunkOutcome::Restart);
        assert_eq!(engine.restarts_used(&file_id), 1);
        assert_eq!(engine.session_count(), 0, "the broken session is discarded");
    }

    #[test]
    fn a_duplicate_chunk_is_ignored() {
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(3);
        assert_eq!(
            engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 5), NOW),
            ChunkOutcome::Accept { chunk_index: 0 }
        );
        assert_eq!(
            engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 5), NOW),
            ChunkOutcome::Ignored,
            "a retransmitted chunk must not be written twice or counted twice"
        );
        assert_eq!(engine.session(&transfer_id).unwrap().chunks_seen, 1);
    }

    #[test]
    fn a_chunk_from_another_device_is_ignored() {
        let (mut engine, transfer_id, file_id, _device, _) = engine_with_session(3);
        let impostor = Uuid::new_v4();
        let outcome = engine.handle_chunk(&chunk(transfer_id, file_id, impostor, 0, 5), NOW);
        assert_eq!(
            outcome,
            ChunkOutcome::Ignored,
            "another device's stream is not ours to fail"
        );
        assert_eq!(engine.restarts_used(&file_id), 0, "and not a restart");
    }

    #[test]
    fn a_chunk_for_an_unknown_transfer_is_ignored() {
        let mut engine = ReceiveEngine::new(3);
        let outcome = engine.handle_chunk(
            &chunk(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 3, 5),
            NOW,
        );
        assert_eq!(outcome, ChunkOutcome::Ignored);
        assert_eq!(engine.session_count(), 0);
    }

    #[test]
    fn four_restarts_fail_the_transfer() {
        // The cap is 3, so the fourth attempt is the one that fails. Counted
        // per file, not per session: each restart mints a new transfer_id.
        let mut engine = ReceiveEngine::new(3);
        let file_id = Uuid::new_v4();
        let device = Uuid::new_v4();

        // Literal 3, not `MAX_RESTARTS`: driving the loop by the constant
        // under test makes a mutation that raises the constant pass, because
        // the test then simply runs more iterations. This was caught by a
        // mutation run, not by reading the code.
        assert_eq!(MAX_RESTARTS, 3, "the spec's cap");
        for attempt in 1..=3u32 {
            let transfer_id = engine.open(file_id, shape(device, 5), NOW);
            engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 5), NOW);
            let outcome = engine.handle_chunk(&chunk(transfer_id, file_id, device, 2, 5), NOW);
            assert_eq!(outcome, ChunkOutcome::Restart, "attempt {attempt}");
            assert_eq!(engine.restarts_used(&file_id), attempt);
        }

        let transfer_id = engine.open(file_id, shape(device, 5), NOW);
        engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 5), NOW);
        assert_eq!(
            engine.handle_chunk(&chunk(transfer_id, file_id, device, 2, 5), NOW),
            ChunkOutcome::Failed("too many restarts"),
            "the fourth attempt must fail"
        );
    }

    #[test]
    fn the_next_pull_advances_by_window() {
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(3);
        assert_eq!(engine.next_pull(&transfer_id), Some((0, 3)));

        engine.mark_pulled(&transfer_id, 0, 3);
        assert_eq!(
            engine.next_pull(&transfer_id),
            None,
            "one window in flight at a time"
        );

        for index in 0..3 {
            engine.handle_chunk(&chunk(transfer_id, file_id, device, index, 5), NOW);
        }
        assert_eq!(
            engine.next_pull(&transfer_id),
            Some((3, 2)),
            "3..5 is the remainder, clamped from the window of 3"
        );
    }

    #[test]
    fn the_last_window_is_clamped_to_the_remainder() {
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(4);
        for index in 0..3 {
            engine.handle_chunk(&chunk(transfer_id, file_id, device, index, 5), NOW);
        }
        assert_eq!(engine.next_pull(&transfer_id), Some((3, 2)));
    }

    #[test]
    fn a_tail_stall_is_detected_after_the_timeout() {
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(3);
        engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 5), NOW);
        engine.mark_pulled(&transfer_id, 1, 3);

        assert_eq!(std::hint::black_box(TAIL_STALL_TIMEOUT_MS), 15_000);
        assert!(engine.stale(NOW + 14_999).is_empty(), "not yet");
        assert_eq!(
            engine.stale(NOW + 15_000),
            vec![(transfer_id, StaleReason::TailStall)]
        );
    }

    #[test]
    fn waiting_for_a_digest_is_not_a_tail_stall() {
        // All chunks in, digest outstanding. `TailStall` requires an
        // outstanding window, and the last chunk cleared it.
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(5);
        for index in 0..5 {
            engine.handle_chunk(&chunk(transfer_id, file_id, device, index, 5), NOW);
        }
        assert_eq!(engine.session(&transfer_id).unwrap().contiguous, 5);

        // Literal deadlines, not the constants: a test that probes at
        // `NOW + DIGEST_WAIT_MS` moves its own goalpost when the constant
        // changes, and would pass a mutation that made the digest wait longer
        // than the stall timeout. The relationship is what matters — the
        // digest wait must be the *shorter* one.
        // Read through `std::hint::black_box` so clippy does not fold the
        // comparison into a constant assertion: the point is to pin the
        // *relationship*, which a mutation can break while both values stay
        // individually plausible.
        let digest_wait = std::hint::black_box(DIGEST_WAIT_MS);
        let stall = std::hint::black_box(TAIL_STALL_TIMEOUT_MS);
        assert!(
            digest_wait < stall,
            "the digest wait must fire before the stall timeout"
        );
        assert_eq!(digest_wait, 10_000);
        assert_eq!(stall, 15_000);

        let stale = engine.stale(NOW + 10_000);
        assert_eq!(stale, vec![(transfer_id, StaleReason::DigestWait)]);
        assert!(
            !stale.iter().any(|(_, r)| *r == StaleReason::TailStall),
            "a complete file is not a stalled tail"
        );
        // Nothing fires before the digest deadline.
        assert!(engine.stale(NOW + 9_999).is_empty());
    }

    #[test]
    fn a_digest_for_an_incomplete_session_restarts() {
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(5);
        engine.handle_chunk(&chunk(transfer_id, file_id, device, 0, 5), NOW);

        let digest = FileDigestFrame {
            file_id,
            transfer_id,
            to: Uuid::new_v4(),
            from_device: Some(device),
            file_sha256: "AA:BB".to_string(),
        };
        assert_eq!(
            engine.handle_digest(&digest, NOW),
            DigestOutcome::Incomplete
        );
        assert_eq!(engine.restarts_used(&file_id), 1);
        assert_eq!(
            engine.session_count(),
            0,
            "the partial session is discarded"
        );
    }

    #[test]
    fn a_digest_for_an_unknown_session_is_dropped() {
        let mut engine = ReceiveEngine::new(3);
        let digest = FileDigestFrame {
            file_id: Uuid::new_v4(),
            transfer_id: Uuid::new_v4(),
            to: Uuid::new_v4(),
            from_device: Some(Uuid::new_v4()),
            file_sha256: "AA:BB".to_string(),
        };
        assert_eq!(engine.handle_digest(&digest, NOW), DigestOutcome::Unknown);
    }

    #[test]
    fn a_complete_session_hands_back_the_digest() {
        let (mut engine, transfer_id, file_id, device, _) = engine_with_session(5);
        for index in 0..5 {
            engine.handle_chunk(&chunk(transfer_id, file_id, device, index, 5), NOW);
        }
        let digest = FileDigestFrame {
            file_id,
            transfer_id,
            to: Uuid::new_v4(),
            from_device: Some(device),
            file_sha256: "AA:BB".to_string(),
        };
        assert_eq!(
            engine.handle_digest(&digest, NOW),
            DigestOutcome::Ready {
                sha256: "AA:BB".to_string()
            }
        );
        // A session holding a digest asks for no more pulls.
        assert_eq!(engine.next_pull(&transfer_id), None);
        // And it is no longer stale-candidate.
        assert!(engine.stale(NOW + TAIL_STALL_TIMEOUT_MS * 10).is_empty());
    }

    #[test]
    fn an_offer_scoped_abort_drops_every_session_for_the_file() {
        let mut engine = ReceiveEngine::new(3);
        let file_id = Uuid::new_v4();
        let device = Uuid::new_v4();
        let first = engine.open(file_id, shape(device, 3), NOW);
        let second = engine.open(file_id, shape(device, 3), NOW);

        assert_eq!(engine.drop_sessions_for_file(&file_id), 2);
        assert!(engine.session(&first).is_none());
        assert!(engine.session(&second).is_none());
    }

    #[test]
    fn the_assembler_writes_at_the_index_offset() {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine as _;

        let dir = std::env::temp_dir().join(format!(
            "cw-receive-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let file_id = Uuid::new_v4();
        let device = Uuid::new_v4();
        let mut assembler = LazyAssembler::new(dir.clone());

        // Two short chunks of a two-chunk file whose nominal size is one full
        // chunk plus four bytes. Writing them out of order is the point: the
        // offset must come from `chunk_index`, not from arrival order.
        let file_size = FILE_CHUNK_BYTES as u64 + 4;
        let mut second = chunk(Uuid::new_v4(), file_id, device, 1, 2);
        second.file_size = file_size;
        second.payload_b64 = STANDARD.encode(b"TAIL");
        assembler.write(&second).unwrap();

        let mut first = chunk(Uuid::new_v4(), file_id, device, 0, 2);
        first.file_size = file_size;
        first.payload_b64 = STANDARD.encode(b"HEAD");
        assembler.write(&first).unwrap();

        let path = assembler.take(&file_id).expect("an open partial file");
        let written = std::fs::read(&path).unwrap();
        assert_eq!(
            written.len() as u64,
            file_size,
            "the size was pre-allocated"
        );
        assert_eq!(&written[..4], b"HEAD", "index 0 lands at offset 0");
        assert_eq!(
            &written[FILE_CHUNK_BYTES..FILE_CHUNK_BYTES + 4],
            b"TAIL",
            "index 1 lands at FILE_CHUNK_BYTES"
        );

        // After `take` the file is no longer managed; discard must not panic.
        assembler.discard(&file_id);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_assembler_overwrites_in_place_on_a_restart() {
        // "Overwriting bytes already written in place": a second open of the
        // same file_id truncates rather than appending to the old run.
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine as _;

        let dir = std::env::temp_dir().join(format!(
            "cw-receive-restart-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let file_id = Uuid::new_v4();
        let device = Uuid::new_v4();
        let mut assembler = LazyAssembler::new(dir.clone());

        let mut stale = chunk(Uuid::new_v4(), file_id, device, 0, 1);
        stale.file_size = 4;
        stale.payload_b64 = STANDARD.encode(b"OLD!");
        assembler.write(&stale).unwrap();

        // A restart discards, then writes the same file_id again.
        assembler.discard(&file_id);
        let mut fresh = chunk(Uuid::new_v4(), file_id, device, 0, 1);
        fresh.file_size = 4;
        fresh.payload_b64 = STANDARD.encode(b"NEW!");
        assembler.write(&fresh).unwrap();

        let path = assembler.take(&file_id).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"NEW!");

        let _ = std::fs::remove_dir_all(&dir);
    }
}

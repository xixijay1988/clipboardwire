// SPDX-License-Identifier: GPL-3.0-or-later

//! Serve `file_pull` requests from peers.
//!
//! The sender side of a lazy transfer: a receiver asks for a window of a file
//! this client advertised, and this module reads it off disk and pushes
//! `file_chunk` frames back.
//!
//! # Why the identity is re-checked every window
//!
//! A single check at offer time is not enough. A file edited mid-pull yields a
//! stream whose first half is old and second half is new, and the running hash
//! covers exactly that torn stream — so the receiver's verification would
//! **pass** on corrupt output. Re-checking before each window (and again before
//! the digest) turns that into a visible abort.
//!
//! # Who the chunks are addressed to
//!
//! [`PullServer::handle_pull`] takes the puller's device as an argument rather
//! than reading it from the frame. `FilePullFrame::to` names the **source**, so
//! it cannot double as the destination — a chunk addressed to it would be
//! routed straight back to us. See `handle_pull`'s note for the wiring
//! consequence.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::protocol::{
    FileAbortFrame, FileChunkFrame, FileDigestFrame, FilePullFrame, Frame, FILE_CHUNK_BYTES,
    MAX_PULL_WINDOW,
};

use super::offer::{Offer, OfferRegistry};

/// How many transfers may actually be reading at once.
///
/// A limit exists because each active transfer holds an open file handle and
/// allocates a 4 MiB read buffer. Four is the design's number.
pub const MAX_ACTIVE_PULLS: usize = 4;

/// One in-flight pull, keyed by `transfer_id`.
struct PullSession {
    file_id: Uuid,
    /// Device that asked for the file. Chunks are addressed to it.
    to: Uuid,
    /// Bytes served so far, for the running hash.
    hasher: Sha256,
    /// Highest `chunk_index` fed into `hasher`. Guards against a re-request
    /// (retransmit) corrupting the digest by hashing a chunk twice.
    highest_hashed: Option<u32>,
    /// When the last chunk was served OR the last pull arrived. The idle
    /// timer resets on either — a queued stream is still alive.
    last_activity: i64,
    total_chunks: u32,
    /// The offer's batch, so pulls from one copy/paste share the concurrency
    /// budget instead of competing for it.
    batch_id: Uuid,
    /// Whether this session holds one of the `MAX_ACTIVE_PULLS` slots. A
    /// queued session does not, so ending it must not decrement the count.
    admitted: bool,
    /// The digests already produced, keyed by `(chunk_index, window)` of the
    /// pull that asked for them.
    ///
    /// **The session outlives its digest on purpose.** A `file_digest` can be
    /// dropped — the receiver's `frames` buffer is bounded and the hub warns
    /// and drops when it is full. The receiver's `DigestWait` recovery is to
    /// re-request the final window, and that works only if the source still
    /// has the session: otherwise `handle_pull` sees an unknown
    /// `transfer_id` with `chunk_index != 0`, answers `invalid_start`, and the
    /// receiver discards a fully-transferred file and starts over.
    ///
    /// Keeping the session costs one `Sha256` state and a small bitmap, and
    /// `sweep_idle` reclaims it after `pull_idle_timeout_secs` — three times
    /// `DIGEST_WAIT_MS`, so the retry window is never cut short.
    digests: Vec<(u32, u32, String)>,
    /// Set once the whole file has been hashed and a digest produced. No more
    /// chunks are served after this; only a repeat digest is.
    complete: bool,
}

/// What `handle_pull` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullOutcome {
    /// Chunks were queued (or the request is queued behind its batch).
    Serving,
    /// Refused with this reason; a `file_abort` must be sent to the puller.
    Refused(&'static str),
}

/// Everything one `file_pull` produced.
///
/// `trailer` is what makes "chunks, then one more frame" expressible without a
/// second return value: on a completed file it is the `file_digest`, and when
/// the file changed under us after the chunks were read it is the
/// `file_abort{source_changed}` that tells the receiver to start over.
pub struct PullResponse {
    pub chunks: Vec<FileChunkFrame>,
    pub trailer: Option<Frame>,
    pub outcome: PullOutcome,
}

impl PullResponse {
    fn refused(reason: &'static str) -> Self {
        Self {
            chunks: Vec::new(),
            trailer: None,
            outcome: PullOutcome::Refused(reason),
        }
    }
}

pub struct PullServer {
    sessions: HashMap<Uuid, PullSession>,
    /// Number of sessions holding a reading slot (`admitted == true`).
    active: usize,
    max_active: usize,
    idle_timeout_ms: i64,
}

impl PullServer {
    pub fn new(max_active: usize, idle_timeout_ms: i64) -> Self {
        Self {
            sessions: HashMap::new(),
            active: 0,
            max_active,
            idle_timeout_ms,
        }
    }

    /// Handle one inbound `file_pull`.
    ///
    /// The puller's device comes from `pull.from_device`, which the hub stamps
    /// on relay. It is the address every chunk and every session-scoped abort
    /// goes to, so a pull without one cannot be served at all — see the
    /// validation order below.
    ///
    /// Returns chunks rather than sending them, so the caller owns the
    /// transport and the tests need no socket.
    ///
    /// # Validation order is load-bearing
    ///
    /// 1. window bounds — cheapest, and a nonsense window says nothing about
    ///    whether the offer exists. Also the most specific protocol error, so
    ///    it outranks the missing-identity case below.
    /// 2. sender identity — without it every chunk would be unaddressable, so
    ///    nothing after this point could succeed. Still after (1): a puller
    ///    that got the window wrong should hear that regardless.
    /// 3. offer lookup — an unknown `file_id` is reported before anything reads
    ///    a byte;
    /// 4. session-opening index — must be 0, checked **before any disk I/O**,
    ///    or a peer could make us read by asking for an impossible start;
    /// 5. index bounds — needs `total_chunks`, which needs the offer;
    /// 6. identity — the first stat;
    /// 7. concurrency;
    /// 8. read.
    ///
    /// Steps 1 and 3 are swapped relative to a "most specific first" instinct
    /// on purpose: `window_too_large` is a property of the request alone, and a
    /// puller that got the window wrong should hear about that even if it also
    /// named a dead offer.
    pub fn handle_pull(
        &mut self,
        registry: &OfferRegistry,
        pull: &FilePullFrame,
        now_ms: i64,
    ) -> PullResponse {
        // Cheap, and this is the only place sessions are created.
        self.sweep_idle(now_ms);

        if pull.window == 0 || pull.window > MAX_PULL_WINDOW {
            return PullResponse::refused("window_too_large");
        }

        // Addressability, before any work that would need an address. The hub
        // stamps this on relay, so `None` means the pull never went through a
        // hub — or a future frame shape dropped the field. Either way there is
        // no device to send to, and guessing one would leak the file.
        let Some(puller) = pull.from_device else {
            return PullResponse::refused("invalid_start");
        };

        let Some(offer) = registry.get(&pull.file_id) else {
            return PullResponse::refused("unknown_offer");
        };

        let total_chunks = total_chunks_for(offer.frame.file_size);
        let known = self.sessions.contains_key(&pull.transfer_id);

        if let Some(existing) = self.sessions.get(&pull.transfer_id) {
            // `transfer_id` is the sole session key (spec §Transfer sessions),
            // so one id cannot name two files. A pull that reuses an open id
            // for a different `file_id` is either a bug or an attempt to mix
            // two streams into one digest; either way it must not be served.
            if existing.file_id != pull.file_id {
                return PullResponse::refused("invalid_start");
            }
        } else if pull.chunk_index != 0 {
            // The digest covers the whole file, so a session that opened
            // mid-file could never be verified. Refuse before touching disk.
            return PullResponse::refused("invalid_start");
        }
        if pull.chunk_index >= total_chunks {
            return PullResponse::refused("invalid_start");
        }

        if !offer.identity.still_matches(&offer.path) {
            // The file this offer promised is gone or changed. End the session
            // too: continuing it would hash a mixture of both versions.
            self.end_session(&pull.transfer_id);
            return PullResponse::refused("source_changed");
        }

        if !known {
            let batch_has_slot = self
                .sessions
                .values()
                .any(|s| s.admitted && s.batch_id == offer.frame.batch_id);
            if self.active >= self.max_active && !batch_has_slot {
                // A different copy/paste competes for the budget; sibling files
                // of the same paste do not (design §5.4). Refusing a five-file
                // paste at file three would fail a routine action.
                return PullResponse::refused("busy");
            }
            let admitted = self.active < self.max_active;
            if admitted {
                self.active += 1;
            }
            self.sessions.insert(
                pull.transfer_id,
                PullSession {
                    file_id: pull.file_id,
                    to: puller,
                    hasher: Sha256::new(),
                    highest_hashed: None,
                    last_activity: now_ms,
                    total_chunks,
                    batch_id: offer.frame.batch_id,
                    admitted,
                    digests: Vec::new(),
                    complete: false,
                },
            );
        }

        let session = self
            .sessions
            .get_mut(&pull.transfer_id)
            .expect("inserted above when unknown");

        session.last_activity = now_ms;
        // A session keeps the address it opened with. Two devices cannot share
        // a `transfer_id` unless one of them guessed it, and honouring the
        // second would let it redirect another peer's transfer.
        let to = session.to;

        if session.complete {
            // The file is already fully served. Re-sending bytes would waste
            // bandwidth and the receiver would drop them (it has them all);
            // what a completed session owes is the digest, again.
            //
            // Identity is re-checked because the file may have changed since:
            // the digest describes bytes the receiver already holds, and
            // claiming it for a file that no longer matches would let the
            // receiver accept a stale file. `source_changed` makes it start
            // over, which is correct — the offer's bytes are gone.
            if !offer.identity.still_matches(&offer.path) {
                self.end_session(&pull.transfer_id);
                return PullResponse::refused("source_changed");
            }
            let sha256 = match session
                .digests
                .iter()
                .find(|(index, window, _)| *index == pull.chunk_index && *window == pull.window)
            {
                Some((_, _, cached)) => cached.clone(),
                None => {
                    let sha = fingerprint_hex(&session.hasher.clone().finalize());
                    session
                        .digests
                        .push((pull.chunk_index, pull.window, sha.clone()));
                    sha
                }
            };
            return PullResponse {
                chunks: Vec::new(),
                trailer: Some(Frame::FileDigest(FileDigestFrame {
                    file_id: pull.file_id,
                    transfer_id: pull.transfer_id,
                    to,
                    from_device: None,
                    file_sha256: sha256,
                })),
                outcome: PullOutcome::Serving,
            };
        }

        let chunks = match build_chunks(offer, pull, session) {
            Ok(chunks) => chunks,
            Err(reason) => {
                self.end_session(&pull.transfer_id);
                return PullResponse::refused(reason);
            }
        };
        let served = chunks.len() as u32;
        let reached_end = pull.chunk_index.saturating_add(served) >= total_chunks;

        if !reached_end {
            return PullResponse {
                chunks,
                trailer: None,
                outcome: PullOutcome::Serving,
            };
        }

        // Last window. Re-check identity *again*: everything above was checked
        // before the bytes were read, and a file replaced in between would make
        // the digest describe a stream the receiver never got.
        if !offer.identity.still_matches(&offer.path) {
            self.end_session(&pull.transfer_id);
            return PullResponse {
                chunks,
                trailer: Some(Frame::FileAbort(FileAbortFrame {
                    file_id: pull.file_id,
                    transfer_id: Some(pull.transfer_id),
                    to: Some(to),
                    from_device: None,
                    reason: "source_changed".to_string(),
                })),
                outcome: PullOutcome::Serving,
            };
        }

        // Only claim a digest for a complete hash. A window that skipped ahead
        // leaves a hole in the hasher; emitting a digest then would assert a
        // value for bytes nobody hashed. The receiver's own gap detection ends
        // the session, which is the recovery path the spec describes.
        let complete = session.highest_hashed == Some(total_chunks - 1);
        if !complete {
            return PullResponse {
                chunks,
                trailer: None,
                outcome: PullOutcome::Serving,
            };
        }

        // The file is fully hashed; from here on the session exists only to
        // answer "please tell me the digest again".
        let sha256 = if let Some((_, _, cached)) = session
            .digests
            .iter()
            .find(|(index, window, _)| *index == pull.chunk_index && *window == pull.window)
        {
            cached.clone()
        } else {
            let sha = fingerprint_hex(&session.hasher.clone().finalize());
            session
                .digests
                .push((pull.chunk_index, pull.window, sha.clone()));
            sha
        };
        session.complete = true;
        // Deliberately **not** `end_session`: see `PullSession::digests`. A
        // dropped digest is recoverable only while this session is still here,
        // and `sweep_idle` reclaims it long after `DIGEST_WAIT_MS`.
        session.last_activity = now_ms;

        PullResponse {
            chunks,
            trailer: Some(Frame::FileDigest(FileDigestFrame {
                file_id: pull.file_id,
                transfer_id: pull.transfer_id,
                to,
                from_device: None,
                file_sha256: sha256,
            })),
            outcome: PullOutcome::Serving,
        }
    }

    /// Drop sessions that have been silent past the timeout.
    pub fn sweep_idle(&mut self, now_ms: i64) -> usize {
        let cutoff = now_ms.saturating_sub(self.idle_timeout_ms);
        let doomed: Vec<Uuid> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.last_activity <= cutoff)
            .map(|(transfer_id, _)| *transfer_id)
            .collect();
        for transfer_id in &doomed {
            self.end_session(transfer_id);
        }
        doomed.len()
    }

    /// Number of sessions currently holding a reading slot. Test-only view of
    /// a counter that would otherwise be invisible.
    #[cfg(test)]
    fn active(&self) -> usize {
        self.active
    }

    #[cfg(test)]
    fn session_count(&self) -> usize {
        self.sessions.len()
    }

    fn end_session(&mut self, transfer_id: &Uuid) {
        if let Some(session) = self.sessions.remove(transfer_id) {
            if session.admitted {
                self.active = self.active.saturating_sub(1);
            }
        }
    }
}

/// Read one window and fold it into the running hash.
///
/// Reads only `[chunk_index, chunk_index + window)` — never the whole file,
/// which is the difference between a pull server and an eager sender.
fn build_chunks(
    offer: &Offer,
    pull: &FilePullFrame,
    session: &mut PullSession,
) -> Result<Vec<FileChunkFrame>, &'static str> {
    let mut file = std::fs::File::open(&offer.path).map_err(|_| "source_gone")?;

    let total_chunks = session.total_chunks;
    let last_exclusive = pull
        .chunk_index
        .saturating_add(pull.window)
        .min(total_chunks);
    let mut chunk = vec![0u8; FILE_CHUNK_BYTES];
    let mut out = Vec::with_capacity((last_exclusive - pull.chunk_index) as usize);

    for index in pull.chunk_index..last_exclusive {
        let offset = u64::from(index) * FILE_CHUNK_BYTES as u64;
        let expected = chunk_len(offer.frame.file_size, index, total_chunks);
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| "source_gone")?;
        // `read` may return short even on a local file; loop so a chunk is
        // never silently truncated.
        let mut filled = 0usize;
        while filled < expected {
            let n = file
                .read(&mut chunk[filled..expected])
                .map_err(|_| "source_gone")?;
            if n == 0 {
                // The file shrank under us. The identity check already ran,
                // so this is a genuinely unexpected EOF.
                return Err("source_changed");
            }
            filled += n;
        }

        // Feed the hasher in index order, and only once per index. A
        // retransmitted window must not hash the same bytes twice, or the
        // digest can never match and the receiver restarts forever.
        let already = session
            .highest_hashed
            .is_some_and(|highest| index <= highest);
        if !already {
            session.hasher.update(&chunk[..filled]);
            session.highest_hashed = Some(index);
        }

        out.push(FileChunkFrame {
            file_id: pull.file_id,
            name: offer.frame.name.clone(),
            content_type: offer.frame.content_type.clone(),
            chunk_index: index,
            total_chunks,
            // Lazy chunks must not carry the whole-file hash: the protocol
            // rejects a lazy chunk that has one, and the digest frame is
            // where it belongs.
            file_sha256: None,
            file_size: offer.frame.file_size,
            payload_b64: {
                use base64::engine::general_purpose::STANDARD;
                use base64::Engine as _;
                STANDARD.encode(&chunk[..filled])
            },
            from: None,
            to: Some(session.to),
            transfer_id: Some(pull.transfer_id),
            from_device: None,
        });
    }

    Ok(out)
}

/// `chunk_index` is 0-based, so `total = ceil(size / CHUNK)`.
fn total_chunks_for(file_size: u64) -> u32 {
    let chunk = FILE_CHUNK_BYTES as u64;
    let total = file_size.div_ceil(chunk);
    // An offer of a zero-byte file never exists (`offer.rs` skips them), but a
    // `total_chunks == 0` would make the last index underflow.
    u32::try_from(total.max(1)).unwrap_or(u32::MAX)
}

/// Byte length of one chunk, shorter for the last.
fn chunk_len(file_size: u64, index: u32, total_chunks: u32) -> usize {
    let chunk = FILE_CHUNK_BYTES as u64;
    if index + 1 == total_chunks {
        let consumed = chunk * u64::from(index);
        file_size.saturating_sub(consumed) as usize
    } else {
        FILE_CHUNK_BYTES
    }
}

/// Colon-separated uppercase hex, matching what the eager path puts on the
/// wire — a receiver compares the two formats with one parser.
fn fingerprint_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::offer::OfferRegistry;
    use std::fs;
    use std::path::{Path, PathBuf};

    const NOW: i64 = 1_700_000_000_000;
    const TTL: i64 = 30 * 60_000;
    const IDLE: i64 = 30_000;

    fn unique_dir(label: &str) -> PathBuf {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("cw-pull-{label}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A registry holding one advertised file of `size` bytes.
    fn registry_with(dir: &Path, name: &str, size: usize) -> (OfferRegistry, Uuid, PathBuf) {
        let path = dir.join(name);
        // Deterministic, position-dependent bytes: a chunk read from the wrong
        // offset then differs, not just a length.
        let body: Vec<u8> = (0..size)
            .map(|i| ((i.wrapping_mul(31)).wrapping_add(7) & 0xff) as u8)
            .collect();
        fs::write(&path, &body).unwrap();

        let mut registry = OfferRegistry::default();
        let outcome = registry.advertise(std::slice::from_ref(&path), Uuid::new_v4(), NOW, TTL);
        assert_eq!(outcome.offers.len(), 1, "fixture must advertise one file");
        (registry, outcome.offers[0].file_id, path)
    }

    /// A pull as the *source* sees it: the hub has already stamped the puller
    /// into `from_device`.
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

    /// A pull for a file nobody offered, still carrying a sender.
    fn pull_without_sender(
        file_id: Uuid,
        source: Uuid,
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
            from_device: None,
        }
    }

    fn sha256_of(path: &Path) -> String {
        let bytes = fs::read(path).unwrap();
        fingerprint_hex(&Sha256::digest(&bytes))
    }

    /// Two chunks plus a short tail, so window arithmetic is exercised.
    fn multi_chunk_size() -> usize {
        FILE_CHUNK_BYTES * 2 + 1000
    }

    #[test]
    fn a_first_pull_must_start_at_index_zero() {
        let dir = unique_dir("start");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        let response = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, Uuid::new_v4(), 1, 4),
            NOW,
        );
        assert_eq!(response.outcome, PullOutcome::Refused("invalid_start"));
        assert!(response.chunks.is_empty(), "a refusal reads nothing");
        assert!(response.trailer.is_none());
        assert_eq!(
            server.session_count(),
            0,
            "a refused pull creates no session"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_later_window_may_start_anywhere() {
        let dir = unique_dir("later");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        let first = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, 1),
            NOW,
        );
        assert_eq!(first.outcome, PullOutcome::Serving);
        assert_eq!(first.chunks.len(), 1);

        let second = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 2, 1),
            NOW + 100,
        );
        assert_eq!(second.outcome, PullOutcome::Serving);
        assert_eq!(second.chunks.len(), 1);
        assert_eq!(second.chunks[0].chunk_index, 2);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_pull_without_a_sender_is_refused() {
        // `from_device` is the address every chunk goes to. Without it the
        // reply has no destination, and guessing one would deliver the file to
        // an arbitrary device — so nothing is read and nothing is served.
        let dir = unique_dir("no-sender");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let response = server.handle_pull(
            &registry,
            &pull_without_sender(file_id, Uuid::new_v4(), Uuid::new_v4(), 0, 4),
            NOW,
        );
        assert_eq!(response.outcome, PullOutcome::Refused("invalid_start"));
        assert!(response.chunks.is_empty());
        assert!(response.trailer.is_none());
        assert_eq!(
            server.session_count(),
            0,
            "no session for an unaddressable pull"
        );

        // The window check still outranks it: a nonsense window is a clearer
        // complaint than a missing field, and reporting order is observable.
        let worse = server.handle_pull(
            &registry,
            &pull_without_sender(file_id, Uuid::new_v4(), Uuid::new_v4(), 0, 0),
            NOW,
        );
        assert_eq!(worse.outcome, PullOutcome::Refused("window_too_large"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_oversized_window_is_refused() {
        let dir = unique_dir("big-window");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", 100);
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let response = server.handle_pull(
            &registry,
            &pull_of(
                file_id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                MAX_PULL_WINDOW + 1,
            ),
            NOW,
        );
        assert_eq!(response.outcome, PullOutcome::Refused("window_too_large"));
        assert!(response.chunks.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_zero_window_is_refused() {
        let dir = unique_dir("zero-window");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", 100);
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let response = server.handle_pull(
            &registry,
            &pull_of(
                file_id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                0,
            ),
            NOW,
        );
        assert_eq!(response.outcome, PullOutcome::Refused("window_too_large"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_pull_for_an_unknown_file_is_refused() {
        let dir = unique_dir("unknown");
        let (registry, _file_id, _path) = registry_with(&dir, "a.bin", 100);
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let response = server.handle_pull(
            &registry,
            &pull_of(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                4,
            ),
            NOW,
        );
        assert_eq!(response.outcome, PullOutcome::Refused("unknown_offer"));
        assert!(response.chunks.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_window_is_checked_before_the_offer_lookup() {
        // Ordering, stated as a test: a puller with a nonsense window and a
        // dead file id must hear about the window. The two answers differ, so
        // this pins which one wins.
        let dir = unique_dir("order");
        let (registry, _file_id, _path) = registry_with(&dir, "a.bin", 100);
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let response = server.handle_pull(
            &registry,
            &pull_of(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                999,
            ),
            NOW,
        );
        assert_eq!(response.outcome, PullOutcome::Refused("window_too_large"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_index_past_the_end_is_refused() {
        let dir = unique_dir("past-end");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", FILE_CHUNK_BYTES + 10);
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        // Open the session legitimately, then ask past the end.
        let _ = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, 1),
            NOW,
        );
        let response = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 2, 1),
            NOW + 100,
        );
        assert_eq!(response.outcome, PullOutcome::Refused("invalid_start"));
        assert!(response.chunks.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_last_window_emits_a_digest() {
        let dir = unique_dir("digest");
        let (registry, file_id, path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        // window = MAX so one pull covers the whole three-chunk file.
        let response = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, MAX_PULL_WINDOW),
            NOW,
        );
        assert_eq!(response.outcome, PullOutcome::Serving);
        assert_eq!(response.chunks.len(), 3);

        let Some(Frame::FileDigest(digest)) = response.trailer else {
            panic!("expected a digest trailer, got {:?}", response.trailer);
        };
        assert_eq!(digest.file_id, file_id);
        assert_eq!(digest.transfer_id, transfer_id);
        assert_eq!(digest.to, puller);
        assert_eq!(digest.from_device, None, "the hub stamps it");
        assert_eq!(
            digest.file_sha256,
            sha256_of(&path),
            "the running hash must equal the file's real digest"
        );
        // The session **stays** after the digest: a dropped digest is only
        // recoverable while the source still remembers the transfer, and
        // `sweep_idle` reclaims it long after the receiver would have given up.
        assert_eq!(
            server.session_count(),
            1,
            "a completed session is kept so the digest can be re-sent"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn re_requesting_the_last_window_after_the_digest_returns_the_digest_again() {
        // The receiver's `DigestWait` recovery re-requests the final window
        // because a `file_digest` can be dropped: the hub's `frames` buffer is
        // bounded and it warns-and-drops when full (see
        // `deliver_frame_to_device`). That recovery only works if the source
        // still has the session — otherwise the pull looks like a brand-new
        // session opening at a non-zero index, is refused as `invalid_start`,
        // and the receiver restarts a file it had already received in full.
        let dir = unique_dir("digest-replay");
        let (registry, file_id, path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        // A full transfer: one window covers all three chunks.
        let first = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, MAX_PULL_WINDOW),
            NOW,
        );
        assert_eq!(first.chunks.len(), 3);
        let Some(Frame::FileDigest(first_digest)) = first.trailer else {
            panic!("the first pull must complete with a digest");
        };
        assert_eq!(first_digest.file_sha256, sha256_of(&path));

        // The digest was lost. Ask for the same final window again.
        let replay = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 2, MAX_PULL_WINDOW),
            NOW + 1_000,
        );
        assert_eq!(
            replay.outcome,
            PullOutcome::Serving,
            "a completed session must still answer; `invalid_start` here would \
             make the receiver re-download the whole file"
        );
        assert!(
            replay.chunks.is_empty(),
            "the receiver already has every chunk; re-sending them wastes bandwidth"
        );
        let Some(Frame::FileDigest(again)) = replay.trailer else {
            panic!("the replay must produce the digest again");
        };
        assert_eq!(again.transfer_id, transfer_id);
        assert_eq!(again.to, puller);
        assert_eq!(
            again.file_sha256, first_digest.file_sha256,
            "the same bytes must hash the same way"
        );
        assert_eq!(again.file_sha256, sha256_of(&path));

        // And the session is still there for a third attempt, until idle.
        assert_eq!(server.session_count(), 1);
        assert_eq!(
            server.sweep_idle(NOW + 1_000 + IDLE),
            1,
            "idle sweeping is what finally reclaims it"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_repeated_digest_request_is_refused_if_the_file_changed_since() {
        // The digest describes bytes the receiver already holds. Claiming it
        // for a path that no longer matches would let that receiver accept a
        // stale file, so the replay path re-checks identity too.
        let dir = unique_dir("digest-replay-changed");
        let (registry, file_id, path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        let first = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, MAX_PULL_WINDOW),
            NOW,
        );
        assert!(matches!(first.trailer, Some(Frame::FileDigest(_))));

        // Same length, different bytes: only the identity check can catch it.
        let len = fs::metadata(&path).unwrap().len();
        let replacement: Vec<u8> = (0..len)
            .map(|i| ((i.wrapping_mul(13)).wrapping_add(5) & 0xff) as u8)
            .collect();
        fs::write(&path, &replacement).unwrap();
        let bumped = std::time::SystemTime::now() + std::time::Duration::from_secs(5);
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(bumped)
            .unwrap();

        let replay = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 2, MAX_PULL_WINDOW),
            NOW + 1_000,
        );
        assert_eq!(replay.outcome, PullOutcome::Refused("source_changed"));
        assert_eq!(server.session_count(), 0, "and the session is dropped");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_mid_file_window_emits_no_digest() {
        let dir = unique_dir("mid");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let response = server.handle_pull(
            &registry,
            &pull_of(
                file_id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                1,
            ),
            NOW,
        );
        assert_eq!(response.chunks.len(), 1);
        assert!(
            response.trailer.is_none(),
            "a partial window claims nothing about the whole file"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn chunks_carry_to_and_transfer_id() {
        let dir = unique_dir("shape");
        let (registry, file_id, _path) = registry_with(&dir, "report.pdf", FILE_CHUNK_BYTES + 10);
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        let response = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, 4),
            NOW,
        );
        assert_eq!(response.chunks.len(), 2);
        for (index, chunk) in response.chunks.iter().enumerate() {
            assert_eq!(chunk.file_id, file_id);
            // The destination is the *puller*, never `pull.to` (the source) —
            // the latter would route the chunk back to this same client.
            assert_eq!(chunk.to, Some(puller));
            assert_ne!(chunk.to, Some(source));
            assert_eq!(chunk.transfer_id, Some(transfer_id));
            assert_eq!(
                chunk.file_sha256, None,
                "a lazy chunk carrying the hash is rejected by the protocol"
            );
            assert_eq!(chunk.chunk_index, index as u32);
            assert_eq!(chunk.total_chunks, 2);
            assert_eq!(chunk.name, "report.pdf");
            assert_eq!(chunk.content_type, "application/pdf");
            assert_eq!(chunk.file_size, (FILE_CHUNK_BYTES + 10) as u64);
            chunk
                .validate()
                .expect("every emitted chunk must pass the protocol's own check");
        }
        // The tail is short, the first chunk is full.
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine as _;
        assert_eq!(
            STANDARD
                .decode(&response.chunks[0].payload_b64)
                .unwrap()
                .len(),
            FILE_CHUNK_BYTES
        );
        assert_eq!(
            STANDARD
                .decode(&response.chunks[1].payload_b64)
                .unwrap()
                .len(),
            10
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_file_changed_before_a_window_is_refused() {
        let dir = unique_dir("changed");
        let (registry, file_id, path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        let first = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, 1),
            NOW,
        );
        assert_eq!(first.outcome, PullOutcome::Serving);

        // **Same length**, different bytes, and an mtime forced forward.
        //
        // A different length would also be caught by the read running off the
        // end, so the test would pass even with the identity check removed —
        // which is exactly what a mutation run showed. Only a same-size
        // rewrite isolates the check this test is named after.
        let original_len = fs::metadata(&path).unwrap().len();
        let replacement: Vec<u8> = (0..original_len)
            .map(|i| ((i.wrapping_mul(17)).wrapping_add(3) & 0xff) as u8)
            .collect();
        fs::write(&path, &replacement).unwrap();
        let bumped = std::time::SystemTime::now() + std::time::Duration::from_secs(5);
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_modified(bumped).unwrap();
        drop(file);
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            original_len,
            "the rewrite must not change the length"
        );

        let second = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 1, 1),
            NOW + 100,
        );
        assert_eq!(second.outcome, PullOutcome::Refused("source_changed"));
        assert!(
            second.chunks.is_empty(),
            "no bytes of the new version leak out"
        );
        assert_eq!(
            server.session_count(),
            0,
            "the session is ended, not resumed"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_re_requested_chunk_does_not_corrupt_the_digest() {
        // The retransmit case: a window is served twice, then the rest is
        // pulled. If the hasher absorbed the repeat, the digest would cover
        // duplicated bytes and never match — the receiver would restart until
        // it gave up.
        let dir = unique_dir("replay");
        let (registry, file_id, path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        let transfer_id = Uuid::new_v4();
        let puller = Uuid::new_v4();
        let source = Uuid::new_v4();

        server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, 2),
            NOW,
        );
        // Same window again: a lost reply the receiver asked for.
        let replay = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 0, 2),
            NOW + 100,
        );
        assert_eq!(replay.outcome, PullOutcome::Serving);
        assert_eq!(replay.chunks.len(), 2, "a replay still re-sends the bytes");

        let tail = server.handle_pull(
            &registry,
            &pull_of(file_id, source, puller, transfer_id, 2, 2),
            NOW + 200,
        );
        let Some(Frame::FileDigest(digest)) = tail.trailer else {
            panic!("expected a digest after the tail, got {:?}", tail.trailer);
        };
        assert_eq!(
            digest.file_sha256,
            sha256_of(&path),
            "the re-sent window must not be hashed twice"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn idle_sessions_are_swept() {
        let dir = unique_dir("idle");
        let (registry, file_id, _path) = registry_with(&dir, "a.bin", multi_chunk_size());
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let _ = server.handle_pull(
            &registry,
            &pull_of(
                file_id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                1,
            ),
            NOW,
        );
        assert_eq!(server.session_count(), 1);
        assert_eq!(server.active(), 1);

        assert_eq!(server.sweep_idle(NOW + IDLE - 1), 0, "not idle yet");
        assert_eq!(server.sweep_idle(NOW + IDLE), 1, "idle at the boundary");
        assert_eq!(server.session_count(), 0);
        assert_eq!(server.active(), 0, "the reading slot is released");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn different_batches_compete_for_the_limit() {
        let dir = unique_dir("limit");
        let mut registry = OfferRegistry::default();
        let mut file_ids = Vec::new();
        for i in 0..=MAX_ACTIVE_PULLS {
            let path = dir.join(format!("f{i}.bin"));
            // Multi-chunk: a one-chunk file completes on its first pull and
            // releases the slot, which would make this test vacuous.
            fs::write(&path, vec![b'x'; multi_chunk_size()]).unwrap();
            // A distinct batch per file, so each one must win a slot.
            let outcome = registry.advertise(std::slice::from_ref(&path), Uuid::new_v4(), NOW, TTL);
            file_ids.push(outcome.offers[0].file_id);
        }

        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        for file_id in file_ids.iter().take(MAX_ACTIVE_PULLS) {
            let response = server.handle_pull(
                &registry,
                &pull_of(
                    *file_id,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    0,
                    1,
                ),
                NOW,
            );
            assert_eq!(response.outcome, PullOutcome::Serving);
        }
        let refused = server.handle_pull(
            &registry,
            &pull_of(
                file_ids[MAX_ACTIVE_PULLS],
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                1,
            ),
            NOW,
        );
        assert_eq!(refused.outcome, PullOutcome::Refused("busy"));
        assert_eq!(server.active(), MAX_ACTIVE_PULLS, "a refusal takes no slot");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn one_batch_queues_instead_of_refusing() {
        // A five-file paste must not fail at file three (design §5.4).
        let dir = unique_dir("batch");
        let mut registry = OfferRegistry::default();
        let batch = Uuid::new_v4();
        let mut file_ids = Vec::new();
        for i in 0..=MAX_ACTIVE_PULLS {
            let path = dir.join(format!("f{i}.bin"));
            // Multi-chunk for the same reason as the batch test below: one
            // pull must not complete the transfer.
            fs::write(&path, vec![b'x'; multi_chunk_size()]).unwrap();
            let outcome = registry.advertise(std::slice::from_ref(&path), batch, NOW, TTL);
            file_ids.push(outcome.offers[0].file_id);
        }

        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);
        for file_id in &file_ids {
            let response = server.handle_pull(
                &registry,
                &pull_of(
                    *file_id,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    0,
                    1,
                ),
                NOW,
            );
            assert_eq!(
                response.outcome,
                PullOutcome::Serving,
                "siblings of one paste queue rather than fail"
            );
        }
        assert_eq!(
            server.active(),
            MAX_ACTIVE_PULLS,
            "only the admitted ones hold slots"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_single_chunk_file_emits_its_digest_immediately() {
        let dir = unique_dir("tiny");
        let (registry, file_id, path) = registry_with(&dir, "a.bin", 3);
        let mut server = PullServer::new(MAX_ACTIVE_PULLS, IDLE);

        let response = server.handle_pull(
            &registry,
            &pull_of(
                file_id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                0,
                1,
            ),
            NOW,
        );
        assert_eq!(response.chunks.len(), 1);
        assert_eq!(response.chunks[0].total_chunks, 1);
        let Some(Frame::FileDigest(digest)) = response.trailer else {
            panic!("expected a digest");
        };
        assert_eq!(digest.file_sha256, sha256_of(&path));

        let _ = fs::remove_dir_all(&dir);
    }
}

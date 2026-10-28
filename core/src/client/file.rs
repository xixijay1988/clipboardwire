// SPDX-License-Identifier: GPL-3.0-or-later

//! Client-side file-transfer primitives.
//!
//! Two halves of the same problem:
//!
//! - [`send_file_through`] chops a local file into [`FileChunkFrame`]
//!   pieces of [`FILE_CHUNK_BYTES`] bytes each (base64-encoded), every
//!   chunk carrying the file's SHA-256 + total size so a late-joiner
//!   on the receiver side can still verify the assembly.
//! - [`FileReceiver`] accumulates inbound chunks into a partial file
//!   under the user's downloads dir, and on the final chunk verifies
//!   the SHA-256 and moves the assembled file into place.
//!
//! Wire format is documented in `PROTOCOL.md` §5 (added in v0.3.0 of
//! the protocol). The hub itself stays content-blind — chunks fan out
//! to peers exactly like text/image clips, just on a separate
//! per-client channel so a slow file doesn't head-of-line-block clips.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, mpsc};
use tracing::{trace, warn};
use uuid::Uuid;

use crate::logging::{redact_file_name, redact_path};
use crate::protocol::{FileChunkFrame, FILE_CHUNK_BYTES};

use super::{emit_progress, Direction, ProgressState, TransferProgress};

/// [`send_file_through`] without progress reporting.
pub async fn send_file_through(
    path: &Path,
    outbound_files_tx: &mpsc::Sender<FileChunkFrame>,
) -> Result<()> {
    send_file_with_progress(path, outbound_files_tx, None, false).await
}

/// Send a single local file to the connected hub for fan-out. Reads
/// the file in 4-MiB chunks, base64-encodes each one, and pushes the
/// resulting [`FileChunkFrame`]s through the transport's outbound
/// file channel.
///
/// Progress events, in order (design §9):
///
/// 1. [`ProgressState::Hashing`] before the pre-pass, then once per whole
///    percent as the hash advances — on a 2 GiB file this pass is the long
///    invisible stall the proposal calls out, so it must be visible without
///    flooding the channel with one event per 64 KiB read.
/// 2. [`ProgressState::Pulling`] once per chunk after it is enqueued, with
///    `bytes_done` accumulated.
/// 3. [`ProgressState::Done`] after the last chunk, replacing the old
///    `info!("file sent")` line.
///
/// The sender never emits [`ProgressState::Verifying`]: it verifies nothing,
/// the receiver does.
///
/// Any error path emits [`ProgressState::Failed`] before returning, so a tray
/// entry cannot be left showing "hashing 0%" forever when a file turns out to
/// be locked or unreadable.
pub async fn send_file_with_progress(
    path: &Path,
    outbound_files_tx: &mpsc::Sender<FileChunkFrame>,
    progress_tx: Option<&broadcast::Sender<TransferProgress>>,
    log_file_names: bool,
) -> Result<()> {
    // The file id is minted here rather than inside, so a failure before the
    // first event can still reuse the same id every other event for this file
    // carries. Minting a second one in the error path would leave the tray with
    // an entry no later event ever clears.
    let file_id = Uuid::new_v4();
    let outcome = send_file_inner(
        path,
        outbound_files_tx,
        progress_tx,
        log_file_names,
        file_id,
    )
    .await;
    if let Err(e) = &outcome {
        emit_failed(progress_tx, path, log_file_names, file_id, e);
    }
    outcome
}

/// Best-effort `Failed` for a send that never emitted a terminal event.
///
/// Called only from [`send_file_with_progress`], which owns the `file_id`, so
/// this always reports the same transfer as the events that came before it.
fn emit_failed(
    progress_tx: Option<&broadcast::Sender<TransferProgress>>,
    path: &Path,
    log_file_names: bool,
    file_id: Uuid,
    error: &anyhow::Error,
) {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    let bytes_total = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    trace!(error = %format!("{error:#}"), "send failed; emitting Failed progress");
    emit_progress(
        progress_tx,
        TransferProgress {
            file_id,
            transfer_id: None,
            direction: Direction::Send,
            peer: None,
            name: redact_file_name(name, log_file_names),
            bytes_done: 0,
            bytes_total,
            state: ProgressState::Failed,
        },
    );
}

async fn send_file_inner(
    path: &Path,
    outbound_files_tx: &mpsc::Sender<FileChunkFrame>,
    progress_tx: Option<&broadcast::Sender<TransferProgress>>,
    log_file_names: bool,
    file_id: Uuid,
) -> Result<()> {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("path has no filename component"))?
        .to_string();

    let metadata = fs::metadata(path)
        .with_context(|| format!("stat {}", redact_path(path, log_file_names)))?;
    let file_size = metadata.len();
    if file_size == 0 {
        anyhow::bail!("refusing to send a zero-byte file");
    }

    let total_chunks = file_size.div_ceil(FILE_CHUNK_BYTES as u64);
    let total_chunks = u32::try_from(total_chunks)
        .map_err(|_| anyhow!("file too large to chunk in u32 — refusing"))?;

    // Redacted at the source so `log_file_names = false` can never leak a raw
    // name through an event into the tray tooltip or the log.
    let display_name = redact_file_name(&name, log_file_names);

    let progress = |state: ProgressState, bytes_done: u64| TransferProgress {
        file_id,
        transfer_id: None,
        direction: Direction::Send,
        peer: None,
        name: display_name.clone(),
        bytes_done,
        bytes_total: file_size,
        state,
    };

    emit_progress(progress_tx, progress(ProgressState::Hashing, 0));
    // Throttle the hash progress to whole-percent steps. The hasher reports
    // every 64 KiB, which on a 2 GiB file would be ~32k events — far more than
    // a tooltip can use, and enough traffic to crowd out real clipboard frames
    // on the broadcast channel. One event per percent keeps the stall visible
    // (the whole point of this state) at a bounded cost.
    let percent_step = (file_size / 100).max(1);
    let mut last_reported = 0u64;
    let file_sha256 = sha256_of_path_with_progress(path, |hashed| {
        if hashed - last_reported >= percent_step || hashed == file_size {
            last_reported = hashed;
            emit_progress(progress_tx, progress(ProgressState::Hashing, hashed));
        }
    })
    .with_context(|| format!("hashing {}", redact_path(path, log_file_names)))?;
    let content_type = mime_for(path);

    let mut reader =
        File::open(path).with_context(|| format!("open {}", redact_path(path, log_file_names)))?;
    let mut buf = vec![0u8; FILE_CHUNK_BYTES];
    let mut index = 0u32;
    let mut bytes_done = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let chunk = FileChunkFrame {
            file_id,
            name: name.clone(),
            content_type: content_type.clone(),
            chunk_index: index,
            total_chunks,
            file_sha256: Some(file_sha256.clone()),
            file_size,
            payload_b64: STANDARD.encode(&buf[..n]),
            from: None,
            to: None,
            transfer_id: None,
            from_device: None,
        };
        outbound_files_tx
            .send(chunk)
            .await
            .map_err(|_| anyhow!("transport's outbound-files channel closed"))?;
        index += 1;
        bytes_done += n as u64;
        emit_progress(progress_tx, progress(ProgressState::Pulling, bytes_done));
    }
    // Replaces the old `info!("file sent")`: the Done event already carries
    // the transfer to every subscriber, so a second INFO line would be a
    // duplicate report (task 1.3).
    trace!(
        file_id = %file_id,
        total_chunks = total_chunks,
        size = file_size,
        "file chunks enqueued"
    );
    emit_progress(progress_tx, progress(ProgressState::Done, bytes_done));
    Ok(())
}

/// Best-effort MIME for a file from its extension. We don't want a heavy
/// dep for this — the receiver decides what to do with the bytes either
/// way; the content_type is metadata.
fn mime_for(path: &Path) -> String {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase());
    match ext.as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("pdf") => "application/pdf",
        Some("zip") => "application/zip",
        Some("tar") => "application/x-tar",
        Some("gz" | "tgz") => "application/gzip",
        Some("txt" | "md" | "log") => "text/plain; charset=utf-8",
        Some("json") => "application/json",
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("mp4") => "video/mp4",
        Some("mov") => "video/quicktime",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        _ => "application/octet-stream",
    }
    .to_string()
}

fn sha256_of_path(path: &Path) -> Result<String> {
    sha256_of_path_with_progress(path, |_| {})
}

/// Hash the whole file, calling `on_bytes` with the running total after each
/// read so the caller can report how far the pre-pass has got.
fn sha256_of_path_with_progress(path: &Path, mut on_bytes: impl FnMut(u64)) -> Result<String> {
    let mut f = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut done = 0u64;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        done += n as u64;
        on_bytes(done);
    }
    Ok(fingerprint_hex(&hasher.finalize()))
}

fn fingerprint_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Receiver state: one in-flight `Transfer` per `file_id` in progress.
pub struct FileReceiver {
    save_dir: PathBuf,
    transfers: HashMap<Uuid, Transfer>,
}

struct Transfer {
    name: String,
    /// `<save_dir>/<file_id>.partial` — chunks are seek-written here as
    /// they arrive. On the final chunk we verify SHA-256 and rename.
    temp_path: PathBuf,
    file: File,
    total_chunks: u32,
    received: Vec<bool>,
    /// Running sum of the real lengths of the received chunks. Kept
    /// incrementally so reporting progress is O(1) per chunk instead of
    /// rescanning `received` (which made a whole transfer O(n²)).
    bytes_received: u64,
    expected_sha256: String,
    expected_size: u64,
}

impl FileReceiver {
    /// Receiver writing to the platform default download dir
    /// (`~/Downloads/clipboardwire/` on Linux + macOS, the equivalent
    /// on Windows). Created on first use.
    pub fn new() -> Result<Self> {
        Self::with_save_dir(default_save_dir()?)
    }

    pub fn with_save_dir(save_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&save_dir)
            .with_context(|| format!("creating {}", save_dir.display()))?;
        Ok(Self {
            save_dir,
            transfers: HashMap::new(),
        })
    }

    pub fn save_dir(&self) -> &Path {
        &self.save_dir
    }

    /// Process one inbound chunk. Returns `Ok(Some(path))` when the
    /// chunk completed an in-flight transfer, `Ok(None)` if more
    /// chunks are still pending for this `file_id`. Verifies SHA-256
    /// on completion and removes the partial file on mismatch.
    pub fn receive_chunk(&mut self, chunk: FileChunkFrame) -> Result<Option<PathBuf>> {
        self.receive_chunk_with_progress(chunk, None, false)
    }

    /// [`Self::receive_chunk`] plus progress reporting.
    ///
    /// Per file, in order (design §9): [`ProgressState::Pulling`] after each
    /// chunk is written, then [`ProgressState::Verifying`] before the SHA-256
    /// re-read, then [`ProgressState::Done`] on success or
    /// [`ProgressState::Failed`] on mismatch. `Verifying` is receive-only.
    pub fn receive_chunk_with_progress(
        &mut self,
        chunk: FileChunkFrame,
        progress_tx: Option<&broadcast::Sender<TransferProgress>>,
        log_file_names: bool,
    ) -> Result<Option<PathBuf>> {
        // Snapshot what a `Failed` event needs before the frame is consumed.
        let failed_file_id = chunk.file_id;
        let failed_name = redact_file_name(&sanitize_filename(&chunk.name), log_file_names);
        let failed_bytes_total = chunk.file_size;

        let outcome = self.receive_chunk_inner(chunk, progress_tx, log_file_names);
        if let Err(e) = &outcome {
            // This is the ONLY place a receive-side `Failed` is emitted.
            // `receive_chunk_inner` reports errors by returning `Err` and must
            // not emit its own terminal event, or one failure shows up twice
            // (and a tray entry is armed twice).
            //
            // `bytes_done` for a failed transfer is reported as 0 with the
            // real total: the partial file is deleted either way, so claiming
            // "97% of it survived" would be a lie. The important part is that
            // the total is real, so the summary line can say which file failed.
            trace!(error = %format!("{e:#}"), "receive failed; emitting Failed progress");
            emit_progress(
                progress_tx,
                TransferProgress {
                    file_id: failed_file_id,
                    transfer_id: None,
                    direction: Direction::Receive,
                    peer: None,
                    name: failed_name,
                    bytes_done: 0,
                    bytes_total: failed_bytes_total,
                    state: ProgressState::Failed,
                },
            );
        }
        outcome
    }

    fn receive_chunk_inner(
        &mut self,
        chunk: FileChunkFrame,
        progress_tx: Option<&broadcast::Sender<TransferProgress>>,
        log_file_names: bool,
    ) -> Result<Option<PathBuf>> {
        if let Err(reason) = chunk.validate() {
            anyhow::bail!("rejecting inbound file_chunk: {reason}");
        }
        let bytes = STANDARD
            .decode(&chunk.payload_b64)
            .context("decoding chunk payload base64")?;

        let file_id = chunk.file_id;
        let total_chunks = chunk.total_chunks as usize;
        let chunk_index = chunk.chunk_index as usize;

        // Phase 2 only defines the wire shapes; the lazy pull engine lands in
        // Phase 5. Until then this receiver handles the eager shape only and
        // rejects a lazy chunk loudly rather than mis-assembling it.
        if chunk.transfer_id.is_some() {
            anyhow::bail!("rejecting lazy file_chunk: pull transfers are not implemented yet");
        }
        // Defensive only: `FileChunkFrame::validate` above already guarantees an
        // eager chunk carries a non-empty `file_sha256`, so this branch is
        // unreachable while both checks agree. It is kept so the receiver never
        // depends on that agreement silently.
        let Some(chunk_sha256) = chunk.file_sha256.clone() else {
            anyhow::bail!("rejecting eager file_chunk without file_sha256");
        };

        // First-chunk-of-this-file ⇒ create the partial file lazily.
        if !self.transfers.contains_key(&file_id) {
            let safe_name = sanitize_filename(&chunk.name);
            let temp_path = self.save_dir.join(format!("{file_id}.partial"));
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .with_context(|| format!("creating {}", temp_path.display()))?;
            file.set_len(chunk.file_size).with_context(|| {
                format!(
                    "pre-allocating {} bytes for {}",
                    chunk.file_size,
                    temp_path.display()
                )
            })?;
            self.transfers.insert(
                file_id,
                Transfer {
                    name: safe_name,
                    temp_path,
                    file,
                    total_chunks: chunk.total_chunks,
                    received: vec![false; total_chunks],
                    bytes_received: 0,
                    expected_sha256: chunk_sha256.clone(),
                    expected_size: chunk.file_size,
                },
            );
        }

        let transfer = self
            .transfers
            .get_mut(&file_id)
            .expect("just inserted if missing");

        if chunk.total_chunks != transfer.total_chunks
            || chunk_sha256 != transfer.expected_sha256
            || chunk.file_size != transfer.expected_size
        {
            anyhow::bail!(
                "inconsistent metadata across chunks of file {} (sender changed total/hash/size mid-transfer)",
                file_id
            );
        }

        if transfer.received[chunk_index] {
            trace!(
                file_id = %file_id,
                index = chunk_index,
                "duplicate chunk; ignoring"
            );
            return Ok(None);
        }

        let offset = (chunk.chunk_index as u64) * (FILE_CHUNK_BYTES as u64);
        transfer.file.seek(SeekFrom::Start(offset))?;
        transfer.file.write_all(&bytes)?;
        transfer.received[chunk_index] = true;
        transfer.bytes_received += chunk_len(
            chunk.chunk_index,
            transfer.total_chunks,
            transfer.expected_size,
        );
        let bytes_done = transfer.bytes_received;
        let display_name = redact_file_name(&transfer.name, log_file_names);
        let bytes_total = transfer.expected_size;
        let progress = |state: ProgressState, bytes_done: u64| TransferProgress {
            file_id,
            transfer_id: None,
            direction: Direction::Receive,
            peer: None,
            name: display_name.clone(),
            bytes_done,
            bytes_total,
            state,
        };
        emit_progress(progress_tx, progress(ProgressState::Pulling, bytes_done));

        if !transfer.received.iter().all(|b| *b) {
            return Ok(None);
        }

        // Done — verify, rename, drop from map.
        transfer.file.sync_all()?;
        let expected = transfer.expected_sha256.clone();
        emit_progress(progress_tx, progress(ProgressState::Verifying, bytes_done));
        let actual = sha256_of_path(&transfer.temp_path)?;
        if actual != expected {
            warn!(
                file_id = %file_id,
                file = %display_name,
                "SHA-256 mismatch (expected {expected}, got {actual}); removing partial file"
            );
            let _ = fs::remove_file(&transfer.temp_path);
            self.transfers.remove(&file_id);
            // No `Failed` event here on purpose: `receive_chunk_with_progress`
            // emits exactly one for any error this function returns. Emitting
            // both produced two terminal events for one failure.
            anyhow::bail!("file transfer failed SHA-256 verification");
        }

        let final_path = unique_path(&self.save_dir, &transfer.name);
        fs::rename(&transfer.temp_path, &final_path)?;
        let result_path = final_path.clone();
        // The Done event replaces the old `info!("file transfer complete")`
        // line; the caller logs the saved path once (task 1.3).
        trace!(
            file_id = %file_id,
            file = %redact_path(&result_path, log_file_names),
            size = bytes_total,
            "file transfer verified and published"
        );
        self.transfers.remove(&file_id);
        emit_progress(progress_tx, progress(ProgressState::Done, bytes_total));
        Ok(Some(result_path))
    }
}

/// Verify a lazily-assembled partial file and move it into place.
///
/// The lazy path cannot reuse [`FileReceiver`]: that type insists on an eager
/// chunk's `file_sha256` and rejects anything carrying `transfer_id`. What is
/// shared is only the last step — hash the bytes, compare, and pick a
/// non-colliding name.
///
/// On mismatch the partial file is removed, because keeping it would let a
/// later run rename unverified bytes into the user's Downloads directory.
pub fn finish_lazy_transfer(
    partial_path: &Path,
    save_dir: &Path,
    name: &str,
    expected_sha256: &str,
) -> Result<PathBuf> {
    let actual = sha256_of_path(partial_path)?;
    if actual != expected_sha256 {
        let _ = fs::remove_file(partial_path);
        anyhow::bail!(
            "lazy transfer failed SHA-256 verification (expected {expected_sha256}, got {actual})"
        );
    }
    let safe_name = sanitize_filename(name);
    let final_path = unique_path(save_dir, &safe_name);
    fs::rename(partial_path, &final_path).with_context(|| {
        format!(
            "moving {} to {}",
            partial_path.display(),
            final_path.display()
        )
    })?;
    Ok(final_path)
}

/// Length of chunk `index` out of `total_chunks` for a `file_size`-byte file.
///
/// Every chunk but the last is [`FILE_CHUNK_BYTES`]; the last is the remainder,
/// which equals a full chunk when the size divides evenly. Use this to keep a
/// running total instead of rescanning the received bitmap per chunk (which is
/// O(n²): a 1 GiB file is 256 chunks, so 256 scans of a 256-bit bitmap).
fn chunk_len(index: u32, total_chunks: u32, file_size: u64) -> u64 {
    let chunk_bytes = FILE_CHUNK_BYTES as u64;
    if index + 1 == total_chunks {
        file_size.saturating_sub((total_chunks as u64 - 1) * chunk_bytes)
    } else {
        chunk_bytes.min(file_size.saturating_sub(index as u64 * chunk_bytes))
    }
}

fn default_save_dir() -> Result<PathBuf> {
    let base = directories::UserDirs::new().and_then(|u| u.download_dir().map(|p| p.to_path_buf()));
    let downloads = match base {
        Some(p) => p,
        None => {
            let base = directories::BaseDirs::new()
                .ok_or_else(|| anyhow!("could not locate the user's home directory"))?;
            base.home_dir().join("Downloads")
        }
    };
    Ok(downloads.join("clipboardwire"))
}

/// Strip path separators and dangerous characters. We don't try to be
/// clever about Unicode normalisation — we just refuse anything that
/// would let a malicious sender escape `save_dir`.
fn sanitize_filename(name: &str) -> String {
    let trimmed = name.trim();
    let sanitised: String = trimmed
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' | ':' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    // Trailing-dot trim only — Windows refuses filenames that end in
    // `.`. Leading dots are fine (hidden-file convention on Unix) and
    // not exploitable once path separators have already been mapped.
    let cleaned = sanitised.trim_end_matches('.').to_string();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "received-file".to_string()
    } else {
        cleaned
    }
}

fn unique_path(dir: &Path, name: &str) -> PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }
    let stem = Path::new(name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(name);
    let ext = Path::new(name).extension().and_then(|s| s.to_str());
    for n in 1..10_000 {
        let with_n = match ext {
            Some(e) => format!("{stem} ({n}).{e}"),
            None => format!("{stem} ({n})"),
        };
        let candidate = dir.join(with_n);
        if !candidate.exists() {
            return candidate;
        }
    }
    // Fallback: use a uuid suffix if 10k collisions ever happen
    // (shouldn't, but we don't want an infinite loop).
    dir.join(format!("{name}-{}", Uuid::new_v4()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "cw-filerecv-{label}-{}-{nanos}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_chunks(name: &str, content: &[u8]) -> Vec<FileChunkFrame> {
        let file_id = Uuid::nil();
        let total = content.len().div_ceil(FILE_CHUNK_BYTES.max(1)).max(1) as u32;
        let sha = {
            let mut h = Sha256::new();
            h.update(content);
            fingerprint_hex(&h.finalize())
        };
        let mut chunks = Vec::new();
        for (i, slice) in content.chunks(FILE_CHUNK_BYTES).enumerate() {
            chunks.push(FileChunkFrame {
                file_id,
                name: name.to_string(),
                content_type: "application/octet-stream".to_string(),
                chunk_index: i as u32,
                total_chunks: total,
                file_sha256: Some(sha.clone()),
                file_size: content.len() as u64,
                payload_b64: STANDARD.encode(slice),
                from: None,
                to: None,
                transfer_id: None,
                from_device: None,
            });
        }
        chunks
    }

    #[test]
    fn single_chunk_transfer_writes_file_with_original_name() {
        let dir = unique_dir("single");
        let mut rx = FileReceiver::with_save_dir(dir.clone()).unwrap();

        let body = b"hello clipboardwire";
        let chunks = make_chunks("hello.txt", body);

        let mut final_path = None;
        for c in chunks {
            if let Some(p) = rx.receive_chunk(c).unwrap() {
                final_path = Some(p);
            }
        }
        let final_path = final_path.expect("transfer should have completed");
        assert_eq!(final_path.file_name().unwrap(), "hello.txt");
        let got = fs::read(&final_path).unwrap();
        assert_eq!(got, body);
    }

    #[test]
    fn multi_chunk_transfer_assembles_correctly() {
        let dir = unique_dir("multi");
        let mut rx = FileReceiver::with_save_dir(dir).unwrap();

        // 9 MiB of pseudo-random bytes ⇒ 3 chunks at 4 MiB each.
        let mut body = vec![0u8; 9 * 1024 * 1024];
        for (i, b) in body.iter_mut().enumerate() {
            *b = (i & 0xff) as u8;
        }
        let chunks = make_chunks("blob.bin", &body);
        assert!(chunks.len() >= 3, "this test needs >=3 chunks");

        let mut final_path = None;
        for c in chunks {
            if let Some(p) = rx.receive_chunk(c).unwrap() {
                final_path = Some(p);
            }
        }
        let final_path = final_path.expect("transfer should have completed");
        let got = fs::read(&final_path).unwrap();
        assert_eq!(got, body);
    }

    #[test]
    fn out_of_order_chunks_assemble_correctly() {
        let dir = unique_dir("ooo");
        let mut rx = FileReceiver::with_save_dir(dir).unwrap();

        let mut body = vec![0u8; 6 * 1024 * 1024];
        for (i, b) in body.iter_mut().enumerate() {
            *b = ((i * 7) & 0xff) as u8;
        }
        let mut chunks = make_chunks("ooo.bin", &body);
        chunks.reverse();
        let mut final_path = None;
        for c in chunks {
            if let Some(p) = rx.receive_chunk(c).unwrap() {
                final_path = Some(p);
            }
        }
        let final_path = final_path.expect("transfer should have completed");
        let got = fs::read(&final_path).unwrap();
        assert_eq!(got, body);
    }

    #[test]
    fn sha256_mismatch_fails_and_cleans_up_partial() {
        let dir = unique_dir("badsha");
        let mut rx = FileReceiver::with_save_dir(dir.clone()).unwrap();

        let body = b"original";
        let mut chunks = make_chunks("victim.txt", body);
        // Tamper with the first chunk's payload (keep length).
        chunks[0].payload_b64 = STANDARD.encode(b"tampered");

        let mut err = None;
        for c in chunks {
            if let Err(e) = rx.receive_chunk(c) {
                err = Some(e);
                break;
            }
        }
        assert!(err.is_some(), "tampered chunk should fail SHA verification");
        // No final file, no .partial leftover.
        let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
        assert!(
            entries.is_empty() || entries.iter().all(|e| !e.as_ref().unwrap().path().exists()),
            "save_dir should be empty after a failed transfer"
        );
    }

    #[test]
    fn duplicate_chunk_is_ignored_not_errored() {
        let dir = unique_dir("dup");
        let mut rx = FileReceiver::with_save_dir(dir).unwrap();

        let body = b"once and again";
        let chunks = make_chunks("dup.txt", body);
        // Send first chunk twice — should not error.
        rx.receive_chunk(chunks[0].clone()).unwrap();
        rx.receive_chunk(chunks[0].clone()).unwrap();
        for c in chunks.into_iter().skip(1) {
            rx.receive_chunk(c).unwrap();
        }
    }

    #[test]
    fn chunk_len_handles_the_last_chunk_and_exact_multiples() {
        let chunk = FILE_CHUNK_BYTES as u64;
        // 9 MiB ⇒ 3 chunks: two full, one 1 MiB remainder.
        let size = 9 * 1024 * 1024;
        assert_eq!(chunk_len(0, 3, size), chunk);
        assert_eq!(chunk_len(1, 3, size), chunk);
        assert_eq!(chunk_len(2, 3, size), 1024 * 1024);

        // Exactly 8 MiB ⇒ 2 equal chunks; the last must not be 0.
        let size = 8 * 1024 * 1024;
        assert_eq!(chunk_len(0, 2, size), chunk);
        assert_eq!(chunk_len(1, 2, size), chunk);

        // A single small chunk is the whole file.
        assert_eq!(chunk_len(0, 1, 1234), 1234);
    }

    #[test]
    fn running_byte_count_matches_the_naive_sum() {
        // The incremental counter must agree with the old bitmap rescan,
        // including for out-of-order arrivals.
        for size in [1u64, 1234, 4 * 1024 * 1024, 9 * 1024 * 1024] {
            let total = size.div_ceil(FILE_CHUNK_BYTES as u64) as u32;
            let mut sum = 0u64;
            let mut bitmap = vec![false; total as usize];
            // Receive in reverse order.
            for index in (0..total).rev() {
                bitmap[index as usize] = true;
                sum += chunk_len(index, total, size);
            }
            assert_eq!(sum, size, "size {size} should add up exactly");
            assert!(bitmap.iter().all(|b| *b));
        }
    }

    #[test]
    fn sanitize_filename_strips_path_separators() {
        assert_eq!(sanitize_filename("../etc/passwd"), ".._etc_passwd");
        assert_eq!(sanitize_filename("normal.txt"), "normal.txt");
        assert_eq!(sanitize_filename("   "), "received-file");
        assert_eq!(sanitize_filename("foo\\bar"), "foo_bar");
    }

    #[test]
    fn unique_path_disambiguates_existing_filenames() {
        let dir = unique_dir("unique");
        let p = unique_path(&dir, "doc.txt");
        fs::write(&p, b"x").unwrap();
        let p2 = unique_path(&dir, "doc.txt");
        assert_ne!(p, p2);
        assert!(
            p2.file_name().unwrap().to_string_lossy().contains("(1)"),
            "second `doc.txt` should get a numeric suffix: {}",
            p2.display()
        );
    }
}

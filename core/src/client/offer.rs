// SPDX-License-Identifier: GPL-3.0-or-later

//! Offer registry: what this client has advertised and how to tell whether a
//! path still refers to the same bytes.
//!
//! The registry is per-process and in-memory: an offer is a promise about a
//! file that exists *now*, and a hub restart loses the hub's copy anyway.
//! Persisting it would only make a stale promise survive longer than the hub's
//! record of it.
//!
//! Publishing costs one `stat` per file, not a read: the whole point of the
//! lazy path is that copying a 2 GiB file is O(1) in bytes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::protocol::FileOfferFrame;

/// Identity pinned at offer time.
///
/// Compared on every served window, not just once: a file modified mid-pull
/// would otherwise produce a stream whose first half is old and second half is
/// new, and the running hash covers exactly that torn stream — so verification
/// would *pass* on corrupt output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedIdentity {
    /// Platform file identity: `(volume, index)` on Windows, `(dev, ino)` on
    /// Unix. `None` where the platform cannot supply one.
    pub file_key: Option<(u64, u64)>,
    pub size: u64,
    /// Modification time in nanoseconds since the epoch where available.
    pub mtime_ns: i128,
}

impl PinnedIdentity {
    /// Stat `path` and pin what can be pinned.
    pub fn of(path: &Path) -> std::io::Result<Self> {
        let meta = std::fs::metadata(path)?;
        Ok(Self {
            file_key: file_key(path),
            size: meta.len(),
            mtime_ns: mtime_ns(&meta),
        })
    }

    /// Whether `path` still has this identity.
    ///
    /// Deliberately re-stats: a cached answer would be a statement about the
    /// past, and the question is about right now.
    pub fn still_matches(&self, path: &Path) -> bool {
        // A file that vanished does not match. Not an error: the caller treats
        // "gone" and "changed" the same way (abort the offer).
        match Self::of(path) {
            Ok(current) => &current == self,
            Err(_) => false,
        }
    }
}

/// The platform's stable file identity, as `(volume, index)`.
///
/// Takes the **path**, not a `Metadata`, because the most useful identity on
/// Windows is not reachable from `Metadata` at all: see the Windows
/// implementation.
///
/// This is a second `open` on Windows, not a reuse of the `stat` that
/// `PinnedIdentity::of` already did. That is the price of a real identity
/// rather than a best-effort one, and it happens once per offered file, not
/// per served window (`still_matches` re-stats, and on Windows re-opens).
#[cfg(unix)]
fn file_key(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    // Stat again rather than plumbing the caller's `Metadata` through: on Unix
    // the extra stat is a no-op relative to the file-system identity check the
    // caller just performed, and one signature for all platforms beats two.
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.dev(), meta.ino()))
}

#[cfg(windows)]
fn file_key(path: &Path) -> Option<(u64, u64)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    // `std::os::windows::fs::MetadataExt::volume_serial_number` and
    // `::file_index` would be the obvious way to get these two numbers, and an
    // earlier version of this function used them. They are **not usable**: both
    // are gated behind the unstable `windows_by_handle` feature (rust issue
    // #63010), so referring to them fails to compile on stable — the note that
    // used to sit here claiming they were ordinary `Option`-returning methods
    // described an API that does not exist on any released toolchain.
    //
    // `GetFileInformationByHandle` is the stable equivalent. It needs a handle,
    // hence the signature change to take the path.
    let file = std::fs::File::open(path).ok()?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is a valid, writable, correctly-typed out-parameter owned
    // by this frame, and `handle` is a live file handle for the duration of the
    // call (`file` is not dropped until after it returns).
    let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    if ok == 0 {
        // Some filesystems (SMB shares without the right extensions, some
        // virtual filesystems) fail this call. `None` is the honest answer.
        return None;
    }
    let index = (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow);
    Some((u64::from(info.dwVolumeSerialNumber), index))
}

#[cfg(not(any(unix, windows)))]
fn file_key(_path: &Path) -> Option<(u64, u64)> {
    None
}

#[cfg(unix)]
fn mtime_ns(meta: &std::fs::Metadata) -> i128 {
    use std::os::unix::fs::MetadataExt;
    // `mtime` is seconds and `mtime_nsec` the sub-second part. Widened to i128
    // so a pre-1970 timestamp cannot overflow on the way through nanoseconds.
    i128::from(meta.mtime()) * 1_000_000_000 + i128::from(meta.mtime_nsec())
}

#[cfg(not(unix))]
fn mtime_ns(meta: &std::fs::Metadata) -> i128 {
    // Windows (and anything else) only exposes a `SystemTime` here. `duration_
    // since` yields `Err` for pre-1970 stamps; those become a large negative
    // value rather than panicking, and a file that old is still comparable
    // against its own earlier reading.
    // `as_nanos()` is `u128` and there is no `i128: From<u128>`: a span that
    // long is not representable, so it saturates. In practice this branch is
    // unreachable — a `SystemTime` more than 5×10^21 years from the epoch cannot
    // exist — but reporting a saturated value beats a `panic!` in a `stat`
    // helper that runs on every offered file.
    fn saturating_nanos(d: std::time::Duration) -> i128 {
        i128::try_from(d.as_nanos()).unwrap_or(i128::MAX)
    }
    match meta.modified() {
        Ok(time) => match time.duration_since(std::time::UNIX_EPOCH) {
            Ok(after) => saturating_nanos(after),
            Err(before) => -saturating_nanos(before.duration()),
        },
        Err(_) => 0,
    }
}

/// One advertised file.
pub struct Offer {
    pub frame: FileOfferFrame,
    /// Path to serve from. Never sent on the wire — a pull must not be able to
    /// make the source read an arbitrary path (spec: *the source SHALL serve
    /// only paths present in its own offer registry*).
    pub path: PathBuf,
    pub identity: PinnedIdentity,
}

/// Why one path in a copy did not become an offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    ZeroBytes,
    Directory,
    NotAFile,
    StatFailed(String),
}

/// What one `advertise` call decided to do.
pub struct AdvertiseOutcome {
    pub offers: Vec<FileOfferFrame>,
    /// `file_id`s whose path is no longer the same file. The caller aborts
    /// these so a peer does not pull a stale offer.
    pub superseded: Vec<Uuid>,
    /// Paths skipped, with the reason, so the caller can warn once each.
    pub skipped: Vec<(PathBuf, SkipReason)>,
}

#[derive(Default)]
pub struct OfferRegistry {
    offers: HashMap<Uuid, Offer>,
    by_path: HashMap<PathBuf, Uuid>,
}

impl OfferRegistry {
    /// Advertise `paths`, reusing a `file_id` when a path's identity is
    /// unchanged.
    ///
    /// Returns the offers to publish plus the `file_id`s that were superseded
    /// (the same path with a *different* identity) so the caller can abort
    /// them.
    pub fn advertise(
        &mut self,
        paths: &[PathBuf],
        batch_id: Uuid,
        now_ms: i64,
        ttl_ms: i64,
    ) -> AdvertiseOutcome {
        let mut outcome = AdvertiseOutcome {
            offers: Vec::new(),
            superseded: Vec::new(),
            skipped: Vec::new(),
        };
        let expires_at_ms = now_ms.saturating_add(ttl_ms);

        for path in paths {
            let meta = match std::fs::metadata(path) {
                Ok(meta) => meta,
                Err(e) => {
                    outcome
                        .skipped
                        .push((path.clone(), SkipReason::StatFailed(e.to_string())));
                    continue;
                }
            };
            if meta.is_dir() {
                outcome.skipped.push((path.clone(), SkipReason::Directory));
                continue;
            }
            if !meta.is_file() {
                outcome.skipped.push((path.clone(), SkipReason::NotAFile));
                continue;
            }
            if meta.len() == 0 {
                outcome.skipped.push((path.clone(), SkipReason::ZeroBytes));
                continue;
            }

            let identity = match PinnedIdentity::of(path) {
                Ok(identity) => identity,
                Err(e) => {
                    outcome
                        .skipped
                        .push((path.clone(), SkipReason::StatFailed(e.to_string())));
                    continue;
                }
            };

            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let file_size = identity.size;
            let content_type = mime_for(path);

            let existing = self.by_path.get(path).copied();

            if let Some(file_id) = existing {
                let unchanged = self
                    .offers
                    .get(&file_id)
                    .is_some_and(|offer| offer.identity == identity);

                if unchanged {
                    // Same bytes, same id: refresh the deadline but **keep the
                    // original `ts`**.
                    //
                    // `ts` is what the receiver consults when deciding whether
                    // to put this on its clipboard: an offer is applied only if
                    // its `file_id` is *new* and its `ts` is newer than the last
                    // applied one. Bumping `ts` here would make the frame look
                    // newer without being a new file, and the two rules would
                    // disagree about what "new" means. Refreshing the deadline
                    // is separate and necessary: re-copying a file should
                    // restart its 30 minutes.
                    let offer = self.offers.get_mut(&file_id).expect("checked above");
                    offer.frame.expires_at_ms = expires_at_ms;
                    offer.path = path.clone();
                    outcome.offers.push(offer.frame.clone());
                    continue;
                }

                // The path now names different bytes. The old id must be
                // aborted, or a peer that already received it will pull a file
                // whose identity check fails — better to tell it now.
                self.offers.remove(&file_id);
                self.by_path.remove(path);
                outcome.superseded.push(file_id);
            }

            let file_id = Uuid::new_v4();
            let frame = FileOfferFrame {
                file_id,
                batch_id,
                // Only ever set on creation. See the reuse branch above.
                ts: now_ms,
                name,
                content_type,
                file_size,
                expires_at_ms,
                from_device: None,
            };
            self.offers.insert(
                file_id,
                Offer {
                    frame: frame.clone(),
                    path: path.clone(),
                    identity,
                },
            );
            self.by_path.insert(path.clone(), file_id);
            outcome.offers.push(frame);
        }

        outcome
    }

    pub fn get(&self, file_id: &Uuid) -> Option<&Offer> {
        self.offers.get(file_id)
    }

    /// Drop expired offers. Called before advertising so the map cannot grow
    /// without bound on a long-running tray.
    pub fn sweep(&mut self, now_ms: i64) -> usize {
        let before = self.offers.len();
        self.offers
            .retain(|_, offer| offer.frame.expires_at_ms > now_ms);
        if self.offers.len() != before {
            // `by_path` is a reverse index; rebuild it from the survivors
            // rather than trying to work out which key went with which
            // removal. It is small (one entry per advertised file) and this
            // runs once per copy.
            self.by_path.clear();
            for (file_id, offer) in &self.offers {
                self.by_path.insert(offer.path.clone(), *file_id);
            }
        }
        before - self.offers.len()
    }
}

/// Best-effort MIME for a file from its extension.
///
/// Duplicated from `file.rs` (which keeps its copy private to the eager path)
/// rather than exposing it: both are three lines of table and the two paths are
/// free to disagree about a new extension without either being wrong.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn unique_dir(label: &str) -> PathBuf {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("cw-offer-{label}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    const NOW: i64 = 1_700_000_000_000;
    const TTL: i64 = 30 * 60_000;

    fn advertise_one(reg: &mut OfferRegistry, path: &Path, now: i64) -> AdvertiseOutcome {
        reg.advertise(&[path.to_path_buf()], Uuid::new_v4(), now, TTL)
    }

    #[test]
    fn a_new_file_is_advertised_with_the_configured_ttl() {
        let dir = unique_dir("new");
        let path = dir.join("report.pdf");
        fs::write(&path, b"hello").unwrap();

        let mut reg = OfferRegistry::default();
        let out = advertise_one(&mut reg, &path, NOW);

        assert_eq!(out.offers.len(), 1);
        assert!(out.superseded.is_empty());
        assert!(out.skipped.is_empty());
        let offer = &out.offers[0];
        assert_eq!(offer.expires_at_ms, NOW + TTL);
        assert_eq!(offer.ts, NOW);
        assert_eq!(offer.file_size, 5);
        assert_eq!(offer.name, "report.pdf");
        assert_eq!(offer.content_type, "application/pdf");
        assert_eq!(
            reg.get(&offer.file_id).map(|o| o.path.as_path()),
            Some(path.as_path())
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_zero_byte_file_is_skipped() {
        let dir = unique_dir("zero");
        let path = dir.join("empty.txt");
        fs::write(&path, b"").unwrap();

        let mut reg = OfferRegistry::default();
        let out = advertise_one(&mut reg, &path, NOW);
        assert!(out.offers.is_empty());
        assert_eq!(out.skipped.len(), 1);
        assert_eq!(out.skipped[0].1, SkipReason::ZeroBytes);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_directory_is_skipped() {
        let dir = unique_dir("dir");
        let sub = dir.join("subdir");
        fs::create_dir_all(&sub).unwrap();

        let mut reg = OfferRegistry::default();
        let out = advertise_one(&mut reg, &sub, NOW);
        assert!(out.offers.is_empty());
        assert_eq!(out.skipped[0].1, SkipReason::Directory);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_missing_file_is_skipped() {
        let dir = unique_dir("missing");
        let path = dir.join("not-there.bin");

        let mut reg = OfferRegistry::default();
        let out = advertise_one(&mut reg, &path, NOW);
        assert!(out.offers.is_empty());
        assert!(matches!(out.skipped[0].1, SkipReason::StatFailed(_)));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unchanged_path_reuses_its_file_id() {
        let dir = unique_dir("reuse");
        let path = dir.join("a.bin");
        fs::write(&path, b"stable").unwrap();

        let mut reg = OfferRegistry::default();
        let first = advertise_one(&mut reg, &path, NOW);
        let second = advertise_one(&mut reg, &path, NOW + 1_000);

        assert_eq!(first.offers[0].file_id, second.offers[0].file_id);
        assert!(
            second.superseded.is_empty(),
            "an unchanged file is not superseded"
        );
        // The map holds one entry, not two.
        assert!(reg.get(&first.offers[0].file_id).is_some());
        assert_eq!(reg.offers.len(), 1);
        assert_eq!(reg.by_path.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_reused_offer_keeps_its_original_ts() {
        // Load-bearing for the receiver: an offer is only put on the clipboard
        // if its `file_id` is new *and* its `ts` is newer than the last applied
        // one. Bumping `ts` on reuse would make the frame look like a fresh
        // copy without being one.
        let dir = unique_dir("ts");
        let path = dir.join("a.bin");
        fs::write(&path, b"stable").unwrap();

        let mut reg = OfferRegistry::default();
        let first = advertise_one(&mut reg, &path, NOW);
        let later = NOW + 60_000;
        let second = advertise_one(&mut reg, &path, later);

        assert_eq!(
            second.offers[0].ts, first.offers[0].ts,
            "reuse must not claim the file was copied again"
        );
        assert_eq!(second.offers[0].ts, NOW);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_reused_offer_refreshes_its_expiry() {
        // Re-copying a file should restart its window; otherwise a file
        // advertised 29 minutes ago would expire a minute after you re-copy it.
        let dir = unique_dir("expiry");
        let path = dir.join("a.bin");
        fs::write(&path, b"stable").unwrap();

        let mut reg = OfferRegistry::default();
        let first = advertise_one(&mut reg, &path, NOW);
        let later = NOW + 60_000;
        let second = advertise_one(&mut reg, &path, later);

        assert_eq!(first.offers[0].expires_at_ms, NOW + TTL);
        assert_eq!(second.offers[0].expires_at_ms, later + TTL);
        assert!(second.offers[0].expires_at_ms > first.offers[0].expires_at_ms);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_changed_file_supersedes_the_old_offer() {
        let dir = unique_dir("supersede");
        let path = dir.join("a.bin");
        fs::write(&path, b"first version").unwrap();

        let mut reg = OfferRegistry::default();
        let first = advertise_one(&mut reg, &path, NOW);
        let old_id = first.offers[0].file_id;

        // Rewrite with different content *and* a different length, so size
        // alone already differs — this keeps the test independent of mtime
        // granularity on fast filesystems.
        fs::write(&path, b"second version, longer").unwrap();
        let second = advertise_one(&mut reg, &path, NOW + 1_000);

        assert_eq!(second.superseded, vec![old_id]);
        let new_id = second.offers[0].file_id;
        assert_ne!(new_id, old_id);
        assert!(reg.get(&old_id).is_none(), "the stale entry must be gone");
        assert!(reg.get(&new_id).is_some());
        assert_eq!(reg.by_path.len(), 1, "the index must not keep both");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sweep_drops_expired_offers() {
        let dir = unique_dir("sweep");
        let path = dir.join("a.bin");
        fs::write(&path, b"x").unwrap();

        let mut reg = OfferRegistry::default();
        let out = advertise_one(&mut reg, &path, NOW);
        let file_id = out.offers[0].file_id;

        assert_eq!(reg.sweep(NOW), 0, "not dead yet");
        assert_eq!(reg.sweep(NOW + TTL), 1, "now == expiry is dead");
        assert!(reg.get(&file_id).is_none());
        // The reverse index must not outlive the offer, or a later advertise
        // would try to reuse a file_id that no longer exists.
        assert!(reg.by_path.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn advertising_again_after_a_sweep_mints_a_fresh_id() {
        // Guards the reverse-index rebuild in `sweep`: a stale `by_path` entry
        // would make `advertise` look up a `None` offer and skip the
        // supersede path, leaving an orphaned id.
        let dir = unique_dir("after-sweep");
        let path = dir.join("a.bin");
        fs::write(&path, b"x").unwrap();

        let mut reg = OfferRegistry::default();
        let first = advertise_one(&mut reg, &path, NOW);
        assert_eq!(reg.sweep(NOW + TTL), 1);

        let second = advertise_one(&mut reg, &path, NOW + TTL + 1);
        assert_ne!(second.offers[0].file_id, first.offers[0].file_id);
        assert!(second.superseded.is_empty(), "nothing live to supersede");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_detects_a_size_change() {
        let dir = unique_dir("id-size");
        let path = dir.join("a.bin");
        fs::write(&path, b"abc").unwrap();
        let identity = PinnedIdentity::of(&path).unwrap();
        assert!(identity.still_matches(&path));

        fs::write(&path, b"abcdef").unwrap();
        assert!(!identity.still_matches(&path));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_detects_an_mtime_change() {
        let dir = unique_dir("id-mtime");
        let path = dir.join("a.bin");
        fs::write(&path, b"abc").unwrap();
        let identity = PinnedIdentity::of(&path).unwrap();

        // Same length, same inode, different mtime: the case size alone cannot
        // catch. Rewrite in place and force the mtime forward, because a
        // filesystem may store whole seconds and the two writes could
        // otherwise land in the same tick.
        fs::write(&path, b"xyz").unwrap();
        let bumped = std::time::SystemTime::now() + std::time::Duration::from_secs(5);
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_modified(bumped).unwrap();
        drop(file);

        assert!(!identity.still_matches(&path));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_matches_an_untouched_file() {
        let dir = unique_dir("id-stable");
        let path = dir.join("a.bin");
        fs::write(&path, b"abc").unwrap();
        let identity = PinnedIdentity::of(&path).unwrap();

        assert!(identity.still_matches(&path));
        // Repeated calls must agree: `still_matches` re-stats but must not
        // accumulate state that changes its own answer.
        assert!(identity.still_matches(&path));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_vanished_file_no_longer_matches() {
        let dir = unique_dir("id-gone");
        let path = dir.join("a.bin");
        fs::write(&path, b"abc").unwrap();
        let identity = PinnedIdentity::of(&path).unwrap();
        fs::remove_file(&path).unwrap();

        assert!(!identity.still_matches(&path), "gone is not a match");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn one_batch_shares_its_batch_id() {
        let dir = unique_dir("batch");
        let a = dir.join("a.txt");
        let b = dir.join("b.txt");
        fs::write(&a, b"a").unwrap();
        fs::write(&b, b"b").unwrap();

        let batch = Uuid::new_v4();
        let mut reg = OfferRegistry::default();
        let out = reg.advertise(&[a, b], batch, NOW, TTL);
        assert_eq!(out.offers.len(), 2);
        assert!(out.offers.iter().all(|o| o.batch_id == batch));
        assert_ne!(out.offers[0].file_id, out.offers[1].file_id);

        let _ = fs::remove_dir_all(&dir);
    }
}

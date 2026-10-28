// SPDX-License-Identifier: GPL-3.0-or-later

//! Durable device identity.
//!
//! `device_id` names this machine to the hub: it survives reconnects and is
//! what `from_device` / `to` carry on the wire. It lives in the **state dir**,
//! never in `config.toml`, because the settings dialog rewrites the whole
//! config from its form fields — an id stored there would be replaced on every
//! save, and every device would look new.
//!
//! # Cloned machines
//!
//! The state dir gets copied: VM images, full-disk backups, "clone this box"
//! scripts. Two machines sharing one `device_id` evict each other in a loop,
//! because the hub's rule is newest-wins. A per-host fingerprint is stored
//! beside the id; a mismatch means "this id belongs to another machine", so a
//! fresh id is minted instead.
//!
//! The fingerprint is **not** a security control — it is a "have I been
//! cloned?" detector. Anyone who can write the file can forge it.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// File holding `<uuid>\n<fingerprint>\n`.
///
/// Two lines of plain text rather than JSON: there is nothing to nest, no
/// serde round-trip to keep aligned with a struct, and a human debugging a
/// cloned VM can `cat` it and see immediately that the id is someone else's.
pub const IDENTITY_FILE: &str = "device_id";

const TMP_SUFFIX: &str = ".tmp";

/// This machine's persistent identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceIdentity {
    pub device_id: Uuid,
}

/// Load the device identity from `state_dir`, creating it on first run.
///
/// - No file: mint a new id, persist it, return it.
/// - File with a matching fingerprint: return the stored id, **without
///   rewriting the file** — a start-up that touches it every time makes the
///   state dir look modified on every launch (and would fight a backup tool).
/// - File with a different fingerprint: warn and mint a new id (cloned state
///   dir). The old id is overwritten rather than kept: two machines holding
///   the same id would evict each other forever, so keeping it on this host
///   preserves exactly the failure the fingerprint exists to break.
/// - File that is unreadable or corrupt: warn and mint a new id. Losing an
///   identity is recoverable (offers simply look new); refusing to start is
///   not.
pub fn load_or_create(state_dir: &Path) -> Result<DeviceIdentity> {
    let path = state_dir.join(IDENTITY_FILE);
    let fingerprint = host_fingerprint();

    match read_stored(&path) {
        Ok(Some((stored_id, stored_fingerprint))) => {
            if stored_fingerprint == fingerprint {
                debug!(%stored_id, path = %path.display(), "device identity loaded");
                return Ok(DeviceIdentity {
                    device_id: stored_id,
                });
            }
            warn!(
                stored_id = %stored_id,
                path = %path.display(),
                "device identity belongs to another host (a cloned state dir?); \
                 minting a new one so two machines cannot evict each other"
            );
        }
        Ok(None) => {
            debug!(path = %path.display(), "no device identity yet; minting one");
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                path = %path.display(),
                "device identity file is unreadable; minting a new one"
            );
        }
    }

    let identity = DeviceIdentity {
        device_id: Uuid::new_v4(),
    };
    persist(state_dir, &path, identity.device_id, &fingerprint)?;
    info!(
        device = %identity.device_id,
        path = %path.display(),
        "device identity created"
    );
    Ok(identity)
}

/// Read and parse a stored identity.
///
/// `Ok(None)` means "no file". `Err` means "there is a file but it does not
/// make sense" — the caller treats both the same way, but keeping them apart
/// makes the log honest about which happened.
fn read_stored(path: &Path) -> Result<Option<(Uuid, String)>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", path.display()));
        }
    };

    let mut lines = raw.lines().map(str::trim);
    let id_line = lines.next().unwrap_or_default();
    let fingerprint_line = lines.next().unwrap_or_default();
    if id_line.is_empty() || fingerprint_line.is_empty() {
        anyhow::bail!("{} is missing a line", path.display());
    }
    // A trailing third line is ignored rather than rejected: a future format
    // may add fields, and a newer writer must not make this build churn ids.
    let device_id =
        Uuid::parse_str(id_line).with_context(|| format!("bad uuid in {}", path.display()))?;
    Ok(Some((device_id, fingerprint_line.to_string())))
}

/// Write the identity atomically: temp file in the same directory, then
/// `rename`.
///
/// Same directory matters — `rename` is only atomic within one filesystem, and
/// the state dir may well live on a different mount from `/tmp`.
fn persist(state_dir: &Path, path: &Path, device_id: Uuid, fingerprint: &str) -> Result<()> {
    fs::create_dir_all(state_dir)
        .with_context(|| format!("creating state dir {}", state_dir.display()))?;
    let tmp: PathBuf = path.with_file_name(format!("{IDENTITY_FILE}{TMP_SUFFIX}"));
    fs::write(&tmp, format!("{device_id}\n{fingerprint}\n"))
        .with_context(|| format!("writing {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))?;
    Ok(())
}

/// A stable-ish identifier for *this machine*.
///
/// Not cryptographic: the only failure it must catch is "the state dir was
/// copied to a different host". Hostname alone is weak (clones often share
/// it), so mix in machine-id where the platform provides one.
///
/// **Determinism is the whole point.** No time, no pid, no salt, no random
/// source: the same machine must produce the same bytes in every process,
/// forever. A fingerprint that drifts would mint a new `device_id` on every
/// start — strictly worse than not implementing this at all, because the
/// hub would see a brand-new device each launch.
///
/// Every component is labelled and absent components are skipped rather than
/// substituted, so "no machine-id" cannot collide with "machine-id literally
/// equal to the next label".
fn host_fingerprint() -> String {
    let mut canonical = String::new();
    canonical.push_str("os=");
    canonical.push_str(std::env::consts::OS);
    canonical.push('\n');

    canonical.push_str("arch=");
    canonical.push_str(std::env::consts::ARCH);
    canonical.push('\n');

    for (label, value) in [
        ("machine-id", machine_id()),
        ("computername", std::env::var("COMPUTERNAME").ok()),
        (
            "hostname",
            hostname::get().ok().and_then(|h| h.into_string().ok()),
        ),
    ] {
        if let Some(value) = value {
            let value = value.trim();
            if !value.is_empty() {
                canonical.push_str(label);
                canonical.push('=');
                canonical.push_str(value);
                canonical.push('\n');
            }
        }
    }

    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        // Writing to a String cannot fail.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// The platform's machine id, where one exists.
///
/// Linux has `/etc/machine-id` (and the older `/var/lib/dbus/machine-id`).
/// macOS has no plain file, and the closest thing (`IOPlatformUUID`) needs
/// `ioreg`; rather than shell out from a start-up path, this returns `None`
/// there and the fingerprint falls back to hostname — weaker against a clone
/// that also copies the hostname, which is the honest limit of a
/// non-cryptographic check.
fn machine_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
            if let Ok(raw) = fs::read_to_string(path) {
                let trimmed = raw.trim();
                if !trimmed.is_empty() {
                    return Some(format!("{path}:{trimmed}"));
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory no other test can land in.
    ///
    /// Counter rather than timestamp, for the reason `client::config` spells
    /// out: two tests can start inside the same clock tick, and a shared path
    /// then makes one of them read the other's file.
    fn unique_dir(label: &str) -> PathBuf {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("cw-identity-{label}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn first_run_creates_and_persists_an_identity() {
        let dir = unique_dir("first");
        let first = load_or_create(&dir).unwrap();
        assert!(dir.join(IDENTITY_FILE).exists(), "the id must be persisted");

        // The point of the file: a second start (a new process, in production)
        // gets the same id back.
        let second = load_or_create(&dir).unwrap();
        assert_eq!(first.device_id, second.device_id);
        assert!(!first.device_id.is_nil());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_matching_fingerprint_keeps_the_identity() {
        let dir = unique_dir("match");
        let first = load_or_create(&dir).unwrap();

        let raw = fs::read_to_string(dir.join(IDENTITY_FILE)).unwrap();
        assert_eq!(
            raw,
            format!("{}\n{}\n", first.device_id, host_fingerprint()),
            "the file is the id and this host's fingerprint, nothing else"
        );

        let second = load_or_create(&dir).unwrap();
        assert_eq!(second.device_id, first.device_id, "same host keeps its id");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_different_fingerprint_mints_a_new_identity() {
        let dir = unique_dir("clone");
        fs::create_dir_all(&dir).unwrap();
        let foreign = Uuid::new_v4();
        fs::write(
            dir.join(IDENTITY_FILE),
            format!("{foreign}\nnot-this-host\n"),
        )
        .unwrap();

        let identity = load_or_create(&dir).unwrap();
        assert_ne!(
            identity.device_id, foreign,
            "another machine's id must not be adopted"
        );

        // Overwritten, not merely ignored: leaving the foreign id on disk would
        // mint a fresh one on *every* start, and two machines would keep
        // evicting each other.
        let raw = fs::read_to_string(dir.join(IDENTITY_FILE)).unwrap();
        assert!(raw.starts_with(&identity.device_id.to_string()));
        assert!(raw.ends_with(&format!("{}\n", host_fingerprint())));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_corrupt_file_is_replaced_not_fatal() {
        for (label, content) in [
            ("garbage", "garbage"),
            ("empty", ""),
            ("uuid-only", "8f14e45f-ceea-467f-a7b1-0f2e1a6f8a11"),
            ("bad-uuid", "not-a-uuid\ndeadbeef\n"),
        ] {
            let dir = unique_dir(label);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join(IDENTITY_FILE), content).unwrap();

            let identity = load_or_create(&dir)
                .unwrap_or_else(|e| panic!("{label}: a bad file must not be fatal: {e:#}"));
            assert!(!identity.device_id.is_nil(), "{label}");
            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn a_missing_state_dir_is_created() {
        let root = unique_dir("nested");
        let dir = root.join("a").join("b");
        assert!(!dir.exists());
        let identity = load_or_create(&dir).expect("the state dir must be created");
        assert!(!identity.device_id.is_nil());
        assert!(dir.join(IDENTITY_FILE).exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn the_fingerprint_is_stable_across_calls() {
        // Guards against the one mistake that makes this feature worse than
        // nothing: mixing in a random salt, a timestamp, or the pid. Any of
        // those mints a new device id on every start.
        let first = host_fingerprint();
        let second = host_fingerprint();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64, "SHA-256 as hex, not some opaque encoding");
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
        // Not a fresh-process check (that needs a child process); it rules out
        // the per-*call* sources — a counter, a fresh `Uuid::new_v4()` — which
        // are the ones that would slip through the two calls above if the
        // randomness were drawn lazily. Process-level sources (pid, start time)
        // stay a code-review property: nothing in `host_fingerprint` reads them.
        assert_eq!(
            first,
            host_fingerprint(),
            "repeated calls in one process must agree"
        );
    }

    #[test]
    fn the_file_is_not_written_twice_on_a_second_load() {
        let dir = unique_dir("no-rewrite");
        let first = load_or_create(&dir).unwrap();
        let path = dir.join(IDENTITY_FILE);
        let inode_before = inode_of(&path);

        // `load_or_create` writes atomically (temp file + rename), so a rewrite
        // necessarily produces a **different inode**. Comparing content would
        // miss a rewrite of identical bytes, and comparing mtime is flaky on
        // filesystems that only store whole seconds.
        let second = load_or_create(&dir).unwrap();
        assert_eq!(second.device_id, first.device_id);
        // Unix only: on platforms without inodes this degrades to the weaker
        // checks below rather than passing the inode comparison vacuously.
        #[cfg(unix)]
        assert_eq!(
            inode_of(&path),
            inode_before,
            "a matching identity must be read, never rewritten"
        );
        #[cfg(not(unix))]
        let _ = inode_before;
        assert!(
            !dir.join(format!("{IDENTITY_FILE}{TMP_SUFFIX}")).exists(),
            "no temp file may be left behind"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Identity of the file itself, so a replace-via-rename is visible.
    #[cfg(unix)]
    fn inode_of(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(path).unwrap().ino()
    }
}

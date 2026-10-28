// SPDX-License-Identifier: GPL-3.0-or-later

//! Logging helpers shared by the client and the hub.
//!
//! File names are private data: a clipboard manager's log should not become a
//! record of everything the user copied. [`redact_file_name`] and
//! [`redact_path`] turn a name into a stable opaque identifier unless
//! `log_file_names` is enabled (client config) or `CLIPBOARDWIRE_LOG_FILE_NAMES`
//! is set (hub).
//!
//! # This is pseudonymisation, not anonymisation
//!
//! The identifier is `file#` plus a truncated keyed hash with a **per-process
//! random salt**. That buys two things:
//!
//! - The same file name maps to the same identifier for the lifetime of one
//!   process, so a single log still correlates a transfer's start and end.
//! - The mapping cannot be reversed with a precomputed dictionary of common
//!   names (`passwords.txt`, `resume.pdf`, …), and identifiers from two
//!   different runs do not line up.
//!
//! It does **not** make the name anonymous: anyone who can guess the candidate
//! set can still confirm a guess by hashing it with the same salt while the
//! process is alive. Treat logs as "contains no names in clear" rather than
//! "contains no information about names".

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::path::Path;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};

/// Process-wide random key for the redaction hash.
///
/// `RandomState` is seeded from the OS entropy the first time it is built, so
/// every process gets a different key without pulling in a random-number crate.
/// Built once and reused, which is what keeps identifiers stable *within* a
/// process.
fn redaction_salt() -> &'static [u8] {
    static SALT: OnceLock<[u8; 32]> = OnceLock::new();
    SALT.get_or_init(|| {
        let state = RandomState::new();
        let mut salt = [0u8; 32];
        // Two independent 64-bit draws fill the 32-byte key.
        for (i, chunk) in salt.chunks_mut(8).enumerate() {
            let mut hasher = state.build_hasher();
            hasher.write_usize(i);
            chunk.copy_from_slice(&hasher.finish().to_le_bytes());
        }
        salt
    })
}

/// Hash `bytes` with the process salt and render the first 64 bits as hex.
fn keyed_id(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(redaction_salt());
    hasher.update(bytes);
    let digest = hasher.finalize();
    // 64 bits: a collision would need ~2^32 names within one process's log,
    // and a collision only means two lines share a pseudonym.
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether the hub should log file names in clear.
///
/// The hub has no config file, so this is the `CLIPBOARDWIRE_LOG_FILE_NAMES`
/// env var, read once per call. Accepts the same truthy spellings as the other
/// `CLIPBOARDWIRE_*` booleans; anything else (including "0"/"false") is off.
pub fn hub_log_file_names() -> bool {
    match std::env::var("CLIPBOARDWIRE_LOG_FILE_NAMES") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

/// Render `name` for a log line: as-is when `log_file_names` is set, otherwise
/// a stable `file#<hex>` pseudonym.
pub fn redact_file_name(name: &str, log_file_names: bool) -> String {
    if log_file_names {
        return name.to_string();
    }
    format!("file#{}", keyed_id(name.as_bytes()))
}

/// Render a path's *file name* for a log line. Logs never carry the directory
/// component when redaction is on, which also keeps local paths out of
/// collected logs.
///
/// Non-UTF-8 file names still get a distinct identifier: the raw `OsStr` bytes
/// are hashed, so two different unreadable names do not collapse into one.
pub fn redact_path(path: &Path, log_file_names: bool) -> String {
    if log_file_names {
        return path.display().to_string();
    }
    // No file name component (e.g. `/`): hash the whole path rather than
    // returning a constant, so distinct paths stay distinct.
    let target = path.file_name().unwrap_or(path.as_os_str());
    format!("file#{}", hashed_os_str(target))
}

#[cfg(unix)]
fn hashed_os_str(s: &std::ffi::OsStr) -> String {
    use std::os::unix::ffi::OsStrExt;
    keyed_id(s.as_bytes())
}

#[cfg(not(unix))]
fn hashed_os_str(s: &std::ffi::OsStr) -> String {
    // Windows `OsStr` is WTF-8 internally but only exposes `encode_wide`.
    // Hashing the lossy UTF-8 view keeps the identifier stable and distinct
    // for the names a log is likely to see; it does not allocate a leak.
    keyed_id(s.to_string_lossy().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_hides_the_name_but_is_stable() {
        let redacted = redact_file_name("tax-return-2025.pdf", false);
        assert!(!redacted.contains("tax-return"));
        assert!(redacted.starts_with("file#"));
        assert_eq!(redacted, redact_file_name("tax-return-2025.pdf", false));
    }

    #[test]
    fn redaction_is_identity_when_enabled() {
        assert_eq!(
            redact_file_name("tax-return-2025.pdf", true),
            "tax-return-2025.pdf"
        );
    }

    #[test]
    fn different_names_get_different_identifiers() {
        assert_ne!(
            redact_file_name("a.txt", false),
            redact_file_name("b.txt", false)
        );
    }

    #[test]
    fn redacted_path_drops_the_directory() {
        let path = Path::new("/home/alice/secret/notes.txt");
        let redacted = redact_path(path, false);
        assert!(!redacted.contains("alice"));
        assert!(!redacted.contains("notes"));
        assert_eq!(redacted, redact_file_name("notes.txt", false));
    }

    #[test]
    fn identifier_is_not_a_bare_hash_of_the_name() {
        // Guards the salt: an unsalted SHA-256 prefix would let a dictionary
        // reverse a common name. We cannot test "unguessable", but we can
        // assert the output is not the plain digest of the input.
        let plain = {
            let digest = Sha256::digest(b"passwords.txt");
            digest[..8]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        assert_ne!(
            redact_file_name("passwords.txt", false),
            format!("file#{plain}")
        );
    }

    #[test]
    fn many_common_names_do_not_collide() {
        // 32-bit truncation was a real collision risk; 64 bits with a salt
        // should keep a modest name set distinct even within one process.
        let names: Vec<String> = (0..2000).map(|i| format!("file-{i}.bin")).collect();
        let mut ids: Vec<String> = names.iter().map(|n| redact_file_name(n, false)).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), names.len(), "identifiers collided");
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_names_stay_distinct() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let a = Path::new(OsStr::from_bytes(b"/tmp/\xff\xfe-a.bin"));
        let b = Path::new(OsStr::from_bytes(b"/tmp/\xff\xfe-b.bin"));
        let id_a = redact_path(a, false);
        let id_b = redact_path(b, false);
        assert!(id_a.starts_with("file#"));
        assert_ne!(id_a, id_b, "non-UTF-8 names must not collapse to one id");
        // Stable within the process.
        assert_eq!(id_a, redact_path(a, false));
    }

    #[test]
    fn hub_side_flag_is_off_by_default_and_parses_truthy_values() {
        let _g = crate::env_lock::lock_env();
        // `hub_log_file_names` reads a process-global env var, so this test
        // must restore whatever was there. The lock is crate-wide, shared with
        // `server::config`'s tests: `set_var` rewrites the whole environment
        // block, so a mutation here races a concurrent `env::var` anywhere in
        // this test binary — even one reading a different variable.
        let previous = std::env::var("CLIPBOARDWIRE_LOG_FILE_NAMES").ok();
        // SAFETY: the guard above serialises every test in this crate that
        // reads or mutates the process environment.
        unsafe { std::env::remove_var("CLIPBOARDWIRE_LOG_FILE_NAMES") };
        assert!(!hub_log_file_names(), "default must redact");

        for truthy in ["1", "true", "TRUE", "yes", "on"] {
            unsafe { std::env::set_var("CLIPBOARDWIRE_LOG_FILE_NAMES", truthy) };
            assert!(hub_log_file_names(), "{truthy} should enable names");
        }
        for falsy in ["0", "false", "no", "off", ""] {
            unsafe { std::env::set_var("CLIPBOARDWIRE_LOG_FILE_NAMES", falsy) };
            assert!(!hub_log_file_names(), "{falsy:?} should keep redaction");
        }

        unsafe {
            match previous {
                Some(v) => std::env::set_var("CLIPBOARDWIRE_LOG_FILE_NAMES", v),
                None => std::env::remove_var("CLIPBOARDWIRE_LOG_FILE_NAMES"),
            }
        }
    }

    #[test]
    fn path_without_a_file_name_still_hashes() {
        let id = redact_path(Path::new("/"), false);
        assert!(id.starts_with("file#"));
        // Not the identifier of the empty name, which is what the old
        // `unwrap_or_default()` fallback produced.
        assert_ne!(id, redact_file_name("", false));
    }
}

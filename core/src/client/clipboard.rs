// SPDX-License-Identifier: GPL-3.0-or-later

//! Dedicated-thread clipboard adapter.
//!
//! The `arboard::Clipboard` handle is created and owned by a single OS thread
//! (X11 / Wayland / Win32 / NSPasteboard all benefit from a stable owner).
//! The thread runs a single hybrid loop that does two things on each tick:
//!
//! 1. **Apply** any pending write requests (from the supervisor).
//! 2. **Poll** the local clipboard for changes and emit any new value.
//!
//! v0.2 polls both text and image clipboard. Each kind has its own
//! `last_seen` tracker so a copy of text doesn't clobber the image cache
//! (or vice versa).
//!
//! Both directions are exposed as channels so the supervisor is portable and
//! easily mockable: tests construct a [`Clipboard`] directly with their own
//! channel ends, without ever spawning the real arboard thread.
//!
//! Echo-loop suppression: when we apply an inbound clip, we update the
//! thread's `last_seen_*` tracker to the value we just wrote. The next poll
//! won't see it as "new" and so won't bounce it back to the hub.
//!
//! One clipboard = one emission: when several representations coexist
//! (a file manager's copy also publishes an icon image and the filename
//! as text), only the highest-priority one is emitted — see [`decide`].

use std::path::PathBuf;
use std::sync::mpsc as smpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};

use super::files_clipboard;
use super::offer::PinnedIdentity;

/// Default-buffer size for the outbound events channel.
const EVENT_BUFFER: usize = 32;

/// Owned RGBA image, in row-major order (4 bytes per pixel, R-G-B-A).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageBytes {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>,
}

/// Per-kind "what we believe the OS clipboard currently holds" state.
///
/// One OS clipboard = one logical value, but it can carry several
/// representations at once (a Finder file copy puts a file URL *and* an
/// icon TIFF *and* the filename as text on the pasteboard). The
/// trackers have two jobs:
///
/// - **Change detection:** a tick only emits a [`ClipChange`] when the
///   winning representation differs from what we last saw.
/// - **Echo suppression:** after we apply a remote clip we record it
///   here, so the next poll doesn't bounce it back to the hub.
///
/// `files_err_streak` bounds the "files still present" suppression we
/// apply when the files backend fails to read — see [`decide`].
#[derive(Default)]
struct Seen {
    text: Option<String>,
    image: Option<ImageBytes>,
    files: Option<Vec<PathBuf>>,
    /// What the files behind `files` looked like when we last emitted them,
    /// one entry per path (`None` = the path did not stat).
    ///
    /// This exists because a path list alone is not an identity. A user who
    /// deletes a file and copies the same path again sends exactly the same
    /// `Vec<PathBuf>`, so path-only dedupe swallowed the second copy silently:
    /// no offer, no log, nothing. Comparing contents is what distinguishes
    /// "the poll loop re-read an unchanged clipboard" from "the user copied
    /// again".
    ///
    /// `None` means "we have no snapshot" — set when the list changes and
    /// filled in immediately afterwards by [`record_files_identity`].
    files_identity: Option<Vec<Option<PinnedIdentity>>>,
    files_err_streak: u32,
    files_absent_streak: u32,
}

/// How many consecutive `read_files` failures we tolerate before giving
/// up file-suppression and letting image/text through. At the default
/// 300 ms poll cadence this is ~1 s — long enough to ride out a
/// transient read error mid-file-copy, short enough that a permanently
/// broken backend doesn't silently kill image/text sync.
const MAX_FILES_READ_ERRORS: u32 = 3;

/// How many consecutive *clean* "no files" reads we tolerate before
/// releasing file-suppression. A single `NoFiles` report can be the
/// backend catching the pasteboard mid-rewrite — clipboard managers
/// (e.g. input-method clipboard history) briefly publish a file copy
/// without its file URLs — so one tick is not proof the files are gone.
/// Same ~1 s budget as the error streak.
const MAX_FILES_ABSENT_TICKS: u32 = 3;

impl Seen {
    /// Record a locally-applied remote clip. Writing any representation
    /// replaces the *whole* OS clipboard, so the other trackers are
    /// cleared — otherwise a later local copy whose content happens to
    /// equal a stale tracker would be wrongly deduped.
    fn mark_applied(&mut self, change: &ClipChange) {
        match change {
            ClipChange::Text(t) => {
                self.text = Some(t.clone());
                self.image = None;
                self.files = None;
                self.files_absent_streak = 0;
            }
            ClipChange::Image(i) => {
                self.image = Some(i.clone());
                self.text = None;
                self.files = None;
                self.files_absent_streak = 0;
            }
            ClipChange::Files(p) => {
                self.files = Some(p.clone());
                // Stat what we just wrote. The point is echo suppression: the
                // very next tick reports these same paths, and without a
                // snapshot the recheck would have to assume "changed" and
                // bounce the files straight back to the hub.
                self.files_identity = Some(identities_of(p));
                self.text = None;
                self.image = None;
                self.files_absent_streak = 0;
            }
        }
    }
}

/// What the files backend reported this tick. Distinct from
/// `Option<Vec<PathBuf>>` because a read *error* must not clear the
/// files tracker (the clipboard may still hold files).
enum FilesRead {
    Files(Vec<PathBuf>),
    NoFiles,
    Error,
}

/// What the text backend reported. Three states because tracker
/// handling differs: `NotAvailable` clears `seen.text` while `Error`
/// preserves it.
enum TextRead {
    Value(String),
    NotAvailable,
    Error,
}

/// Stat every path, keeping the failures as `None`.
///
/// Called only when the path list actually needs an identity — a change, or a
/// same-list recheck — so the steady state (a poll re-reading an unchanged
/// clipboard) still costs zero stats in `decide` itself. See
/// [`record_files_identity`].
fn identities_of(paths: &[PathBuf]) -> Vec<Option<PinnedIdentity>> {
    paths
        .iter()
        .map(|path| PinnedIdentity::of(path).ok())
        .collect()
}

/// Refresh `seen.files_identity` for the current path list.
///
/// Separate from [`decide`] because the decision is a pure function over its
/// inputs and this is the one part that must touch the filesystem. The poll
/// loop calls it exactly when `decide` says the list is unchanged but should be
/// rechecked.
fn record_files_identity(seen: &mut Seen) {
    if let Some(paths) = seen.files.clone() {
        seen.files_identity = Some(identities_of(&paths));
    }
}

/// What a poll tick concluded about the files representation.
///
/// A bare `Option<ClipChange>` cannot express the case this type exists for:
/// "the path list is identical, but the files behind it may have changed". That
/// third state is what a deleted-then-recopied file produces, and collapsing it
/// to `None` is the bug this replaces.
#[derive(Debug)]
enum FilesDecision {
    /// The list differs from the last one — emit it.
    Changed(Vec<PathBuf>),
    /// The list is identical. `stale` is true when the files behind it no
    /// longer match the snapshot taken when they were emitted, i.e. the user
    /// re-copied something we would otherwise ignore.
    Same { stale: bool },
    /// Files still occupy the clipboard but this tick has nothing to say:
    /// **emit nothing at all**, not even image/text. Their shadow
    /// representations (a Finder icon TIFF, the filename as text) would
    /// otherwise ride the wire behind the file list.
    Suppress,
    /// Files no longer occupy the clipboard; image/text may flow.
    Quiet,
}

/// Decide which [`ClipChange`] (if any) a poll tick should emit, given
/// the raw backend reads and the running [`Seen`] state. Pure function
/// over explicit inputs so the policy is testable without arboard.
///
/// Priority: files > image > text, one emission per tick. Crucially,
/// while files occupy the clipboard *no other representation is
/// emitted* — otherwise the shadow representations (file icon, file
/// name) ride the wire behind the file chunks and, being applied
/// instantly on the receiver while the file list waits out the batch
/// window, can end up *last writer* and clobber the peer's file
/// clipboard with an icon bitmap or a filename string.
fn decide(
    seen: &mut Seen,
    files: FilesRead,
    image: Option<ImageBytes>,
    text: TextRead,
) -> Option<ClipChange> {
    match decide_files(seen, files) {
        FilesDecision::Changed(paths) => {
            // New clipboard state: reset the other trackers so a later
            // identical icon/name isn't deduped away.
            seen.image = None;
            seen.text = None;
            return Some(ClipChange::Files(paths));
        }
        FilesDecision::Same { stale: true } => {
            // The path list is identical but the files behind it are not what
            // we advertised. That is a *re-copy*, and the user expects an
            // offer for it — silently swallowing it was the "sometimes copying
            // does nothing" bug. Emit the same list; `OfferRegistry::advertise`
            // decides whether it can reuse the `file_id` (same bytes) or must
            // supersede it (different bytes, or gone).
            let paths = seen.files.clone().unwrap_or_default();
            seen.image = None;
            seen.text = None;
            return Some(ClipChange::Files(paths));
        }
        FilesDecision::Same { stale: false } => {
            // Unchanged clipboard: the steady state this dedupe exists for.
            // Still a suppression, not a fall-through: the files are on the
            // clipboard, so their shadow representations must not escape.
            return None;
        }
        FilesDecision::Suppress => return None,
        FilesDecision::Quiet => {}
    }
    decide_not_files(seen, image, text)
}

/// Files half of [`decide`]: classify the file list without touching the
/// image/text trackers, so the caller can decide what a change means.
///
/// Pure. The identity comparison uses the snapshot in `Seen`, which the poll
/// loop refreshes via [`record_files_identity`] — this function never stats.
fn decide_files(seen: &mut Seen, files: FilesRead) -> FilesDecision {
    match files {
        FilesRead::Files(paths) => {
            seen.files_err_streak = 0;
            seen.files_absent_streak = 0;
            if seen.files.as_deref() == Some(paths.as_slice()) {
                // Same list. Whether this is a no-op or a re-copy depends on
                // the files' identities, not on their names — a delete and
                // re-copy of one path produces an identical list.
                // "Stale" means the files behind this list are not what we
                // last saw — a *transition*, not a state. The distinction
                // matters for the deleted case: reporting a missing file once
                // is the visible signal the user needs; reporting it on every
                // 300 ms tick for as long as the clipboard holds that path
                // would bury the log.
                let stale = match seen.files_identity.as_ref() {
                    Some(snapshot) => snapshot.iter().zip(paths.iter()).any(|(was, path)| {
                        match (was, PinnedIdentity::of(path)) {
                            // Both readable: compare contents, not names.
                            (Some(before), Ok(now)) => before != &now,
                            // Present -> missing. A real transition: the user
                            // deleted the file the clipboard still points at.
                            // `publish_offers` answers `StatFailed`, which is
                            // the one visible signal this path has.
                            (Some(_), Err(_)) => true,
                            // Missing -> present: a new file at the same path.
                            (None, Ok(_)) => true,
                            // Missing -> still missing. Already reported; a
                            // re-copy of a path that does not exist cannot
                            // produce an offer, and repeating the warning
                            // every tick would be noise.
                            (None, Err(_)) => false,
                        }
                    }),
                    // No snapshot at all. Should not happen (every path that
                    // sets `seen.files` also sets `files_identity`), but if it
                    // does, emitting is the safe direction — the next tick has
                    // a snapshot again.
                    None => true,
                };
                if stale {
                    // Re-snapshot as part of the transition, so this reports
                    // the change **once**. Doing it in the caller instead left
                    // the snapshot on the old value and re-reported every tick:
                    // ~3 warnings a second for as long as the clipboard held a
                    // path that no longer exists.
                    record_files_identity(seen);
                }
                return FilesDecision::Same { stale };
            }
            seen.files = Some(paths.clone());
            seen.files_identity = Some(identities_of(&paths));
            return FilesDecision::Changed(paths);
        }
        FilesRead::NoFiles => {
            seen.files_err_streak = 0;
            if seen.files.is_some() {
                seen.files_absent_streak += 1;
                if seen.files_absent_streak <= MAX_FILES_ABSENT_TICKS {
                    // Keep treating the clipboard as file-occupied: a
                    // lone empty read is likely the backend catching a
                    // mid-rewrite. Releasing here would let the shadow
                    // icon/name escape over the wire.
                    return FilesDecision::Suppress;
                }
                seen.files = None;
                seen.files_identity = None;
            } else {
                seen.files_absent_streak = 0;
            }
        }
        FilesRead::Error => {
            seen.files_err_streak += 1;
            if seen.files.is_some() && seen.files_err_streak <= MAX_FILES_READ_ERRORS {
                // Transient failure while files still sit on the
                // clipboard — skip the tick rather than risk emitting
                // the shadow icon/name representations.
                return FilesDecision::Suppress;
            }
            if seen.files_err_streak > MAX_FILES_READ_ERRORS {
                seen.files = None;
                seen.files_identity = None;
            }
        }
    }
    FilesDecision::Quiet
}

/// Image-then-text half of [`decide`], run only when files produced nothing.
///
/// Split out so the files classifier can return without threading the other
/// two trackers through a function that has no business touching them.
fn decide_not_files(
    seen: &mut Seen,
    image: Option<ImageBytes>,
    text: TextRead,
) -> Option<ClipChange> {
    // Image: on most platforms the image clipboard slot is distinct
    // from the text slot, and screenshots are the common "I want this on
    // another machine" case. We still check text every tick.
    if let Some(img) = image {
        if seen.image.as_ref() != Some(&img) {
            seen.image = Some(img.clone());
            return Some(ClipChange::Image(img));
        }
    }

    match text {
        TextRead::Value(t) => {
            if seen.text.as_deref() != Some(t.as_str()) {
                seen.text = Some(t.clone());
                return Some(ClipChange::Text(t));
            }
        }
        TextRead::NotAvailable => {
            seen.text = None;
        }
        TextRead::Error => {}
    }

    None
}

/// A change to the local clipboard, emitted by the poll loop and accepted
/// by the apply path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClipChange {
    Text(String),
    Image(ImageBytes),
    /// A list of local file paths the user copied (e.g. selected
    /// files in Nautilus / Explorer / Finder + Ctrl+C). On the
    /// sender, the supervisor turns each path into a sequence of
    /// `file_chunk` frames. On the receiver, the clipboard adapter
    /// sets the OS clipboard to these paths so a `Ctrl+V` in the
    /// peer's file manager pastes the saved files.
    Files(Vec<std::path::PathBuf>),
}

/// Async-side handle to a clipboard:
/// - `events_rx` yields the local clipboard's content whenever it changes.
/// - `apply_tx` requests that a value be written to the local clipboard.
///
/// Construct via [`spawn`] for the production arboard-backed loop, or
/// directly (the fields are `pub`) for tests.
pub struct Clipboard {
    pub events_rx: mpsc::Receiver<ClipChange>,
    pub apply_tx: smpsc::Sender<ClipChange>,
}

/// Spawn the arboard-backed clipboard thread. Returns the [`Clipboard`] handle
/// and a [`JoinHandle`] so callers can wait on graceful shutdown.
pub fn spawn(poll_ms: u64) -> Result<(Clipboard, JoinHandle<()>)> {
    let (apply_tx, apply_rx) = smpsc::channel::<ClipChange>();
    let (events_tx, events_rx) = mpsc::channel::<ClipChange>(EVENT_BUFFER);
    let join = std::thread::Builder::new()
        .name("clipboardwire-clipboard".into())
        .spawn(move || run_arboard_loop(apply_rx, events_tx, poll_ms))?;
    Ok((
        Clipboard {
            events_rx,
            apply_tx,
        },
        join,
    ))
}

fn run_arboard_loop(
    apply_rx: smpsc::Receiver<ClipChange>,
    events_tx: mpsc::Sender<ClipChange>,
    poll_ms: u64,
) {
    let mut cb = match arboard::Clipboard::new() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "arboard initialization failed; clipboard thread exiting");
            return;
        }
    };
    debug!(poll_ms, "clipboard thread started");

    // Per-kind last-seen trackers — copying text doesn't reset the image
    // cache, so each medium needs its own slot.
    let mut seen = Seen::default();

    let poll = Duration::from_millis(poll_ms);
    let mut next_poll = Instant::now() + poll;

    loop {
        let now = Instant::now();
        let wait = next_poll.saturating_duration_since(now);

        match apply_rx.recv_timeout(wait) {
            Ok(change) => {
                apply_change(&mut cb, change, &mut seen);
                continue;
            }
            Err(smpsc::RecvTimeoutError::Timeout) => {}
            Err(smpsc::RecvTimeoutError::Disconnected) => {
                debug!("apply channel closed; clipboard thread exiting");
                return;
            }
        }

        next_poll = Instant::now() + poll;

        if let Some(change) = poll_clipboard(&mut cb, &mut seen) {
            match events_tx.try_send(change) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!("events channel full; dropping clipboard change");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!("events channel closed; clipboard thread exiting");
                    return;
                }
            }
        }
    }
}

fn apply_change(cb: &mut arboard::Clipboard, change: ClipChange, seen: &mut Seen) {
    match change {
        ClipChange::Text(text) => {
            trace!(kind = "text", len = text.len(), "applying clip");
            if let Err(e) = cb.set_text(text.clone()) {
                warn!(error = %e, "clipboard set_text failed");
            } else {
                seen.mark_applied(&ClipChange::Text(text));
            }
        }
        ClipChange::Image(img) => {
            trace!(
                kind = "image",
                width = img.width,
                height = img.height,
                "applying clip"
            );
            let arboard_image = arboard::ImageData {
                width: img.width as usize,
                height: img.height as usize,
                bytes: std::borrow::Cow::Owned(img.rgba.clone()),
            };
            if let Err(e) = cb.set_image(arboard_image) {
                warn!(error = %e, "clipboard set_image failed");
            } else {
                seen.mark_applied(&ClipChange::Image(img));
            }
        }
        ClipChange::Files(paths) => {
            trace!(kind = "files", count = paths.len(), "applying clip");
            match files_clipboard::write_files(&paths) {
                Ok(()) => {
                    // Echo suppression: when our own poll next runs
                    // it'll find these exact paths and skip them.
                    seen.mark_applied(&ClipChange::Files(paths));
                }
                Err(e) => {
                    warn!(error = %format!("{e:#}"), "clipboard set_files failed");
                }
            }
        }
    }
}

fn poll_clipboard(cb: &mut arboard::Clipboard, seen: &mut Seen) -> Option<ClipChange> {
    // Files first: they're the most explicit "user intent to share"
    // (Ctrl+C in a file manager) and have higher priority than a
    // stray text selection that may also be on the clipboard.
    let files = match files_clipboard::read_files() {
        Ok(Some(paths)) if !paths.is_empty() => FilesRead::Files(paths),
        Ok(_) => FilesRead::NoFiles,
        Err(e) => {
            trace!(error = %format!("{e:#}"), "files_clipboard read transient failure");
            FilesRead::Error
        }
    };

    // While files occupy the clipboard their shadow representations
    // (Finder also puts an icon TIFF and the filename as text) are
    // suppressed inside `decide` — skip decoding them entirely.
    if let FilesRead::Files(_) = files {
        return decide(seen, files, None, TextRead::Error);
    }

    let image = match cb.get_image() {
        Ok(img) => Some(ImageBytes {
            width: img.width as u32,
            height: img.height as u32,
            rgba: img.bytes.into_owned(),
        }),
        Err(arboard::Error::ContentNotAvailable) => {
            // No image on the clipboard right now. Don't clear the
            // tracker: a *text* copy doesn't invalidate the prior image,
            // and on X11/Wayland the image slot may transiently fail
            // to read.
            None
        }
        Err(e) => {
            trace!(error = %e, "clipboard get_image transient failure");
            None
        }
    };

    let text = match cb.get_text() {
        Ok(text) => TextRead::Value(text),
        Err(arboard::Error::ContentNotAvailable) => {
            // Empty or non-text clipboard.
            TextRead::NotAvailable
        }
        Err(e) => {
            trace!(error = %e, "clipboard get_text transient failure");
            TextRead::Error
        }
    };

    decide(seen, files, image, text)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The arboard backend can't be exercised without a display; these tests
    // construct Clipboard directly with test-controlled channels.

    #[tokio::test]
    async fn supervisor_receives_text_events() {
        let (apply_tx, _apply_rx) = smpsc::channel::<ClipChange>();
        let (events_tx, events_rx) = mpsc::channel::<ClipChange>(4);
        let cb = Clipboard {
            events_rx,
            apply_tx,
        };

        events_tx
            .send(ClipChange::Text("hello".to_string()))
            .await
            .unwrap();
        drop(events_tx);

        let mut cb = cb;
        match cb.events_rx.recv().await.unwrap() {
            ClipChange::Text(t) => assert_eq!(t, "hello"),
            other => panic!("expected text change, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn supervisor_receives_image_events() {
        let (apply_tx, _apply_rx) = smpsc::channel::<ClipChange>();
        let (events_tx, events_rx) = mpsc::channel::<ClipChange>(4);
        let cb = Clipboard {
            events_rx,
            apply_tx,
        };

        let img = ImageBytes {
            width: 2,
            height: 1,
            rgba: vec![255, 0, 0, 255, 0, 255, 0, 255],
        };
        events_tx
            .send(ClipChange::Image(img.clone()))
            .await
            .unwrap();
        drop(events_tx);

        let mut cb = cb;
        match cb.events_rx.recv().await.unwrap() {
            ClipChange::Image(got) => assert_eq!(got, img),
            other => panic!("expected image change, got {other:?}"),
        }
    }

    #[test]
    fn supervisor_can_send_apply_requests_for_both_kinds() {
        let (apply_tx, apply_rx) = smpsc::channel::<ClipChange>();
        let (_events_tx, events_rx) = mpsc::channel::<ClipChange>(4);
        let cb = Clipboard {
            events_rx,
            apply_tx,
        };

        cb.apply_tx
            .send(ClipChange::Text("write me".into()))
            .unwrap();
        cb.apply_tx
            .send(ClipChange::Image(ImageBytes {
                width: 1,
                height: 1,
                rgba: vec![1, 2, 3, 4],
            }))
            .unwrap();
        match apply_rx.recv().unwrap() {
            ClipChange::Text(t) => assert_eq!(t, "write me"),
            other => panic!("expected text, got {other:?}"),
        }
        match apply_rx.recv().unwrap() {
            ClipChange::Image(img) => {
                assert_eq!(img.width, 1);
                assert_eq!(img.rgba, vec![1, 2, 3, 4]);
            }
            other => panic!("expected image, got {other:?}"),
        }
    }

    fn icon() -> ImageBytes {
        ImageBytes {
            width: 1,
            height: 1,
            rgba: vec![9, 9, 9, 255],
        }
    }

    fn docx_paths() -> Vec<PathBuf> {
        vec![PathBuf::from("/Users/alice/report.docx")]
    }

    /// The reported bug: a Finder file copy puts file URL + icon TIFF +
    /// filename text on the pasteboard. After the Files change emits,
    /// the same paths must keep suppressing image/text every tick so
    /// the shadow representations never reach the wire.
    #[test]
    fn files_suppress_shadow_representations() {
        let mut seen = Seen::default();
        let paths = docx_paths();

        let out = decide(
            &mut seen,
            FilesRead::Files(paths.clone()),
            None,
            TextRead::Error,
        );
        assert_eq!(out, Some(ClipChange::Files(paths.clone())));

        // Subsequent ticks: files still present → nothing else emits,
        // even though image/text reads would see the icon and filename.
        for _ in 0..5 {
            let out = decide(
                &mut seen,
                FilesRead::Files(paths.clone()),
                Some(icon()),
                TextRead::Value("report.docx".into()),
            );
            assert_eq!(out, None);
        }
    }

    /// After the file copy leaves the clipboard (a new copy replaces
    /// it), image/text detection resumes normally.
    #[test]
    fn text_and_image_resume_after_files_leave() {
        let mut seen = Seen::default();
        decide(
            &mut seen,
            FilesRead::Files(docx_paths()),
            None,
            TextRead::Error,
        );

        // New copy is plain text; files read reports none for long
        // enough that suppression releases.
        for _ in 0..=MAX_FILES_ABSENT_TICKS {
            decide(&mut seen, FilesRead::NoFiles, None, TextRead::Error);
        }
        let out = decide(
            &mut seen,
            FilesRead::NoFiles,
            None,
            TextRead::Value("hello".into()),
        );
        assert_eq!(out, Some(ClipChange::Text("hello".into())));

        // And an image copy works too.
        let out = decide(
            &mut seen,
            FilesRead::NoFiles,
            Some(icon()),
            TextRead::NotAvailable,
        );
        assert_eq!(out, Some(ClipChange::Image(icon())));
    }

    /// Copying the *same* files again after an intervening non-file
    /// copy must re-emit — the tracker clears when files leave the
    /// clipboard.
    #[test]
    fn same_files_recopied_reemits() {
        let mut seen = Seen::default();
        let paths = docx_paths();

        let out = decide(
            &mut seen,
            FilesRead::Files(paths.clone()),
            None,
            TextRead::Error,
        );
        assert_eq!(out, Some(ClipChange::Files(paths.clone())));

        // Intervening text copy — files absent long enough to clear
        // the tracker.
        for _ in 0..=MAX_FILES_ABSENT_TICKS {
            decide(&mut seen, FilesRead::NoFiles, None, TextRead::Error);
        }

        let out = decide(
            &mut seen,
            FilesRead::Files(paths.clone()),
            None,
            TextRead::Error,
        );
        assert_eq!(out, Some(ClipChange::Files(paths)));
    }

    /// A transient read_files error while files are (probably) still
    /// on the clipboard must not open a window for the shadow reps.
    #[test]
    fn transient_files_error_while_files_present_suppresses() {
        let mut seen = Seen::default();
        decide(
            &mut seen,
            FilesRead::Files(docx_paths()),
            None,
            TextRead::Error,
        );

        for _ in 0..MAX_FILES_READ_ERRORS {
            let out = decide(
                &mut seen,
                FilesRead::Error,
                Some(icon()),
                TextRead::Value("report.docx".into()),
            );
            assert_eq!(out, None);
        }
    }

    /// If the backend keeps failing, stop suppressing so a permanently
    /// broken files adapter doesn't take text/image sync down with it.
    #[test]
    fn persistent_files_errors_release_suppression() {
        let mut seen = Seen::default();
        decide(
            &mut seen,
            FilesRead::Files(docx_paths()),
            None,
            TextRead::Error,
        );

        for _ in 0..MAX_FILES_READ_ERRORS {
            decide(&mut seen, FilesRead::Error, None, TextRead::Error);
        }

        // One more error: suppression gives up, image flows again.
        let out = decide(&mut seen, FilesRead::Error, Some(icon()), TextRead::Error);
        assert_eq!(out, Some(ClipChange::Image(icon())));
    }

    /// Applying a remote clip replaces the whole OS clipboard, so the
    /// *other* trackers clear — a later local copy equal to a stale
    /// tracker must still emit.
    #[test]
    fn mark_applied_clears_other_trackers() {
        let mut seen = Seen {
            image: Some(icon()),
            text: Some("old".into()),
            ..Default::default()
        };

        seen.mark_applied(&ClipChange::Files(docx_paths()));
        assert_eq!(seen.files.as_deref(), Some(docx_paths().as_slice()));
        assert_eq!(seen.image, None);
        assert_eq!(seen.text, None);

        seen.mark_applied(&ClipChange::Text("hi".into()));
        assert_eq!(seen.text.as_deref(), Some("hi"));
        assert_eq!(seen.files, None);
    }

    /// Regression for the original symptom ordering: Files emits first,
    /// then while the same clipboard keeps reporting files the icon and
    /// filename never emit — so the receiver's file-list write can't be
    /// raced by a later text clip.
    #[test]
    fn file_copy_emits_exactly_one_change() {
        let mut seen = Seen::default();
        let mut emitted = Vec::new();
        for _ in 0..10 {
            if let Some(c) = decide(
                &mut seen,
                FilesRead::Files(docx_paths()),
                Some(icon()),
                TextRead::Value("report.docx".into()),
            ) {
                emitted.push(c);
            }
        }
        assert_eq!(emitted, vec![ClipChange::Files(docx_paths())]);
    }

    /// A single NoFiles report must not release file-suppression: the
    /// backend can catch the pasteboard mid-rewrite (e.g. a clipboard
    /// manager briefly publishing a copy without file URLs), and an
    /// immediate release lets the icon/name shadows ride the wire.
    #[test]
    fn nofiles_report_is_debounced() {
        let mut seen = Seen::default();
        decide(
            &mut seen,
            FilesRead::Files(docx_paths()),
            None,
            TextRead::Error,
        );

        for _ in 0..MAX_FILES_ABSENT_TICKS {
            let out = decide(
                &mut seen,
                FilesRead::NoFiles,
                Some(icon()),
                TextRead::Value("report.docx".into()),
            );
            assert_eq!(out, None);
        }

        // Streak exceeded — the clipboard is truly file-free now, so the
        // image rep may flow again.
        let out = decide(
            &mut seen,
            FilesRead::NoFiles,
            Some(icon()),
            TextRead::Value("report.docx".into()),
        );
        assert_eq!(out, Some(ClipChange::Image(icon())));
    }

    // ---- Same paths, but different files (the "sometimes nothing happens" bug) ----

    /// A real file in a fresh temp dir, so identity comparisons are about
    /// actual inodes rather than fabricated paths.
    fn real_file(label: &str, body: &[u8]) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("cw-clip-decide-{label}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.bin");
        std::fs::write(&path, body).unwrap();
        path
    }

    /// A poll tick with the given file list, nothing else on the clipboard.
    fn tick(seen: &mut Seen, paths: Vec<PathBuf>) -> Option<ClipChange> {
        decide(seen, FilesRead::Files(paths), None, TextRead::Error)
    }

    #[test]
    fn an_unchanged_path_list_is_not_republished() {
        // The dedupe this all exists for: the poll loop re-reads the same
        // clipboard every 300 ms. Each tick must NOT mint a new `file_id`.
        let file = real_file("steady", b"steady contents");
        let mut seen = Seen::default();

        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            Some(ClipChange::Files(vec![file.clone()])),
            "the first tick must emit"
        );
        for _ in 0..5 {
            assert_eq!(
                tick(&mut seen, vec![file.clone()]),
                None,
                "an unchanged file must not be re-published"
            );
        }
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    #[test]
    fn a_deleted_file_recopied_at_the_same_path_is_not_silently_dropped() {
        // The reported bug. Copy a file, delete it (the clipboard still points
        // at the path — confirmed on macOS), copy the same path again. The list
        // is byte-identical to the last one, so path-only dedupe swallowed it:
        // no offer, no log, nothing. The user saw "copying does nothing".
        let file = real_file("deleted", b"first contents");
        let mut seen = Seen::default();

        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            Some(ClipChange::Files(vec![file.clone()])),
            "the first copy emits"
        );
        assert_eq!(tick(&mut seen, vec![file.clone()]), None, "steady state");

        std::fs::remove_file(&file).unwrap();

        // The path is gone. This must not be silent: emitting hands the list
        // to `publish_offers`, which answers `StatFailed` — the only visible
        // signal the deletion can produce.
        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            Some(ClipChange::Files(vec![file.clone()])),
            "a disappeared file must produce a visible outcome, not silence"
        );

        // And it must be reported *once*, not on every 300 ms tick while the
        // clipboard still holds that path.
        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            None,
            "an already-reported disappearance must not repeat every tick"
        );

        // Recreate it: the path is back and now names a real file again.
        std::fs::write(&file, b"second contents").unwrap();
        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            Some(ClipChange::Files(vec![file.clone()])),
            "a reappearing file at the same path is a new copy"
        );

        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    #[test]
    fn a_rewritten_file_at_the_same_path_is_republished() {
        // Same path, different bytes. `OfferRegistry::advertise` supersedes the
        // old `file_id` when it sees this; it can only do that if the clipboard
        // layer stops swallowing the re-copy.
        let file = real_file("rewritten", b"version one");
        let mut seen = Seen::default();

        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            Some(ClipChange::Files(vec![file.clone()]))
        );
        assert_eq!(tick(&mut seen, vec![file.clone()]), None, "steady state");

        // Same length so only the content, and therefore only the identity
        // (mtime), can reveal this — a length-only check would miss it.
        std::fs::write(&file, b"version TWO").unwrap();
        // Force a strictly different mtime; some filesystems have coarse
        // timestamps and the two writes can land in the same tick.
        let bumped = std::time::SystemTime::now() + std::time::Duration::from_secs(2);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(bumped)
            .unwrap();

        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            Some(ClipChange::Files(vec![file.clone()])),
            "rewritten contents at the same path must be re-published"
        );
        assert_eq!(
            tick(&mut seen, vec![file.clone()]),
            None,
            "and then settle back into steady state"
        );

        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    /// A Files read between absent ticks resets the streak — only
    /// *consecutive* empty reads release suppression.
    #[test]
    fn files_read_resets_absent_streak() {
        let mut seen = Seen::default();
        decide(
            &mut seen,
            FilesRead::Files(docx_paths()),
            None,
            TextRead::Error,
        );

        for _ in 0..MAX_FILES_ABSENT_TICKS {
            decide(&mut seen, FilesRead::NoFiles, None, TextRead::Error);
        }
        // Same files back on the clipboard — no emission, streak reset.
        let out = decide(
            &mut seen,
            FilesRead::Files(docx_paths()),
            None,
            TextRead::Error,
        );
        assert_eq!(out, None);

        // Full budget again before release.
        for _ in 0..MAX_FILES_ABSENT_TICKS {
            let out = decide(&mut seen, FilesRead::NoFiles, Some(icon()), TextRead::Error);
            assert_eq!(out, None);
        }
        let out = decide(&mut seen, FilesRead::NoFiles, Some(icon()), TextRead::Error);
        assert_eq!(out, Some(ClipChange::Image(icon())));
    }
}

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
    files_err_streak: u32,
}

/// How many consecutive `read_files` failures we tolerate before giving
/// up file-suppression and letting image/text through. At the default
/// 300 ms poll cadence this is ~1 s — long enough to ride out a
/// transient read error mid-file-copy, short enough that a permanently
/// broken backend doesn't silently kill image/text sync.
const MAX_FILES_READ_ERRORS: u32 = 3;

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
            }
            ClipChange::Image(i) => {
                self.image = Some(i.clone());
                self.text = None;
                self.files = None;
            }
            ClipChange::Files(p) => {
                self.files = Some(p.clone());
                self.text = None;
                self.image = None;
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
    match files {
        FilesRead::Files(paths) => {
            seen.files_err_streak = 0;
            if seen.files.as_deref() == Some(paths.as_slice()) {
                return None;
            }
            seen.files = Some(paths.clone());
            // New clipboard state: reset the other trackers so a later
            // identical icon/name isn't deduped away.
            seen.image = None;
            seen.text = None;
            return Some(ClipChange::Files(paths));
        }
        FilesRead::NoFiles => {
            seen.files_err_streak = 0;
            seen.files = None;
        }
        FilesRead::Error => {
            seen.files_err_streak += 1;
            if seen.files.is_some() && seen.files_err_streak <= MAX_FILES_READ_ERRORS {
                // Transient failure while files still sit on the
                // clipboard — skip the tick rather than risk emitting
                // the shadow icon/name representations.
                return None;
            }
            if seen.files_err_streak > MAX_FILES_READ_ERRORS {
                seen.files = None;
            }
        }
    }

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

        // New copy is plain text; files read now reports none.
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

        // Intervening text copy clears the files tracker.
        decide(
            &mut seen,
            FilesRead::NoFiles,
            None,
            TextRead::Value("x".into()),
        );

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
}

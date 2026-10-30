// SPDX-License-Identifier: GPL-3.0-or-later

//! Clipboard client: arboard-driven poll loop, WebSocket transport,
//! supervisor that ties them together with echo-loop suppression.

pub mod clipboard;
pub mod config;
pub mod file;
pub mod files_clipboard;
pub mod offer;
pub mod pull;
pub mod receive;
pub mod tls;
pub mod transport;

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::protocol::{
    is_text_content_type, Caps, ClipFrame, FileAbortFrame, Frame, IMAGE_PNG_CONTENT_TYPE,
    TEXT_CONTENT_TYPE,
};

pub use clipboard::{ClipChange, Clipboard, ImageBytes};
pub use config::{ClientConfig, ReceiveMode, SendMode};
pub use transport::{ClientStatus, Transport};

/// Re-exported so binaries can key their progress bookkeeping by the same
/// `file_id` type the events carry, without taking their own `uuid` dependency.
pub use uuid::Uuid as TransferId;

/// Which way the bytes of a transfer are moving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Send,
    Receive,
}

/// Phase a transfer is in. See design §9.
///
/// The sender of a transfer never reports [`Self::Verifying`]: only the
/// receiver verifies, so a sender-side `Verifying` would be a lie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressState {
    /// Reading the file end-to-end to compute its SHA-256. This is the eager
    /// pre-pass: on a large file it is a long invisible stall without this
    /// event.
    Hashing,
    /// Publishing an offer. Unused until the lazy path lands (Phase 5).
    Offering,
    /// Moving bytes: one event per chunk enqueued (send) or written (receive).
    Pulling,
    /// Recomputing the received file's SHA-256 before publishing it. Receive
    /// side only.
    Verifying,
    Done,
    Failed,
    Aborted,
}

impl ProgressState {
    /// Lowercase token used by the headless and tray renderers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hashing => "hashing",
            Self::Offering => "offering",
            Self::Pulling => "pulling",
            Self::Verifying => "verifying",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
        }
    }

    /// Whether this state ends the transfer, after which no further event for
    /// the same `file_id` is expected.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Aborted)
    }
}

/// One progress observation for one file transfer.
///
/// `name` is already redacted when the sender was built with redaction on (see
/// [`crate::logging`]): the tray renders it as-is, so a raw name here would
/// walk straight into the tooltip and the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferProgress {
    pub file_id: Uuid,
    /// Session key. `None` on the eager path, which has no session.
    pub transfer_id: Option<Uuid>,
    pub direction: Direction,
    /// Peer `device_id`, when known. The eager fan-out does not identify the
    /// receiver, so send-side events leave this `None`.
    pub peer: Option<Uuid>,
    /// Display name, redacted unless `log_file_names` is enabled.
    pub name: String,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub state: ProgressState,
}

impl TransferProgress {
    /// Fraction complete in `0..=100`, or `None` when the total is unknown.
    pub fn percent(&self) -> Option<u64> {
        if self.bytes_total == 0 {
            return None;
        }
        Some((self.bytes_done.min(self.bytes_total) * 100) / self.bytes_total)
    }

    /// One human-readable line, e.g. `sending report.pdf 42%`.
    ///
    /// Names are truncated so a long (or hostile) file name cannot blow a
    /// caller's line budget — the tray tooltip has a hard 128-character limit.
    pub fn summary_line(&self, max_name_chars: usize) -> String {
        let send = self.direction == Direction::Send;
        let verb = match (self.state, send) {
            // Terminal states must not borrow the in-progress verbs: both
            // sides report `bytes_done: 0` on failure (the partial file is
            // discarded), so falling through would render a failure as
            // "sending x 0%" — indistinguishable from a transfer that just
            // started.
            (ProgressState::Failed, true) => "failed to send",
            (ProgressState::Failed, false) => "failed to receive",
            (ProgressState::Aborted, _) => "cancelled",
            (ProgressState::Done, true) => "sent",
            (ProgressState::Done, false) => "received",
            (ProgressState::Hashing, _) => "hashing",
            (ProgressState::Offering, _) => "offering",
            (ProgressState::Pulling, true) => "sending",
            (ProgressState::Pulling, false) => "receiving",
            (ProgressState::Verifying, _) => "verifying",
        };
        let name = truncate_chars(&self.name, max_name_chars);
        // Terminal states carry no percentage: "sent x 100%" and "failed to
        // send x 0%" both read as live measurements.
        if self.state.is_terminal() {
            format!("{verb} {name}")
        } else {
            match self.percent() {
                Some(pct) => format!("{verb} {name} {pct}%"),
                None => format!("{verb} {name}"),
            }
        }
    }
}

/// Truncate to at most `max` characters, appending `…` when cut.
fn truncate_chars(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.pop();
        out.push('…');
    }
    out
}

/// Emit one progress event, ignoring "no subscriber" — progress is advisory.
///
/// Takes `Option<&broadcast::Sender<..>>` rather than owning one because the
/// caller owns the channel (design §9: the tray creates it, since the tray
/// spawns the supervisor and an internally-created channel would have no
/// receiver).
pub(crate) fn emit_progress(
    tx: Option<&tokio::sync::broadcast::Sender<TransferProgress>>,
    event: TransferProgress,
) {
    if let Some(tx) = tx {
        // A send with no receivers is reported as an error by `broadcast`, and
        // that is fine: progress is best-effort and must never fail a transfer.
        let _ = tx.send(event);
    }
}

/// Run the clipboard client until the process is signalled or an unrecoverable
/// error occurs. Spawns the arboard thread, the transport task, and bridges
/// them in a single select loop.
pub async fn run(config: ClientConfig) -> Result<()> {
    run_with_status(config, None, None).await
}

/// Variant of [`run`] that emits [`ClientStatus`] transitions to the
/// provided `watch::Sender`. The tray uses this to surface connection
/// state in the menu and tooltip.
///
/// `progress_tx` is **created by the caller** for the same reason `status_tx`
/// is: the tray spawns this supervisor itself, so a channel created inside
/// would have no receiver. Pass `None` when nobody is watching.
pub async fn run_with_status(
    config: ClientConfig,
    status_tx: Option<tokio::sync::watch::Sender<ClientStatus>>,
    progress_tx: Option<tokio::sync::broadcast::Sender<TransferProgress>>,
) -> Result<()> {
    let poll_ms = config.poll_ms;
    let log_file_names = config.log_file_names;

    // Durable identity: resolved here because this is the last layer that
    // still has the config. A failure is **not** fatal — a corrupt or
    // unwritable state dir should cost a stable id, not the connection — so
    // an ephemeral one is used and logged. The consequence is bounded: this
    // machine looks like a new device until the next start, which is the same
    // state as a first run.
    let device_id = match config.resolved_state_dir() {
        Ok(state_dir) => match crate::identity::load_or_create(&state_dir) {
            Ok(identity) => identity.device_id,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "could not load the device identity; continuing with an ephemeral one"
                );
                uuid::Uuid::new_v4()
            }
        },
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "could not resolve the state dir; continuing with an ephemeral device identity"
            );
            uuid::Uuid::new_v4()
        }
    };
    info!(device = %device_id, "device identity loaded");

    let caps = client_caps(&config);
    debug!(caps = ?caps.tokens(), "advertising client capabilities");
    // Read from the config here, while it is still in scope: the supervisor
    // never sees a `ClientConfig`, and threading the whole thing through for
    // two values would make every test caller name fields it does not use.
    let send_policy = SendPolicy::from_config(&config);
    let receive_policy = ReceivePolicy::from_config(&config);
    debug!(
        send_mode = ?send_policy.mode,
        offer_ttl_ms = send_policy.offer_ttl_ms,
        "file publishing policy"
    );
    let (clipboard, _clipboard_join) = clipboard::spawn(poll_ms)?;
    let (transport, transport_join) = transport::spawn_with_options(
        config,
        transport::SpawnOptions {
            status_tx,
            receive_files: true,
            one_shot: false,
            device: Some(device_id),
            caps,
        },
    );
    // Killing the supervisor task (e.g. tray config reload) drops this
    // future, so the guard aborts the transport run_loop too. Without
    // it the orphaned transport reconnects forever — its channels are
    // closed, so every connection is a ~100 ms phantom that the hub
    // sees churning once per backoff cycle.
    let _transport_guard = crate::task::AbortOnDrop(transport_join);
    run_supervisor_with_policy(
        clipboard,
        transport,
        progress_tx,
        log_file_names,
        None,
        send_policy,
        receive_policy,
    )
    .await;
    Ok(())
}

/// What this client advertises on the upgrade.
///
/// `lazy-file` says "I can consume offers". The spec is explicit that this is
/// advertised **whenever the client can consume offers**, regardless of its
/// `receive_mode` — and that is the reading implemented here.
///
/// # Why `receive_mode = "eager"` still advertises nothing
///
/// This looks like a contradiction of the sentence above, so it needs saying
/// plainly: with `eager`, this build genuinely *cannot* consume an offer. The
/// eager receive path is the shared [`file::FileReceiver`], which rejects any
/// chunk carrying `transfer_id` outright — it only knows the v0.3 shape. There
/// is no engine behind an `eager` socket to answer a `file_offer`.
///
/// So the three non-eager modes advertise, and `eager` does not. That keeps
/// the capability a statement about code that exists rather than about intent:
/// advertising `lazy-file` in eager mode would make the hub list this client as
/// a valid receiver, deliver offers nothing answers, and leave the sender
/// waiting for a pull that never comes.
///
/// `auto` resolves per platform; on Linux it resolves to `eager`, so the
/// default Linux config keeps v0.5.4 behaviour exactly.
fn client_caps(config: &ClientConfig) -> crate::protocol::Caps {
    if ReceivePolicy::from_config(config).can_pull() {
        crate::protocol::Caps::LAZY_FILE
    } else {
        crate::protocol::Caps::default()
    }
}

/// How this process publishes files.
///
/// A struct rather than two arguments: `(mode, ttl_ms)` would be two values a
/// caller could silently swap, and the eager path ignores one of them entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendPolicy {
    pub mode: SendMode,
    /// Offer lifetime, already converted to milliseconds.
    pub offer_ttl_ms: i64,
    /// How long a pull session may sit silent before it is released.
    pub pull_idle_timeout_ms: i64,
}

impl Default for SendPolicy {
    fn default() -> Self {
        Self {
            mode: SendMode::default(),
            offer_ttl_ms: i64::from(crate::client::config::default_offer_ttl_minutes()) * 60_000,
            pull_idle_timeout_ms: i64::try_from(
                crate::client::config::default_pull_idle_timeout_secs(),
            )
            .unwrap_or(i64::MAX)
                * 1_000,
        }
    }
}

impl SendPolicy {
    pub fn from_config(config: &ClientConfig) -> Self {
        Self {
            mode: config.send_mode,
            offer_ttl_ms: i64::from(config.offer_ttl_minutes) * 60_000,
            pull_idle_timeout_ms: i64::try_from(config.pull_idle_timeout_secs).unwrap_or(i64::MAX)
                * 1_000,
        }
    }

    /// Whether this copy may be published as an offer at all.
    ///
    /// Both halves matter: this client chose lazy sending, **and** the hub on
    /// the other end must understand offers. Publishing to a hub that does not
    /// advertise `lazy-file` would look successful to the user while no peer
    /// could ever pull the file — the hub's reader ignores an unknown frame
    /// type by design (§3 of the wire spec), so there is no error to surface
    /// and nothing to retry. That is a silent failure, which is worse than a
    /// loud one.
    ///
    /// The spec states it as a requirement on the sender: "A sender SHALL
    /// publish offers only when the hub advertises `lazy-file`".
    pub fn may_publish_offers(&self, hub_caps: Caps) -> bool {
        self.mode == SendMode::Lazy && hub_caps.contains(Caps::LAZY_FILE)
    }
}

/// How this process receives lazily-arrived files.
///
/// Separate from [`SendPolicy`] because the two are read at different points
/// and mean different things: `SendPolicy.mode` decides whether a copy becomes
/// an offer or a stream, while `ReceivePolicy.mode` decides whether an incoming
/// offer is pulled automatically.
#[derive(Debug, Clone)]
pub struct ReceivePolicy {
    pub mode: ReceiveMode,
    /// Chunks requested per `file_pull`.
    pub pull_window_chunks: u32,
    /// Where completed files land. `None` means the platform download dir,
    /// resolved lazily so a process that never receives pays nothing.
    pub save_dir: Option<std::path::PathBuf>,
}

impl Default for ReceivePolicy {
    fn default() -> Self {
        Self {
            mode: ReceiveMode::default(),
            pull_window_chunks: crate::client::config::default_pull_window_chunks(),
            save_dir: None,
        }
    }
}

impl ReceivePolicy {
    pub fn from_config(config: &ClientConfig) -> Self {
        Self {
            mode: config.receive_mode,
            pull_window_chunks: config.pull_window_chunks,
            save_dir: None,
        }
    }

    /// Whether this build can consume offers at all.
    ///
    /// True for every mode except `eager`: `eager` means "stream chunks as
    /// before", and its receive path only understands the eager shape (the
    /// shared [`file::FileReceiver`] rejects anything carrying `transfer_id`).
    /// A `pickup`/`paste`/`auto` client runs the pull engine, so it can.
    ///
    /// This is deliberately the *same* predicate `client_caps` uses, so the
    /// capability advertises exactly what the code does.
    pub fn can_pull(&self) -> bool {
        !matches!(self.resolved_mode(), ReceiveMode::Eager)
    }

    /// Whether an inbound offer should be pulled without asking the user.
    ///
    /// - `pickup` pulls immediately; `pickup_auto` only matters for a headless
    ///   build, which is expressed by the caller passing `None` for the tray.
    /// - `paste` is Phase 6/7 and deliberately not implemented: it needs a
    ///   platform clipboard hook, and half-implementing it here would look like
    ///   it worked on the platforms that do not have one.
    /// - `eager` and `auto`-resolving-to-`eager` do not pull.
    pub fn pulls_offers_automatically(&self) -> bool {
        matches!(self.resolved_mode(), ReceiveMode::Pickup)
    }

    /// `receive_mode` after `auto` has been resolved for this platform.
    pub fn resolved_mode(&self) -> ReceiveMode {
        self.mode.resolved()
    }
}

/// Sweep, advertise, and queue the resulting frames.
///
/// Split out from the select arm so the lazy path is testable without a
/// supervisor: the whole decision — which ids are reused, which are superseded,
/// which paths are skipped — lives here, and its only output is frames on the
/// **control** channel. Nothing in this function can produce a chunk, which is
/// what "lazy does not read the file" means mechanically.
///
/// Returns the paths that were skipped, for the caller to warn about.
fn publish_offers(
    registry: &mut offer::OfferRegistry,
    paths: &[std::path::PathBuf],
    offer_ttl_ms: i64,
    outbound_control: &tokio::sync::mpsc::Sender<Frame>,
) -> Vec<(std::path::PathBuf, offer::SkipReason)> {
    let now = now_millis();
    // Before advertising, so a registry holding only corpses cannot shadow a
    // path's live `file_id` and force an unnecessary supersede.
    registry.sweep(now);
    let outcome = registry.advertise(paths, TransferId::new_v4(), now, offer_ttl_ms);

    // Aborts first: a peer must never hold two live offers for one path, and
    // the replacement is only meaningful once the old id is dead.
    for file_id in outcome.superseded {
        let abort = FileAbortFrame {
            file_id,
            transfer_id: None,
            // Offer-scoped: the hub expands this into one directed copy per
            // recipient, each carrying that recipient's `to`.
            to: None,
            from_device: None,
            reason: "superseded".to_string(),
        };
        if let Err(e) = outbound_control.try_send(Frame::FileAbort(abort)) {
            warn!(error = %e, "could not abort the superseded offer");
        }
    }

    for frame in outcome.offers {
        if let Err(e) = outbound_control.try_send(Frame::FileOffer(frame)) {
            warn!(error = %e, "could not publish an offer");
        }
    }

    outcome.skipped
}

/// Ask for the next window of `transfer_id`, if one is due.
///
/// Marks the request in flight *only after* it is queued: a failed send must
/// not leave the engine waiting for a reply nobody asked for.
/// Ask for one specific window, bypassing the "what is due" computation.
///
/// Used by the digest-wait watchdog, which re-asks for the final window to
/// elicit a digest the sender may have dropped — not for the next window.
fn request_next_window_for(
    receives: &mut receive::ReceiveEngine,
    outbound_control: &tokio::sync::mpsc::Sender<Frame>,
    transfer_id: &Uuid,
    to: Uuid,
    (start, count): (u32, u32),
) {
    let Some(file_id) = receives.session(transfer_id).map(|s| s.file_id) else {
        return;
    };
    let pull = crate::protocol::FilePullFrame {
        file_id,
        transfer_id: *transfer_id,
        to,
        chunk_index: start,
        window: count,
        from_device: None,
    };
    if outbound_control.try_send(Frame::FilePull(pull)).is_err() {
        warn!(%transfer_id, "outbound control full; could not re-request the last window");
    }
}

/// Ask for the next window of `transfer_id`, if one is due.
fn request_next_window(
    receives: &mut receive::ReceiveEngine,
    outbound_control: &tokio::sync::mpsc::Sender<Frame>,
    transfer_id: &Uuid,
    to: Option<Uuid>,
) {
    let Some((start, count)) = receives.next_pull(transfer_id) else {
        return;
    };
    let Some(source) = to else {
        // Without the offer's device there is no address for the pull. The
        // hub stamps `from_device` on the *relayed* frame, but a pull must be
        // addressed to the source, which only the offer knows.
        warn!(%transfer_id, "cannot request a window without the source device");
        return;
    };
    let file_id = match receives.session(transfer_id) {
        Some(session) => session.file_id,
        None => return,
    };
    let pull = crate::protocol::FilePullFrame {
        file_id,
        transfer_id: *transfer_id,
        to: source,
        chunk_index: start,
        window: count,
        // Only the hub may write this; sending `None` keeps a client's frame
        // free of an unauthenticated claim.
        from_device: None,
    };
    if outbound_control.try_send(Frame::FilePull(pull)).is_err() {
        // Leave `in_flight` unset so the next tick retries rather than waiting
        // for a reply to a request that never left.
        warn!(%transfer_id, "outbound control full; will retry the pull window");
        return;
    }
    receives.mark_pulled(transfer_id, start, count);
}

/// Discard a broken session and, if the budget allows, start over.
///
/// With `seed` (the session that just failed) the new session *resumes*: it
/// keeps the bitmap and asks only for the window that died, so a transfer
/// interrupted at 90% does not re-download the 90% it already has. The digest
/// still covers the whole file — resuming only changes how the bytes get
/// here, never what they are checked against. Without a seed the bitmap is
/// gone and the only honest restart is from index 0.
#[allow(clippy::too_many_arguments)]
fn restart_receive(
    receives: &mut receive::ReceiveEngine,
    assembler: &mut Option<receive::LazyAssembler>,
    transport: &Transport,
    file_id: &Uuid,
    seed: Option<receive::ReceiveSession>,
    window: u32,
    log_file_names: bool,
) {
    let Some(shape) = receives.shape(file_id).cloned() else {
        // Without the offer's shape a restart cannot be built: the failed
        // session is gone and nothing else remembers what it was pulling.
        debug!(file_id = %file_id, "no remembered offer to restart from");
        if let Some(assembler) = assembler.as_mut() {
            assembler.discard(file_id);
        }
        return;
    };
    // The engine counts its own restarts (a hole in the stream, a digest
    // that arrived early) before it ever reaches this function — those
    // callers pass `seed: None`. A session dropped *here* — a tail stall,
    // an abort — was never counted, so count it now. Skipping the count is
    // what used to let this path re-download a file from zero on every
    // flap, forever.
    if seed.is_some() && receives.bump_restart(*file_id).is_none() {
        warn!(
            file_id = %file_id,
            restarts = receives.restarts_used(file_id),
            "giving up on a lazy transfer after too many restarts"
        );
        if let Some(assembler) = assembler.as_mut() {
            assembler.discard(file_id);
        }
        return;
    }
    let _ = window;
    let transfer_id = match seed {
        Some(old) => {
            debug!(
                file_id = %file_id,
                file = %crate::logging::redact_file_name(&shape.name, log_file_names),
                from_chunk = old.contiguous,
                total = old.total_chunks,
                "resuming a lazy transfer where it stopped"
            );
            receives.resume(old, now_millis())
        }
        None => {
            // No usable bitmap: the partial on disk cannot be trusted to
            // line up, so start clean rather than assemble garbage.
            if let Some(assembler) = assembler.as_mut() {
                assembler.discard(file_id);
            }
            debug!(
                file_id = %file_id,
                file = %crate::logging::redact_file_name(&shape.name, log_file_names),
                "restarting a lazy transfer from index 0"
            );
            receives.open(*file_id, shape.clone(), now_millis())
        }
    };
    request_next_window(
        &mut *receives,
        &transport.outbound_control_tx,
        &transfer_id,
        Some(shape.from_device),
    );
}

/// Verify a fully-received lazy transfer, move it to the save dir, and queue
/// it for the clipboard batch window.
///
/// Reached from two places that mean the same thing — "every byte is here and
/// so is the digest": a `file_digest` landing after the last chunk, or the
/// last chunk landing after an early digest. Whichever order the wire chose,
/// the completion path is identical.
#[allow(clippy::too_many_arguments)]
fn finish_ready_transfer(
    receives: &mut receive::ReceiveEngine,
    assembler: &mut Option<receive::LazyAssembler>,
    pending_files: &mut Vec<std::path::PathBuf>,
    pending_deadline: &mut Option<tokio::time::Instant>,
    resolved_save_dir: &Option<std::path::PathBuf>,
    log_file_names: bool,
    transfer_id: Uuid,
    sha256: String,
) {
    let Some(assembler) = assembler.as_mut() else {
        // Unreachable in practice: `assembler` is built from
        // `resolved_save_dir`, which is only `None` when the platform cannot
        // report a home directory — and that case was already logged at ERROR
        // before this loop started. Kept as a hard error rather than a warning
        // because reaching it means a completed file is about to be thrown
        // away, which is exactly the failure users report as "nothing
        // happens".
        error!(
            %transfer_id,
            "digest arrived but no directory is configured for received files; \
             discarding a completed transfer"
        );
        receives.finish(&transfer_id);
        return;
    };
    let Some(session) = receives.session(&transfer_id) else {
        return;
    };
    // The same directory the partial file was assembled under. This used to
    // read `receive_policy.save_dir`, which `ReceivePolicy::from_config`
    // always leaves `None` — so every file that survived the earlier bug
    // landed in the temp directory instead of the user's Downloads folder.
    let (file_id, name, save_dir) = (
        session.file_id,
        session.name.clone(),
        resolved_save_dir.clone().unwrap_or_else(std::env::temp_dir),
    );
    let Some(partial) = assembler.take(&file_id) else {
        warn!(file = %crate::logging::redact_file_name(&name, log_file_names),
              "transfer completed with no partial file; dropping");
        receives.finish(&transfer_id);
        return;
    };
    receives.finish(&transfer_id);
    match file::finish_lazy_transfer(&partial, &save_dir, &name, &sha256) {
        Ok(path) => {
            tracing::info!(
                file = %crate::logging::redact_path(&path, log_file_names),
                "saved received file"
            );
            pending_files.push(path);
            *pending_deadline = Some(tokio::time::Instant::now() + RECEIVE_BATCH_WINDOW);
        }
        Err(e) => warn!(
            file = %crate::logging::redact_file_name(&name, log_file_names),
            error = %format!("{e:#}"),
            "lazy transfer failed verification"
        ),
    }
}

/// Bridge clipboard events ↔ transport frames. Public for the integration
/// tests, which inject test-controlled [`Clipboard`] and [`Transport`] handles
/// without spawning the real arboard thread or hitting the network.
pub async fn run_supervisor(clipboard: Clipboard, transport: Transport) {
    run_supervisor_with_progress(clipboard, transport, None, false).await;
}

/// [`run_supervisor`] plus progress reporting and file-name redaction.
pub async fn run_supervisor_with_progress(
    clipboard: Clipboard,
    transport: Transport,
    progress_tx: Option<tokio::sync::broadcast::Sender<TransferProgress>>,
    log_file_names: bool,
) {
    run_supervisor_with_options(clipboard, transport, progress_tx, log_file_names, None).await;
}

/// Full-control entry point. `save_dir` overrides where received files land;
/// production passes `None` (the platform downloads directory), tests pass a
/// temp dir so a suite run cannot litter the developer's real
/// `~/Downloads/clipboardwire/`.
pub async fn run_supervisor_with_options(
    clipboard: Clipboard,
    transport: Transport,
    progress_tx: Option<tokio::sync::broadcast::Sender<TransferProgress>>,
    log_file_names: bool,
    save_dir: Option<std::path::PathBuf>,
) {
    run_supervisor_with_policy(
        clipboard,
        transport,
        progress_tx,
        log_file_names,
        save_dir,
        SendPolicy::default(),
        ReceivePolicy::default(),
    )
    .await;
}

/// [`run_supervisor_with_options`] plus an explicit [`SendPolicy`].
///
/// Separate from `..._with_options` so the existing callers — the integration
/// tests, which never publish files through the supervisor — do not have to
/// name a policy they do not care about.
pub async fn run_supervisor_with_policy(
    mut clipboard: Clipboard,
    mut transport: Transport,
    progress_tx: Option<tokio::sync::broadcast::Sender<TransferProgress>>,
    log_file_names: bool,
    save_dir: Option<std::path::PathBuf>,
    send_policy: SendPolicy,
    receive_policy: ReceivePolicy,
) {
    // **One** resolution of "where do received files go", shared by both receive
    // paths. `None` from the caller means "the platform default", not "nowhere":
    // `FileReceiver` already read it that way (via `FileReceiver::new`), while
    // the lazy path tested `save_dir.is_some()` and therefore had no default at
    // all — every pull that completed without an explicit directory was
    // discarded with "digest arrived but no save dir is configured". Resolving
    // here is what makes the two paths agree by construction instead of by
    // coincidence.
    //
    // Precedence: `ReceivePolicy::save_dir`, then the `save_dir` argument, then
    // the platform default. The first two are explicit overrides from a caller
    // that knows better (the tray, a test); the third is what "not specified"
    // has to mean. `None` stays in both signatures because that override
    // capability is real, but neither may be treated as "nowhere to write".
    let resolved_save_dir: Option<std::path::PathBuf> =
        match receive_policy.save_dir.clone().or(save_dir) {
            Some(dir) => Some(dir),
            None => match file::default_save_dir() {
                Ok(dir) => Some(dir),
                Err(e) => {
                    // Only reachable when the platform cannot report a home
                    // directory at all. Both receive paths are unusable then, and
                    // that is worth an ERROR rather than a warning: no file can ever
                    // land, and the user's symptom is "nothing happens".
                    error!(
                        error = %format!("{e:#}"),
                        "could not resolve a directory for received files; file transfer is \
                         disabled for this process"
                    );
                    None
                }
            },
        };

    let mut file_receiver = match resolved_save_dir.as_ref() {
        Some(dir) => file::FileReceiver::with_save_dir(dir.clone()),
        None => file::FileReceiver::new(),
    }
    .map_err(|e| {
        warn!(
            error = %format!("{e:#}"),
            "could not initialise the file-transfer receiver; inbound files will be dropped"
        );
        e
    })
    .ok();

    // Receiver-side multi-file batching. When the sender does a single
    // Ctrl+C of N files, the chunks arrive interleaved and each file
    // completes at a different moment. Without grouping, the OS
    // clipboard would only ever hold the *last* finished file.
    //
    // Strategy: accumulate completed paths in `pending_files`, set a
    // deadline `RECEIVE_BATCH_WINDOW` after each completion, and only
    // apply when the deadline expires without another arrival. The
    // window is wide enough to ride out the inter-file gap in a
    // multi-file send (typically a few ms) but short enough that the
    // user doesn't notice a "single file" copy as laggy.
    let mut pending_files: Vec<std::path::PathBuf> = Vec::new();
    let mut pending_deadline: Option<tokio::time::Instant> = None;

    // Sessions dropped by a `file_abort`, waiting one tick for their restart.
    // Retrying inside the abort arm would burn the whole restart budget in
    // milliseconds against a still-full queue; the 1 s receive tick is the
    // backoff.
    let mut pending_restarts: Vec<receive::ReceiveSession> = Vec::new();

    // Lives across clipboard events, not inside one: reusing a `file_id` for an
    // unchanged path is the whole point of supersession, and that requires
    // remembering what was advertised last time.
    let mut offers = offer::OfferRegistry::default();

    // Whether the "hub cannot do lazy, falling back to eager" warning has been
    // logged on the *current* connection. A user who copies ten files against a
    // v0.3 hub should read why they are slow once, not ten times; resetting it
    // when the verdict changes keeps a genuine reconnect from hiding a second,
    // different downgrade.
    let mut lazy_downgrade_warned = false;

    // One per process: sessions are keyed by `transfer_id` and are what the
    // running digest accumulates into, so they must outlive a single pull.
    let mut pulls = pull::PullServer::new(pull::MAX_ACTIVE_PULLS, send_policy.pull_idle_timeout_ms);

    // The receiving half. Built unconditionally, not only for `pickup`/`paste`:
    // `receive_mode` decides whether an offer is pulled *automatically*, not
    // whether this engine exists. That distinction is what lets an `eager`
    // client still advertise `lazy-file` — see `client_caps`.
    // Same source of truth as the capability this build advertises, so the two
    // cannot disagree: advertising `lazy-file` and then ignoring offers would
    // strand every sender that tries to use it.
    let receives_can_pull = receive_policy.can_pull();
    let mut receives = receive::ReceiveEngine::new(receive_policy.pull_window_chunks);

    // Drives the two receive timers. Coarse on purpose: the deadlines it
    // enforces are 10 s and 15 s, so polling them once a second costs nothing
    // and keeps the resolution well inside the slack either timer tolerates.
    let mut receive_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    receive_tick.tick().await; // skip the immediate first tick
                               // Same directory as the eager receiver above — see `resolved_save_dir`.
    let mut assembler = resolved_save_dir.clone().map(receive::LazyAssembler::new);

    loop {
        // Snapshot for the async closure to avoid borrowing
        // pending_deadline — the closure pins through the select
        // and would otherwise block mutation in the arms below.
        let current_deadline = pending_deadline;
        let deadline_fut = async move {
            match current_deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(deadline_fut);
        tokio::select! {
            local = clipboard.events_rx.recv() => {
                let Some(change) = local else {
                    debug!("clipboard events channel closed; supervisor exiting");
                    return;
                };
                match change {
                    // Text + image: one frame on the wire each.
                    ClipChange::Text(_) | ClipChange::Image(_) => {
                        let frame = match change_to_frame(change) {
                            Ok(f) => f,
                            Err(e) => {
                                warn!(error = %format!("{e:#}"), "failed to encode local clip; dropping");
                                continue;
                            }
                        };
                        if let Err(e) = transport.outbound_tx.try_send(frame) {
                            warn!(error = %e, "transport outbound full or closed; dropping local change");
                        }
                    }
                    // Files: either advertise (lazy) or stream (eager).
                    //
                    // `lazy` alone is not enough to publish: the hub has to
                    // understand offers. Gating on the live `hub_caps` is what
                    // turns "the user configured lazy against a v0.3 hub" from a
                    // silent no-op into a working transfer.
                    ClipChange::Files(paths) => {
                        let hub_caps = *transport.hub_caps.borrow();
                        if send_policy.may_publish_offers(hub_caps) {
                            lazy_downgrade_warned = false;
                            let skipped = publish_offers(
                                &mut offers,
                                &paths,
                                send_policy.offer_ttl_ms,
                                &transport.outbound_control_tx,
                            );
                            for (path, reason) in skipped {
                                // A skipped path is the difference between
                                // "I copied a file" and "a file left this
                                // machine", and it was previously visible only
                                // in the log file. The tray already renders a
                                // `Failed` transfer as "failed to send <name>"
                                // and counts it in the menu header, so report
                                // it there instead of inventing a second
                                // notification path.
                                let display_name = crate::logging::redact_file_name(
                                    &path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().into_owned())
                                        .unwrap_or_else(|| path.display().to_string()),
                                    log_file_names,
                                );
                                if let Some(tx) = progress_tx.as_ref() {
                                    let _ = tx.send(TransferProgress {
                                        // No offer was minted, so there is no
                                        // `file_id`; a fresh one keeps two
                                        // skipped files of the same name from
                                        // collapsing into one tray row.
                                        file_id: TransferId::new_v4(),
                                        transfer_id: None,
                                        direction: Direction::Send,
                                        peer: None,
                                        name: display_name,
                                        bytes_done: 0,
                                        bytes_total: 0,
                                        state: ProgressState::Failed,
                                    });
                                }
                                match reason {
                                    offer::SkipReason::StatFailed(ref detail) => {
                                        // The user-visible symptom of the
                                        // delete-then-recopy path: the
                                        // clipboard names a file that is no
                                        // longer there. ERROR rather than WARN
                                        // because nothing was sent and the
                                        // tray line will not say why.
                                        error!(
                                            file = %crate::logging::redact_path(&path, log_file_names),
                                            reason = %detail,
                                            "could not offer a file: it cannot be read; \
                                             re-copy it after the file exists again"
                                        );
                                    }
                                    other => {
                                        warn!(
                                            file = %crate::logging::redact_path(&path, log_file_names),
                                            reason = ?other,
                                            "skipping file in offer"
                                        );
                                    }
                                }
                            }
                        } else {
                            if send_policy.mode == SendMode::Lazy && !lazy_downgrade_warned {
                                // Degrade, do not fail: the user asked for the
                                // file to leave this machine, and eager is the
                                // pre-lazy behaviour that still does that. The
                                // warning is the only signal they get, so it
                                // says what changed and why.
                                warn!(
                                    hub_caps = ?hub_caps.tokens(),
                                    "hub does not advertise lazy-file; falling back to \
                                     eager transfer for this and later copies"
                                );
                                lazy_downgrade_warned = true;
                            }
                            for path in paths {
                                if let Err(e) = file::send_file_with_progress(
                                    &path,
                                    &transport.outbound_files_tx,
                                    progress_tx.as_ref(),
                                    log_file_names,
                                )
                                .await
                                {
                                    warn!(
                                        file = %crate::logging::redact_path(&path, log_file_names),
                                        error = %format!("{e:#}"),
                                        "failed to send file from clipboard"
                                    );
                                }
                            }
                        }
                    },
                }
            }
            remote = transport.inbound_rx.recv() => {
                let Some(clip) = remote else {
                    debug!("transport inbound channel closed; supervisor exiting");
                    return;
                };
                let change = match frame_to_change(&clip) {
                    Ok(c) => c,
                    Err(FrameRejection::UnsupportedType) => {
                        warn!(
                            content_type = %clip.content_type,
                            "ignoring inbound clip with unsupported content_type"
                        );
                        continue;
                    }
                    Err(FrameRejection::Decode(e)) => {
                        warn!(error = %format!("{e:#}"), "failed to decode inbound clip");
                        continue;
                    }
                };
                match &change {
                    ClipChange::Text(t) => {
                        tracing::info!(len = t.len(), "applying inbound text clip to local clipboard")
                    }
                    ClipChange::Image(i) => {
                        tracing::info!(bytes = i.rgba.len(), "applying inbound image clip to local clipboard")
                    }
                    ClipChange::Files(_) => {}
                }
                if let Err(e) = clipboard.apply_tx.send(change) {
                    warn!(error = %e, "clipboard thread is gone");
                    return;
                }
            }
            file_chunk = recv_maybe(transport.inbound_files_rx.as_mut()) => {
                let Some(chunk) = file_chunk else {
                    // No inbound-files channel wired, or it closed.
                    // Either way, this branch just goes quiet — the
                    // other arms keep the loop alive.
                    continue;
                };
                // Lazy chunks (those carrying `transfer_id`) go to the pull
                // engine; eager ones keep their v0.3 path below. One channel
                // carries both because the hub routes them the same way.
                if chunk.transfer_id.is_some() {
                    let outcome = receives.handle_chunk(&chunk, now_millis());
                    match outcome {
                        receive::ChunkOutcome::Accept { .. } => {
                            if let Some(assembler) = assembler.as_mut() {
                                if let Err(e) = assembler.write(&chunk) {
                                    warn!(error = %e, "could not write a pulled chunk");
                                    continue;
                                }
                            }
                            // The window's last chunk has landed, so ask for
                            // the next one.
                            if let Some(transfer_id) = chunk.transfer_id {
                                let to = receives
                                    .session(&transfer_id)
                                    .map(|session| session.from_device);
                                request_next_window(
                                    &mut receives,
                                    &transport.outbound_control_tx,
                                    &transfer_id,
                                    to,
                                );
                            }
                        }
                        receive::ChunkOutcome::Ready { sha256 } => {
                            // The last chunk landed after the digest had
                            // already been stored — identical completion to
                            // the digest-arriving-last path, except this
                            // chunk's bytes still have to be written first.
                            if let Some(transfer_id) = chunk.transfer_id {
                                if let Some(assembler) = assembler.as_mut() {
                                    if let Err(e) = assembler.write(&chunk) {
                                        warn!(error = %e, "could not write a pulled chunk");
                                        continue;
                                    }
                                }
                                finish_ready_transfer(
                                    &mut receives,
                                    &mut assembler,
                                    &mut pending_files,
                                    &mut pending_deadline,
                                    &resolved_save_dir,
                                    log_file_names,
                                    transfer_id,
                                    sha256,
                                );
                            }
                        }
                        receive::ChunkOutcome::Ignored => {}
                        receive::ChunkOutcome::Restart => {
                            // The engine already dropped the broken session, so
                            // the chunk's own `file_id` is the only handle left
                            // — and it is the right one: same file, new session.
                            {
                                let file_id = chunk.file_id;
                                warn!(
                                    file_id = %file_id,
                                    "gap in a pulled transfer; restarting from index 0"
                                );
                                restart_receive(
                                    &mut receives,
                                    &mut assembler,
                                    &transport,
                                    &file_id,
                                    // The engine already dropped the broken
                                    // session — and already counted the
                                    // restart — so there is no bitmap to
                                    // resume from.
                                    None,
                                    receive_policy.pull_window_chunks,
                                    log_file_names,
                                );
                            }
                        }
                        receive::ChunkOutcome::Failed(reason) => {
                            warn!(
                                file_id = %chunk.file_id,
                                reason,
                                "lazy transfer failed; no more retries"
                            );
                            receives.forget_file(&chunk.file_id);
                            if let Some(assembler) = assembler.as_mut() {
                                assembler.discard(&chunk.file_id);
                            }
                        }
                    }
                    continue;
                }
                if let Some(receiver) = file_receiver.as_mut() {
                    match receiver.receive_chunk_with_progress(
                        chunk,
                        progress_tx.as_ref(),
                        log_file_names,
                    ) {
                        Ok(Some(path)) => {
                            tracing::info!(
                                file = %crate::logging::redact_path(&path, log_file_names),
                                "saved received file"
                            );
                            // Stash for the batch window; the deadline
                            // arm below will apply the whole list at
                            // once when no further file completes for
                            // RECEIVE_BATCH_WINDOW.
                            pending_files.push(path);
                            pending_deadline = Some(
                                tokio::time::Instant::now() + RECEIVE_BATCH_WINDOW,
                            );
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!(error = %format!("{e:#}"), "file chunk rejected");
                        }
                    }
                }
            }
            control = recv_maybe(transport.inbound_control_rx.as_mut()) => {
                let Some(frame) = control else {
                    // Either the transport was built without inbound control
                    // (`receive_files = false`), or the channel closed. In both
                    // cases this arm goes quiet; the others keep the loop.
                    continue;
                };
                match frame {
                    Frame::FilePull(pull_request) => {
                        let response = pulls.handle_pull(&offers, &pull_request, now_millis());
                        for chunk in response.chunks {
                            if let Err(e) = transport.outbound_files_tx.try_send(chunk) {
                                // Dropping a chunk costs the receiver a restart
                                // from index 0, which is its recovery path; a
                                // silent gap would be worse.
                                warn!(error = %e, "outbound files full; dropping pulled chunk");
                                break;
                            }
                        }
                        if let Some(trailer) = response.trailer {
                            if let Err(e) = transport.outbound_control_tx.try_send(trailer) {
                                warn!(error = %e, "outbound control full; dropping pull trailer");
                            }
                        }
                        if let pull::PullOutcome::Refused(reason) = response.outcome {
                            // A refusal must be addressed to the puller.
                            // `to: None` on a `file_abort` does not mean "nobody"
                            // — the hub reads it as *offer-scoped* and fans it
                            // out to every peer, telling them all to discard a
                            // session they never had. The hub is supposed to
                            // stamp `from_device` before relaying, but this end
                            // must not depend on that: an unattributable pull is
                            // simply not answered.
                            let Some(puller) = pull_request.from_device else {
                                warn!(
                                    file_id = %pull_request.file_id,
                                    reason,
                                    "refusing a pull with no sender; dropping the refusal \
                                     rather than broadcasting it"
                                );
                                continue;
                            };
                            let abort = FileAbortFrame {
                                file_id: pull_request.file_id,
                                transfer_id: Some(pull_request.transfer_id),
                                // Back to the puller. `from_device` is where the
                                // hub put it; `pull.to` names *us*.
                                to: Some(puller),
                                // The hub stamps this on relay, so leave it out.
                                from_device: None,
                                reason: reason.to_string(),
                            };
                            if let Err(e) =
                                transport.outbound_control_tx.try_send(Frame::FileAbort(abort))
                            {
                                warn!(error = %e, "could not send the pull refusal");
                            }
                        }
                    }
                    Frame::FileOffer(offer) => {
                        let name = crate::logging::redact_file_name(&offer.name, log_file_names);
                        // An offer from a hub is only actionable if this build
                        // can pull at all; `client_caps` decides that, and the
                        // hub should never send one otherwise.
                        if !receives_can_pull {
                            debug!(file_id = %offer.file_id, file = %name,
                                   "ignoring offer; this build advertises no lazy capability");
                        } else if !receive_policy.pulls_offers_automatically() {
                            // `eager`/`paste`: we *can* pull, but this is not a
                            // "pull on arrival" mode. The tray will offer it
                            // (5.11); until then the offer is only logged.
                            debug!(
                                file_id = %offer.file_id,
                                file = %name,
                                mode = ?receive_policy.resolved_mode(),
                                "offer noted; mode does not pull automatically"
                            );
                        } else if receives.has_session_for(&offer.file_id)
                            || receives.shape(&offer.file_id).is_some()
                        {
                            // A pull is in flight, or this exact offer was
                            // answered before (completed, aborted mid-file, a
                            // restart waiting on the tick). The second case is
                            // what `welcome.offers` replays produce: `file_id`
                            // identifies (path, identity), so a replay of an
                            // answered offer is always a duplicate — and
                            // pulling it again would drop a fresh copy into
                            // Downloads on every reconnect.
                            debug!(file_id = %offer.file_id, "already engaged this offer");
                        } else {
                            let from_device = match offer.from_device {
                                Some(device) => device,
                                None => {
                                    // Without a sender there is no address for
                                    // the pull; a hub that omits it is broken.
                                    warn!(file_id = %offer.file_id,
                                           "offer without from_device; cannot pull");
                                    continue;
                                }
                            };
                            let shape = receive::ReceiveEngine::shape_from_offer(&offer, from_device);
                            let total = shape.total_chunks;
                            let transfer_id =
                                receives.open(offer.file_id, shape, now_millis());
                            debug!(
                                file_id = %offer.file_id,
                                %transfer_id,
                                file = %name,
                                chunks = total,
                                "pulling an offered file"
                            );
                            request_next_window(
                                &mut receives,
                                &transport.outbound_control_tx,
                                &transfer_id,
                                offer.from_device,
                            );
                        }
                    }
                    Frame::FileDigest(digest) => {
                        match receives.handle_digest(&digest, now_millis()) {
                            receive::DigestOutcome::Ready { sha256 } => {
                                finish_ready_transfer(
                                    &mut receives,
                                    &mut assembler,
                                    &mut pending_files,
                                    &mut pending_deadline,
                                    &resolved_save_dir,
                                    log_file_names,
                                    digest.transfer_id,
                                    sha256,
                                );
                            }
                            receive::DigestOutcome::Early => {
                                debug!(
                                    %digest.transfer_id,
                                    "digest overtook the tail chunks; stored, still pulling"
                                );
                            }
                            receive::DigestOutcome::Unknown => {
                                debug!(%digest.transfer_id, "digest for an unknown session");
                            }
                        }
                    }
                    Frame::FileAbort(abort) => {
                        // `transfer_id` present ⇒ session-scoped; absent ⇒ the
                        // whole offer. Session scope is decided by the field,
                        // never by the reason string (spec §Transfer sessions).
                        match abort.transfer_id {
                            Some(transfer_id) => {
                                // Session-scoped: the pull died, the offer did
                                // not. The reasons that reach here (`busy`,
                                // `unknown_offer`, `invalid_start`,
                                // `source_gone`, `expired`) all describe a
                                // dead *attempt*, not a retracted file, so the
                                // session's bitmap is still valid — hand it to
                                // the restart queue instead of throwing the
                                // whole transfer away, which is what used to
                                // turn one dropped window into a lost file.
                                if let Some(old) = receives.drop_session(&transfer_id) {
                                    debug!(
                                        %transfer_id,
                                        reason = %abort.reason,
                                        "session aborted; restart pending"
                                    );
                                    pending_restarts.push(old);
                                }
                            }
                            None => {
                                let dropped = receives.drop_sessions_for_file(&abort.file_id);
                                receives.forget_file(&abort.file_id);
                                if let Some(assembler) = assembler.as_mut() {
                                    assembler.discard(&abort.file_id);
                                }
                                if dropped > 0 {
                                    debug!(
                                        file_id = %abort.file_id,
                                        reason = %abort.reason,
                                        sessions = dropped,
                                        "offer aborted"
                                    );
                                }
                            }
                        }
                    }
                    Frame::PeerChanged(changed) => {
                        debug!(device = %changed.device, "peer membership changed");
                    }
                    // A `clip`, `welcome` or `error` on this channel would mean
                    // the transport misrouted it; naming them keeps `Frame`'s
                    // exhaustiveness check honest instead of hiding behind `_`.
                    Frame::Clip(_) | Frame::Welcome(_) | Frame::Error(_) | Frame::FileChunk(_) => {
                        warn!("unexpected frame on the control channel");
                    }
                }
            }
            _ = receive_tick.tick() => {
                let now = now_millis();
                // Aborted sessions restart here, one tick after the abort:
                // far enough apart that a `busy` answer is not retried into
                // a still-full queue, close enough that a dropped transfer
                // does not visibly stall.
                for old in std::mem::take(&mut pending_restarts) {
                    let file_id = old.file_id;
                    restart_receive(
                        &mut receives,
                        &mut assembler,
                        &transport,
                        &file_id,
                        Some(old),
                        receive_policy.pull_window_chunks,
                        log_file_names,
                    );
                }
                for (transfer_id, reason) in receives.stale(now) {
                    match reason {
                        receive::StaleReason::TailStall => {
                            warn!(
                                %transfer_id,
                                "pulled transfer stalled mid-file; restarting"
                            );
                            if let Some(old) = receives.drop_session(&transfer_id) {
                                let file_id = old.file_id;
                                restart_receive(
                                    &mut receives,
                                    &mut assembler,
                                    &transport,
                                    &file_id,
                                    Some(old),
                                    receive_policy.pull_window_chunks,
                                    log_file_names,
                                );
                            }
                        }
                        receive::StaleReason::DigestWait => {
                            // Every chunk is here; only the digest is missing,
                            // and a `file_digest` *can* be lost: the receiver's
                            // routed-frame buffer is bounded and the hub
                            // warns-and-drops when it fills.
                            //
                            // Re-asking the final window works because the
                            // source keeps its session after emitting the digest
                            // (`PullServer` hands it to `sweep_idle`, which is
                            // `pull_idle_timeout_secs` — three times
                            // `DIGEST_WAIT_MS`). A completed session answers a
                            // repeat request with the digest alone, no chunks
                            // and no re-read. Restarting would instead
                            // re-download the entire file to recover one frame.
                            //
                            // The one way this fails is the source having
                            // already idle-swept the session; the next
                            // `DigestWait` tick then gets `invalid_start`, and
                            // the engine restarts from zero — correct, just
                            // expensive, and it means the transfer has been
                            // idle for longer than the sender's whole timeout.
                            warn!(%transfer_id, "all chunks received but no digest; re-requesting the last window");
                            if let Some(session) = receives.session(&transfer_id) {
                                let last = session.total_chunks.saturating_sub(1);
                                let to = session.from_device;
                                request_next_window_for(
                                    &mut receives,
                                    &transport.outbound_control_tx,
                                    &transfer_id,
                                    to,
                                    (last, 1),
                                );
                            }
                        }
                    }
                }
            }
            _ = &mut deadline_fut, if pending_deadline.is_some() => {
                pending_deadline = None;
                if !pending_files.is_empty() {
                    let batch = std::mem::take(&mut pending_files);
                    tracing::info!(
                        count = batch.len(),
                        "applying received-files batch to local clipboard"
                    );
                    if let Err(e) = clipboard.apply_tx.send(ClipChange::Files(batch)) {
                        warn!(
                            error = %e,
                            "clipboard thread gone; can't expose received files in clipboard"
                        );
                    }
                }
            }
            else => return,
        }
    }
}

/// How long to wait for additional file completions before applying
/// the batch to the local clipboard. Tuned to be invisible to a
/// single-file `Ctrl+C` (~paste latency) while reliably collapsing a
/// multi-file `Ctrl+C` into one clipboard set even on slow networks.
const RECEIVE_BATCH_WINDOW: std::time::Duration = std::time::Duration::from_millis(500);

/// `tokio::select!` wants every arm to be a future. When the supervisor
/// is configured *without* inbound files (the headless `send`
/// subprocess), we still want `select!` to compile cleanly — return a
/// future that just yields `None` from a never-resolving recv if the
/// channel is absent. Equivalent to `std::future::pending()` but with
/// the same return type as the receiver.
async fn recv_maybe<T>(rx: Option<&mut tokio::sync::mpsc::Receiver<T>>) -> Option<T> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Encode a *single-frame* clipboard change (text or image) into a
/// wire frame. File-list changes go through a separate path —
/// [`send_files_through`] — because each file is a multi-chunk
/// transfer.
fn change_to_frame(change: ClipChange) -> Result<ClipFrame> {
    Ok(match change {
        ClipChange::Text(text) => ClipFrame {
            id: Uuid::new_v4(),
            ts: now_millis(),
            content_type: TEXT_CONTENT_TYPE.to_string(),
            content: Some(text),
            content_b64: None,
            from: None,
        },
        ClipChange::Image(img) => {
            let png = encode_png(&img).context("encoding clipboard image to PNG")?;
            ClipFrame {
                id: Uuid::new_v4(),
                ts: now_millis(),
                content_type: IMAGE_PNG_CONTENT_TYPE.to_string(),
                content: None,
                content_b64: Some(STANDARD.encode(png)),
                from: None,
            }
        }
        ClipChange::Files(_) => {
            // Should never get here — run_supervisor routes Files
            // events through the file-chunk channel.
            anyhow::bail!("ClipChange::Files cannot be encoded as a single ClipFrame")
        }
    })
}

/// Decode an inbound wire frame into a clipboard change.
fn frame_to_change(clip: &ClipFrame) -> std::result::Result<ClipChange, FrameRejection> {
    if is_text_content_type(&clip.content_type) {
        let Some(text) = clip.content.clone() else {
            return Err(FrameRejection::Decode(anyhow::anyhow!(
                "text/* frame missing `content`"
            )));
        };
        Ok(ClipChange::Text(text))
    } else if clip.content_type == IMAGE_PNG_CONTENT_TYPE {
        let Some(b64) = clip.content_b64.as_deref() else {
            return Err(FrameRejection::Decode(anyhow::anyhow!(
                "image/png frame missing `content_b64`"
            )));
        };
        let png = STANDARD
            .decode(b64)
            .map_err(|e| FrameRejection::Decode(anyhow::anyhow!("base64 decode failed: {e}")))?;
        let img = decode_png(&png).map_err(FrameRejection::Decode)?;
        Ok(ClipChange::Image(img))
    } else {
        Err(FrameRejection::UnsupportedType)
    }
}

#[derive(Debug)]
enum FrameRejection {
    UnsupportedType,
    Decode(anyhow::Error),
}

/// Encode RGBA → PNG bytes via the `image` crate.
fn encode_png(img: &ImageBytes) -> Result<Vec<u8>> {
    use std::io::Cursor;
    let buffer: image::ImageBuffer<image::Rgba<u8>, &[u8]> =
        image::ImageBuffer::from_raw(img.width, img.height, img.rgba.as_slice())
            .context("invalid RGBA buffer (width*height*4 != bytes)")?;
    let mut out = Vec::with_capacity(img.rgba.len() / 4);
    buffer
        .write_to(&mut Cursor::new(&mut out), image::ImageFormat::Png)
        .context("PNG encoder")?;
    Ok(out)
}

/// Decode PNG bytes → RGBA via the `image` crate.
fn decode_png(bytes: &[u8]) -> Result<ImageBytes> {
    let dynamic = image::load_from_memory_with_format(bytes, image::ImageFormat::Png)
        .context("PNG decoder")?;
    let rgba = dynamic.to_rgba8();
    Ok(ImageBytes {
        width: rgba.width(),
        height: rgba.height(),
        rgba: rgba.into_raw(),
    })
}

/// Wall clock in Unix milliseconds, for clip timestamps and offer deadlines.
///
/// Not injectable, unlike the hub's clock: these values go on the wire to a
/// peer that compares them against its own clock, so they must be the real
/// time. A test that needs a fixed value drives the offer registry directly.
///
/// A pre-1970 clock clamps to 0 rather than wrapping: an offer dated 1970 is
/// merely expired, while a negative one would confuse every ordering rule.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_frame_roundtrip() {
        let in_change = ClipChange::Text("hello".into());
        let frame = change_to_frame(in_change.clone()).unwrap();
        assert_eq!(frame.content_type, TEXT_CONTENT_TYPE);
        assert!(frame.content_b64.is_none());

        let out_change = frame_to_change(&frame).unwrap();
        assert_eq!(out_change, in_change);
    }

    #[test]
    fn image_frame_roundtrip_preserves_pixels() {
        // 2x2 pure red / green / blue / white image.
        let in_img = ImageBytes {
            width: 2,
            height: 2,
            rgba: vec![
                255, 0, 0, 255, // R
                0, 255, 0, 255, // G
                0, 0, 255, 255, // B
                255, 255, 255, 255, // W
            ],
        };
        let frame = change_to_frame(ClipChange::Image(in_img.clone())).unwrap();
        assert_eq!(frame.content_type, IMAGE_PNG_CONTENT_TYPE);
        assert!(frame.content.is_none());

        let out_change = frame_to_change(&frame).unwrap();
        match out_change {
            ClipChange::Image(out_img) => assert_eq!(out_img, in_img),
            other => panic!("expected Image, got {other:?}"),
        }
    }

    #[test]
    fn frame_with_unknown_content_type_is_rejected() {
        let frame = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: "application/octet-stream".into(),
            content: None,
            content_b64: Some(STANDARD.encode(b"xx")),
            from: None,
        };
        assert!(matches!(
            frame_to_change(&frame),
            Err(FrameRejection::UnsupportedType)
        ));
    }

    #[test]
    fn corrupt_image_b64_is_rejected() {
        let frame = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: IMAGE_PNG_CONTENT_TYPE.into(),
            content: None,
            content_b64: Some("!!!not-base64!!!".into()),
            from: None,
        };
        assert!(matches!(
            frame_to_change(&frame),
            Err(FrameRejection::Decode(_))
        ));
    }

    #[test]
    fn corrupt_png_payload_is_rejected() {
        // Valid base64, but the bytes aren't a PNG.
        let frame = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: IMAGE_PNG_CONTENT_TYPE.into(),
            content: None,
            content_b64: Some(STANDARD.encode(b"not a png file")),
            from: None,
        };
        assert!(matches!(
            frame_to_change(&frame),
            Err(FrameRejection::Decode(_))
        ));
    }

    fn progress(state: ProgressState, direction: Direction) -> TransferProgress {
        TransferProgress {
            file_id: Uuid::nil(),
            transfer_id: None,
            direction,
            peer: None,
            name: "report.pdf".to_string(),
            bytes_done: 0,
            bytes_total: 100,
            state,
        }
    }

    #[test]
    fn failed_summary_names_the_failure_and_omits_the_percentage() {
        // A failure reports `bytes_done: 0` (the partial file is discarded),
        // so rendering the percentage would show "… 0%" — a transfer that has
        // not started rather than one that died.
        for direction in [Direction::Send, Direction::Receive] {
            let line = progress(ProgressState::Failed, direction).summary_line(24);
            assert!(line.contains("failed"), "{direction:?}: {line}");
            assert!(!line.contains('%'), "{direction:?}: {line}");
        }
    }

    #[test]
    fn failed_send_and_failed_receive_use_distinct_verbs() {
        assert_eq!(
            progress(ProgressState::Failed, Direction::Send).summary_line(24),
            "failed to send report.pdf"
        );
        assert_eq!(
            progress(ProgressState::Failed, Direction::Receive).summary_line(24),
            "failed to receive report.pdf"
        );
    }

    #[test]
    fn aborted_summary_says_cancelled_without_a_percentage() {
        for direction in [Direction::Send, Direction::Receive] {
            let line = progress(ProgressState::Aborted, direction).summary_line(24);
            assert!(line.contains("cancelled"), "{direction:?}: {line}");
            assert!(!line.contains('%'), "{direction:?}: {line}");
        }
    }

    // ---- Lazy publishing (5a) ----

    fn unique_dir(label: &str) -> std::path::PathBuf {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("cw-supervisor-{label}-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Drive a real supervisor and report what reached each outbound channel.
    ///
    /// Needed because `publish_offers` on its own cannot distinguish "lazy
    /// mode is wired to it" from "lazy mode is not wired at all": a mutation
    /// that leaves the eager loop in place passes every test that calls the
    /// helper directly.
    #[tokio::test]
    async fn the_supervisor_routes_lazy_copies_to_the_control_channel_only() {
        use tokio::sync::mpsc;

        let dir = unique_dir("supervisor-lazy");
        let path = dir.join("blob.bin");
        std::fs::write(&path, b"never read in lazy mode").unwrap();

        let (events_tx, events_rx) = mpsc::channel::<ClipChange>(8);
        let (apply_tx, _apply_rx) = std::sync::mpsc::channel::<ClipChange>();
        let clipboard = Clipboard {
            events_rx,
            apply_tx,
        };

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<ClipFrame>(8);
        let (outbound_files_tx, mut outbound_files_rx) =
            mpsc::channel::<crate::protocol::FileChunkFrame>(8);
        let (outbound_control_tx, mut outbound_control_rx) = mpsc::channel::<Frame>(8);
        let (_inbound_tx, inbound_rx) = mpsc::channel::<ClipFrame>(8);
        // The hub must advertise `lazy-file`, or the supervisor is right to
        // refuse to publish an offer this hub could not serve (see
        // `SendPolicy::may_publish_offers`).
        let (_hub_caps_tx, hub_caps) = tokio::sync::watch::channel(Caps::LAZY_FILE);
        let transport = Transport {
            inbound_rx,
            inbound_files_rx: None,
            outbound_tx,
            outbound_files_tx,
            inbound_control_rx: None,
            outbound_control_tx,
            hub_caps,
        };

        let supervisor = tokio::spawn(run_supervisor_with_policy(
            clipboard,
            transport,
            None,
            false,
            None,
            SendPolicy {
                mode: SendMode::Lazy,
                offer_ttl_ms: 60_000,
                ..SendPolicy::default()
            },
            ReceivePolicy::default(),
        ));

        events_tx
            .send(ClipChange::Files(vec![path.clone()]))
            .await
            .unwrap();

        // The supervisor processes the event, then keeps looping.
        let frame = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            outbound_control_rx.recv(),
        )
        .await
        .expect("an offer must reach the control channel")
        .expect("control channel open");
        match frame {
            Frame::FileOffer(offer) => assert_eq!(offer.name, "blob.bin"),
            other => panic!("expected file_offer, got {other:?}"),
        }

        // And nothing at all on the two data channels.
        assert!(
            outbound_files_rx.try_recv().is_err(),
            "lazy mode must not emit a single chunk"
        );
        assert!(
            outbound_rx.try_recv().is_err(),
            "lazy mode must not emit a clip either"
        );

        drop(events_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), supervisor).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_file_that_cannot_be_read_is_reported_to_the_tray() {
        // The user-visible half of "sometimes copying a file does nothing".
        // The clipboard can name a path that no longer exists (a delete
        // followed by a re-copy keeps the same path), and `publish_offers`
        // answers `StatFailed`. Before this, that answer existed only in the
        // log file: the tray showed nothing, so the failure looked like the
        // app ignoring the copy.
        let dir = unique_dir("unreadable");
        let missing = dir.join("already-deleted.bin");
        // Deliberately not created: this is the shape the clipboard is in after
        // the file is deleted while its path stays on the pasteboard.

        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<ClipChange>(8);
        let (apply_tx, _apply_rx) = std::sync::mpsc::channel::<ClipChange>();
        let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::channel::<ClipFrame>(8);
        let (outbound_files_tx, _outbound_files_rx) =
            tokio::sync::mpsc::channel::<crate::protocol::FileChunkFrame>(8);
        let (outbound_control_tx, mut outbound_control_rx) = tokio::sync::mpsc::channel::<Frame>(8);
        let (_inbound_tx, inbound_rx) = tokio::sync::mpsc::channel::<ClipFrame>(8);
        let (_hub_caps_tx, hub_caps) = tokio::sync::watch::channel(Caps::LAZY_FILE);

        let (progress_tx, mut progress_rx) = tokio::sync::broadcast::channel::<TransferProgress>(8);

        let supervisor = tokio::spawn(run_supervisor_with_policy(
            Clipboard {
                events_rx,
                apply_tx,
            },
            Transport {
                inbound_rx,
                inbound_files_rx: None,
                outbound_tx,
                outbound_files_tx,
                inbound_control_rx: None,
                outbound_control_tx,
                hub_caps,
            },
            Some(progress_tx),
            false,
            None,
            SendPolicy {
                mode: SendMode::Lazy,
                offer_ttl_ms: 60_000,
                ..SendPolicy::default()
            },
            ReceivePolicy::default(),
        ));

        events_tx
            .send(ClipChange::Files(vec![missing.clone()]))
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(5), progress_rx.recv())
            .await
            .expect("a failed offer must surface as a progress event")
            .expect("progress channel open");

        assert_eq!(
            event.state,
            ProgressState::Failed,
            "the tray renders `Failed` as a visible failure row"
        );
        assert_eq!(event.direction, Direction::Send);
        // `TransferProgress.name` is redacted unless `log_file_names` is on —
        // the same rule `file.rs` applies (`TRANSFER_PROGRESS` goes to the tray
        // tooltip, which is a log surface in every sense that matters). What
        // has to hold here is that it is non-empty and derived from the file,
        // not that it is the raw name.
        assert!(
            !event.name.is_empty(),
            "the row must identify a file, not render blank"
        );
        assert!(
            event.name.starts_with("file#") || event.name == "already-deleted.bin",
            "unexpected name rendering: {}",
            event.name
        );
        assert!(
            event.summary_line(40).starts_with("failed to send"),
            "the tray line comes from `summary_line`: {}",
            event.summary_line(40)
        );

        // Nothing was offered: a missing file must not become an offer that no
        // peer can ever pull.
        assert!(
            outbound_control_rx.try_recv().is_err(),
            "a file that cannot be read must not be advertised"
        );

        drop(events_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), supervisor).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lazy_mode_publishes_offers_and_sends_no_chunks() {
        // The load-bearing property of the whole lazy path: copying a file
        // costs one `stat`, not a read. The only way to show that from outside
        // is that no chunk ever reaches the file channel.
        let dir = unique_dir("lazy");
        let path = dir.join("payload.bin");
        std::fs::write(&path, b"this must never be read in lazy mode").unwrap();

        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(8);
        let (files_tx, mut files_rx) =
            tokio::sync::mpsc::channel::<crate::protocol::FileChunkFrame>(8);
        let mut registry = offer::OfferRegistry::default();

        let skipped = publish_offers(
            &mut registry,
            std::slice::from_ref(&path),
            30 * 60_000,
            &control_tx,
        );
        assert!(skipped.is_empty(), "a plain file is not skipped");

        let frame = control_rx
            .try_recv()
            .expect("the offer must go out on the control channel");
        match frame {
            Frame::FileOffer(offer) => {
                assert_eq!(offer.name, "payload.bin");
                // Exactly the on-disk length, taken from `stat`.
                assert_eq!(offer.file_size, std::fs::metadata(&path).unwrap().len());
                assert!(
                    !offer.batch_id.is_nil(),
                    "the batch is named even for one file"
                );
                assert!(offer.expires_at_ms > offer.ts, "the TTL is positive");
            }
            other => panic!("expected file_offer, got {other:?}"),
        }
        assert!(
            files_rx.try_recv().is_err(),
            "lazy mode must publish an offer, never a chunk"
        );

        // The function cannot write chunks at all: it is handed no file
        // channel. `files_tx` is kept alive only so `files_rx` can be polled;
        // nothing in the call path can reach it.
        drop(files_tx);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `Transport` wired to channels the caller keeps, for supervisor tests.
    #[allow(clippy::type_complexity)]
    fn supervised_transport(
        hub_caps: Caps,
    ) -> (
        Transport,
        tokio::sync::mpsc::Receiver<Frame>,
        tokio::sync::mpsc::Receiver<crate::protocol::FileChunkFrame>,
        tokio::sync::watch::Sender<Caps>,
    ) {
        use tokio::sync::mpsc;
        let (outbound_tx, _outbound_rx) = mpsc::channel::<ClipFrame>(8);
        let (outbound_files_tx, outbound_files_rx) =
            mpsc::channel::<crate::protocol::FileChunkFrame>(8);
        let (outbound_control_tx, outbound_control_rx) = mpsc::channel::<Frame>(8);
        let (inbound_tx, inbound_rx) = mpsc::channel::<ClipFrame>(8);
        let (hub_caps_tx, hub_caps_rx) = tokio::sync::watch::channel(hub_caps);
        // Leak both ends of the clip path. A dropped outbound receiver makes
        // the supervisor's `try_send` fail; a dropped inbound *sender* closes
        // `inbound_rx`, which the supervisor reads as "transport gone" and
        // exits on. Neither channel is part of what these tests assert.
        Box::leak(Box::new(_outbound_rx));
        Box::leak(Box::new(inbound_tx));
        (
            Transport {
                inbound_rx,
                inbound_files_rx: None,
                outbound_tx,
                outbound_files_tx,
                inbound_control_rx: None,
                outbound_control_tx,
                hub_caps: hub_caps_rx,
            },
            outbound_control_rx,
            outbound_files_rx,
            hub_caps_tx,
        )
    }

    #[test]
    fn lazy_sending_falls_back_to_eager_on_a_hub_without_lazy_file() {
        // The silent failure this closes: `send_mode = "lazy"` against a v0.3
        // hub used to publish `file_offer` frames that hub ignored, so the user
        // saw "copied" and the file never moved. Eager is the pre-lazy
        // behaviour and still delivers.
        let policy = SendPolicy {
            mode: SendMode::Lazy,
            ..SendPolicy::default()
        };
        assert!(
            !policy.may_publish_offers(Caps::default()),
            "a hub with no capabilities cannot serve offers"
        );
        assert!(policy.may_publish_offers(Caps::LAZY_FILE));
    }

    #[tokio::test]
    async fn lazy_sending_streams_eagerly_when_the_hub_lacks_lazy_file() {
        // The behavioural half of the test above: it is not enough for the
        // predicate to say "no", the supervisor has to actually take the eager
        // path — chunks on the file channel, nothing on control.
        let dir = unique_dir("lazy-downgrade");
        let path = dir.join("fallback.bin");
        std::fs::write(&path, vec![7u8; 1024]).unwrap();

        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<ClipChange>(8);
        let (apply_tx, _apply_rx) = std::sync::mpsc::channel::<ClipChange>();
        let (transport, mut control_rx, mut files_rx, _caps_tx) =
            supervised_transport(Caps::default());

        let supervisor = tokio::spawn(run_supervisor_with_policy(
            Clipboard {
                events_rx,
                apply_tx,
            },
            transport,
            None,
            false,
            None,
            SendPolicy {
                mode: SendMode::Lazy,
                offer_ttl_ms: 60_000,
                ..SendPolicy::default()
            },
            ReceivePolicy::default(),
        ));

        events_tx
            .send(ClipChange::Files(vec![path.clone()]))
            .await
            .unwrap();

        let chunk = tokio::time::timeout(std::time::Duration::from_secs(5), files_rx.recv())
            .await
            .expect("lazy mode against a non-lazy hub must fall back to eager chunks")
            .expect("file channel open");
        assert_eq!(chunk.name, "fallback.bin");
        assert!(
            chunk.file_sha256.is_some(),
            "a fallback chunk is an EAGER chunk: it carries the whole-file hash"
        );
        assert!(chunk.transfer_id.is_none(), "an eager chunk has no session");
        assert!(
            control_rx.try_recv().is_err(),
            "no offer may be published to a hub that cannot serve it"
        );

        drop(events_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), supervisor).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn eager_sending_never_publishes_offers_even_when_the_hub_could_serve_them() {
        // The other direction: a capable hub must not turn an eager sender into
        // a lazy one. `mode` is the operator's choice; `hub_caps` only ever
        // *removes* a permission, never grants one.
        let policy = SendPolicy {
            mode: SendMode::Eager,
            ..SendPolicy::default()
        };
        assert!(
            !policy.may_publish_offers(Caps::LAZY_FILE),
            "eager mode must not publish offers even to a capable hub"
        );

        let dir = unique_dir("eager-capable-hub");
        let path = dir.join("eager.bin");
        std::fs::write(&path, vec![3u8; 512]).unwrap();

        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<ClipChange>(8);
        let (apply_tx, _apply_rx) = std::sync::mpsc::channel::<ClipChange>();
        let (transport, mut control_rx, mut files_rx, _caps_tx) =
            supervised_transport(Caps::LAZY_FILE);

        let supervisor = tokio::spawn(run_supervisor_with_policy(
            Clipboard {
                events_rx,
                apply_tx,
            },
            transport,
            None,
            false,
            None,
            SendPolicy {
                mode: SendMode::Eager,
                ..SendPolicy::default()
            },
            ReceivePolicy::default(),
        ));

        events_tx
            .send(ClipChange::Files(vec![path.clone()]))
            .await
            .unwrap();

        let chunk = tokio::time::timeout(std::time::Duration::from_secs(5), files_rx.recv())
            .await
            .expect("eager mode must stream chunks")
            .expect("file channel open");
        assert_eq!(chunk.name, "eager.bin");
        assert!(control_rx.try_recv().is_err(), "no offer in eager mode");

        drop(events_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), supervisor).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_hub_caps_change_is_visible_to_the_supervisor() {
        // `hub_caps` is a `watch` because a reconnect can reach a hub that
        // advertises something else. If the supervisor cached the value at
        // startup, the gate would keep using the old verdict for the life of
        // the process — publishing offers to a hub that has since been swapped
        // for one that cannot serve them.
        let dir = unique_dir("caps-change");
        let path = dir.join("later.bin");
        std::fs::write(&path, vec![9u8; 256]).unwrap();

        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<ClipChange>(8);
        let (apply_tx, _apply_rx) = std::sync::mpsc::channel::<ClipChange>();
        // Start on a hub that cannot serve offers.
        let (transport, mut control_rx, mut files_rx, caps_tx) =
            supervised_transport(Caps::default());

        let supervisor = tokio::spawn(run_supervisor_with_policy(
            Clipboard {
                events_rx,
                apply_tx,
            },
            transport,
            None,
            false,
            None,
            SendPolicy {
                mode: SendMode::Lazy,
                offer_ttl_ms: 60_000,
                ..SendPolicy::default()
            },
            ReceivePolicy::default(),
        ));

        // First copy: the empty caps force eager.
        events_tx
            .send(ClipChange::Files(vec![path.clone()]))
            .await
            .unwrap();
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(5), files_rx.recv())
            .await
            .expect("first copy falls back to eager")
            .expect("file channel open");
        assert_eq!(chunk.name, "later.bin");

        // The hub is now known to support lazy. The very next copy must take
        // the offer path, without a restart.
        caps_tx.send(Caps::LAZY_FILE).unwrap();
        events_tx
            .send(ClipChange::Files(vec![path.clone()]))
            .await
            .unwrap();

        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), control_rx.recv())
            .await
            .expect("the supervisor must pick up the new capabilities")
            .expect("control channel open");
        match frame {
            Frame::FileOffer(offer) => assert_eq!(offer.name, "later.bin"),
            other => panic!("expected file_offer after the caps change, got {other:?}"),
        }

        drop(events_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), supervisor).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn republishing_a_changed_file_supersedes_it_before_the_new_offer() {
        // Ordering matters on the wire: a peer that saw the new offer first
        // could pull a file whose id is about to be aborted.
        let dir = unique_dir("supersede");
        let path = dir.join("a.bin");
        std::fs::write(&path, b"v1").unwrap();

        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(8);
        let mut registry = offer::OfferRegistry::default();
        publish_offers(
            &mut registry,
            std::slice::from_ref(&path),
            60_000,
            &control_tx,
        );
        let first = match control_rx.try_recv().unwrap() {
            Frame::FileOffer(f) => f,
            other => panic!("expected offer, got {other:?}"),
        };

        std::fs::write(&path, b"v2, clearly different").unwrap();
        publish_offers(
            &mut registry,
            std::slice::from_ref(&path),
            60_000,
            &control_tx,
        );

        match control_rx.try_recv().expect("abort first") {
            Frame::FileAbort(abort) => {
                assert_eq!(abort.file_id, first.file_id);
                assert_eq!(abort.reason, "superseded");
                assert_eq!(abort.to, None, "offer-scoped: the hub expands it");
                assert_eq!(abort.transfer_id, None);
            }
            other => panic!("expected file_abort first, got {other:?}"),
        }
        match control_rx.try_recv().expect("then the new offer") {
            Frame::FileOffer(f) => assert_ne!(f.file_id, first.file_id),
            other => panic!("expected file_offer second, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lazy_mode_reports_skips_instead_of_publishing_them() {
        let dir = unique_dir("skips");
        let empty = dir.join("empty.txt");
        std::fs::write(&empty, b"").unwrap();
        let sub = dir.join("subdir");
        std::fs::create_dir_all(&sub).unwrap();

        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(8);
        let mut registry = offer::OfferRegistry::default();
        let skipped = publish_offers(
            &mut registry,
            &[empty.clone(), sub.clone()],
            60_000,
            &control_tx,
        );

        assert_eq!(skipped.len(), 2);
        assert_eq!(skipped[0].1, offer::SkipReason::ZeroBytes);
        assert_eq!(skipped[1].1, offer::SkipReason::Directory);
        assert!(
            control_rx.try_recv().is_err(),
            "a skipped path must produce no frame at all"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eager_mode_is_still_the_default_policy() {
        // 8.1 flips this. Until then a config that says nothing must behave
        // exactly like v0.5.4, which streams chunks.
        let config: ClientConfig =
            toml::from_str("server = \"ws://localhost/sync\"\nuser = \"u\"\npassword = \"p\"\n")
                .unwrap();
        let policy = SendPolicy::from_config(&config);
        assert_eq!(policy.mode, SendMode::Eager);
        assert_eq!(policy.offer_ttl_ms, 30 * 60_000);
    }

    #[test]
    fn the_policy_takes_the_ttl_from_the_config() {
        let config: ClientConfig = toml::from_str(
            "server = \"ws://localhost/sync\"\nuser = \"u\"\npassword = \"p\"\n\
             send_mode = \"lazy\"\noffer_ttl_minutes = 5\n",
        )
        .unwrap();
        let policy = SendPolicy::from_config(&config);
        assert_eq!(policy.mode, SendMode::Lazy);
        assert_eq!(policy.offer_ttl_ms, 5 * 60_000);
    }

    #[test]
    fn client_caps_follows_the_receive_mode_not_the_send_mode() {
        // `lazy-file` is a claim about *consuming* offers. `send_mode` says how
        // this client publishes and has no bearing on it.
        for (receive_mode, can_pull) in [("eager", false), ("pickup", true), ("paste", true)] {
            for send_mode in ["eager", "lazy"] {
                let raw = format!(
                    "server = \"ws://localhost/sync\"\nuser = \"u\"\npassword = \"p\"\n\
                     send_mode = \"{send_mode}\"\nreceive_mode = \"{receive_mode}\"\n"
                );
                let config: ClientConfig = toml::from_str(&raw).unwrap();
                let caps = client_caps(&config);
                assert_eq!(
                    caps.contains(crate::protocol::Caps::LAZY_FILE),
                    can_pull,
                    "receive_mode={receive_mode} send_mode={send_mode}"
                );
            }
        }
    }

    #[test]
    fn eager_mode_does_not_advertise_lazy_file() {
        // Not because "eager means we prefer streaming", but because the eager
        // receive path genuinely cannot consume an offer: it is the shared
        // `FileReceiver`, which rejects every chunk carrying `transfer_id`.
        // Advertising would have the hub deliver offers nothing answers.
        let config: ClientConfig =
            toml::from_str("server = \"ws://localhost/sync\"\nuser = \"u\"\npassword = \"p\"\n")
                .unwrap();
        let caps = client_caps(&config);
        assert!(
            !caps.contains(crate::protocol::Caps::LAZY_FILE),
            "an eager build has no engine behind the socket to answer an offer"
        );
        assert!(caps.is_empty());
    }

    #[test]
    fn a_pull_capable_mode_advertises_lazy_file() {
        let config: ClientConfig = toml::from_str(
            "server = \"ws://localhost/sync\"\nuser = \"u\"\npassword = \"p\"\n\
             receive_mode = \"pickup\"\n",
        )
        .unwrap();
        assert!(client_caps(&config).contains(crate::protocol::Caps::LAZY_FILE));
    }

    #[test]
    fn the_advertised_capability_and_the_engine_agree() {
        // The two must never disagree: advertising `lazy-file` while
        // `receives_can_pull` is false would strand every sender, and the
        // reverse would make this client unreachable as a receiver.
        for mode in [
            ReceiveMode::Eager,
            ReceiveMode::Auto,
            ReceiveMode::Pickup,
            ReceiveMode::Paste,
        ] {
            let policy = ReceivePolicy {
                mode,
                ..ReceivePolicy::default()
            };
            let raw = format!(
                "receive_mode = \"{}\"",
                match mode {
                    ReceiveMode::Eager => "eager",
                    ReceiveMode::Auto => "auto",
                    ReceiveMode::Pickup => "pickup",
                    ReceiveMode::Paste => "paste",
                }
            );
            let config: ClientConfig = toml::from_str(&format!(
                "server = \"ws://localhost/sync\"\nuser = \"u\"\npassword = \"p\"\n{raw}\n"
            ))
            .unwrap();
            assert_eq!(
                client_caps(&config).contains(crate::protocol::Caps::LAZY_FILE),
                policy.can_pull(),
                "{mode:?}"
            );
            assert_eq!(
                client_caps(&config).contains(crate::protocol::Caps::LAZY_FILE),
                !matches!(mode.resolved(), ReceiveMode::Eager),
                "{mode:?}"
            );
        }
    }

    #[test]
    fn done_summary_still_omits_the_percentage() {
        // The pre-existing rule; locked here so the terminal-state branch
        // cannot regress while adding Failed/Aborted.
        assert_eq!(
            progress(ProgressState::Done, Direction::Send).summary_line(24),
            "sent report.pdf"
        );
        assert_eq!(
            progress(ProgressState::Done, Direction::Receive).summary_line(24),
            "received report.pdf"
        );
    }
}

// SPDX-License-Identifier: GPL-3.0-or-later

//! `/sync` WebSocket handler.
//!
//! Lifecycle:
//! 1. The HTTP upgrade request is authenticated via HTTP Basic.
//! 2. A capacity permit is acquired from a shared semaphore.
//! 3. After upgrade we register with the hub, send the `welcome` frame, and
//!    split the socket into a reader task and a writer task.
//! 4. The reader parses inbound frames and forwards `clip`s to the hub.
//! 5. The writer multiplexes:
//!    - hub-relayed `clip` frames,
//!    - locally-generated `error` frames,
//!    - periodic WebSocket Pings every 30 s.
//! 6. On either task exiting, we deregister and the semaphore permit is
//!    dropped, freeing capacity.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::interval;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::protocol::{
    Caps, ClipFrame, ErrorCode, ErrorFrame, FileAbortFrame, FileChunkFrame, FilePullFrame, Frame,
    WelcomeFrame, WelcomePeer, PROTOCOL_VERSION,
};

use super::auth::check_basic_auth;
use super::config::ServerConfig;
use super::hub::{
    hub_caps, ControlMsg, HubHandle, OfferResult, PullResult, RegisterResult,
    PER_CLIENT_CHANNEL_BUF, PER_CLIENT_CONTROL_CHANNEL_BUF, PER_CLIENT_FILE_CHANNEL_BUF,
    PER_CLIENT_FILE_OVERFLOW, PER_CLIENT_FRAME_CHANNEL_BUF,
};

/// Application state passed to every request handler.
#[derive(Clone)]
pub struct AppState {
    pub hub: HubHandle,
    pub config: Arc<ServerConfig>,
    pub conn_sem: Arc<Semaphore>,
}

const PING_INTERVAL: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(90);

/// Frame types this build understands.
///
/// A `type` outside this list is from a later protocol revision and must be
/// **ignored**, not rejected: an un-upgraded hub must not kick a newer client
/// off the socket (PROTOCOL.md §3, design §4.6).
///
/// Keep this in one-to-one correspondence with the variants of [`Frame`]; the
/// `known_frame_types_match_frame_variants` test fails if the two drift.
const KNOWN_FRAME_TYPES: &[&str] = &[
    "clip",
    "welcome",
    "error",
    "file_chunk",
    "file_offer",
    "file_pull",
    "file_digest",
    "file_abort",
    "peer_changed",
];

/// How many out-of-order chunks one transfer may accumulate before the writer
/// gives up on ordering and emits the oldest one.
///
/// Bounded by the same budget as the overflow queue: beyond that, holding more
/// would just move the memory problem from the hub to the writer.
const MAX_REORDER_HOLD: usize = PER_CLIENT_FILE_OVERFLOW;

/// `(file_id, transfer_id)` — one lazy transfer.
type TransferKey = (Uuid, Uuid);

/// Emit `chunk` in `chunk_index` order for its transfer.
///
/// The hub has two outbound paths (`file` and its overflow), and two `mpsc`
/// queues give no cross-channel ordering. Worse, the naive "drain the primary
/// first" rule is itself wrong: as soon as the writer frees a slot, the hub can
/// put a *newer* chunk back into the primary path, and draining it first writes
/// that newer chunk ahead of an older one sitting in overflow.
///
/// So ordering is enforced here, per transfer, on exactly the quantity the
/// receiver cares about: `chunk_index` must strictly increase. Cross-transfer
/// order is untouched — different files assemble independently.
///
/// Returns the chunks that are now safe to write, in order. An empty result
/// means this chunk is being held until the gap before it is filled.
///
/// **Held chunks can be stranded.** If the sender stops mid-transfer, anything
/// in `pending` never gets a successor and is never written. That is deliberate:
/// the receiver's own no-progress timeout (3.8) fails the transfer, which is
/// strictly better than writing a gap and letting it assemble a corrupt file.
fn ordered_ready(
    next_expected: &mut HashMap<TransferKey, u32>,
    pending: &mut HashMap<TransferKey, BTreeMap<u32, FileChunkFrame>>,
    chunk: FileChunkFrame,
) -> Vec<FileChunkFrame> {
    // Eager (v0.3) chunks carry no session, and that path has no restart or
    // reorder mechanism to speak of. Reordering it would add state for the one
    // shape that predates sessions.
    let Some(transfer_id) = chunk.transfer_id else {
        return vec![chunk];
    };
    let key = (chunk.file_id, transfer_id);
    let index = chunk.chunk_index;

    let Some(expected) = next_expected.get(&key).copied() else {
        // First chunk of this transfer: nothing to order against yet.
        next_expected.insert(key, index.saturating_add(1));
        return vec![chunk];
    };

    if index < expected {
        // A duplicate. Re-sending it would move the receiver's assembly
        // backwards, so dropping is the lesser harm.
        warn!(
            file_id = %chunk.file_id,
            %transfer_id,
            index,
            expected,
            "dropping duplicate file_chunk"
        );
        return Vec::new();
    }

    if index > expected {
        let (out, next) = {
            let held = pending.entry(key).or_default();
            held.insert(index, chunk);
            if held.len() <= MAX_REORDER_HOLD {
                return Vec::new();
            }
            // Bounded buffering: rather than hold without limit, emit exactly
            // one chunk — the oldest held — and accept the one-time out-of-order
            // write. The rest stay held, so the next event continues from where
            // this one stopped instead of dumping the whole buffer at once.
            let oldest = *held.keys().next().expect("just inserted");
            warn!(
                %transfer_id,
                held = held.len(),
                limit = MAX_REORDER_HOLD,
                oldest,
                "reorder buffer full; emitting the oldest held chunk"
            );
            let chunk = held.remove(&oldest).expect("key came from the map");
            let next = oldest.saturating_add(1);
            (vec![chunk], next)
        };
        if pending.get(&key).is_some_and(BTreeMap::is_empty) {
            pending.remove(&key);
        }
        next_expected.insert(key, next);
        return out;
    }

    // Contiguous: emit it, then any held chunks that now follow on.
    let mut out = vec![chunk];
    let mut next = expected.saturating_add(1);
    let mut drained = false;
    if let Some(held) = pending.get_mut(&key) {
        while let Some(chunk) = held.remove(&next) {
            out.push(chunk);
            next = next.saturating_add(1);
        }
        drained = held.is_empty();
    }
    if drained {
        pending.remove(&key);
    }
    next_expected.insert(key, next);
    out
}

/// Build the abort that answers a refused pull. `None` when the pull succeeded.
///
/// Three things are easy to get wrong here, and all three are load-bearing:
///
/// - `to` is the **puller**, not `pull.to`. `FilePullFrame::to` names the
///   *source*; `FileAbortFrame::to` names the recipient. Sending `pull.to`
///   would address the abort to the source and the puller — filtering on `to`
///   as the spec requires — would drop it and wait for a timeout.
/// - `transfer_id` is echoed from the pull (spec §Transfer sessions), so a
///   receiver running two concurrent pastes of the same file can tell which one
///   failed.
/// - `from_device` is `None`: this frame is the hub's own answer, not the
///   source's. On `unknown_offer` the source may not exist at all, and on
///   `source_gone` it is by definition absent.
fn pull_refusal_abort(
    pull: &FilePullFrame,
    puller_device: Uuid,
    result: &PullResult,
) -> Option<FileAbortFrame> {
    let reason = match result {
        PullResult::Forward { .. } => return None,
        PullResult::UnknownOffer => "unknown_offer",
        PullResult::Expired => "expired",
        PullResult::SourceGone => "source_gone",
        PullResult::Busy => "busy",
        // A field-level refusal is `window_too_large` when the typed error names
        // that bound (the only case `validate()` can produce today), and
        // `invalid_start` otherwise — the session-level failure the protocol
        // reserves that name for.
        PullResult::Invalid(error) => error
            .abort_reason()
            .and_then(|reason| reason.to_wire())
            .unwrap_or("invalid_start"),
    };
    Some(FileAbortFrame {
        file_id: pull.file_id,
        transfer_id: Some(pull.transfer_id),
        to: Some(puller_device),
        from_device: None,
        reason: reason.to_string(),
    })
}

/// What the reader should do with one inbound text message.
#[derive(Debug)]
enum ReaderAction {
    /// A type this build does not know: a newer peer's frame. Drop it and keep
    /// the connection.
    Ignore,
    /// Refuse the frame and close the connection. `message` goes to the peer,
    /// `detail` to the log.
    Reject {
        code: ErrorCode,
        message: &'static str,
        detail: String,
    },
    /// A frame this build can relay. Boxed because framing types are large and
    /// the other variants are not.
    Forward(Box<Frame>),
}

/// Decide what to do with an inbound text frame, without touching the socket.
///
/// Split out from the reader so the forward-compatibility rule can be tested
/// directly: the behaviour that matters is "unknown `type` keeps the connection
/// open", and a real WebSocket makes that assertion slower and flakier than the
/// decision itself deserves.
///
/// Three outcomes, and the first two are deliberately *not* merged:
///
/// - a well-formed envelope with an unknown `type` → [`ReaderAction::Ignore`];
/// - anything whose `type` cannot be read at all (bad JSON, missing `type`, a
///   non-string `type`) → [`ReaderAction::Reject`]. An unidentifiable frame
///   cannot be assumed to be from the future, so it stays a bad frame.
/// - a frame this build knows → [`ReaderAction::Forward`]. Which of those the
///   reader can actually relay is its own decision: in Phase 3 the control
///   frames gain routing, and until then they hit the reader's fallback arm.
fn classify_inbound(text: &str) -> ReaderAction {
    let type_tag = serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(str::to_owned));

    if let Some(tag) = &type_tag {
        if !KNOWN_FRAME_TYPES.contains(&tag.as_str()) {
            return ReaderAction::Ignore;
        }
    }

    match serde_json::from_str::<Frame>(text) {
        // Server→client only. A client sending these is a protocol violation
        // and is still refused.
        Ok(Frame::Welcome(_)) | Ok(Frame::Error(_)) => ReaderAction::Reject {
            code: ErrorCode::BadFrame,
            message: "unexpected frame type from client",
            detail: "client sent a server-only frame".to_string(),
        },
        Ok(frame) => ReaderAction::Forward(Box::new(frame)),
        // Known `type` but fields that do not fit it, or no readable `type`.
        Err(e) => ReaderAction::Reject {
            code: ErrorCode::BadFrame,
            message: "malformed frame",
            detail: e.to_string(),
        },
    }
}

/// Connection parameters carried in the `/sync` query string.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UpgradeParams {
    /// `None` when the client sent no `device` parameter at all (pre-0.4).
    device: Option<Uuid>,
    caps: Caps,
}

/// Decode a `application/x-www-form-urlencoded` component.
///
/// `+` is a space in that encoding; a `%` not followed by two hex digits is
/// passed through literally rather than treated as an error, because the only
/// thing that can fail for us is a malformed `device` — and that is caught by
/// the UUID parse a moment later with a better message than "bad escape".
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                match (hi, lo) {
                    (Some(hi), Some(lo)) => {
                        out.push((hi * 16 + lo) as u8);
                        i += 3;
                    }
                    // Not a valid escape: keep the '%' and continue scanning
                    // from the next byte.
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    // Decoding arbitrary bytes can produce invalid UTF-8; a lossy conversion
    // keeps the parse total instead of failing the upgrade on one stray byte.
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse the `/sync` query string.
///
/// Rules (spec: *Device parameter compatibility*):
///
/// - no `device` → `None`, and the caller mints a per-connection id. Refusing
///   here would lock out every client deployed before 0.4.
/// - `device` present but not a UUID → `Err`. Falling back to an ephemeral id
///   would silently give a device a second identity.
/// - `caps` missing → no capabilities.
/// - a parameter given more than once → the first occurrence wins. Ordering is
///   not specified by the protocol, so rejecting would be a compatibility trap;
///   taking the first is deterministic and matches how browsers serialise.
fn parse_upgrade_params(query: Option<&str>) -> Result<UpgradeParams, &'static str> {
    let mut device_raw: Option<&str> = None;
    let mut caps_raw: Option<&str> = None;
    let mut device_seen = 0u32;
    let mut caps_seen = 0u32;

    for pair in query.unwrap_or("").split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            // A bare flag (`?caps`) is not a declared parameter form; ignore it
            // rather than treat it as an empty value, which would turn `?device`
            // into "device present but empty" and reject the upgrade.
            None => continue,
        };
        match key {
            "device" => {
                device_seen += 1;
                if device_raw.is_none() {
                    device_raw = Some(value);
                }
            }
            "caps" => {
                caps_seen += 1;
                if caps_raw.is_none() {
                    caps_raw = Some(value);
                }
            }
            // Forward compatibility at the query level too: a later revision may
            // add parameters, and an un-upgraded hub must still accept it.
            _ => {}
        }
    }

    let device = match device_raw {
        None => None,
        Some(raw) => {
            if device_seen > 1 {
                debug!(
                    occurrences = device_seen,
                    "duplicate device parameter; using the first"
                );
            }
            let decoded = percent_decode(raw);
            // Empty is a malformed value, not "absent": a client that meant to
            // use an ephemeral identity omits the parameter entirely.
            if decoded.is_empty() {
                return Err("device parameter is empty");
            }
            match Uuid::parse_str(&decoded) {
                Ok(id) => Some(id),
                Err(_) => return Err("device parameter is not a UUID"),
            }
        }
    };

    // MUST be the guarded entry point: `Caps::parse` has no length bound, so a
    // hostile peer could send a multi-megabyte `caps=` and have it parsed in
    // full. The guard treats an oversized value as "advertised nothing".
    let caps = match caps_raw {
        None => Caps::default(),
        Some(raw) => {
            if caps_seen > 1 {
                debug!(
                    occurrences = caps_seen,
                    "duplicate caps parameter; using the first"
                );
            }
            Caps::parse_guarded(&percent_decode(raw))
        }
    };

    Ok(UpgradeParams { device, caps })
}

/// Axum handler for `GET /sync`. Returns the upgrade response on success, or
/// 400 / 401 / 503 on failure.
pub async fn sync_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    // 1. Auth.
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if !check_basic_auth(auth, &state.config.user, &state.config.password) {
        debug!("rejecting upgrade: bad auth");
        return (
            StatusCode::UNAUTHORIZED,
            [(
                header::WWW_AUTHENTICATE,
                "Basic realm=\"clipboardwire\", charset=\"UTF-8\"",
            )],
            "",
        )
            .into_response();
    }

    // 2. Query parameters. Rejected before the upgrade so a client with a
    // broken `device` gets an HTTP status it can act on, and before a capacity
    // permit is taken.
    let params = match parse_upgrade_params(uri.query()) {
        Ok(p) => p,
        Err(reason) => {
            debug!(reason, "rejecting upgrade: bad query parameters");
            return (StatusCode::BAD_REQUEST, "invalid device parameter").into_response();
        }
    };

    // 3. Capacity. Hold the permit across the upgrade so the count is
    // bounded between the check and the registration.
    let permit = match state.conn_sem.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            warn!("rejecting upgrade: at capacity");
            return (StatusCode::SERVICE_UNAVAILABLE, "at capacity").into_response();
        }
    };

    // 4. Configure frame-size limits and complete the upgrade.
    let max_frame = state.config.max_frame_bytes;
    ws.max_message_size(max_frame)
        .max_frame_size(max_frame)
        .on_upgrade(move |socket| handle_socket(socket, state, permit, peer, params))
}

#[instrument(skip_all, fields(client = tracing::field::Empty, peer = %peer))]
async fn handle_socket(
    socket: WebSocket,
    state: AppState,
    _permit: tokio::sync::OwnedSemaphorePermit,
    peer: SocketAddr,
    params: UpgradeParams,
) {
    let client_id = Uuid::new_v4();

    // The device identity a peer advertises, or one minted for this connection
    // only. Pre-0.4 clients send none, so the fallback is what keeps them
    // working; it is deliberately *not* a refusal.
    let ephemeral_device = params.device.is_none();
    let device_id = params.device.unwrap_or_else(Uuid::new_v4);
    let caps = params.caps;
    // 3.4 registers these into the hub (`device_index`, caps broadcast).
    debug!(
        %device_id,
        ?caps,
        ephemeral = ephemeral_device,
        "connection identity resolved"
    );

    tracing::Span::current().record("client", tracing::field::display(client_id));
    // Both are carried for 3.4; logging them now is what makes the resolved
    // identity diagnosable while the hub still ignores it.
    info!(
        %device_id,
        ?caps,
        ephemeral = ephemeral_device,
        "connection opened"
    );

    let (clip_tx, mut clip_rx) = mpsc::channel::<ClipFrame>(PER_CLIENT_CHANNEL_BUF);
    let (file_tx, mut file_rx) = mpsc::channel::<FileChunkFrame>(PER_CLIENT_FILE_CHANNEL_BUF);
    // Second-level queue. The hub owns it in the only sense that matters: it
    // decides what goes in, while this writer is the sole consumer.
    let (overflow_tx, mut overflow_rx) = mpsc::channel::<FileChunkFrame>(PER_CLIENT_FILE_OVERFLOW);
    let (internal_tx, mut internal_rx) = mpsc::channel::<Frame>(8);
    // Routed non-chunk frames (`file_digest`, `file_abort`). Separate from the
    // chunk channel: different item type, and a close must never queue behind a
    // relayed frame, so `ctl` stays its own lane.
    let (frames_tx, mut frames_rx) = mpsc::channel::<Frame>(PER_CLIENT_FRAME_CHANNEL_BUF);
    // Control channel: how the hub asks this connection to close. It cannot
    // send a close code itself (no socket, and `Hub::handle` must not await).
    let (ctl_tx, mut ctl_rx) = mpsc::channel::<ControlMsg>(PER_CLIENT_CONTROL_CHANNEL_BUF);

    let registration = state
        .hub
        .register(
            client_id,
            device_id,
            caps,
            clip_tx,
            file_tx,
            overflow_tx,
            frames_tx,
            ctl_tx,
        )
        .await;
    let (last_clip, peers, offers) = match registration {
        Ok(RegisterResult::Accepted {
            last_clip,
            peers,
            offers,
        }) => (last_clip, peers, offers),
        Ok(RegisterResult::AtCapacity) | Err(_) => {
            // Should not happen: the semaphore bounds connections strictly
            // tighter than the hub. Defend anyway.
            warn!("hub refused registration; closing");
            close_with(socket, ErrorCode::DuplicateSession, "hub at capacity").await;
            return;
        }
    };

    let welcome = Frame::Welcome(WelcomeFrame {
        server: PROTOCOL_VERSION.to_string(),
        client_id,
        caps: hub_caps().tokens(),
        peers: peers
            .into_iter()
            .map(|(device, caps)| WelcomePeer {
                device,
                caps: caps.tokens(),
            })
            .collect(),
        offers,
        last_clip,
    });

    // Inject the welcome ahead of anything else.
    if internal_tx.send(welcome).await.is_err() {
        state.hub.deregister(client_id, device_id).await;
        return;
    }

    let (sink, mut stream) = socket.split();

    // Writer task: owns the sink, multiplexes the three sources.
    let writer_state = state.clone();
    let mut writer = tokio::spawn(async move {
        let mut sink = sink;
        let mut ping_timer = interval(PING_INTERVAL);
        // Per-transfer reordering state. Allocated empty: most connections never
        // see out-of-order chunks, so these stay empty for their whole life.
        let mut next_expected: HashMap<TransferKey, u32> = HashMap::new();
        let mut pending: HashMap<TransferKey, BTreeMap<u32, FileChunkFrame>> = HashMap::new();
        // Skip the immediate first tick so we don't ping on connect.
        ping_timer.tick().await;

        loop {
            tokio::select! {
                biased;

                // Highest priority: a close instruction is the one message that
                // must not queue behind traffic.
                //
                // Written as an explicit `match` over both `Option` and
                // `ControlMsg`, with no binding or wildcard arm: a `let ... else`
                // with a refutable pattern would treat *any* future variant as
                // "close", which is the wrong default for a control channel.
                // This shape makes adding a `ControlMsg` variant an E0004 here
                // until its behaviour is decided.
                maybe_ctl = ctl_rx.recv() => {
                    match maybe_ctl {
                        Some(ControlMsg::Close { code, reason }) => {
                            let _ = sink
                                .send(Message::Close(Some(CloseFrame {
                                    code,
                                    reason: reason.into(),
                                })))
                                .await;
                            break;
                        }
                        // Sender dropped: the hub actor is gone, so this
                        // connection has nothing left to serve.
                        None => break,
                    }
                }

                maybe_frame = internal_rx.recv() => {
                    let Some(frame) = maybe_frame else { break };
                    let json = match serde_json::to_string(&frame) {
                        Ok(s) => s,
                        Err(e) => { error!(error = %e, "serializing internal frame"); break }
                    };
                    if sink.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                    // If we just sent an Error frame, the protocol asks us
                    // to close the socket.
                    if let Frame::Error(_) = frame {
                        let _ = sink.close().await;
                        break;
                    }
                }

                maybe_clip = clip_rx.recv() => {
                    let Some(clip) = maybe_clip else { break };
                    let json = match serde_json::to_string(&Frame::Clip(clip)) {
                        Ok(s) => s,
                        Err(e) => { error!(error = %e, "serializing clip"); continue }
                    };
                    if sink.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }

                maybe_file = file_rx.recv() => {
                    let Some(chunk) = maybe_file else { break };
                    for ready in ordered_ready(&mut next_expected, &mut pending, chunk) {
                        let json = match serde_json::to_string(&Frame::FileChunk(ready)) {
                            Ok(s) => s,
                            Err(e) => { error!(error = %e, "serializing file_chunk"); continue }
                        };
                        if sink.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                }

                // Overflow chunks go through the same per-transfer ordering as
                // the primary path. Drain-before-write is *not* enough on its
                // own: once a write frees a slot the hub can put a newer chunk
                // back into `file_rx`, and draining it first would emit that
                // newer chunk ahead of an older overflowed one. `ordered_ready`
                // is what actually keeps `chunk_index` monotonic.
                maybe_overflow = overflow_rx.recv() => {
                    let Some(chunk) = maybe_overflow else { break };
                    for ready in ordered_ready(&mut next_expected, &mut pending, chunk) {
                        let json = match serde_json::to_string(&Frame::FileChunk(ready)) {
                            Ok(s) => s,
                            Err(e) => { error!(error = %e, "serializing overflow chunk"); continue }
                        };
                        if sink.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                }

                // Routed control frames (`file_digest`, `file_abort`). Without
                // this arm the hub could enqueue them and nothing would ever
                // reach the socket.
                maybe_frame = frames_rx.recv() => {
                    let Some(frame) = maybe_frame else { break };
                    let json = match serde_json::to_string(&frame) {
                        Ok(s) => s,
                        Err(e) => { error!(error = %e, "serializing routed frame"); continue }
                    };
                    if sink.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }

                _ = ping_timer.tick() => {
                    if sink.send(Message::Ping(Default::default())).await.is_err() {
                        break;
                    }
                }
            }
        }

        debug!("writer exiting");
        drop(writer_state);
    });

    // Reader task: parses inbound frames and either forwards `clip` to the
    // hub or emits an `error` and exits.
    let reader_hub = state.hub.clone();
    let reader_internal_tx = internal_tx.clone();
    // The puller's own device, needed to address refusals. `FilePullFrame::to`
    // is the source, so it cannot stand in for this.
    let reader_device = device_id;
    let mut reader = tokio::spawn(async move {
        loop {
            let next = tokio::time::timeout(READ_TIMEOUT, stream.next()).await;
            let msg = match next {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    debug!(error = %e, "ws read error");
                    break;
                }
                Ok(None) => {
                    debug!("ws stream ended");
                    break;
                }
                Err(_) => {
                    warn!("read timeout; closing");
                    break;
                }
            };

            match msg {
                Message::Text(s) => {
                    let frame = match classify_inbound(&s) {
                        ReaderAction::Ignore => {
                            // A `type` from a later revision. Ignoring (not
                            // closing) is the forward-compatibility contract:
                            // an un-upgraded hub must not kick a newer client
                            // off. See `KNOWN_FRAME_TYPES`.
                            debug!("ignoring inbound frame of unknown type");
                            continue;
                        }
                        ReaderAction::Reject {
                            code,
                            message,
                            detail,
                        } => {
                            debug!(detail, "rejecting inbound frame");
                            let _ = reader_internal_tx
                                .send(Frame::Error(ErrorFrame {
                                    code,
                                    message: message.into(),
                                }))
                                .await;
                            break;
                        }
                        ReaderAction::Forward(frame) => frame,
                    };

                    match *frame {
                        Frame::Clip(mut clip) => {
                            // The hub is content-type agnostic — v0.2 added image
                            // payloads and reserves additional types for future
                            // revisions. We only validate that the frame carries
                            // exactly one of `content` / `content_b64` matching
                            // its content_type.
                            if let Err(reason) = clip.validate() {
                                debug!(reason, "rejecting bad clip frame");
                                let _ = reader_internal_tx
                                    .send(Frame::Error(ErrorFrame {
                                        code: ErrorCode::BadFrame,
                                        message: reason.into(),
                                    }))
                                    .await;
                                break;
                            }
                            // Clients should not set `from`; reset it before
                            // forwarding.
                            clip.from = None;
                            if reader_hub.publish(client_id, clip).await.is_err() {
                                break;
                            }
                        }
                        Frame::FileChunk(mut chunk) => {
                            if let Err(reason) = chunk.validate() {
                                debug!(reason, "rejecting bad file_chunk");
                                let _ = reader_internal_tx
                                    .send(Frame::Error(ErrorFrame {
                                        code: ErrorCode::BadFrame,
                                        message: reason.into(),
                                    }))
                                    .await;
                                break;
                            }
                            chunk.from = None;
                            // `from_device` is client-supplied and
                            // unauthenticated. Clearing it here means the hub
                            // is the only writer, and a peer cannot attribute
                            // its file to another device.
                            chunk.from_device = None;
                            if reader_hub.publish_file(client_id, chunk).await.is_err() {
                                break;
                            }
                        }
                        Frame::FileOffer(mut offer) => {
                            // Client-supplied `from_device` is unauthenticated;
                            // the hub stamps its own value from the registry.
                            offer.from_device = None;
                            if offer.validate().is_err() {
                                // Field-level refusal: answers with `error` and
                                // keeps the connection (spec: rejecting an offer
                                // must not close the sender).
                                let _ = reader_internal_tx
                                    .send(Frame::Error(ErrorFrame {
                                        code: ErrorCode::BadFrame,
                                        message: "malformed file_offer".into(),
                                    }))
                                    .await;
                                continue;
                            }
                            match reader_hub.publish_offer(client_id, offer).await {
                                Ok(OfferResult::Accepted) => {}
                                Ok(verdict) => {
                                    // Caps and expiry are the sender's mistake,
                                    // not a protocol violation: report and stay
                                    // connected.
                                    debug!(?verdict, "offer refused");
                                    let _ = reader_internal_tx
                                        .send(Frame::Error(ErrorFrame {
                                            code: ErrorCode::BadFrame,
                                            message: format!("offer refused: {verdict:?}"),
                                        }))
                                        .await;
                                }
                                Err(_) => break,
                            }
                        }
                        Frame::FilePull(pull) => {
                            // Keep a copy for the refusal: the reply has to echo
                            // this pull's `transfer_id` and be addressed to the
                            // puller, neither of which survives `PullResult`.
                            let refusal_source = pull.clone();
                            match reader_hub.publish_pull(client_id, pull).await {
                                Ok(PullResult::Forward { .. }) => {}
                                Ok(result) => {
                                    debug!(?result, "pull refused");
                                    if let Some(abort) =
                                        pull_refusal_abort(&refusal_source, reader_device, &result)
                                    {
                                        let _ =
                                            reader_internal_tx.send(Frame::FileAbort(abort)).await;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        Frame::FileDigest(mut digest) => {
                            digest.from_device = None;
                            if digest.validate().is_err() {
                                let _ = reader_internal_tx
                                    .send(Frame::Error(ErrorFrame {
                                        code: ErrorCode::BadFrame,
                                        message: "malformed file_digest".into(),
                                    }))
                                    .await;
                                continue;
                            }
                            match reader_hub.publish_digest(client_id, digest).await {
                                Ok(_) => {}
                                Err(_) => break,
                            }
                        }
                        Frame::FileAbort(mut abort) => {
                            abort.from_device = None;
                            if abort.validate().is_err() {
                                let _ = reader_internal_tx
                                    .send(Frame::Error(ErrorFrame {
                                        code: ErrorCode::BadFrame,
                                        message: "malformed file_abort".into(),
                                    }))
                                    .await;
                                continue;
                            }
                            match reader_hub.publish_abort(client_id, abort).await {
                                Ok(_) => {}
                                Err(_) => break,
                            }
                        }
                        // Hub→client only: a peer must not forge membership
                        // notices. `classify_inbound` lets it through (it is a
                        // known type), so it is refused here.
                        Frame::PeerChanged(_) => {
                            debug!("client sent peer_changed; refusing");
                            let _ = reader_internal_tx
                                .send(Frame::Error(ErrorFrame {
                                    code: ErrorCode::BadFrame,
                                    message: "unexpected frame type from client".into(),
                                }))
                                .await;
                            break;
                        }
                        // Server→client only: `classify_inbound` rejects these
                        // before they can reach the reader. They are listed
                        // explicitly so this match stays exhaustive without a
                        // catch-all, which is what keeps a newly added `Frame`
                        // variant a compile error here.
                        Frame::Welcome(_) | Frame::Error(_) => {
                            debug!("server-only frame reached the reader; refusing");
                            let _ = reader_internal_tx
                                .send(Frame::Error(ErrorFrame {
                                    code: ErrorCode::BadFrame,
                                    message: "unexpected frame type from client".into(),
                                }))
                                .await;
                            break;
                        }
                    }
                }
                Message::Binary(_) => {
                    let _ = reader_internal_tx
                        .send(Frame::Error(ErrorFrame {
                            code: ErrorCode::BadFrame,
                            message: "binary WebSocket frames are not supported".into(),
                        }))
                        .await;
                    break;
                }
                Message::Ping(_) | Message::Pong(_) => {
                    // Pings are auto-responded by the WebSocket layer;
                    // pongs reset our deadline via the timeout reset on the
                    // next loop iteration. Either way we just continue.
                }
                Message::Close(_) => {
                    debug!("peer sent Close");
                    break;
                }
            }
        }

        debug!("reader exiting");
    });

    // Wait for either side to end, then tear the other down — the
    // surviving half still owns its share of the socket, so without
    // the abort a half-dead session lingers as a live-but-silent
    // connection (e.g. hub actor killed on reload → writer exits on
    // channel close → reader would otherwise sit on `stream.next()`
    // holding the socket open until READ_TIMEOUT).
    tokio::select! {
        _ = &mut reader => writer.abort(),
        _ = &mut writer => reader.abort(),
    }
    state.hub.deregister(client_id, device_id).await;
    info!("connection closed");
}

async fn close_with(socket: WebSocket, code: ErrorCode, msg: &str) {
    let mut socket = socket;
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: code.close_code(),
            reason: msg.to_string().into(),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ControlFrameError, MAX_CAPS_BYTES};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        let (hub, _join) = crate::server::hub::spawn(8);
        AppState {
            hub,
            config: Arc::new(ServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                user: "alice".into(),
                password: "hunter2".into(),
                max_conns: 8,
                max_frame_bytes: 1024,
                tls_cert_file: None,
                tls_key_file: None,
                tls_disabled: true,
                state_dir: None,
                stats: None,
            }),
            conn_sem: Arc::new(Semaphore::new(8)),
        }
    }

    // Real WebSocket handshake / auth flows are covered by the integration
    // test in `tests/end_to_end.rs` (task #9). Unit tests here cover only
    // request paths that the WebSocketUpgrade extractor doesn't gate.

    fn action(text: &str) -> ReaderAction {
        classify_inbound(text)
    }

    // ---- Pull refusals (3.9 fix) ----

    fn test_pull(source_device: Uuid) -> FilePullFrame {
        FilePullFrame {
            file_id: Uuid::new_v4(),
            transfer_id: Uuid::new_v4(),
            to: source_device,
            chunk_index: 0,
            window: 4,
            from_device: None,
        }
    }

    const REFUSALS: [PullResult; 5] = [
        PullResult::UnknownOffer,
        PullResult::Expired,
        PullResult::SourceGone,
        PullResult::Busy,
        PullResult::Invalid(ControlFrameError::EmptyName),
    ];

    #[test]
    fn a_refused_pull_is_answered_at_the_puller_not_the_source() {
        // `pull.to` names the source; the abort must name the puller, or a
        // receiver filtering on `to` drops it and waits for a timeout.
        let source = Uuid::new_v4();
        let puller = Uuid::new_v4();
        assert_ne!(source, puller);
        let pull = test_pull(source);

        for result in REFUSALS {
            let abort = pull_refusal_abort(&pull, puller, &result)
                .unwrap_or_else(|| panic!("{result:?} must produce an abort"));
            assert_eq!(abort.to, Some(puller), "{result:?}");
            assert_ne!(abort.to, Some(pull.to), "{result:?}: addressed the source");
            // The hub answers these itself; the source took no part.
            assert_eq!(abort.from_device, None, "{result:?}");
        }
    }

    #[test]
    fn a_refused_pull_echoes_the_pulls_transfer_id() {
        // Two concurrent pastes of the same file must be distinguishable.
        let pull = test_pull(Uuid::new_v4());
        for result in REFUSALS {
            let abort = pull_refusal_abort(&pull, Uuid::new_v4(), &result).unwrap();
            assert_eq!(
                abort.transfer_id,
                Some(pull.transfer_id),
                "{result:?} dropped the session id"
            );
            assert_eq!(abort.file_id, pull.file_id, "{result:?}");
        }
    }

    #[test]
    fn each_refusal_reason_maps_to_its_wire_string() {
        let pull = test_pull(Uuid::new_v4());
        let puller = Uuid::new_v4();
        for (result, expected) in [
            (PullResult::UnknownOffer, "unknown_offer"),
            (PullResult::Expired, "expired"),
            (PullResult::SourceGone, "source_gone"),
            (PullResult::Busy, "busy"),
            // A field error with no wire reason falls back to the
            // session-level name rather than inventing one.
            (
                PullResult::Invalid(ControlFrameError::EmptyName),
                "invalid_start",
            ),
        ] {
            let abort = pull_refusal_abort(&pull, puller, &result).unwrap();
            assert_eq!(abort.reason, expected, "{result:?}");
        }
        // A successful pull produces no answer of its own.
        assert!(pull_refusal_abort(
            &pull,
            puller,
            &PullResult::Forward {
                source_client: Uuid::new_v4()
            }
        )
        .is_none());
    }

    #[test]
    fn a_window_too_large_pull_is_refused_with_window_too_large() {
        // The protocol names this bound explicitly, so it must not be
        // flattened into the generic `invalid_start`.
        let pull = test_pull(Uuid::new_v4());
        let abort = pull_refusal_abort(
            &pull,
            Uuid::new_v4(),
            &PullResult::Invalid(ControlFrameError::WindowTooLarge),
        )
        .expect("a window refusal must be answered");
        assert_eq!(abort.reason, "window_too_large");
    }

    #[test]
    fn a_zero_window_is_also_window_too_large() {
        // `abort_reason` merges the two window offences, which is right: a
        // zero window is the same violation from the receiver's point of view.
        let pull = test_pull(Uuid::new_v4());
        let abort = pull_refusal_abort(
            &pull,
            Uuid::new_v4(),
            &PullResult::Invalid(ControlFrameError::EmptyWindow),
        )
        .expect("a zero-window refusal must be answered");
        assert_eq!(abort.reason, "window_too_large");
    }

    #[test]
    fn a_field_error_without_a_wire_reason_falls_back_to_invalid_start() {
        // Headroom for a future session-level starting-index check: no honest
        // wire reason exists today, so the session-level name is used rather
        // than inventing one.
        let pull = test_pull(Uuid::new_v4());
        let abort = pull_refusal_abort(
            &pull,
            Uuid::new_v4(),
            &PullResult::Invalid(ControlFrameError::EmptyName),
        )
        .expect("a field refusal must be answered");
        assert_eq!(abort.reason, "invalid_start");
    }

    #[tokio::test]
    async fn an_out_of_range_window_really_produces_window_too_large_end_to_end() {
        // The whole chain: a real `PublishPull` through a spawned hub, whose
        // `Authorize` verdict is mapped by this module's refusal builder. Unit
        // tests on either side alone cannot catch a change that loses the
        // window variant in transit.
        let (hub, _join) = crate::server::hub::spawn(8);
        let client = Uuid::new_v4();
        let device = Uuid::new_v4();
        let (clip_tx, _clip_rx) = mpsc::channel(1);
        let (file_tx, _file_rx) = mpsc::channel(1);
        let (overflow_tx, _overflow_rx) = mpsc::channel(1);
        let (frames_tx, _frames_rx) = mpsc::channel(1);
        let (ctl_tx, _ctl_rx) = mpsc::channel(1);
        hub.register(
            client,
            device,
            Caps::default(),
            clip_tx,
            file_tx,
            overflow_tx,
            frames_tx,
            ctl_tx,
        )
        .await
        .expect("hub must accept a registration");

        for window in [0u32, 33u32] {
            let mut pull = test_pull(Uuid::new_v4());
            pull.window = window;
            let result = hub
                .publish_pull(client, pull.clone())
                .await
                .expect("hub must answer every pull");
            let abort = pull_refusal_abort(&pull, device, &result)
                .expect("a refused pull must produce an abort");
            assert_eq!(abort.reason, "window_too_large", "window={window}");
        }
    }

    // ---- Per-transfer chunk ordering (3.7b fix) ----

    fn chunk_of(transfer: TransferKey, index: u32) -> FileChunkFrame {
        FileChunkFrame {
            file_id: transfer.0,
            name: "payload.bin".to_string(),
            content_type: "application/octet-stream".to_string(),
            chunk_index: index,
            total_chunks: 32,
            file_sha256: None,
            file_size: 32,
            payload_b64: "AAEC".to_string(),
            from: None,
            to: Some(Uuid::new_v4()),
            transfer_id: Some(transfer.1),
            from_device: None,
        }
    }

    fn indices(chunks: &[FileChunkFrame]) -> Vec<u32> {
        chunks.iter().map(|c| c.chunk_index).collect()
    }

    #[test]
    fn chunks_are_released_in_index_order_when_the_newer_arrives_first() {
        // The exact window the hub cannot prevent: index 2 arrives before 1,
        // because 1 was still sitting in the primary queue when 2 overflowed.
        let mut next = HashMap::new();
        let mut pending = HashMap::new();
        let t = (Uuid::new_v4(), Uuid::new_v4());

        assert_eq!(
            indices(&ordered_ready(&mut next, &mut pending, chunk_of(t, 0))),
            vec![0]
        );
        // Held: 1 has not arrived.
        assert!(ordered_ready(&mut next, &mut pending, chunk_of(t, 2)).is_empty());
        // 1 closes the gap and releases both, in order.
        assert_eq!(
            indices(&ordered_ready(&mut next, &mut pending, chunk_of(t, 1))),
            vec![1, 2]
        );
        assert_eq!(
            indices(&ordered_ready(&mut next, &mut pending, chunk_of(t, 3))),
            vec![3]
        );
        assert!(pending.is_empty(), "the held map is not left growing");
    }

    #[test]
    fn a_duplicate_chunk_is_dropped_not_rewritten() {
        let mut next = HashMap::new();
        let mut pending = HashMap::new();
        let t = (Uuid::new_v4(), Uuid::new_v4());

        assert_eq!(
            indices(&ordered_ready(&mut next, &mut pending, chunk_of(t, 0))),
            vec![0]
        );
        assert_eq!(
            indices(&ordered_ready(&mut next, &mut pending, chunk_of(t, 1))),
            vec![1]
        );
        // Re-sending 1 must not move the receiver's assembly backwards.
        assert!(ordered_ready(&mut next, &mut pending, chunk_of(t, 1)).is_empty());
        assert!(pending.is_empty());
    }

    #[test]
    fn an_eager_chunk_bypasses_reordering() {
        // v0.3 compatibility: no session, so there is nothing to order against.
        let mut next = HashMap::new();
        let mut pending = HashMap::new();
        let mut chunk = chunk_of((Uuid::new_v4(), Uuid::new_v4()), 7);
        chunk.transfer_id = None;

        assert_eq!(
            indices(&ordered_ready(&mut next, &mut pending, chunk)),
            vec![7]
        );
        assert!(next.is_empty(), "the eager path keeps no state");
        assert!(pending.is_empty());
    }

    #[test]
    fn a_stalled_gap_is_bounded_by_dropping_the_oldest_held_chunk() {
        // A sender that stops mid-transfer would otherwise let `pending` grow
        // without bound. Past the limit, the writer gives up on ordering and
        // emits from the oldest held index.
        let mut next = HashMap::new();
        let mut pending = HashMap::new();
        let t = (Uuid::new_v4(), Uuid::new_v4());

        assert_eq!(
            indices(&ordered_ready(&mut next, &mut pending, chunk_of(t, 0))),
            vec![0]
        );

        // 2..=10 is nine chunks; the limit is eight.
        let mut released = Vec::new();
        for index in 2..=10u32 {
            let out = ordered_ready(&mut next, &mut pending, chunk_of(t, index));
            if !out.is_empty() {
                released = indices(&out);
            }
        }
        assert_eq!(
            released,
            vec![2],
            "exactly the oldest held chunk is emitted to break the stall"
        );
        // The other eight are still held: the degrade releases one chunk, it
        // does not dump the buffer.
        assert_eq!(
            pending.get(&t).map(|held| held.len()),
            Some(MAX_REORDER_HOLD)
        );
        assert_eq!(next.get(&t).copied(), Some(3));
    }

    #[test]
    fn two_transfers_are_reordered_independently() {
        let mut next = HashMap::new();
        let mut pending = HashMap::new();
        let a = (Uuid::new_v4(), Uuid::new_v4());
        let b = (Uuid::new_v4(), Uuid::new_v4());
        assert_ne!(a, b);

        // Both start at 0, then each has a hole at 1.
        ordered_ready(&mut next, &mut pending, chunk_of(a, 0));
        ordered_ready(&mut next, &mut pending, chunk_of(b, 0));
        assert!(ordered_ready(&mut next, &mut pending, chunk_of(a, 2)).is_empty());
        assert!(ordered_ready(&mut next, &mut pending, chunk_of(b, 2)).is_empty());

        // Filling A's hole releases only A's chunks.
        let out = ordered_ready(&mut next, &mut pending, chunk_of(a, 1));
        assert_eq!(indices(&out), vec![1, 2]);
        assert!(out.iter().all(|c| c.file_id == a.0));
        // B is untouched and still waiting.
        assert_eq!(pending.get(&b).map(|held| held.len()), Some(1));
    }

    fn params(query: Option<&str>) -> UpgradeParams {
        parse_upgrade_params(query).expect("query should be accepted")
    }

    #[test]
    fn missing_device_yields_an_absent_device() {
        // Pre-0.4 clients send no `device` at all. They must still be accepted:
        // the hub mints a per-connection identity instead.
        assert_eq!(params(None).device, None);
        assert_eq!(params(Some("")).device, None);
        assert!(params(None).caps.is_empty());
    }

    #[test]
    fn a_valid_device_parameter_is_parsed() {
        let id = Uuid::new_v4();
        let p = params(Some(&format!("device={id}")));
        assert_eq!(p.device, Some(id));
    }

    #[test]
    fn caps_are_parsed_through_the_guarded_parser() {
        let p = params(Some("caps=lazy-file"));
        assert!(p.caps.contains(Caps::LAZY_FILE));
        assert!(!p.caps.contains(Caps::FILE_RECV));
    }

    #[test]
    fn device_and_caps_parse_together() {
        let id = Uuid::new_v4();
        let p = params(Some(&format!("caps=lazy-file,transfer-id&device={id}")));
        assert_eq!(p.device, Some(id));
        assert!(p.caps.contains(Caps::LAZY_FILE));
        assert!(p.caps.contains(Caps::TRANSFER_ID));
    }

    #[test]
    fn an_invalid_device_is_rejected() {
        // Falling back to an ephemeral id here would silently give the device a
        // second identity, so a malformed value fails the upgrade instead.
        assert!(parse_upgrade_params(Some("device=not-a-uuid")).is_err());
        assert!(parse_upgrade_params(Some("device=")).is_err());
        // URLs in the wild are percent-encoded; a differently-encoded valid
        // UUID must still be accepted, not rejected for its encoding.
        let id = Uuid::new_v4();
        let encoded = id.to_string().replace('-', "%2D");
        assert_eq!(
            parse_upgrade_params(Some(&format!("device={encoded}")))
                .expect("percent-encoded UUID is still a UUID")
                .device,
            Some(id)
        );
    }

    #[test]
    fn oversized_caps_uses_the_guarded_parser_not_the_plain_one() {
        // Exactly at the limit: parsed. One byte over: the whole value is
        // ignored. If this ever called `Caps::parse` directly, the 513-byte case
        // would still yield LAZY_FILE and this assertion would fail.
        let padded = format!("caps=lazy-file,{}", "x".repeat(MAX_CAPS_BYTES - 10));
        assert_eq!(padded.len(), "caps=".len() + MAX_CAPS_BYTES);
        assert!(
            params(Some(&padded)).caps.contains(Caps::LAZY_FILE),
            "a value at the byte limit is still parsed"
        );

        let over = format!("caps=lazy-file,{}", "x".repeat(MAX_CAPS_BYTES - 9));
        assert_eq!(over.len(), "caps=".len() + MAX_CAPS_BYTES + 1);
        assert!(
            params(Some(&over)).caps.is_empty(),
            "one byte over the limit drops the whole value, even with a known token in front"
        );
        // The plain parser would still find LAZY_FILE in the 513-byte value;
        // asserting that here pins down which entry point is in use.
        assert!(
            Caps::parse(&over["caps=".len()..]).contains(Caps::LAZY_FILE),
            "Caps::parse is unbounded, so the drop above can only come from the guard"
        );
    }

    #[test]
    fn duplicate_parameters_take_the_first_occurrence() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let p = params(Some(&format!("device={first}&device={second}")));
        assert_eq!(p.device, Some(first), "the first value wins");

        let p = params(Some("caps=lazy-file&caps=transfer-id"));
        assert!(p.caps.contains(Caps::LAZY_FILE));
        assert!(
            !p.caps.contains(Caps::TRANSFER_ID),
            "the second caps value is ignored, not merged"
        );
    }

    #[test]
    fn unknown_query_parameters_are_ignored() {
        let id = Uuid::new_v4();
        let p = params(Some(&format!("unknown=1&device={id}&other=x")));
        assert_eq!(p.device, Some(id));
        assert!(parse_upgrade_params(Some("unknown=1")).is_ok());
    }

    #[test]
    fn percent_decoding_handles_escapes_and_stray_input() {
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("a+b"), "a b");
        // A malformed escape is passed through rather than failing the upgrade;
        // the UUID parse is what rejects a truly broken `device`.
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }

    #[test]
    fn unknown_frame_type_is_ignored_and_does_not_close() {
        // Regression for task 3.1. Before the split, an unknown `type` failed
        // inside `serde_json::from_str::<Frame>`, landed in the `Err` arm, and
        // the hub answered `bad_frame` and closed the socket — so a newer
        // client was kicked off by an older hub, contrary to PROTOCOL.md §3.
        for text in [
            r#"{"type":"some_future_frame","x":1}"#,
            r#"{"type":"file_offer_v2"}"#,
            r#"{"type":"peer_changed_v2","device":"00000000-0000-0000-0000-000000000000"}"#,
        ] {
            assert!(
                matches!(action(text), ReaderAction::Ignore),
                "unknown type must be ignored, not rejected: {text}"
            );
        }
    }

    #[test]
    fn known_relayable_frames_are_forwarded() {
        let clip = serde_json::to_string(&Frame::Clip(ClipFrame {
            id: Uuid::nil(),
            ts: 1,
            content_type: crate::protocol::TEXT_CONTENT_TYPE.to_string(),
            content: Some("hi".into()),
            content_b64: None,
            from: None,
        }))
        .unwrap();
        assert!(matches!(action(&clip), ReaderAction::Forward(_)));

        // A control frame parses and is forwarded by the classifier; the
        // reader's own arm decides it cannot relay it yet.
        let pull = serde_json::to_string(&Frame::FilePull(crate::protocol::FilePullFrame {
            file_id: Uuid::nil(),
            transfer_id: Uuid::new_v4(),
            to: Uuid::new_v4(),
            chunk_index: 0,
            window: 4,
            from_device: None,
        }))
        .unwrap();
        match action(&pull) {
            ReaderAction::Forward(f) => assert!(matches!(*f, Frame::FilePull(_))),
            other => panic!("file_pull should classify as Forward, got {other:?}"),
        }
    }

    #[test]
    fn server_only_frames_are_still_rejected() {
        let welcome = serde_json::to_string(&Frame::Welcome(WelcomeFrame {
            server: "hub".into(),
            client_id: Uuid::nil(),
            caps: Vec::new(),
            peers: Vec::new(),
            offers: Vec::new(),
            last_clip: None,
        }))
        .unwrap();
        match action(&welcome) {
            ReaderAction::Reject { code, message, .. } => {
                assert_eq!(code, ErrorCode::BadFrame);
                assert_eq!(message, "unexpected frame type from client");
            }
            other => panic!("welcome must stay rejected, got {other:?}"),
        }

        let error = serde_json::to_string(&Frame::Error(ErrorFrame {
            code: ErrorCode::BadFrame,
            message: "x".into(),
        }))
        .unwrap();
        assert!(matches!(action(&error), ReaderAction::Reject { .. }));
    }

    #[test]
    fn unidentifiable_frames_are_rejected_not_ignored() {
        // An unknown *type* is a future revision and is ignored. Anything whose
        // type cannot be read is not entitled to that assumption, so it stays a
        // bad frame — otherwise malformed input would silently disappear.
        for text in [
            "not json at all",
            r#"{"no_type":1}"#,
            r#"{"type":42}"#,
            r#"{"type":null}"#,
            "[]",
        ] {
            match action(text) {
                ReaderAction::Reject { code, .. } => {
                    assert_eq!(code, ErrorCode::BadFrame, "{text}")
                }
                other => panic!("{text} must be rejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn known_type_with_invalid_fields_is_rejected() {
        // A known type clears the "unknown, ignore it" check, so a body that
        // does not fit it must fall through to `malformed frame` rather than
        // being dropped as a future frame.
        //
        // Field-level rules (`window: 0`, empty name) are enforced later by each
        // frame's `validate()`, not by deserialisation; this test covers the
        // deserialisation layer, which is what `classify_inbound` owns.
        for text in [
            // `window` must be an integer.
            r#"{"type":"file_pull","file_id":"00000000-0000-0000-0000-000000000000","transfer_id":"00000000-0000-0000-0000-000000000000","to":"00000000-0000-0000-0000-000000000000","chunk_index":0,"window":"four"}"#,
            // `file_id` is required.
            r#"{"type":"file_pull","transfer_id":"00000000-0000-0000-0000-000000000000","to":"00000000-0000-0000-0000-000000000000","chunk_index":0,"window":4}"#,
            // A known type with an empty body.
            r#"{"type":"file_chunk"}"#,
        ] {
            match action(text) {
                ReaderAction::Reject { code, message, .. } => {
                    assert_eq!(code, ErrorCode::BadFrame, "{text}");
                    assert_eq!(message, "malformed frame", "{text}");
                }
                other => panic!("{text} must be rejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn reader_match_is_exhaustive_over_forwarded_frames() {
        // The five control frames parse successfully, so `classify_inbound`
        // returns `Forward` for them and they reach the reader's explicit
        // control-frame arm, which refuses them until Phase 3 routes them.
        //
        // This test guards the *other* direction: if someone later changes one
        // of these to `Ignore`, the frame is silently dropped instead of
        // refused — a worse failure than a refusal, and one the compile-time
        // exhaustiveness check cannot see.
        let control_frames = [
            Frame::FileOffer(crate::protocol::FileOfferFrame {
                file_id: Uuid::nil(),
                batch_id: Uuid::nil(),
                ts: 0,
                name: "a.bin".into(),
                content_type: "application/octet-stream".into(),
                file_size: 1,
                expires_at_ms: 1,
                from_device: None,
            }),
            Frame::FilePull(crate::protocol::FilePullFrame {
                file_id: Uuid::nil(),
                transfer_id: Uuid::new_v4(),
                to: Uuid::new_v4(),
                chunk_index: 0,
                window: 4,
                from_device: None,
            }),
            Frame::FileDigest(crate::protocol::FileDigestFrame {
                file_id: Uuid::nil(),
                transfer_id: Uuid::new_v4(),
                to: Uuid::new_v4(),
                from_device: None,
                file_sha256: "AA:BB".into(),
            }),
            Frame::FileAbort(crate::protocol::FileAbortFrame {
                file_id: Uuid::nil(),
                transfer_id: None,
                to: None,
                from_device: None,
                reason: "busy".into(),
            }),
            Frame::PeerChanged(crate::protocol::PeerChangedFrame {
                device: Uuid::new_v4(),
                caps: vec!["lazy-file".into()],
                event: crate::protocol::PeerEvent::Joined,
            }),
        ];
        for frame in &control_frames {
            let text = serde_json::to_string(frame).unwrap();
            match action(&text) {
                ReaderAction::Forward(f) => assert_eq!(
                    std::mem::discriminant(&*f),
                    std::mem::discriminant(frame),
                    "classifier must forward the frame it parsed"
                ),
                other => panic!(
                    "{} must be Forward (and refused by the reader), got {other:?}",
                    text
                ),
            }
        }
        assert_eq!(
            control_frames.len(),
            5,
            "one sample per non-relayable control frame"
        );
    }

    #[test]
    fn known_frame_types_match_frame_variants() {
        // Guards two failure modes at once: an empty/duplicate entry, and the
        // list drifting away from `Frame` (a new variant whose type is missing
        // here would be silently ignored instead of relayed).
        let mut seen: Vec<&str> = KNOWN_FRAME_TYPES.to_vec();
        let count = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), count, "KNOWN_FRAME_TYPES has duplicates");
        assert!(
            KNOWN_FRAME_TYPES.iter().all(|t| !t.is_empty()),
            "KNOWN_FRAME_TYPES must not contain an empty string"
        );

        // Serialise one instance of every variant and check its tag is listed.
        let samples = [
            Frame::Clip(ClipFrame {
                id: Uuid::nil(),
                ts: 0,
                content_type: crate::protocol::TEXT_CONTENT_TYPE.to_string(),
                content: Some(String::new()),
                content_b64: None,
                from: None,
            }),
            Frame::Welcome(WelcomeFrame {
                server: String::new(),
                client_id: Uuid::nil(),
                caps: Vec::new(),
                peers: Vec::new(),
                offers: Vec::new(),
                last_clip: None,
            }),
            Frame::Error(ErrorFrame {
                code: ErrorCode::BadFrame,
                message: String::new(),
            }),
            Frame::FileChunk(FileChunkFrame {
                file_id: Uuid::nil(),
                name: "x".into(),
                content_type: "application/octet-stream".into(),
                chunk_index: 0,
                total_chunks: 1,
                file_sha256: Some("00".into()),
                file_size: 1,
                payload_b64: "AA==".into(),
                from: None,
                to: None,
                transfer_id: None,
                from_device: None,
            }),
            Frame::FileOffer(crate::protocol::FileOfferFrame {
                file_id: Uuid::nil(),
                batch_id: Uuid::nil(),
                ts: 0,
                name: "x".into(),
                content_type: "application/octet-stream".into(),
                file_size: 1,
                expires_at_ms: 1,
                from_device: None,
            }),
            Frame::FilePull(crate::protocol::FilePullFrame {
                file_id: Uuid::nil(),
                transfer_id: Uuid::nil(),
                to: Uuid::nil(),
                chunk_index: 0,
                window: 1,
                from_device: None,
            }),
            Frame::FileDigest(crate::protocol::FileDigestFrame {
                file_id: Uuid::nil(),
                transfer_id: Uuid::nil(),
                to: Uuid::nil(),
                from_device: None,
                file_sha256: "00".into(),
            }),
            Frame::FileAbort(crate::protocol::FileAbortFrame {
                file_id: Uuid::nil(),
                transfer_id: None,
                to: None,
                from_device: None,
                reason: "busy".into(),
            }),
            Frame::PeerChanged(crate::protocol::PeerChangedFrame {
                device: Uuid::nil(),
                caps: Vec::new(),
                event: crate::protocol::PeerEvent::Joined,
            }),
        ];
        for frame in &samples {
            let json = serde_json::to_value(frame).unwrap();
            let tag = json.get("type").and_then(|t| t.as_str()).unwrap();
            assert!(
                KNOWN_FRAME_TYPES.contains(&tag),
                "Frame variant `{tag}` is missing from KNOWN_FRAME_TYPES"
            );
        }
        // And the other direction: every listed type is produced by a variant,
        // so a stale entry left behind by a removed variant also fails.
        let mut from_variants: Vec<String> = samples
            .iter()
            .map(|f| {
                serde_json::to_value(f).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        from_variants.sort();
        let mut listed: Vec<String> = KNOWN_FRAME_TYPES.iter().map(|s| s.to_string()).collect();
        listed.sort();
        assert_eq!(
            from_variants, listed,
            "KNOWN_FRAME_TYPES and `Frame` have drifted apart"
        );
    }

    #[tokio::test]
    async fn healthz_responds_200() {
        let state = test_state();
        let app = crate::server::router(state);
        let req = Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

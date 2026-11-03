// SPDX-License-Identifier: GPL-3.0-or-later

//! WebSocket transport for the client.
//!
//! Owns the connection lifecycle: connect → consume welcome → relay frames
//! → reconnect with exponential backoff. Inbound `clip` frames are surfaced
//! to the supervisor via a channel; outbound clips are pulled from another.
//!
//! Reconnect policy follows `PROTOCOL.md` §2.5: initial backoff 1 s,
//! doubling to a cap of 60 s, reset to 1 s after a connection has been
//! stable for at least 30 s. (Jitter is left as a follow-up; for a
//! single-user personal tool a thundering-herd at reconnect is impossible.)

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, instrument, trace, warn};
use uuid::Uuid;

use crate::protocol::{Caps, ClipFrame, FileChunkFrame, Frame};
use crate::server::auth::basic_header_value;

use super::config::ClientConfig;
use super::tls;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const RESET_AFTER_STABLE: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const INBOUND_BUF: usize = 32;
const OUTBOUND_BUF: usize = 8;
/// File chunks are bigger than clips and naturally back-pressured by
/// the network; keep the queues short so we don't accidentally buffer
/// hundreds of MB in process memory while waiting for the socket.
const INBOUND_FILES_BUF: usize = 4;
const OUTBOUND_FILES_BUF: usize = 4;
/// Control frames (`file_offer` / `file_pull` / `file_digest` / `file_abort` /
/// `peer_changed`) are small and low-rate — one per offer or session, not one
/// per chunk — but they must not be dropped: a missed `file_pull` stalls a
/// transfer until the idle timeout, and a missed `file_abort` leaks a session.
/// 32 matches the clip channel: generous enough that a burst of
/// `peer_changed`s at connect time (one per existing peer) never fills it, and
/// small enough that a wedged supervisor is noticeable immediately.
const INBOUND_CONTROL_BUF: usize = 32;
/// Outbound control frames are produced by the user's actions, so there is
/// never a burst: an offer per send, a pull per window, an abort per cancel.
/// 8 is the same headroom the clip channel uses.
const OUTBOUND_CONTROL_BUF: usize = 8;

/// Connection-state snapshot the transport emits via the optional
/// `status_tx` watch channel passed to [`spawn_with_status`]. The tray
/// uses this to surface "connecting", "connected", "disconnected" in
/// the menu + tooltip without having to poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStatus {
    /// Connect attempt in progress.
    Connecting,
    /// Welcome frame received; relaying clipboard traffic.
    Connected,
    /// Last connection ended (clean or with an error); will retry after
    /// `will_retry_in`. The duration mirrors the next backoff sleep.
    Disconnected { will_retry_in: Duration },
}

/// Handle held by the supervisor.
pub struct Transport {
    /// Inbound `clip` frames from the hub (other peers' publishes plus, on
    /// each new connection, the cached `last_clip` from the welcome).
    pub inbound_rx: mpsc::Receiver<ClipFrame>,
    /// Inbound `file_chunk` frames from other peers. Only carries traffic
    /// when the supervisor opted in by passing a sender; the headless
    /// `send` subcommand sets this to `None`.
    pub inbound_files_rx: Option<mpsc::Receiver<FileChunkFrame>>,
    /// Outbound `clip` frames to push to the hub.
    pub outbound_tx: mpsc::Sender<ClipFrame>,
    /// Outbound `file_chunk` frames to push to the hub.
    pub outbound_files_tx: mpsc::Sender<FileChunkFrame>,
    /// Inbound routed frames from the hub: `file_offer`, `file_pull`,
    /// `file_digest`, `file_abort`, `peer_changed`. `welcome` shares the frame
    /// type but is consumed on connect and never reaches this channel.
    ///
    /// `None` when the caller passed `receive_files = false` — a one-shot
    /// sender has no use for frames it cannot act on.
    pub inbound_control_rx: Option<mpsc::Receiver<Frame>>,
    /// Outbound control frames: offers, pulls, digests, aborts.
    pub outbound_control_tx: mpsc::Sender<Frame>,
    /// Capabilities the hub advertised in its most recent `welcome`.
    ///
    /// Empty for a pre-0.4 hub, and empty until the first welcome arrives.
    ///
    /// A `watch` rather than a plain field because **a reconnect can change
    /// it** — a hub restart or a switch to a different server may advertise a
    /// different set — while [`Transport`] itself is created once and the
    /// supervisor's loop runs across every reconnect. The supervisor reads this
    /// per copy rather than caching it, so it always gates on the connection
    /// that is actually live.
    pub hub_caps: watch::Receiver<Option<Caps>>,
}

impl Transport {
    /// Close the outbound half, then wait for the transport task to finish.
    ///
    /// This is what a **one-shot sender** does when it has queued everything it
    /// intends to send. The transport task's `select!` ends the connection when
    /// any outbound channel closes (`connect_and_serve`, "outbound clips
    /// closed"), and it then drains whatever is still queued
    /// (`drain_and_finish`) before returning.
    ///
    /// `let _ = transport.outbound_tx;` does **not** do this. `let _ = expr`
    /// evaluates and discards *the value's place*, not the value: the
    /// `mpsc::Sender` stays alive inside `transport`, so no channel ever
    /// closes, the select never sees `None`, and a one-shot `send` hangs until
    /// the user presses Ctrl-C. Consuming `self` makes the drop unconditional
    /// and impossible to get wrong at the call site.
    ///
    /// The inbound receivers are dropped too: nothing will read them once the
    /// task is gone, and holding them open would keep the inbound senders in
    /// the task alive for no reason.
    pub async fn close_outbound(self, join: tokio::task::JoinHandle<()>) -> Result<()> {
        let Transport {
            inbound_rx,
            inbound_files_rx,
            outbound_tx,
            outbound_files_tx,
            inbound_control_rx,
            outbound_control_tx,
            hub_caps,
        } = self;
        // Explicit, one statement per channel: a reader has to be able to see
        // that every sender really dies here.
        drop(outbound_tx);
        drop(outbound_files_tx);
        drop(outbound_control_tx);
        drop(inbound_rx);
        drop(inbound_files_rx);
        drop(inbound_control_rx);
        drop(hub_caps);

        join.await
            .map_err(|e| anyhow!("transport task panicked: {e}"))?;
        Ok(())
    }
}

/// Configuration for [`spawn_with_options`]. Optional flags layered on
/// top of the bare [`spawn`] call so the supervisor + a one-shot `send`
/// subprocess can share the same transport code without each having
/// fields the other doesn't need.
#[derive(Default)]
pub struct SpawnOptions {
    pub status_tx: Option<watch::Sender<ClientStatus>>,
    /// If true, the transport pipes inbound file chunks through a new
    /// channel surfaced on the returned [`Transport`]. False for
    /// senders that don't want to receive.
    pub receive_files: bool,
    /// If true, the transport exits cleanly after a single
    /// connect-and-serve cycle (clean close OR error). Used by the
    /// one-shot `clipboardwire send` subcommand which doesn't want
    /// the supervisor's reconnect-forever behavior.
    pub one_shot: bool,
    /// Device identity to advertise on the upgrade. `None` for a one-shot
    /// `send`, which uses an ephemeral id (4.5) rather than a persisted one.
    pub device: Option<uuid::Uuid>,
    /// Capabilities this client advertises. Empty for a v0.3-shaped client.
    pub caps: Caps,
}

/// Spawn the transport task. Returns the supervisor-facing handle and a join
/// handle for orderly shutdown (abort it to disconnect cleanly).
pub fn spawn(config: ClientConfig) -> (Transport, JoinHandle<()>) {
    spawn_with_options(config, SpawnOptions::default())
}

/// Like [`spawn`] but additionally accepts a `watch::Sender` that the
/// task uses to report [`ClientStatus`] transitions. Pass `None` if the
/// caller doesn't need status updates (the headless-server case).
pub fn spawn_with_status(
    config: ClientConfig,
    status_tx: Option<watch::Sender<ClientStatus>>,
) -> (Transport, JoinHandle<()>) {
    spawn_with_options(
        config,
        SpawnOptions {
            status_tx,
            receive_files: true,
            one_shot: false,
            ..SpawnOptions::default()
        },
    )
}

/// Full-control spawn. Used by the supervisor (wants status + files)
/// and by the `send` subprocess (one-shot, no status, no inbound file
/// stream — it just publishes).
pub fn spawn_with_options(
    config: ClientConfig,
    options: SpawnOptions,
) -> (Transport, JoinHandle<()>) {
    let (inbound_tx, inbound_rx) = mpsc::channel::<ClipFrame>(INBOUND_BUF);
    let (outbound_tx, outbound_rx) = mpsc::channel::<ClipFrame>(OUTBOUND_BUF);
    let (outbound_files_tx, outbound_files_rx) =
        mpsc::channel::<FileChunkFrame>(OUTBOUND_FILES_BUF);
    let (inbound_files_tx, inbound_files_rx) = if options.receive_files {
        let (tx, rx) = mpsc::channel::<FileChunkFrame>(INBOUND_FILES_BUF);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let (inbound_control_tx, inbound_control_rx) = if options.receive_files {
        let (tx, rx) = mpsc::channel::<Frame>(INBOUND_CONTROL_BUF);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let (outbound_control_tx, outbound_control_rx) = mpsc::channel::<Frame>(OUTBOUND_CONTROL_BUF);
    // Starts empty, which is the correct answer while no welcome has arrived:
    // a sender that copies a file before the handshake completes must not
    // assume the hub understands offers.
    let (hub_caps_tx, hub_caps_rx) = watch::channel(None);
    let SpawnOptions {
        status_tx,
        one_shot,
        device,
        caps,
        ..
    } = options;
    let links = Links {
        inbound_tx,
        inbound_files_tx,
        inbound_control_tx,
        outbound_rx,
        outbound_files_rx,
        outbound_control_rx,
    };
    let join = tokio::spawn(async move {
        run_loop(
            config,
            WireOptions { device, caps },
            links,
            status_tx,
            hub_caps_tx,
            one_shot,
        )
        .await;
    });
    (
        Transport {
            inbound_rx,
            inbound_files_rx,
            outbound_tx,
            outbound_files_tx,
            inbound_control_rx,
            outbound_control_tx,
            hub_caps: hub_caps_rx,
        },
        join,
    )
}

/// What goes into the upgrade query, kept together so `run_loop` does not grow
/// two more positional arguments that are easy to swap.
#[derive(Debug, Clone, Copy)]
struct WireOptions {
    device: Option<Uuid>,
    caps: Caps,
}

/// The six data paths one connection serves.
///
/// Bundled rather than passed positionally: six arguments of four similar types
/// (`Sender<X>` / `Receiver<X>`, all `mpsc`, two of them `Option`) is a
/// signature where a silent swap compiles fine and misroutes traffic.
struct Links {
    inbound_tx: mpsc::Sender<ClipFrame>,
    inbound_files_tx: Option<mpsc::Sender<FileChunkFrame>>,
    inbound_control_tx: Option<mpsc::Sender<Frame>>,
    outbound_rx: mpsc::Receiver<ClipFrame>,
    outbound_files_rx: mpsc::Receiver<FileChunkFrame>,
    outbound_control_rx: mpsc::Receiver<Frame>,
}

#[instrument(skip_all, fields(server = %config.server))]

async fn run_loop(
    config: ClientConfig,
    wire: WireOptions,
    mut links: Links,
    status_tx: Option<watch::Sender<ClientStatus>>,
    hub_caps_tx: watch::Sender<Option<Caps>>,
    one_shot: bool,
) {
    let emit = |s: ClientStatus| {
        if let Some(tx) = status_tx.as_ref() {
            // send_replace is fine even when there are no receivers;
            // the watch is "latest state," nobody starves.
            let _ = tx.send(s);
        }
    };
    let mut backoff = INITIAL_BACKOFF;
    loop {
        emit(ClientStatus::Connecting);
        let attempt_start = Instant::now();
        let outcome =
            connect_and_serve(&config, wire, &mut links, status_tx.as_ref(), &hub_caps_tx).await;
        match &outcome {
            Ok(()) => debug!("connection ended cleanly"),
            Err(e) => warn!(error = %format!("{e:#}"), "connection error"),
        }
        if one_shot {
            // The `clipboardwire send` path: a single connect+publish+
            // close cycle is the whole job. Don't reconnect.
            return;
        }
        // If we held the connection for a while, the backoff resets.
        if attempt_start.elapsed() >= RESET_AFTER_STABLE {
            backoff = INITIAL_BACKOFF;
        }
        emit(ClientStatus::Disconnected {
            will_retry_in: backoff,
        });
        debug!(sleep_s = backoff.as_secs_f64(), "reconnecting");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Drain any file chunks still buffered in `outbound_files_rx` to the
/// WebSocket sink. Called when the outbound clip channel closes (the
/// caller is winding down) and we want to make sure we don't drop a
/// queued file mid-flight just because `select!` happened to fire the
/// clip-empty arm first. `recv()` here returns `Some` for queued
/// items, then `None` when the file channel is also closed.
async fn drain_outbound_files<S>(
    outbound_files_rx: &mut mpsc::Receiver<FileChunkFrame>,
    sink: &mut S,
) -> Result<()>
where
    S: futures_util::sink::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    while let Some(chunk) = outbound_files_rx.recv().await {
        let json = serde_json::to_string(&Frame::FileChunk(chunk))?;
        if let Err(e) = sink.send(Message::Text(json.into())).await {
            warn!(error = %e, "drain: file_chunk send failed");
            return Err(e.into());
        }
    }
    Ok(())
}

/// Flush everything still queued and close cleanly.
///
/// **Every** arm that observes a closed channel must come through here. The
/// outbound channels close together when the owner drops the `Transport`, and
/// `select!` picks among ready arms at random — an arm that returns directly
/// discards whatever the other two still hold. That is not theoretical: giving
/// only the clip arm a drain left a ~1-in-3 chance that the control arm won
/// the race with file chunks still queued, and the receiver then waited for a
/// chunk that would never arrive.
///
/// The order is deliberate, worst-consequence first:
///
/// 1. file chunks — a dropped chunk means a corrupt or incomplete file, and
///    nothing downstream can repair it;
/// 2. control frames — a dropped `file_pull` stalls a transfer until the idle
///    timeout, which looks the same to the user as a hang;
/// 3. clips — last-writer-wins, and the next poll re-sends, so losing one
///    costs at most a single clipboard update.
///
/// Only what is *already queued* is flushed: `try_recv` stops at `Empty`, so a
/// still-open channel does not make this wait for new work.
async fn drain_and_finish<S>(links: &mut Links, sink: &mut S) -> Result<()>
where
    S: futures_util::sink::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    drain_outbound_files(&mut links.outbound_files_rx, sink).await?;

    while let Ok(frame) = links.outbound_control_rx.try_recv() {
        let json = serde_json::to_string(&frame)?;
        if let Err(e) = sink.send(Message::Text(json.into())).await {
            warn!(error = %e, "control frame send failed during drain");
            // Do not abort the rest of the drain: a failure here is about this
            // connection, and the remaining channel may still be writable.
            break;
        }
    }

    while let Ok(clip) = links.outbound_rx.try_recv() {
        let json = serde_json::to_string(&Frame::Clip(clip))?;
        if let Err(e) = sink.send(Message::Text(json.into())).await {
            warn!(error = %e, "clip send failed during drain");
            break;
        }
    }

    // Say goodbye on the wire. A plain `sink` drop tears the TCP connection
    // down without a WebSocket close frame — and on wss, without the TLS
    // `close_notify` either, which is exactly the "peer closed connection
    // without close_notify" warning peers logged on every Quit. `Sink::close`
    // sends the Close frame and shuts the underlying stream down politely;
    // the timeout keeps a hung peer from turning Quit into a hang.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), sink.close()).await;
    Ok(())
}

/// Build the upgrade URL, appending `caps` and `device` when present.
///
/// `into_client_request` takes a `&str`, so the query has to be assembled here
/// rather than through a URL builder. Both values are percent-encoded: a device
/// id is a UUID and `caps` is a comma-joined list of `[a-z0-9-]` tokens, so
/// neither needs it today — but the token grammar can gain characters, and an
/// unescaped `&` in a value would silently split the query.
///
/// A server URL that already carries a query is joined with `&`, not a second
/// `?` — the user may have written one by hand (a proxy hint, a token).
pub(crate) fn upgrade_url(server: &str, device: Option<Uuid>, caps: Caps) -> String {
    let mut query = String::new();
    if !caps.is_empty() {
        query.push_str("caps=");
        query.push_str(&percent_encode(&caps.tokens().join(",")));
    }
    if let Some(device) = device {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str("device=");
        query.push_str(&percent_encode(&device.to_string()));
    }
    if query.is_empty() {
        return server.to_string();
    }

    // A fragment would have to stay last, so insert before it. No real server
    // URL carries one, but silently producing `ws://h/sync?caps=…#frag` is
    // wrong in a way that is hard to see.
    let (base, fragment) = match server.find('#') {
        Some(index) => (&server[..index], &server[index..]),
        None => (server, ""),
    };
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}{query}{fragment}")
}

/// Whether the hub contradicted itself: it listed offers but does not claim
/// the capability to serve them.
///
/// A hub builds both from the same state, so this is a hub bug or a proxy
/// rewriting the frame. Logged rather than acted on: the safer reading is
/// "there is nothing I can pull", which is what an unadvertised capability
/// already means.
fn welcome_is_inconsistent(welcome: &crate::protocol::WelcomeFrame) -> bool {
    !welcome.offers.is_empty()
        && !Caps::parse_guarded(&welcome.caps.join(",")).contains(Caps::LAZY_FILE)
}

/// Percent-encode everything outside the RFC 3986 unreserved set.
///
/// Hand-rolled rather than pulled from a crate: the input is one UUID and a
/// handful of `[a-z0-9-]` tokens, so this is 15 lines against a dependency, and
/// the encoding has to be *stable* not clever.
fn percent_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            other => {
                use std::fmt::Write as _;
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

async fn connect_and_serve(
    config: &ClientConfig,
    wire: WireOptions,
    links: &mut Links,
    status_tx: Option<&watch::Sender<ClientStatus>>,
    hub_caps_tx: &watch::Sender<Option<Caps>>,
) -> Result<()> {
    let url = upgrade_url(&config.server, wire.device, wire.caps);
    let mut req = url
        .as_str()
        .into_client_request()
        .map_err(|e| anyhow!("invalid server URL: {e}"))?;
    req.headers_mut().insert(
        "authorization",
        basic_header_value(&config.user, &config.password)
            .parse()
            .map_err(|e| anyhow!("auth header: {e}"))?,
    );

    let (ws, _resp) = if config.server.starts_with("wss://") {
        let connector = tls::make_connector(config)?;
        tokio_tungstenite::connect_async_tls_with_config(req, None, false, Some(connector)).await?
    } else {
        tokio_tungstenite::connect_async(req).await?
    };
    info!("connected");
    if let Some(tx) = status_tx {
        let _ = tx.send(ClientStatus::Connected);
    }
    let (mut sink, mut stream) = ws.split();

    // First frame must be `welcome`.
    let first = stream
        .next()
        .await
        .ok_or_else(|| anyhow!("connection closed before welcome"))??;
    let welcome = match first {
        Message::Text(s) => match serde_json::from_str::<Frame>(&s)? {
            Frame::Welcome(w) => w,
            other => bail!("expected welcome, got {other:?}"),
        },
        other => bail!("expected text welcome, got {other:?}"),
    };
    // A v0.3 hub sends none of these; an absent `caps` means "no
    // capabilities", which is exactly what makes a mixed deployment fall back
    // to eager.
    let hub_caps = Caps::parse_guarded(&welcome.caps.join(","));
    // Hand the supervisor what this hub can do, so a lazy send is gated on the
    // hub actually understanding offers. Published per connection: a reconnect
    // may reach a hub that advertises something else, and the supervisor must
    // gate on the live one.
    let _ = hub_caps_tx.send(Some(hub_caps));
    debug!(
        server = %welcome.server,
        client_id = %welcome.client_id,
        hub_caps = ?hub_caps.tokens(),
        peers = welcome.peers.len(),
        offers = welcome.offers.len(),
        has_last_clip = welcome.last_clip.is_some(),
        "welcomed"
    );

    // `peers` is only logged for now. `offers`, by contrast, must be fed to
    // the supervisor: a broadcast offer only reaches the peers connected at
    // that instant, so this replay is the *only* way a late-joining or
    // reconnected client ever learns the file exists. Dropping it loses the
    // transfer with no error on either side — the remote's clipboard simply
    // stays old, which is exactly the "copied a file and nothing happened"
    // failure that is impossible to debug from the sender's side.
    if welcome_is_inconsistent(&welcome) {
        warn!(
            offers = welcome.offers.len(),
            hub_caps = ?hub_caps.tokens(),
            "hub sent offers without advertising lazy-file; the hub and its \
             own capability list disagree"
        );
    }
    if let Some(tx) = links.inbound_control_tx.as_ref() {
        for offer in welcome.offers {
            if tx.send(Frame::FileOffer(offer)).await.is_err() {
                // The supervisor is gone: same condition as a closed clip
                // channel, so end the connection rather than queue frames
                // nobody will read.
                return Ok(());
            }
        }
    }

    // Surface a cached clip (if any) to the supervisor as an inbound event.
    if let Some(clip) = welcome.last_clip {
        let _ = links.inbound_tx.send(clip).await;
    }

    let mut ping_timer = tokio::time::interval(PING_INTERVAL);
    ping_timer.tick().await; // skip the immediate first tick

    loop {
        tokio::select! {
            outbound = links.outbound_rx.recv() => {
                let Some(clip) = outbound else {
                    debug!("outbound clips closed; flushing before exit");
                    return drain_and_finish(links, &mut sink).await;
                };
                let json = serde_json::to_string(&Frame::Clip(clip))?;
                if let Err(e) = sink.send(Message::Text(json.into())).await {
                    warn!(error = %e, "send failed");
                    return Err(e.into());
                }
            }
            outbound_file = links.outbound_files_rx.recv() => {
                let Some(chunk) = outbound_file else {
                    // Closing the file channel does not mean the others are
                    // empty: the one-shot `send` path drops the whole
                    // `Transport`, so all three close at once and this arm can
                    // win with a clip or a pull still queued.
                    debug!("outbound files closed; flushing before exit");
                    return drain_and_finish(links, &mut sink).await;
                };
                let json = serde_json::to_string(&Frame::FileChunk(chunk))?;
                if let Err(e) = sink.send(Message::Text(json.into())).await {
                    warn!(error = %e, "file_chunk send failed");
                    return Err(e.into());
                }
            }
            outbound_control = links.outbound_control_rx.recv() => {
                let Some(frame) = outbound_control else {
                    // The arm most likely to win the shutdown race, and the
                    // one that used to be the most destructive: this channel is
                    // usually empty-but-closed, so it reports `None` instantly
                    // while the file channel still holds most of a transfer.
                    debug!("outbound control closed; flushing before exit");
                    return drain_and_finish(links, &mut sink).await;
                };
                let json = serde_json::to_string(&frame)?;
                if let Err(e) = sink.send(Message::Text(json.into())).await {
                    warn!(error = %e, "control frame send failed");
                    return Err(e.into());
                }
            }
            msg = stream.next() => {
                let Some(msg) = msg else {
                    debug!("stream ended");
                    return Ok(());
                };
                match msg? {
                    Message::Text(s) => {
                        let frame: Frame = match serde_json::from_str(&s) {
                            Ok(f) => f,
                            Err(e) => {
                                warn!(error = %e, "rejecting bad frame from server");
                                continue;
                            }
                        };
                        match frame {
                            Frame::Clip(clip) => {
                                if links.inbound_tx.send(clip).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Frame::FileChunk(chunk) => {
                                if let Some(tx) = links.inbound_files_tx.as_ref() {
                                    if tx.send(chunk).await.is_err() {
                                        return Ok(());
                                    }
                                } else {
                                    trace!("ignoring inbound file_chunk; no receiver wired");
                                }
                            }
                            Frame::Error(err) => {
                                warn!(code = ?err.code, message = %err.message, "server error");
                            }
                            Frame::Welcome(_) => {
                                warn!("ignoring unexpected second welcome");
                            }
                            // The five routed control frames. Listed one by
                            // one rather than through `_`: a new `Frame`
                            // variant must then make this match fail to
                            // compile, which is the reminder to decide where
                            // it belongs.
                            Frame::FileOffer(_)
                            | Frame::FilePull(_)
                            | Frame::FileDigest(_)
                            | Frame::FileAbort(_)
                            | Frame::PeerChanged(_) => {
                                match links.inbound_control_tx.as_ref() {
                                    Some(tx) => {
                                        // The supervisor is gone: same
                                        // condition as a closed clip channel,
                                        // so end the connection rather than
                                        // dropping frames nobody will read.
                                        if tx.send(frame).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                    None => {
                                        trace!("ignoring inbound control frame; no receiver wired");
                                    }
                                }
                            }
                        }
                    }
                    Message::Binary(_) => warn!("ignoring binary frame"),
                    Message::Ping(_) | Message::Pong(_) => {
                        // Pings are auto-pong'd by the WebSocket layer.
                        trace!("ping/pong");
                    }
                    Message::Close(_) => {
                        debug!("server sent close");
                        return Ok(());
                    }
                    _ => {}
                }
            }
            _ = ping_timer.tick() => {
                if let Err(e) = sink.send(Message::Ping(Default::default())).await {
                    warn!(error = %e, "ping send failed");
                    return Err(e.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{FileOfferFrame, WelcomeFrame, WelcomePeer};

    fn welcome_with(caps: &[&str], offers: usize) -> WelcomeFrame {
        WelcomeFrame {
            server: "clipboardwire/0.4.0".to_string(),
            client_id: Uuid::new_v4(),
            caps: caps.iter().map(|c| c.to_string()).collect(),
            peers: vec![WelcomePeer {
                device: Uuid::new_v4(),
                caps: vec!["lazy-file".to_string()],
            }],
            offers: (0..offers)
                .map(|_| FileOfferFrame {
                    file_id: Uuid::new_v4(),
                    batch_id: Uuid::new_v4(),
                    ts: 0,
                    name: "report.pdf".to_string(),
                    content_type: "application/pdf".to_string(),
                    file_size: 1024,
                    expires_at_ms: 1_000_000,
                    from_device: None,
                })
                .collect(),
            last_clip: None,
        }
    }

    // ---- upgrade query ----

    #[test]
    fn upgrade_url_appends_device_and_caps() {
        let device = Uuid::new_v4();
        let url = upgrade_url(
            "ws://h.lan:8484/sync",
            Some(device),
            Caps::LAZY_FILE | Caps::FILE_RECV,
        );
        assert!(url.starts_with("ws://h.lan:8484/sync?"), "{url}");
        assert!(url.contains("device="), "{url}");
        assert!(url.contains(&device.to_string()), "{url}");
        assert!(url.contains("caps="), "{url}");
        // Order inside `caps` follows `Caps::KNOWN`, which is what `tokens()`
        // guarantees; asserting both tokens rather than the joined string keeps
        // this from breaking on an unrelated KNOWN reordering.
        assert!(url.contains("lazy-file"), "{url}");
        assert!(url.contains("file-recv"), "{url}");
    }

    #[test]
    fn upgrade_url_omits_empty_values() {
        // The v0.3 shape: nothing to say. A stray `?` here would change the
        // path the server routes on.
        assert_eq!(
            upgrade_url("ws://h.lan:8484/sync", None, Caps::default()),
            "ws://h.lan:8484/sync"
        );
    }

    #[test]
    fn upgrade_url_uses_ampersand_when_a_query_exists() {
        let device = Uuid::new_v4();
        let url = upgrade_url("ws://h/sync?x=1", Some(device), Caps::default());
        assert!(url.starts_with("ws://h/sync?x=1&device="), "{url}");
        assert!(!url.contains("?device="), "second `?` in {url}");
        let with_caps = upgrade_url("ws://h/sync?x=1", None, Caps::LAZY_FILE);
        assert!(
            with_caps.starts_with("ws://h/sync?x=1&caps="),
            "{with_caps}"
        );
    }

    #[test]
    fn upgrade_url_escapes_values_rather_than_splitting_the_query() {
        // No current token needs this, but the function must not be the reason
        // a future one breaks: a raw `&` would silently end the value.
        assert_eq!(percent_encode("a&b=c"), "a%26b%3Dc");
        assert_eq!(percent_encode("lazy-file"), "lazy-file");
        assert_eq!(percent_encode("a b"), "a%20b");
    }

    #[test]
    fn upgrade_url_keeps_a_fragment_last() {
        let device = Uuid::new_v4();
        let url = upgrade_url("ws://h/sync#frag", Some(device), Caps::default());
        assert!(url.ends_with("#frag"), "{url}");
        assert!(url.contains("device="), "{url}");
        assert!(!url.contains("#frag?"), "{url}");
    }

    // ---- welcome parsing ----

    #[test]
    fn a_v03_welcome_yields_no_hub_caps() {
        // A v0.3 hub omits the field entirely; serde fills in the empty vec,
        // and "no capabilities" is what makes a mixed deployment fall back to
        // eager instead of waiting for offers that will never come.
        let raw = r#"{"type":"welcome","server":"clipboardwire/0.3.0",
                      "client_id":"00000000-0000-0000-0000-000000000001",
                      "last_clip":null}"#;
        let frame: Frame = serde_json::from_str(raw).unwrap();
        let Frame::Welcome(welcome) = frame else {
            panic!("expected welcome");
        };
        assert!(welcome.caps.is_empty());
        assert!(Caps::parse_guarded(&welcome.caps.join(",")).is_empty());
        assert!(
            !welcome_is_inconsistent(&welcome),
            "an empty hub is consistent"
        );
    }

    #[test]
    fn a_v04_welcome_yields_lazy_file() {
        let welcome = welcome_with(&["lazy-file"], 0);
        let caps = Caps::parse_guarded(&welcome.caps.join(","));
        assert!(caps.contains(Caps::LAZY_FILE), "{:?}", caps.tokens());
        assert!(!welcome_is_inconsistent(&welcome));
    }

    #[test]
    fn welcome_offers_without_the_cap_are_logged() {
        // Pure logic only: the warn! itself needs a subscriber. The condition
        // is what matters — offers with no capability is a hub that disagrees
        // with itself, and the client's reading must be "nothing to pull".
        assert!(
            welcome_is_inconsistent(&welcome_with(&[], 2)),
            "offers without lazy-file must be flagged"
        );
        assert!(
            !welcome_is_inconsistent(&welcome_with(&["lazy-file"], 2)),
            "offers with lazy-file are normal"
        );
        assert!(
            !welcome_is_inconsistent(&welcome_with(&[], 0)),
            "no offers is not an inconsistency"
        );
        // `file-recv` alone does not license serving offers.
        assert!(welcome_is_inconsistent(&welcome_with(&["file-recv"], 1)));
    }
}

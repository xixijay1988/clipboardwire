// SPDX-License-Identifier: GPL-3.0-or-later

//! Central fan-out hub.
//!
//! A single Tokio task owns the canonical state — the set of connected
//! clients and the `last_clip` cache. WebSocket connection tasks talk to it
//! over an mpsc inbox. Single-ownership keeps the state lock-free.
//!
//! Per-client outbound channels are bounded (see [`PER_CLIENT_CHANNEL_BUF`]).
//! When a slow peer's buffer is full, the hub drops the frame for that one
//! peer rather than blocking everyone — clipboard sync is last-write-wins,
//! so dropping an intermediate value is acceptable.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::logging::{hub_log_file_names, redact_file_name};
use crate::protocol::{
    Caps, ClipFrame, ControlFrameError, ErrorCode, FileAbortFrame, FileChunkFrame, FileDigestFrame,
    FileOfferFrame, FilePullFrame, Frame, PeerChangedFrame, PeerEvent,
};

/// Cheap, lock-free counter of currently-connected hub clients.
///
/// Embedded-hub callers (the tray) clone this into [`ServerConfig::stats`]
/// before spawning the hub, then read [`HubStatsSink::current`] from any
/// thread to learn the live count. The hub task is the only writer:
/// `+= 1` on a successful Register, `-= 1` on a Deregister that
/// actually removed an entry. Initial value is 0.
#[derive(Debug, Clone, Default)]
pub struct HubStatsSink {
    count: Arc<AtomicUsize>,
}

impl HubStatsSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of clients currently registered with the hub.
    pub fn current(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

/// Capacity of each per-client outbound clip mpsc.
pub const PER_CLIENT_CHANNEL_BUF: usize = 32;
/// Capacity of each per-client outbound file-chunk mpsc. Smaller than
/// the clip channel because chunks are megabyte-sized and a slow
/// receiver shouldn't be allowed to balloon process memory.
pub const PER_CLIENT_FILE_CHANNEL_BUF: usize = 4;

/// Extra chunks a peer may fall behind by before its transfer is aborted.
///
/// Two levels because one is not enough in practice: the writer is usually
/// mid-`send` on a slow socket, so a buffer of one chunk turns every slight
/// wobble into an abort. Eight rides out a `send` that is waiting on the kernel
/// without letting memory grow with file size.
pub const PER_CLIENT_FILE_OVERFLOW: usize = 8;

/// Capacity of each per-client routed-frame mpsc (`file_digest`,
/// `file_abort`).
///
/// These are control frames, not bulk data: a session produces one digest and
/// at most a handful of aborts, so a small buffer is plenty and a slow peer
/// cannot balloon memory through it.
pub const PER_CLIENT_FRAME_CHANNEL_BUF: usize = 8;

/// Capacity of each connection's control inbox.
///
/// Only a handful of instructions ever reach it (a close, and whatever
/// eviction grows into), so it stays tiny: the hub must never be able to
/// queue meaningful backpressure here.
pub const PER_CLIENT_CONTROL_CHANNEL_BUF: usize = 4;

/// What this hub build can do, advertised in `welcome`.
///
/// `lazy-file` is the only meaningful one today: it tells a v0.4 client that
/// offering files lazily is worthwhile here. `file-recv` and `transfer-id`
/// describe parsing this hub has by construction, so listing them would only
/// add noise a client cannot act on.
pub fn hub_caps() -> Caps {
    Caps::LAZY_FILE
}

/// How long a device's offers survive after its last connection goes away.
///
/// A transient drop must not make a source's files unpullable: the source may be
/// reconnecting. Offers are dropped only when the device has been absent this
/// long, or when the offer's own TTL expires, whichever comes first.
pub const DEVICE_GRACE_MS: i64 = 60_000;

/// Cap on live offers advertised by one device (design §4.4).
const MAX_OFFERS_PER_DEVICE: usize = 256;

/// Cap on live offers across the whole hub.
///
/// Exceeding it rejects the *new* offer. Evicting someone else's would let one
/// peer push another out of the registry, which the spec forbids.
const MAX_OFFERS_TOTAL: usize = 4096;

/// Furthest an offer may be dated into the future.
const MAX_OFFER_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// How long the hub remembers a started transfer.
///
/// Longer than any plausible gap between windows of one transfer, and far
/// longer than a resume would take, so a slow receiver is never mistaken for a
/// finished one. Nothing depends on precision here: forgetting early only means
/// a later window is judged by TTL again, which fails closed.
const TRANSFER_MEMORY_MS: i64 = 30 * 60 * 1000;

/// How many transfer sessions the hub will remember at once.
///
/// `active_transfers` exists only to answer "has this transfer started?" at
/// `authorize_pull`, and every accepted pull creates an entry that lives for
/// `TRANSFER_MEMORY_MS`. Without a bound, one peer can mint distinct
/// `transfer_id`s against a single live offer and grow the map for 30 minutes
/// straight — the pull is authorized each time, and `Delivered` only means the
/// frame reached the source's channel, not that the source accepted the
/// session. The source's own `MAX_ACTIVE_PULLS` cap does not help here: it
/// answers `busy` and the hub still counted the pull.
///
/// Sized well above any honest workload: `MAX_OFFERS_TOTAL` offers times a few
/// concurrent pastes each. At ~80 bytes an entry this is ~1.3 MiB, which is
/// acceptable to hold and impossible to exceed.
const MAX_ACTIVE_TRANSFERS: usize = 4 * MAX_OFFERS_TOTAL;

/// How many sessions one eviction pass removes.
///
/// Evicting a single entry at the cap would rescan the whole map on *every*
/// subsequent pull, turning a memory bound into a CPU one: a few hundred pulls
/// a second against 16k entries is millions of comparisons a second, which is
/// its own denial of service. Removing an eighth amortises the scan over the
/// next 2048 inserts.
///
/// Hitting the cap fails *closed*: forgetting a real transfer only sends its
/// later windows back to the offer's TTL gate, which is the pre-existing
/// behaviour.
const EVICT_BATCH: usize = MAX_ACTIVE_TRANSFERS / 8;

/// A live offer, keyed by `(owner device, file_id)`.
///
/// Keying on the pair rather than `file_id` alone matters: `file_id` is chosen
/// by the client, so a bare key would let one peer overwrite another peer's
/// offer by reusing its id.
///
/// No `owner` field: it is the first half of the key, and every use is inside a
/// loop that already destructures the key — a copy would only create a second
/// source of truth to keep in sync.
struct StoredOffer {
    frame: FileOfferFrame,
}

/// Outcome of registering an offer.
///
/// Every variant is a refusal the sender may retry differently except
/// [`Self::Accepted`]; none of them closes the connection (spec: *Rejecting an
/// offer SHALL NOT close the sender's connection*).
#[derive(Debug, PartialEq, Eq)]
pub enum OfferResult {
    Accepted,
    /// Field-shape validation failed (see [`FileOfferFrame::validate`]).
    Invalid(String),
    /// `expires_at_ms` is not in the future, or beyond [`MAX_OFFER_TTL_MS`].
    BadExpiry,
    /// This device already holds [`MAX_OFFERS_PER_DEVICE`] live offers.
    PerDeviceCap,
    /// The hub already holds [`MAX_OFFERS_TOTAL`] live offers.
    TotalCap,
}

/// Outcome of a `file_pull` authorization.
///
/// `to` in the pull names the **source**, never the puller. A legitimate pull
/// therefore always names a device other than its sender, so nothing here
/// compares the puller with the offer's owner.
#[derive(Debug, PartialEq, Eq)]
pub enum PullResult {
    /// Authorized. Relaying is 3.7; the hub names the source's connection.
    Forward { source_client: Uuid },
    /// No live offer with that `file_id` owned by `to`.
    UnknownOffer,
    /// The offer exists but its `expires_at_ms` has passed.
    Expired,
    /// The offer is live but its owner has no current connection.
    SourceGone,
    /// Authorized and addressed, but the source's outbound buffer was full.
    /// The receiver is told `busy` and may retry under a new session.
    Busy,
    /// The frame's own fields are invalid (`validate()` failed).
    ///
    /// The typed error is kept rather than flattened to a string so the caller
    /// can map it to the right abort reason: a bad window is
    /// `window_too_large` ([`ControlFrameError::abort_reason`] already knows
    /// that, and the protocol names that bound explicitly), while a
    /// field error with no wire reason falls back to `invalid_start`.
    ///
    /// Today `FilePullFrame::validate` can only fail on `window`, so
    /// `invalid_start` is headroom for a future session-level check of the
    /// starting index — the failure the protocol reserves that name for — not
    /// a case that exists now.
    Invalid(ControlFrameError),
}

/// Result of handing a chunk to one peer's outbound path.
///
/// `Full` and `Gone` are deliberately distinct even though both mean "not
/// queued": `Full` is a live peer that is behind, which the protocol answers
/// with `file_abort{busy}`; `Gone` is a peer that is already tearing down, where
/// an abort would be addressed to nobody.
enum QueueOutcome {
    Queued,
    /// Both buffers full: the peer is alive but cannot keep up.
    Full(FileChunkFrame),
    /// The peer's data channels are closed.
    Gone,
}

/// Outcome of relaying a directed frame (`file_digest`, `file_abort`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryResult {
    /// Handed to the recipient's channel.
    Delivered,
    /// `to` named a device with no live connection.
    UnknownDevice,
    /// The target's channel was full. 3.7b turns this into `file_abort{busy}`;
    /// for now the frame is dropped and the drop is logged.
    Busy,
    /// An offer-scoped abort was fanned out to every peer but the sender.
    Broadcast { recipients: usize },
}

/// Hub→connection instructions that are not relayed frames.
///
/// The hub never awaits a peer, so eviction has to be a message the
/// connection's own writer acts on. A WebSocket close code (e.g. `4005`) cannot
/// be sent from [`Hub::handle`] — it has no socket and must not block.
#[derive(Debug, Clone)]
pub enum ControlMsg {
    /// Close the socket with this code and reason, and stop the connection.
    Close { code: u16, reason: String },
}

/// One registered connection: its identity, its capabilities, and the three
/// channels the hub (or the connection's own tasks) can write to it.
struct PeerEntry {
    /// Identity the peer advertised, or the per-connection id minted for it.
    device: Uuid,
    /// Capabilities from the upgrade query. Read by capability filtering
    /// (3.4b/3.5); recorded from 3.4a.
    #[allow(dead_code)]
    caps: Caps,
    clip: mpsc::Sender<ClipFrame>,
    file: mpsc::Sender<FileChunkFrame>,
    /// Second-level queue for chunks the primary `file` channel could not
    /// take. See [`PER_CLIENT_FILE_OVERFLOW`] and [`Hub::try_queue_chunk`].
    overflow: mpsc::Sender<FileChunkFrame>,
    /// Routed non-chunk frames (`file_digest`, `file_abort`).
    ///
    /// Kept separate from `ctl` so that a close instruction never queues behind
    /// a relayed frame — `ctl` is the high-priority lane. Also separate from
    /// `file`, whose item type is `FileChunkFrame` and whose backpressure
    /// behaviour (drop / buffered overflow) is specific to bulk bytes.
    frames: mpsc::Sender<Frame>,
    /// Close instructions for this connection. Written by [`Hub::evict`];
    /// the connection's writer is the only reader.
    ctl: mpsc::Sender<ControlMsg>,
}

/// Capacity of the hub's own inbox. Held by every connection task plus the
/// supervisor; sized generously since the hub drains synchronously.
const HUB_INBOX_BUF: usize = 1024;

#[derive(Debug)]
pub enum HubMessage {
    Register {
        id: Uuid,
        device: Uuid,
        caps: Caps,
        outbound: mpsc::Sender<ClipFrame>,
        file_outbound: mpsc::Sender<FileChunkFrame>,
        /// Second-level queue; same item type as `file_outbound`.
        overflow: mpsc::Sender<FileChunkFrame>,
        frames: mpsc::Sender<Frame>,
        ctl: mpsc::Sender<ControlMsg>,
        ack: oneshot::Sender<RegisterResult>,
    },
    Deregister {
        id: Uuid,
        /// The device this connection registered under.
        ///
        /// Carried explicitly rather than read out of `clients` because a
        /// deregister can arrive *after* the entry was already removed (double
        /// deregister) or after a newer connection took the slot. The comparison
        /// below needs the device even when the client row is gone.
        device: Uuid,
    },
    Publish {
        from: Uuid,
        clip: ClipFrame,
    },
    /// Fan-out a file-chunk frame to every other connected client. Not
    /// cached for late joiners (unlike text/image clips).
    PublishFile {
        from: Uuid,
        chunk: FileChunkFrame,
    },
    /// Advertise a file without moving any bytes.
    ///
    /// Hub-side only for now: `ws.rs` still refuses inbound `file_offer`
    /// (see the reader's control-frame arm). Wiring the route up is 3.9.
    PublishOffer {
        /// `client_id` of the sender, used to look up its registered device.
        from: Uuid,
        offer: FileOfferFrame,
        reply: oneshot::Sender<OfferResult>,
    },
    /// Relay a `file_digest` to the receiving device named by its `to`.
    ///
    /// Hub-side only for now: `ws.rs` still refuses inbound `file_digest`.
    PublishDigest {
        from: Uuid,
        digest: FileDigestFrame,
        reply: oneshot::Sender<DeliveryResult>,
    },
    /// Relay a `file_abort`. Session-scoped aborts go to `to`; offer-scoped
    /// ones (no `to`) are expanded into one copy per recipient.
    PublishAbort {
        from: Uuid,
        abort: FileAbortFrame,
        reply: oneshot::Sender<DeliveryResult>,
    },
    /// Periodic wake-up so grace deadlines can fire while the hub is idle.
    ///
    /// The hub is synchronous and single-actor, so it cannot sleep; a ticker
    /// outside the actor is the only way time-driven cleanup happens when no
    /// other message arrives.
    Tick,
    /// Ask the hub whether a `file_pull` may be served by its named source.
    ///
    /// Hub-side only for now: `ws.rs` still refuses inbound `file_pull` (see the
    /// reader's control-frame arm). Wiring the route — and the actual relay —
    /// is 3.9; chunk-level delivery to one receiver is 3.7.
    PublishPull {
        /// `client_id` of the puller.
        ///
        /// Not part of authorization: a pull names the *source* in `to`, and the
        /// puller is never the source. It is carried because 3.7 attaches the
        /// chunk stream to the session this puller opened.
        from: Uuid,
        pull: FilePullFrame,
        reply: oneshot::Sender<PullResult>,
    },
}

#[derive(Debug)]
pub enum RegisterResult {
    Accepted {
        last_clip: Option<ClipFrame>,
        /// Connected peers at the moment of registration, excluding the
        /// registrant itself — a client already knows it is connected.
        peers: Vec<(Uuid, Caps)>,
        /// Live offers at registration. Expired ones are swept first so a
        /// newcomer never sees a deadline that already passed.
        offers: Vec<FileOfferFrame>,
    },
    AtCapacity,
}

/// Cheaply-cloneable handle that connection tasks use to talk to the hub.
#[derive(Clone)]
pub struct HubHandle {
    tx: mpsc::Sender<HubMessage>,
}

impl HubHandle {
    /// Register a connection. On success, returns the `last_clip` snapshot
    /// that the connection should embed in its outbound `welcome` frame.
    ///
    /// `device` is the identity from the upgrade query (or one minted per
    /// connection); `caps` is what the peer advertised. Neither changes how
    /// frames are relayed yet — they are recorded so duplicate-device
    /// handling and capability filtering have something to read.
    #[allow(clippy::too_many_arguments)]
    pub async fn register(
        &self,
        id: Uuid,
        device: Uuid,
        caps: Caps,
        outbound: mpsc::Sender<ClipFrame>,
        file_outbound: mpsc::Sender<FileChunkFrame>,
        overflow: mpsc::Sender<FileChunkFrame>,
        frames: mpsc::Sender<Frame>,
        ctl: mpsc::Sender<ControlMsg>,
    ) -> anyhow::Result<RegisterResult> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(HubMessage::Register {
                id,
                device,
                caps,
                outbound,
                file_outbound,
                overflow,
                frames,
                ctl,
                ack: ack_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("hub task is gone"))?;
        ack_rx
            .await
            .map_err(|_| anyhow::anyhow!("hub dropped registration ack"))
    }

    /// Publish a file chunk for fan-out. The hub stamps `chunk.from =
    /// Some(from)` and relays to every other connected peer; no
    /// caching, so late joiners don't auto-receive a half-finished
    /// transfer from before they connected.
    pub async fn publish_file(&self, from: Uuid, chunk: FileChunkFrame) -> anyhow::Result<()> {
        self.tx
            .send(HubMessage::PublishFile { from, chunk })
            .await
            .map_err(|_| anyhow::anyhow!("hub task is gone"))
    }

    /// Best-effort deregistration; ignores send failure because if the hub
    /// is gone, there is no state to clean up anyway.
    pub async fn deregister(&self, id: Uuid, device: Uuid) {
        let _ = self.tx.send(HubMessage::Deregister { id, device }).await;
    }

    /// Advertise a file offer and learn whether the hub accepted it.
    ///
    /// Returns the verdict rather than an error for refusals: a rejected offer
    /// is a normal protocol outcome (bad expiry, caps), not a hub failure. An
    /// `Err` means the hub task itself is gone.
    pub async fn publish_offer(
        &self,
        from: Uuid,
        offer: FileOfferFrame,
    ) -> anyhow::Result<OfferResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(HubMessage::PublishOffer {
                from,
                offer,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("hub task is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("hub dropped the offer reply"))
    }

    /// Authorize a `file_pull` and report which connection should serve it.
    ///
    /// Returns the verdict rather than an error for refusals: `unknown_offer`
    /// and `expired` are normal `file_abort` answers, not hub faults.
    pub async fn publish_pull(
        &self,
        from: Uuid,
        pull: FilePullFrame,
    ) -> anyhow::Result<PullResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(HubMessage::PublishPull {
                from,
                pull,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("hub task is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("hub dropped the pull reply"))
    }

    /// Relay a `file_digest` to the receiving device and report the delivery.
    pub async fn publish_digest(
        &self,
        from: Uuid,
        digest: FileDigestFrame,
    ) -> anyhow::Result<DeliveryResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(HubMessage::PublishDigest {
                from,
                digest,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("hub task is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("hub dropped the digest reply"))
    }

    /// Relay a `file_abort` and report the delivery.
    pub async fn publish_abort(
        &self,
        from: Uuid,
        abort: FileAbortFrame,
    ) -> anyhow::Result<DeliveryResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(HubMessage::PublishAbort {
                from,
                abort,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("hub task is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("hub dropped the abort reply"))
    }

    /// Publish a clip frame from the given origin connection. The hub stamps
    /// `clip.from = Some(from)` before relaying to peers.
    pub async fn publish(&self, from: Uuid, clip: ClipFrame) -> anyhow::Result<()> {
        self.tx
            .send(HubMessage::Publish { from, clip })
            .await
            .map_err(|_| anyhow::anyhow!("hub task is gone"))
    }
}

/// A device whose connection went away, remembered until its grace period ends.
///
/// `caps` is carried because the `left` broadcast happens *after* the peer row
/// is gone: without it the hub could only report an empty capability list, and
/// a receiver diffing `welcome` against `peer_changed` would see the device's
/// capabilities change on the way out.
#[derive(Debug, Clone, Copy)]
struct PendingSweep {
    deadline: i64,
    caps: Caps,
}

/// What the hub remembers about a transfer it has forwarded.
#[derive(Debug, Clone, Copy)]
struct ActiveTransfer {
    /// Last time a pull for this session reached the source.
    last_pull: i64,
    /// The offer it started from, as `(owner device, file_id)` — the key
    /// [`Hub::offers`] uses. Lets an expiry sweep tell "dead offer" from "dead
    /// offer a live transfer is still reading".
    offer: (Uuid, Uuid),
}

pub(crate) struct Hub {
    clients: HashMap<Uuid, PeerEntry>,
    /// Device → the `client_id` currently claiming it.
    ///
    /// A device can reconnect before its old connection finished tearing down,
    /// so the newest registration wins the slot. Whether the superseded
    /// connection is actively closed is a separate decision (3.3); this map only
    /// records who currently holds it.
    device_index: HashMap<Uuid, Uuid>,
    /// Live offers, keyed by `(owner device, file_id)`.
    offers: HashMap<(Uuid, Uuid), StoredOffer>,
    /// Devices whose last connection went away, and when their grace ends.
    ///
    /// Keyed by device rather than client: a device that reconnects must clear
    /// its own pending expiry, and two departed connections of the same device
    /// must not each schedule one.
    pending_sweep: HashMap<Uuid, PendingSweep>,
    /// Transfer sessions the hub has already forwarded.
    ///
    /// The hub does not need to know anything about a session's contents; it
    /// only needs to answer one question at `authorize_pull`: "has this
    /// transfer already started?" TTL gates the *start* of a pull (spec: Offer
    /// lifecycle), so a session that began inside the offer's window must keep
    /// working after the window closes — otherwise a file large enough to
    /// outlive `offer_ttl_minutes` (30 by default) could never be transferred
    /// at all, which is the case lazy transfer exists for.
    ///
    /// Keyed by `transfer_id` alone, matching the protocol rule that it is the
    /// sole session key. The value records when the hub last forwarded a pull
    /// for it (to forget finished sessions) and which offer it started from.
    ///
    /// The offer key is not decoration. `authorize_pull` is only one of two
    /// places an expired offer is reaped: [`Self::sweep_expired_offers`] runs on
    /// every registration and whenever an offer cap is about to bite. Without
    /// knowing which offers a live transfer depends on, that sweep would delete
    /// the offer under a running transfer and the next window would fail with
    /// `unknown_offer` — the same bug through a different door.
    ///
    /// Deliberately unaware of the source's own session table. When the source
    /// has ended a session but the hub still remembers it, the next window is
    /// forwarded and the source answers `invalid_start`; the receiver restarts
    /// under a *new* `transfer_id`, which the hub has never seen and therefore
    /// judges by TTL again. Tracking source-side session state here would turn
    /// the hub into a state machine it has no business being.
    active_transfers: HashMap<Uuid, ActiveTransfer>,
    last_clip: Option<ClipFrame>,
    max_clients: usize,
    stats: Option<HubStatsSink>,
    /// Fixed clock for tests, so expiry rules can be asserted without sleeping.
    #[cfg(test)]
    now_override: Option<i64>,
}

impl Hub {
    pub(crate) fn new(max_clients: usize, stats: Option<HubStatsSink>) -> Self {
        Self {
            clients: HashMap::new(),
            device_index: HashMap::new(),
            offers: HashMap::new(),
            pending_sweep: HashMap::new(),
            active_transfers: HashMap::new(),
            last_clip: None,
            max_clients,
            stats,
            #[cfg(test)]
            now_override: None,
        }
    }

    /// Milliseconds since the Unix epoch, or the test's fixed clock.
    ///
    /// A clock before the epoch is not worth handling: it would make `now`
    /// negative and every expiry look valid. Saturating at 0 is safer, and the
    /// conversion is checked rather than `as i64` — that cast would wrap into a
    /// negative number, which is exactly the value that makes every expiry pass.
    fn now_ms(&self) -> i64 {
        #[cfg(test)]
        if let Some(t) = self.now_override {
            return t;
        }
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        i64::try_from(millis).unwrap_or(i64::MAX)
    }

    #[instrument(skip_all)]
    pub(crate) async fn run(mut self, mut inbox: mpsc::Receiver<HubMessage>) {
        info!(max_clients = self.max_clients, "hub started");
        while let Some(msg) = inbox.recv().await {
            self.handle(msg);
        }
        info!("hub stopping (inbox closed)");
    }

    pub(crate) fn handle(&mut self, msg: HubMessage) {
        match msg {
            HubMessage::Register {
                id,
                device,
                caps,
                outbound,
                file_outbound,
                overflow,
                frames,
                ctl,
                ack,
            } => {
                if self.clients.len() >= self.max_clients {
                    warn!(client = %id, "rejecting registration: at capacity");
                    let _ = ack.send(RegisterResult::AtCapacity);
                    return;
                }
                self.clients.insert(
                    id,
                    PeerEntry {
                        device,
                        caps,
                        clip: outbound,
                        file: file_outbound,
                        overflow,
                        frames,
                        ctl,
                    },
                );
                // Newest wins for the device slot. The slot is repointed
                // *before* the eviction is attempted, so the mapping is correct
                // even if the old connection cannot be reached — it then
                // deregisters late and must not reclaim the slot.
                if let Some(previous) = self.device_index.insert(device, id) {
                    if previous != id {
                        // Closing the superseded connection is a message its
                        // own writer acts on: the hub must not await a peer.
                        //
                        // This is *not* impersonation-proof. Anyone holding the
                        // shared credential can present another device's id and
                        // evict it; that is a recorded credential-level risk,
                        // not a defence this code claims to provide.
                        self.evict(previous, device);
                    }
                }
                // A reconnect cancels the pending sweep: the device is
                // reachable again, so its offers must not be reaped. This is
                // also what makes a flapping source keep its *original* offer
                // TTL — the deadline is never touched here, only the grace
                // timer.
                if self.pending_sweep.remove(&device).is_some() {
                    debug!(%device, "device reconnected; cancelled the pending offer sweep");
                }
                if let Some(s) = &self.stats {
                    s.count.fetch_add(1, Ordering::Relaxed);
                }

                // Sweep before snapshotting: a late joiner must not be handed an
                // offer whose deadline has already passed.
                self.sweep_expired_offers();
                let peers: Vec<(Uuid, Caps)> = self
                    .clients
                    .iter()
                    .filter(|(peer_id, _)| **peer_id != id)
                    .map(|(_, peer)| (peer.device, peer.caps))
                    .collect();
                let offers: Vec<FileOfferFrame> = self
                    .offers
                    .values()
                    .map(|stored| stored.frame.clone())
                    .collect();

                let snapshot = self.last_clip.clone();
                info!(client = %id, total = self.clients.len(), "client registered with hub");
                let _ = ack.send(RegisterResult::Accepted {
                    last_clip: snapshot,
                    peers,
                    offers,
                });
                // Skip the registrant: it learns about itself from `welcome`.
                self.broadcast_peer_changed(device, caps, PeerEvent::Joined, Some(id));
            }
            HubMessage::Deregister { id, device } => {
                // Only the connection that currently holds the device slot may
                // clear it. A superseded connection deregisters late (after the
                // newer one registered), and clearing unconditionally would
                // erase the live mapping.
                //
                // The `device` recorded at registration (`peer.device`) is the
                // authoritative one; the message carries the same value so the
                // hub can still name the device when the client row is already
                // gone (double deregister).
                match self.clients.remove(&id) {
                    None => {
                        // Already gone: leave `device_index` alone, a later
                        // registrant may legitimately own the slot now.
                        debug!(client = %id, %device, "deregister for unknown client");
                    }
                    Some(peer) => {
                        if peer.device != device {
                            debug!(
                                client = %id,
                                registered_device = %peer.device,
                                reported_device = %device,
                                "deregister device does not match registration; using the registered one"
                            );
                        }
                        // Compare against the id, not just the device: if a newer
                        // connection took this device, the index points at *it*,
                        // and a late deregister from the superseded connection
                        // must neither clear it nor schedule a sweep.
                        if self.device_index.get(&peer.device) == Some(&id) {
                            self.device_index.remove(&peer.device);
                            // The device is now unreachable, but its offers live
                            // on until the grace period ends: the source may be
                            // reconnecting, and a transient drop must not make
                            // its files unpullable.
                            //
                            // `device_index` still has to go immediately. It is
                            // what `authorize_pull` consults, and relaying a pull
                            // to a connection that no longer exists would drop it
                            // silently; `SourceGone` is the honest answer.
                            let deadline = self.now_ms().saturating_add(DEVICE_GRACE_MS);
                            self.pending_sweep.insert(
                                peer.device,
                                PendingSweep {
                                    deadline,
                                    caps: peer.caps,
                                },
                            );
                            debug!(
                                device = %peer.device,
                                deadline,
                                "scheduled offer sweep after the grace period"
                            );
                        }
                        if let Some(s) = &self.stats {
                            s.count.fetch_sub(1, Ordering::Relaxed);
                        }
                        info!(client = %id, total = self.clients.len(), "client deregistered from hub");
                    }
                }
            }
            HubMessage::Publish { from, mut clip } => {
                clip.from = Some(from);
                self.last_clip = Some(clip.clone());

                for (peer_id, peer) in self.clients.iter() {
                    if *peer_id == from {
                        continue;
                    }
                    match peer.clip.try_send(clip.clone()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(client = %peer_id, "outbound clip buffer full; dropping frame");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            // Peer's task already exited; deregistration is
                            // either in flight or will happen shortly.
                        }
                    }
                }
            }
            HubMessage::PublishFile { from, mut chunk } => {
                chunk.from = Some(from);
                // Overwrite, never fill-if-empty: the frame came from a client,
                // so any `from_device` it carried is unauthenticated. Only the
                // sender's registered device may be relayed. If `from` is no
                // longer registered (a deregister racing this publish) there is
                // no authoritative device, and `None` is the honest answer —
                // better than relaying a guess.
                chunk.from_device = self.clients.get(&from).map(|peer| peer.device);

                // Lazy transfers name one receiver; eager ones (v0.3) name none
                // and keep the original broadcast behaviour.
                if let Some(target_device) = chunk.to {
                    self.deliver_to_device(target_device, chunk);
                    return;
                }

                let mut delivered = 0usize;
                for (peer_id, peer) in self.clients.iter() {
                    if *peer_id == from {
                        continue;
                    }
                    match Self::try_queue_chunk(peer, chunk.clone()) {
                        QueueOutcome::Queued => delivered += 1,
                        QueueOutcome::Gone => {}
                        QueueOutcome::Full(_) => {
                            // File transfers tolerate a missed chunk
                            // poorly (the assembled file would be
                            // corrupt). We could close the peer here;
                            // for v0.4 first cut we just drop and log.
                            // TODO: surface to receiver as a transfer
                            // abort marker.
                            // Broadcast (eager) transfers have no session to
                            // abort, and one slow peer must not cost every other
                            // peer a `file_abort`. Only directed delivery turns a
                            // full queue into a protocol answer.
                            warn!(
                                client = %peer_id,
                                file = %redact_file_name(&chunk.name, hub_log_file_names()),
                                index = chunk.chunk_index,
                                "outbound file buffers full; dropping chunk \
                                 (receiver's transfer will be incomplete)"
                            );
                        }
                    }
                }
                if delivered == 0 {
                    // A sender relaying into a hub with no other
                    // registered clients is a silent data hole — this
                    // is exactly what a leaked/zombie hub looks like.
                    warn!(
                        file = %redact_file_name(&chunk.name, hub_log_file_names()),
                        index = chunk.chunk_index,
                        total = self.clients.len(),
                        "file_chunk relayed to zero peers"
                    );
                }
            }
            HubMessage::PublishOffer {
                from,
                mut offer,
                reply,
            } => match self.register_offer(from, &mut offer) {
                Ok(device) => {
                    self.offers
                        .insert((device, offer.file_id), StoredOffer { frame: offer });
                    let _ = reply.send(OfferResult::Accepted);
                }
                Err(verdict) => {
                    let _ = reply.send(verdict);
                }
            },
            HubMessage::PublishDigest {
                from,
                mut digest,
                reply,
            } => {
                // The hub is the only writer of `from_device`, exactly as for
                // chunks.
                digest.from_device = self.clients.get(&from).map(|peer| peer.device);
                let result = self.deliver_frame_to_device(digest.to, Frame::FileDigest(digest));
                let _ = reply.send(result);
            }
            HubMessage::PublishAbort {
                from,
                mut abort,
                reply,
            } => {
                abort.from_device = self.clients.get(&from).map(|peer| peer.device);
                // Read the target out before moving `abort` into a frame; `Copy`
                // on `Option<Uuid>` keeps this a borrow of nothing in particular.
                let target = abort.to;
                let result = match target {
                    // Session-scoped: one receiver, named directly.
                    Some(device) => self.deliver_frame_to_device(device, Frame::FileAbort(abort)),
                    None => self.broadcast_abort(Some(from), abort),
                };
                let _ = reply.send(result);
            }
            HubMessage::PublishPull {
                from,
                mut pull,
                reply,
            } => {
                // Captured before `pull` is moved into the frame below; the
                // session key is what `active_transfers` records, and the offer
                // key is what keeps that offer alive for a second sweep path.
                let transfer_id = pull.transfer_id;
                let offer_key = (pull.to, pull.file_id);
                let result = match self.authorize_pull(from, &pull) {
                    PullResult::Forward { source_client } => {
                        // Authorization is only half the job: the frame has to
                        // reach the source's connection, and the ws layer has no
                        // access to the hub's channels. Delivering here makes
                        // `Forward` mean "delivered", not "deliverable".
                        //
                        // `from_device` is written here and nowhere else, exactly
                        // as for chunks and offers. It is load-bearing: the
                        // source addresses every chunk and every session-scoped
                        // abort to the puller, so a wrong value delivers file
                        // contents to another device. Overwrite, never
                        // fill-if-empty — a client-supplied value is
                        // unauthenticated and would let a peer aim the reply.
                        //
                        // `authorize_pull` deliberately does not consult `from`
                        // (it authorizes the *source*), so a deregister racing
                        // this publish can leave no entry. That is the one case
                        // the `None` guard below catches; relaying an
                        // unattributable pull is worse than refusing it.
                        pull.from_device = self.clients.get(&from).map(|peer| peer.device);
                        if pull.from_device.is_none() {
                            debug!(%from, "pull from an unregistered client; not relaying");
                            let _ = reply.send(PullResult::SourceGone);
                            return;
                        }
                        match self.deliver_frame_to_device(pull.to, Frame::FilePull(pull)) {
                            DeliveryResult::Delivered => {
                                // Bound the map before inserting, and only
                                // when this pull actually adds a session: a
                                // repeat window of a known transfer refreshes
                                // an existing entry and cannot grow the map, so
                                // it must not pay for an eviction pass.
                                if !self.active_transfers.contains_key(&transfer_id)
                                    && self.active_transfers.len() >= MAX_ACTIVE_TRANSFERS
                                {
                                    let evicted = self.evict_oldest_transfers(EVICT_BATCH);
                                    debug!(
                                        evicted,
                                        remaining = self.active_transfers.len(),
                                        "evicted the oldest transfer sessions"
                                    );
                                }
                                // Remember the session so its later windows
                                // survive the offer's TTL. Recorded only on
                                // delivery: a pull that never reached the source
                                // started nothing, and remembering it would let
                                // a peer keep an offer alive by sending pulls
                                // into a dead connection.
                                //
                                // `insert` overwrites, so a repeat window
                                // refreshes `last_pull` and the session is not
                                // swept while it is in use.
                                self.active_transfers.insert(
                                    transfer_id,
                                    ActiveTransfer {
                                        last_pull: self.now_ms(),
                                        offer: offer_key,
                                    },
                                );
                                PullResult::Forward { source_client }
                            }
                            DeliveryResult::Busy => PullResult::Busy,
                            DeliveryResult::UnknownDevice => PullResult::SourceGone,
                            DeliveryResult::Broadcast { .. } => {
                                unreachable!("a pull is never broadcast")
                            }
                        }
                    }
                    other => other,
                };
                let _ = reply.send(result);
            }
            HubMessage::Tick => {
                self.run_pending_sweep();
                self.sweep_active_transfers();
            }
        }
    }

    /// Fan an offer-scoped abort out to every peer but `from`.
    ///
    /// The spec is explicit that *each* expanded copy carries its own
    /// recipient's `to`, so a receiver never has to decide whether an abort is
    /// meant for it (spec §Offer lifecycle, design §3.6).
    ///
    /// `from` is `None` for aborts the hub originates itself (a grace-period
    /// sweep): there is no sender to skip, so every peer gets a copy.
    fn broadcast_abort(&mut self, from: Option<Uuid>, abort: FileAbortFrame) -> DeliveryResult {
        let mut recipients = 0usize;
        for (peer_id, peer) in self.clients.iter() {
            if Some(*peer_id) == from {
                continue;
            }
            let mut copy = abort.clone();
            copy.to = Some(peer.device);
            match peer.frames.try_send(Frame::FileAbort(copy)) {
                Ok(()) => recipients += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        client = %peer_id,
                        "routed-frame buffer full; dropping broadcast abort"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        DeliveryResult::Broadcast { recipients }
    }

    /// Drop offers for devices whose grace period has elapsed.
    fn run_pending_sweep(&mut self) {
        let now = self.now_ms();
        let due: Vec<(Uuid, Caps)> = self
            .pending_sweep
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(device, pending)| (*device, pending.caps))
            .collect();
        for (device, caps) in due {
            self.pending_sweep.remove(&device);
            // A reconnect between scheduling and now removes the entry, so
            // reaching here means the device is still absent.
            self.drop_offers_for_device(device, "source_gone");
            // The device is gone for good, not just disconnected: tell the rest.
            self.broadcast_peer_changed(device, caps, PeerEvent::Left, None);
        }
    }

    /// Forget transfers nobody has pulled for [`TRANSFER_MEMORY_MS`].
    ///
    /// Piggybacks on the existing tick rather than adding a timer: the hub
    /// already wakes for `pending_sweep`, and a stale entry costs one `Uuid` +
    /// `i64`. Forgetting is safe in one direction only — a later window then
    /// gets judged by TTL again, which can refuse a transfer the hub could have
    /// kept alive, never accept one it should have refused.
    fn sweep_active_transfers(&mut self) -> usize {
        let cutoff = self.now_ms().saturating_sub(TRANSFER_MEMORY_MS);
        let before = self.active_transfers.len();
        self.active_transfers
            .retain(|_, active| active.last_pull > cutoff);
        before - self.active_transfers.len()
    }

    /// Drop up to `count` least-recently-pulled sessions.
    ///
    /// `select_nth_unstable_by_key` partitions in O(n) instead of sorting in
    /// O(n log n): only the identity of the oldest `count` matters, not their
    /// order. With [`EVICT_BATCH`] that is 2048 ids copied out of 16384 and one
    /// linear pass, paid once per 2048 inserts rather than on every pull.
    ///
    /// Returns how many were removed, so the caller can log a real number.
    fn evict_oldest_transfers(&mut self, count: usize) -> usize {
        if count == 0 || self.active_transfers.is_empty() {
            return 0;
        }
        if count >= self.active_transfers.len() {
            let removed = self.active_transfers.len();
            self.active_transfers.clear();
            return removed;
        }

        let mut by_age: Vec<(i64, Uuid)> = self
            .active_transfers
            .iter()
            .map(|(transfer_id, active)| (active.last_pull, *transfer_id))
            .collect();
        // The oldest `count` end up in `by_age[..count]`; their relative order
        // is unspecified, which is fine — they are all leaving.
        by_age.select_nth_unstable_by_key(count - 1, |(last_pull, _)| *last_pull);
        for (_, transfer_id) in by_age.into_iter().take(count) {
            self.active_transfers.remove(&transfer_id);
        }
        count
    }

    /// Tell every peer (except `skip`) that `device` joined or left.
    fn broadcast_peer_changed(
        &mut self,
        device: Uuid,
        caps: Caps,
        event: PeerEvent,
        skip: Option<Uuid>,
    ) {
        let frame = Frame::PeerChanged(PeerChangedFrame {
            device,
            caps: caps.tokens(),
            event,
        });
        for (peer_id, peer) in self.clients.iter() {
            if Some(*peer_id) == skip {
                continue;
            }
            match peer.frames.try_send(frame.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(client = %peer_id, "routed-frame buffer full; dropping peer_changed");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
    }

    /// Remove every offer owned by `device` and tell the remaining peers.
    ///
    /// Distinct from `sweep_expired_offers`: that one drops offers whose own
    /// deadline passed, this one drops a whole device's offers because the
    /// device went away. Both can remove the same offer; whichever fires first
    /// wins, which is exactly the spec's "whichever comes first".
    fn drop_offers_for_device(&mut self, device: Uuid, reason: &str) {
        let doomed: Vec<Uuid> = self
            .offers
            .keys()
            .filter(|(owner, _)| *owner == device)
            .map(|(_, file_id)| *file_id)
            .collect();
        if doomed.is_empty() {
            return;
        }
        for file_id in &doomed {
            self.offers.remove(&(device, *file_id));
        }
        debug!(
            %device,
            count = doomed.len(),
            reason,
            "dropped offers for an absent device"
        );
        for file_id in doomed {
            let abort = FileAbortFrame {
                file_id,
                transfer_id: None,
                // Broadcast form: the fan-out fills each copy's `to`.
                to: None,
                from_device: Some(device),
                reason: reason.to_string(),
            };
            self.broadcast_abort(None, abort);
        }
    }

    /// Deliver a chunk to one device's current connection.
    ///
    /// `to` names a **device**, not a connection: the device may have
    /// reconnected since the pull that produced this chunk, so the lookup goes
    /// through `device_index` every time rather than caching a client id.
    ///
    /// Returns whether it was handed over, so `PublishAbort`/`PublishDigest`
    /// can report it; the chunk path ignores the value for now.
    fn deliver_to_device(&mut self, device: Uuid, chunk: FileChunkFrame) -> DeliveryResult {
        let Some(client) = self.device_index.get(&device).copied() else {
            debug!(%device, file_id = %chunk.file_id, "directed chunk for an unknown device");
            return DeliveryResult::UnknownDevice;
        };
        let Some(peer) = self.clients.get(&client) else {
            debug!(%device, %client, "directed chunk for a device whose connection is gone");
            return DeliveryResult::UnknownDevice;
        };

        // Two levels: the primary channel, then the overflow. Both are bounded
        // queues the hub owns in the sense that only the hub pushes into them;
        // the peer's writer is the only consumer. Nothing is dropped silently
        // unless a transfer is aborted or the peer is gone.
        match Self::try_queue_chunk(peer, chunk) {
            QueueOutcome::Queued => DeliveryResult::Delivered,
            QueueOutcome::Gone => {
                debug!(%device, %client, "directed chunk channel closed");
                DeliveryResult::UnknownDevice
            }
            QueueOutcome::Full(chunk) => {
                warn!(
                    %device,
                    %client,
                    file_id = %chunk.file_id,
                    "directed chunk channels full; telling the receiver the source is busy"
                );
                self.abort_busy(device, &client, &chunk);
                DeliveryResult::Busy
            }
        }
    }

    /// Hand `chunk` to `peer`, using the overflow queue before giving up.
    ///
    /// Both levels are bounded `mpsc` queues. That is where the design's
    /// "per-peer overflow queue owned by the hub" lives: the hub decides what
    /// goes into which queue, and the peer's writer is the only thing that
    /// drains them. Ownership never moves — only draining is delegated.
    fn try_queue_chunk(peer: &PeerEntry, chunk: FileChunkFrame) -> QueueOutcome {
        match peer.file.try_send(chunk) {
            Ok(()) => QueueOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(chunk)) => match peer.overflow.try_send(chunk) {
                Ok(()) => QueueOutcome::Queued,
                Err(mpsc::error::TrySendError::Full(chunk)) => QueueOutcome::Full(chunk),
                Err(mpsc::error::TrySendError::Closed(_)) => QueueOutcome::Gone,
            },
            Err(mpsc::error::TrySendError::Closed(_)) => QueueOutcome::Gone,
        }
    }

    /// Tell a receiver that the source cannot keep up with it.
    ///
    /// Sent to the **receiver**, not the sender: the protocol answer is for the
    /// receiver to restart the transfer under a new `transfer_id`
    /// (design §4.5). It goes out on `frames`, which is a different buffer from
    /// the saturated data path, so it can still get through.
    ///
    /// An eager chunk (`transfer_id: None`) gets no abort at all. There is no
    /// session to name and v0.3 has no restart mechanism, so the frame would
    /// only confuse the receiver; the drop is logged instead.
    fn abort_busy(&self, device: Uuid, client: &Uuid, chunk: &FileChunkFrame) {
        let Some(transfer_id) = chunk.transfer_id else {
            warn!(
                %device,
                %client,
                file_id = %chunk.file_id,
                "eager transfer overran both buffers; dropping (no session to abort)"
            );
            return;
        };
        let Some(peer) = self.clients.get(client) else {
            return;
        };
        let abort = FileAbortFrame {
            file_id: chunk.file_id,
            transfer_id: Some(transfer_id),
            // Addressed to the slow receiver ...
            to: Some(device),
            // ... and attributed to the source, which is the device the chunk
            // was relayed from. `peer` here is the *receiver's* entry, so its
            // `device` would name the wrong side; the hub already stamped the
            // sender into the chunk.
            from_device: chunk.from_device,
            reason: "busy".to_string(),
        };
        if let Err(e) = peer.frames.try_send(Frame::FileAbort(abort)) {
            debug!(
                %device,
                %client,
                error = %e,
                "could not deliver busy abort; the receiver will time out instead"
            );
        }
    }

    /// Same routing as [`Self::deliver_to_device`], for non-chunk frames.
    fn deliver_frame_to_device(&mut self, device: Uuid, frame: Frame) -> DeliveryResult {
        let Some(client) = self.device_index.get(&device).copied() else {
            debug!(%device, "routed frame for an unknown device");
            return DeliveryResult::UnknownDevice;
        };
        let Some(peer) = self.clients.get(&client) else {
            debug!(%device, %client, "routed frame for a device whose connection is gone");
            return DeliveryResult::UnknownDevice;
        };
        match peer.frames.try_send(frame) {
            Ok(()) => DeliveryResult::Delivered,
            Err(mpsc::error::TrySendError::Full(_)) => {
                // 3.7b: same overflow handling as chunks.
                warn!(%device, %client, "routed-frame buffer full; dropping frame");
                DeliveryResult::Busy
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(%device, %client, "routed-frame channel closed");
                DeliveryResult::UnknownDevice
            }
        }
    }

    /// Drop offers whose deadline has passed. Returns how many were removed.
    ///
    /// Expiry is lazy for *authorisation* — a pull re-checks the deadline — but
    /// the cap counters must not count dead entries. With the 30 min default
    /// TTL, a source that advertises steadily would otherwise fill its
    /// 256-offer budget with entries nobody can pull and stop being able to
    /// advertise anything at all.
    ///
    /// The comparison is strict: an offer expiring exactly `now` is dead, which
    /// matches [`Self::authorize_pull`]'s `expires_at_ms <= now` rule.
    fn sweep_expired_offers(&mut self) -> usize {
        let now = self.now_ms();
        // An offer past its deadline but still being read by a live transfer is
        // not dead: expiry gates the *start* of a pull (spec: Offer lifecycle),
        // and this sweep runs on every registration, so a peer joining mid-file
        // would otherwise destroy a transfer in progress. The protection is
        // bounded by `sweep_active_transfers`, which drops the session — and
        // with it this exemption — after `TRANSFER_MEMORY_MS` of silence.
        let protected: std::collections::HashSet<(Uuid, Uuid)> = self
            .active_transfers
            .values()
            .map(|active| active.offer)
            .collect();
        let before = self.offers.len();
        self.offers
            .retain(|key, stored| stored.frame.expires_at_ms > now || protected.contains(key));
        before - self.offers.len()
    }

    /// Decide whether `pull` may be served, and by which connection.
    ///
    /// Three conditions, in this order:
    ///
    /// 1. the frame's own fields are valid;
    /// 2. an offer for this `file_id` exists under the **source** device named
    ///    by `pull.to`, and has not expired;
    /// 3. that source has a live connection.
    ///
    /// There is deliberately no fourth condition. `pull.to` names the source,
    /// so a legitimate pull always names a device *other* than the sender;
    /// requiring the puller to own the offer would reject every real pull.
    fn authorize_pull(&mut self, from: Uuid, pull: &FilePullFrame) -> PullResult {
        // 1. Shape first. An absurd window is a bad frame, not an authorization
        //    failure; the caller maps the error to `window_too_large`.
        if let Err(e) = pull.validate() {
            return PullResult::Invalid(e);
        }

        // 2. The offer is keyed by its owner, and `pull.to` names the source.
        let key = (pull.to, pull.file_id);
        let Some(stored) = self.offers.get(&key) else {
            return PullResult::UnknownOffer;
        };
        let expires_at_ms = stored.frame.expires_at_ms;

        // TTL gates the START of a transfer, not its continuation (spec: Offer
        // lifecycle -- "an in-flight transfer SHALL continue until it
        // finishes"). A session the hub has already forwarded keeps working
        // after the offer's deadline; otherwise a file big enough to outlive a
        // 30-minute TTL could never be delivered, which is the case the feature
        // exists for.
        //
        // A continued transfer must NOT drop the offer: later windows still
        // read `file_size`, `name` and the source path through it, and removing
        // it here would turn a mid-file window into `unknown_offer`. Only the
        // outright-expired case is lazy-swept.
        let now = self.now_ms();
        let continued = self.active_transfers.contains_key(&pull.transfer_id);
        if !continued && expires_at_ms <= now {
            self.offers.remove(&key);
            return PullResult::Expired;
        }

        // 3. The source must be reachable. Both lookups are needed: an entry in
        //    `device_index` can outlive its row in `clients` while a deregister
        //    is in flight, and relaying to a connection that is gone would
        //    silently drop the pull.
        let Some(source_client) = self.device_index.get(&pull.to).copied() else {
            return PullResult::SourceGone;
        };
        if !self.clients.contains_key(&source_client) {
            return PullResult::SourceGone;
        }

        // `from` is intentionally not consulted above: it is the puller's
        // client id and plays no part in authorization. It is kept in the
        // signature because 3.7 needs to bind the relayed chunks to the session
        // this puller started.
        let _ = from;
        PullResult::Forward { source_client }
    }

    /// Validate an offer and apply the non-expiry fields to any existing entry.
    ///
    /// Returns the verdict; the caller inserts on [`OfferResult::Accepted`].
    /// Keeping the mutation here (rather than in `handle`) lets the ordering be
    /// read as the spec states it: identity → shape → clock → caps → overwrite.
    fn register_offer(
        &mut self,
        from: Uuid,
        offer: &mut FileOfferFrame,
    ) -> Result<Uuid, OfferResult> {
        // 1. Identity comes from the registry, never from the frame.
        let Some(device) = self.clients.get(&from).map(|p| p.device) else {
            return Err(OfferResult::Invalid("sender is not registered".into()));
        };

        // 2. Field shape. Clock rules are deliberately not here: `validate` is
        // clock-free so two peers cannot disagree about an offer because their
        // clocks do.
        if let Err(e) = offer.validate() {
            return Err(OfferResult::Invalid(format!("{e}")));
        }

        // 3. Clock. `now` is bounded by `i64` already, and the upper bound is
        // computed with `checked_add` so a hostile `now` or a huge TTL cannot
        // wrap into a limit that accepts everything.
        let now = self.now_ms();
        if offer.expires_at_ms <= now {
            return Err(OfferResult::BadExpiry);
        }
        let Some(limit) = now.checked_add(MAX_OFFER_TTL_MS) else {
            return Err(OfferResult::BadExpiry);
        };
        if offer.expires_at_ms > limit {
            return Err(OfferResult::BadExpiry);
        }

        // 4. Caps. Reject, never evict: the spec forbids pushing another peer's
        // offer out to make room for this one.
        //
        // An entry only counts as "existing" while it is still live. A dead one
        // is not something to extend or replace in place: it must not make this
        // offer look like a re-advertisement, or the anti-extension rule below
        // would clamp the new deadline to a value that is already in the past
        // and the entry could never come back.
        let existing = self
            .offers
            .get(&(device, offer.file_id))
            .filter(|stored| stored.frame.expires_at_ms > now)
            .map(|stored| stored.frame.expires_at_ms);

        if existing.is_none() {
            let mine = |hub: &Self| {
                hub.offers
                    .keys()
                    .filter(|(owner, _)| *owner == device)
                    .count()
            };

            // Sweep only when a cap is about to bite: the counters are an O(n)
            // scan each, so paying it on every accepted offer would turn a
            // normal advertise into quadratic work. On the rejection path it is
            // exactly what decides between "full of live offers" and "full of
            // corpses". After sweeping, the counts must be recomputed — that is
            // the whole point.
            let mut mine_count = mine(self);
            if mine_count >= MAX_OFFERS_PER_DEVICE || self.offers.len() >= MAX_OFFERS_TOTAL {
                let swept = self.sweep_expired_offers();
                if swept > 0 {
                    debug!(swept, "dropped expired offers before enforcing caps");
                }
                mine_count = mine(self);
            }

            if mine_count >= MAX_OFFERS_PER_DEVICE {
                return Err(OfferResult::PerDeviceCap);
            }
            if self.offers.len() >= MAX_OFFERS_TOTAL {
                return Err(OfferResult::TotalCap);
            }
        }

        // 5. Re-advertising must not extend the deadline. A reconnect re-sends
        // the offer with a freshly computed expiry; honouring it would let a
        // peer keep an offer alive indefinitely by reconnecting. A *shorter*
        // deadline is still applied — that is the source genuinely changing its
        // mind, and the spec only forbids extension.
        if let Some(stored_expiry) = existing {
            if stored_expiry < offer.expires_at_ms {
                offer.expires_at_ms = stored_expiry;
            }
        }

        // 6. The hub is the only writer of `from_device`.
        offer.from_device = Some(device);
        Ok(device)
    }

    /// Ask a superseded connection to close itself with `4005 duplicate_session`.
    ///
    /// Synchronous and best-effort: it runs inside the actor loop, so it must
    /// never block. A full or closed control channel means the connection is
    /// already tearing down — its own `Deregister` finishes the cleanup, and
    /// `device_index` already points at the new owner.
    ///
    /// Deliberately does **not** remove the client here. Closing is asynchronous
    /// (the peer's writer has to run), and removing early would make the later
    /// `Deregister` take its "unknown client" branch, leaving the live state and
    /// the recorded state disagreeing for no benefit.
    fn evict(&mut self, client: Uuid, device: Uuid) {
        let Some(peer) = self.clients.get(&client) else {
            debug!(%client, %device, "eviction target already gone");
            return;
        };
        match peer.ctl.try_send(ControlMsg::Close {
            code: ErrorCode::DuplicateSession.close_code(),
            reason: "duplicate device id; a newer connection took over".to_string(),
        }) {
            Ok(()) => info!(
                %client,
                %device,
                "closing superseded connection: a newer connection presented the same device id"
            ),
            Err(mpsc::error::TrySendError::Full(_)) => warn!(
                %client,
                %device,
                "superseded connection's control channel is full; it will not be closed promptly"
            ),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(%client, %device, "superseded connection already closing");
            }
        }
    }
}

/// Spawn the hub task on the current Tokio runtime.
///
/// Returns a cloneable handle for connection tasks and the join handle for
/// the supervisor (used for orderly shutdown).
pub fn spawn(max_clients: usize) -> (HubHandle, JoinHandle<()>) {
    spawn_with_stats(max_clients, None)
}

/// Like [`spawn`] but additionally takes an optional live counter the
/// hub task updates on every successful register/deregister. The tray
/// uses this to show connected-peer counts in the tooltip without
/// polling the hub over its inbox.
pub fn spawn_with_stats(
    max_clients: usize,
    stats: Option<HubStatsSink>,
) -> (HubHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<HubMessage>(HUB_INBOX_BUF);
    let hub = Hub::new(max_clients, stats);
    let join = tokio::spawn(hub.run(rx));

    // Grace-period ticker. The hub actor cannot sleep, so time-driven cleanup
    // needs a wake-up from outside; without one, a hub with no traffic would
    // never reap a departed device's offers.
    //
    // `DEVICE_GRACE_MS / 4` bounds the extra latency: an offer is dropped at
    // most 15 s after its grace deadline, rather than exactly on time.
    let tick_tx = tx.clone();
    tokio::spawn(async move {
        let period = Duration::from_millis(DEVICE_GRACE_MS as u64 / 4);
        let mut ticker = tokio::time::interval(period);
        // Skip the immediate first tick: interval fires once straight away and
        // a sweep with nothing pending would just wake the actor.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if tick_tx.send(HubMessage::Tick).await.is_err() {
                // Actor gone; nothing left to tick.
                break;
            }
        }
    });

    (HubHandle { tx }, join)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TEXT_CONTENT_TYPE;

    /// Tests in this module use the hub's clip channel; the file channel
    /// just has to exist (the hub `Drop`s the sender on deregister).
    fn dummy_file_tx() -> mpsc::Sender<FileChunkFrame> {
        let (tx, _rx) = mpsc::channel(1);
        tx
    }

    /// Second-level chunk queue for tests that do not inspect overflow.
    fn dummy_overflow_tx() -> mpsc::Sender<FileChunkFrame> {
        let (tx, _rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        tx
    }

    /// Routed-frame channel for tests that do not inspect digests/aborts.
    fn dummy_frames_tx() -> mpsc::Sender<Frame> {
        let (tx, _rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        tx
    }

    /// Control channel for tests that do not exercise eviction.
    fn dummy_ctl_tx() -> mpsc::Sender<ControlMsg> {
        let (tx, _rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        tx
    }

    fn dummy_clip(content: &str) -> ClipFrame {
        ClipFrame {
            id: Uuid::new_v4(),
            ts: 0,
            content_type: TEXT_CONTENT_TYPE.to_string(),
            content: Some(content.to_string()),
            content_b64: None,
            from: None,
        }
    }

    /// Killing the hub actor drops the client registry — every
    /// registered connection's outbound channels close, which is what
    /// terminates the old hub's sockets on a reload.
    #[tokio::test]
    async fn actor_death_closes_registered_client_channels() {
        let (hub, join) = spawn(8);
        let (clip_tx, mut clip_rx) = mpsc::channel::<ClipFrame>(1);
        let (file_tx, mut file_rx) = mpsc::channel::<FileChunkFrame>(1);
        let (ctl_tx, mut ctl_rx) = mpsc::channel::<ControlMsg>(1);
        let id = Uuid::new_v4();
        assert!(matches!(
            hub.register(
                id,
                Uuid::new_v4(),
                Caps::default(),
                clip_tx,
                file_tx,
                dummy_overflow_tx(),
                dummy_frames_tx(),
                ctl_tx
            )
            .await
            .unwrap(),
            RegisterResult::Accepted { .. }
        ));
        join.abort();
        assert!(clip_rx.recv().await.is_none());
        assert!(file_rx.recv().await.is_none());
        assert!(
            ctl_rx.recv().await.is_none(),
            "control channel must close too"
        );
    }

    #[tokio::test]
    async fn publish_fans_out_to_peers_but_not_sender() {
        let (hub, _join) = spawn(8);

        let id_a = Uuid::new_v4();
        let dev_a = Uuid::new_v4();
        let (tx_a, mut rx_a) = mpsc::channel(8);
        assert!(matches!(
            hub.register(
                id_a,
                dev_a,
                Caps::default(),
                tx_a,
                dummy_file_tx(),
                dummy_overflow_tx(),
                dummy_frames_tx(),
                dummy_ctl_tx()
            )
            .await
            .unwrap(),
            RegisterResult::Accepted {
                last_clip: None,
                ..
            }
        ));

        let id_b = Uuid::new_v4();
        let dev_b = Uuid::new_v4();
        let (tx_b, mut rx_b) = mpsc::channel(8);
        hub.register(
            id_b,
            dev_b,
            Caps::default(),
            tx_b,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();

        hub.publish(id_a, dummy_clip("hello")).await.unwrap();

        let received = rx_b.recv().await.expect("B should receive the clip");
        assert_eq!(received.content.as_deref(), Some("hello"));
        assert_eq!(received.from, Some(id_a));

        // A should NOT receive its own publish.
        let try_a = tokio::time::timeout(std::time::Duration::from_millis(50), rx_a.recv()).await;
        assert!(
            try_a.is_err(),
            "sender should not get an echo of its own publish"
        );
    }

    #[tokio::test]
    async fn last_clip_is_served_to_late_joiners() {
        let (hub, _join) = spawn(8);

        let id_a = Uuid::new_v4();
        let dev_a = Uuid::new_v4();
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(
            id_a,
            dev_a,
            Caps::default(),
            tx_a,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();
        hub.publish(id_a, dummy_clip("cached")).await.unwrap();

        // Give the hub a tick to apply the publish.
        tokio::task::yield_now().await;

        let id_b = Uuid::new_v4();
        let dev_b = Uuid::new_v4();
        let (tx_b, _rx_b) = mpsc::channel(8);
        let result = hub
            .register(
                id_b,
                dev_b,
                Caps::default(),
                tx_b,
                dummy_file_tx(),
                dummy_overflow_tx(),
                dummy_frames_tx(),
                dummy_ctl_tx(),
            )
            .await
            .unwrap();
        match result {
            RegisterResult::Accepted { last_clip, .. } => {
                let clip = last_clip.expect("late joiner should see the cached clip");
                assert_eq!(clip.content.as_deref(), Some("cached"));
                assert_eq!(clip.from, Some(id_a));
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn registration_is_rejected_at_capacity() {
        let (hub, _join) = spawn(2);

        for _ in 0..2 {
            let (tx, _rx) = mpsc::channel(8);
            assert!(matches!(
                hub.register(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Caps::default(),
                    tx,
                    dummy_file_tx(),
                    dummy_overflow_tx(),
                    dummy_frames_tx(),
                    dummy_ctl_tx()
                )
                .await
                .unwrap(),
                RegisterResult::Accepted { .. }
            ));
        }

        let (tx, _rx) = mpsc::channel(8);
        let third = hub
            .register(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Caps::default(),
                tx,
                dummy_file_tx(),
                dummy_overflow_tx(),
                dummy_frames_tx(),
                dummy_ctl_tx(),
            )
            .await
            .unwrap();
        assert!(matches!(third, RegisterResult::AtCapacity));
    }

    #[tokio::test]
    async fn stats_sink_tracks_register_and_deregister() {
        let stats = HubStatsSink::new();
        assert_eq!(stats.current(), 0);
        let (hub, _join) = spawn_with_stats(8, Some(stats.clone()));

        let id_a = Uuid::new_v4();
        let dev_a = Uuid::new_v4();
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(
            id_a,
            dev_a,
            Caps::default(),
            tx_a,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();
        assert_eq!(stats.current(), 1);

        let id_b = Uuid::new_v4();
        let dev_b = Uuid::new_v4();
        let (tx_b, _rx_b) = mpsc::channel(8);
        hub.register(
            id_b,
            dev_b,
            Caps::default(),
            tx_b,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();
        assert_eq!(stats.current(), 2);

        hub.deregister(id_a, dev_a).await;
        // The deregister has to round-trip through the hub task's
        // inbox before the counter ticks; one yield is enough on
        // the current-thread runtime.
        for _ in 0..10 {
            if stats.current() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(stats.current(), 1);
    }

    #[tokio::test]
    async fn stats_sink_does_not_decrement_for_unknown_id() {
        let stats = HubStatsSink::new();
        let (hub, _join) = spawn_with_stats(8, Some(stats.clone()));
        // Deregistering a never-registered id is a no-op and must
        // not underflow the counter (it's a usize).
        hub.deregister(Uuid::new_v4(), Uuid::new_v4()).await;
        tokio::task::yield_now().await;
        assert_eq!(stats.current(), 0);
    }

    #[tokio::test]
    async fn deregister_frees_capacity() {
        let (hub, _join) = spawn(1);

        let id_a = Uuid::new_v4();
        let dev_a = Uuid::new_v4();
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(
            id_a,
            dev_a,
            Caps::default(),
            tx_a,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();

        // Second registration is rejected.
        let (tx_b, _rx_b) = mpsc::channel(8);
        assert!(matches!(
            hub.register(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Caps::default(),
                tx_b,
                dummy_file_tx(),
                dummy_overflow_tx(),
                dummy_frames_tx(),
                dummy_ctl_tx()
            )
            .await
            .unwrap(),
            RegisterResult::AtCapacity
        ));

        hub.deregister(id_a, dev_a).await;
        tokio::task::yield_now().await;

        // Now there is room again.
        let (tx_c, _rx_c) = mpsc::channel(8);
        assert!(matches!(
            hub.register(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Caps::default(),
                tx_c,
                dummy_file_tx(),
                dummy_overflow_tx(),
                dummy_frames_tx(),
                dummy_ctl_tx()
            )
            .await
            .unwrap(),
            RegisterResult::Accepted { .. }
        ));
    }

    #[tokio::test]
    async fn slow_peer_does_not_block_others() {
        let (hub, _join) = spawn(8);

        // Peer A registers with a 0-capacity channel. Any try_send will fail
        // immediately with Full — but that must not affect deliveries to B.
        let id_a = Uuid::new_v4();
        let dev_a = Uuid::new_v4();
        let (tx_a, _rx_a) = mpsc::channel(1);
        hub.register(
            id_a,
            dev_a,
            Caps::default(),
            tx_a,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();

        // Fill A's buffer by sending from someone else first.
        let id_pub = Uuid::new_v4();
        let dev_pub = Uuid::new_v4();
        let (tx_pub, _rx_pub) = mpsc::channel(8);
        hub.register(
            id_pub,
            dev_pub,
            Caps::default(),
            tx_pub,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();

        let id_b = Uuid::new_v4();
        let dev_b = Uuid::new_v4();
        let (tx_b, mut rx_b) = mpsc::channel(8);
        hub.register(
            id_b,
            dev_b,
            Caps::default(),
            tx_b,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();

        // First publish fills A's slot (capacity 1).
        hub.publish(id_pub, dummy_clip("first")).await.unwrap();
        // Second publish — A's buffer is now full and the frame should be
        // dropped *for A only*. B should still receive both.
        hub.publish(id_pub, dummy_clip("second")).await.unwrap();

        let m1 = rx_b.recv().await.unwrap();
        let m2 = rx_b.recv().await.unwrap();
        assert_eq!(m1.content.as_deref(), Some("first"));
        assert_eq!(m2.content.as_deref(), Some("second"));
    }

    #[tokio::test]
    async fn last_clip_carries_from_field() {
        let (hub, _join) = spawn(8);

        let id_a = Uuid::new_v4();
        let dev_a = Uuid::new_v4();
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(
            id_a,
            dev_a,
            Caps::default(),
            tx_a,
            dummy_file_tx(),
            dummy_overflow_tx(),
            dummy_frames_tx(),
            dummy_ctl_tx(),
        )
        .await
        .unwrap();

        let mut clip = dummy_clip("x");
        clip.from = None;
        hub.publish(id_a, clip).await.unwrap();
        tokio::task::yield_now().await;

        let id_b = Uuid::new_v4();
        let dev_b = Uuid::new_v4();
        let (tx_b, _rx_b) = mpsc::channel(8);
        let r = hub
            .register(
                id_b,
                dev_b,
                Caps::default(),
                tx_b,
                dummy_file_tx(),
                dummy_overflow_tx(),
                dummy_frames_tx(),
                dummy_ctl_tx(),
            )
            .await
            .unwrap();
        if let RegisterResult::Accepted {
            last_clip: Some(cached),
            ..
        } = r
        {
            assert_eq!(cached.from, Some(id_a));
        } else {
            panic!("expected cached clip with from set");
        }
    }

    /// Drive a `Hub` directly so tests can read `device_index`, which is private.
    fn hub_direct() -> Hub {
        Hub::new(8, None)
    }

    fn register_msg(id: Uuid, device: Uuid) -> HubMessage {
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (frames, _frames_rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        // The receivers are dropped: `try_send` on a closed channel is not
        // exercised here, and the entry only has to exist.
        let (ack, _ack_rx) = oneshot::channel();
        HubMessage::Register {
            id,
            device,
            caps: Caps::default(),
            outbound: clip,
            file_outbound: file,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames,
            ctl,
            ack,
        }
    }

    /// Keep a test-side receiver alive for the rest of the process.
    ///
    /// `register_msg` drops its receivers, so the hub's senders are closed and a
    /// delivery attempt answers `Gone` instead of `Delivered`. Tests that only
    /// inspect hub state do not care; tests that expect a *delivery* need a live
    /// receiver, and leaking a handful of channels in a test binary is cheaper
    /// than threading receivers through every helper signature.
    fn keep_alive<T: Send + 'static>(rx: mpsc::Receiver<T>) {
        Box::leak(Box::new(rx));
    }

    /// The next frame that is not a `peer_changed`, discarding any that are.
    ///
    /// Registration now broadcasts membership to everyone already connected, so
    /// a receiver may hold join notices before the frame a test actually cares
    /// about. Filtering exactly that one variant keeps each test's assertion
    /// about *its own* frame honest without hiding an unexpected frame type.
    fn next_non_membership_frame(rx: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
        loop {
            match rx.try_recv() {
                Ok(Frame::PeerChanged(_)) => continue,
                Ok(other) => return Some(other),
                Err(_) => return None,
            }
        }
    }

    /// Register `id` under `device` and hand back the control-channel receiver,
    /// so a test can see what the hub tells this connection to do.
    fn register_watching_ctl(hub: &mut Hub, id: Uuid, device: Uuid) -> mpsc::Receiver<ControlMsg> {
        let (ctl_tx, ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id,
            device,
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF).0,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames: mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF).0,
            ctl: ctl_tx,
            ack,
        });
        ctl_rx
    }

    /// Register with a control channel whose receiver the test drops, so the
    /// sender observes `Closed` — the "connection is already tearing down" case.
    fn register_without_ctl_reader(hub: &mut Hub, id: Uuid, device: Uuid) {
        let (ctl_tx, ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        drop(ctl_rx);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id,
            device,
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF).0,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames: mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF).0,
            ctl: ctl_tx,
            ack,
        });
    }

    #[test]
    fn a_newer_connection_with_the_same_device_evicts_the_older() {
        let mut hub = hub_direct();
        let device = Uuid::new_v4();
        let older = Uuid::new_v4();
        let newer = Uuid::new_v4();

        let mut older_ctl = register_watching_ctl(&mut hub, older, device);
        let mut newer_ctl = register_watching_ctl(&mut hub, newer, device);

        match older_ctl.try_recv() {
            Ok(ControlMsg::Close { code, reason }) => {
                assert_eq!(code, 4005, "duplicate_session");
                assert!(reason.contains("duplicate"), "{reason}");
            }
            other => panic!("older connection should be told to close, got {other:?}"),
        }
        assert!(
            newer_ctl.try_recv().is_err(),
            "the winner must not be closed"
        );
        assert_eq!(hub.device_index.get(&device), Some(&newer));
        // Eviction is asynchronous: the hub sends the instruction and leaves
        // the row in place until the connection's own deregister arrives.
        assert!(
            hub.clients.contains_key(&older),
            "the hub must not remove the superseded row itself"
        );
        assert!(hub.clients.contains_key(&newer));
    }

    #[test]
    fn different_devices_do_not_evict_each_other() {
        let mut hub = hub_direct();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mut a_ctl = register_watching_ctl(&mut hub, a, Uuid::new_v4());
        let mut b_ctl = register_watching_ctl(&mut hub, b, Uuid::new_v4());

        assert!(a_ctl.try_recv().is_err(), "A must not be closed");
        assert!(b_ctl.try_recv().is_err(), "B must not be closed");
        assert_eq!(hub.clients.len(), 2);
    }

    #[test]
    fn re_registering_the_same_id_does_not_evict_itself() {
        // The `previous != id` guard: a connection that re-registers (or a hub
        // path that replays Register for the same id) must not close itself.
        let mut hub = hub_direct();
        let id = Uuid::new_v4();
        let device = Uuid::new_v4();

        let mut ctl = register_watching_ctl(&mut hub, id, device);
        // Second registration reuses the same client id, so its ctl receiver is
        // a different one; only the first is asserted on.
        let _replacement_ctl = register_watching_ctl(&mut hub, id, device);

        assert!(
            ctl.try_recv().is_err(),
            "the same connection must not be told to close"
        );
        assert_eq!(hub.device_index.get(&device), Some(&id));
    }

    #[test]
    fn evicting_a_closed_control_channel_does_not_panic() {
        // The connection is already tearing down, so its ctl receiver is gone.
        // Eviction must tolerate `TrySendError::Closed` and still move the slot.
        let mut hub = hub_direct();
        let device = Uuid::new_v4();
        let older = Uuid::new_v4();
        let newer = Uuid::new_v4();

        register_without_ctl_reader(&mut hub, older, device);
        let mut newer_ctl = register_watching_ctl(&mut hub, newer, device);

        assert_eq!(hub.device_index.get(&device), Some(&newer));
        assert!(newer_ctl.try_recv().is_err());
        assert!(hub.clients.contains_key(&newer));
    }

    #[test]
    fn device_index_records_the_current_connection() {
        let mut hub = hub_direct();
        let id = Uuid::new_v4();
        let device = Uuid::new_v4();
        hub.handle(register_msg(id, device));
        assert_eq!(hub.device_index.get(&device), Some(&id));
    }

    #[test]
    fn a_second_connection_takes_the_device_slot_and_retires_the_older_one() {
        // The 3.4a behaviour (slot moves, nothing else) plus 3.3's eviction.
        // This test uses `register_msg`, whose ctl receiver is dropped, so it
        // also exercises the "cannot reach the peer" path; the delivered
        // message itself is asserted in `a_newer_connection_..._evicts_the_older`.
        let mut hub = hub_direct();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let device = Uuid::new_v4();

        hub.handle(register_msg(first, device));
        hub.handle(register_msg(second, device));

        assert_eq!(
            hub.clients.len(),
            2,
            "eviction is asynchronous: the row stays until the peer's own deregister"
        );
        assert_eq!(
            hub.device_index.get(&device),
            Some(&second),
            "the newest connection owns the device slot"
        );
        assert!(hub.clients.contains_key(&first));
    }

    #[test]
    fn deregister_clears_the_device_slot_only_for_the_current_holder() {
        // A superseded connection tears down after its replacement registered.
        // Its deregister must not erase the live mapping.
        let mut hub = hub_direct();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let device = Uuid::new_v4();

        hub.handle(register_msg(first, device));
        hub.handle(register_msg(second, device));

        hub.handle(HubMessage::Deregister { id: first, device });

        assert_eq!(
            hub.device_index.get(&device),
            Some(&second),
            "the superseded connection must not clear the newer mapping"
        );
        assert!(
            hub.clients.contains_key(&second),
            "the current holder is untouched"
        );
        assert!(!hub.clients.contains_key(&first));

        // The real holder can clear it.
        hub.handle(HubMessage::Deregister { id: second, device });
        assert_eq!(hub.device_index.get(&device), None);
    }

    #[test]
    fn deregister_of_an_unknown_client_leaves_the_device_slot_alone() {
        // Double deregister: the row is gone, so the slot belongs to whoever
        // holds it now.
        let mut hub = hub_direct();
        let holder = Uuid::new_v4();
        let device = Uuid::new_v4();
        hub.handle(register_msg(holder, device));

        hub.handle(HubMessage::Deregister {
            id: Uuid::new_v4(),
            device,
        });
        assert_eq!(hub.device_index.get(&device), Some(&holder));
    }

    /// A minimal valid eager chunk, as a client would send it.
    fn chunk_from_client(from_device: Option<Uuid>) -> FileChunkFrame {
        FileChunkFrame {
            file_id: Uuid::new_v4(),
            name: "payload.bin".to_string(),
            content_type: "application/octet-stream".to_string(),
            chunk_index: 0,
            total_chunks: 1,
            file_sha256: Some("00:11".to_string()),
            file_size: 3,
            payload_b64: "AAEC".to_string(),
            from: None,
            to: None,
            transfer_id: None,
            from_device,
        }
    }

    #[test]
    fn a_client_supplied_from_device_is_overwritten_by_the_hub() {
        // The sender's frame arrives with `from_device` forged to a third
        // device. The hub must *overwrite* it with the sender's registered
        // device — fill-if-empty would let a peer attribute its file to any
        // device it names.
        let mut hub = hub_direct();
        let sender_client = Uuid::new_v4();
        let sender_device = Uuid::new_v4();
        let forged_device = Uuid::new_v4();
        let receiver_client = Uuid::new_v4();

        hub.handle(register_msg(sender_client, sender_device));

        // Register the receiver with a file channel this test owns.
        let (file_tx, mut file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: receiver_client,
            device: Uuid::new_v4(),
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: file_tx,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames: mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF).0,
            ctl: mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF).0,
            ack,
        });

        assert_ne!(forged_device, sender_device);
        hub.handle(HubMessage::PublishFile {
            from: sender_client,
            chunk: chunk_from_client(Some(forged_device)),
        });

        let delivered = file_rx.try_recv().expect("receiver should get the chunk");
        assert_eq!(
            delivered.from_device,
            Some(sender_device),
            "from_device must be the registered device, not the forged value"
        );
        assert_ne!(
            delivered.from_device,
            Some(forged_device),
            "the forged device must never reach a peer"
        );
        assert_eq!(delivered.from, Some(sender_client));
    }

    #[test]
    fn a_chunk_from_an_unregistered_client_stamps_no_from_device() {
        // Racing deregister: `from` is not in `clients`, so there is no
        // authoritative device. The hub relays with `None` rather than echoing
        // whatever the frame claimed.
        let mut hub = hub_direct();
        let receiver_client = Uuid::new_v4();
        let (file_tx, mut file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: receiver_client,
            device: Uuid::new_v4(),
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: file_tx,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames: mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF).0,
            ctl: mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF).0,
            ack,
        });

        let forged = Uuid::new_v4();
        hub.handle(HubMessage::PublishFile {
            from: Uuid::new_v4(), // never registered
            chunk: chunk_from_client(Some(forged)),
        });

        let delivered = file_rx
            .try_recv()
            .expect("the unregistered sender is still relayed to others");
        assert_eq!(
            delivered.from_device, None,
            "no registered device means no stamp, not a forwarded guess"
        );
    }

    // ---- Offer registry (3.5) ----
    //
    // Every failure mode here is *reported*, never fatal: a rejected offer must
    // not close the sender's connection (spec: Offer limits). These tests drive
    // `Hub::handle` directly, so "the connection survives" is visible as
    // "`handle` returned a verdict instead of panicking"; the socket-level
    // guarantee belongs to 3.9 when the route is wired.

    /// A `Hub` with a fixed clock, plus one registered sender.
    fn hub_at(now: i64) -> (Hub, Uuid, Uuid) {
        let mut hub = hub_direct();
        hub.now_override = Some(now);
        let client = Uuid::new_v4();
        let device = Uuid::new_v4();
        hub.handle(register_msg(client, device));
        (hub, client, device)
    }

    fn offer_at(file_id: Uuid, expires_at_ms: i64) -> FileOfferFrame {
        FileOfferFrame {
            file_id,
            batch_id: Uuid::new_v4(),
            ts: 0,
            name: "report.pdf".to_string(),
            content_type: "application/pdf".to_string(),
            file_size: 1024,
            expires_at_ms,
            from_device: None,
        }
    }

    fn publish_offer(hub: &mut Hub, from: Uuid, offer: FileOfferFrame) -> OfferResult {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        hub.handle(HubMessage::PublishOffer {
            from,
            offer,
            reply: reply_tx,
        });
        reply_rx.try_recv().expect("hub must answer every offer")
    }

    const MINUTE_MS: i64 = 60_000;
    const DAY_MS: i64 = 24 * 60 * MINUTE_MS;

    #[test]
    fn an_offer_in_the_future_is_accepted() {
        let (mut hub, client, device) = hub_at(1_000);
        let file_id = Uuid::new_v4();
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(file_id, 1_000 + MINUTE_MS)),
            OfferResult::Accepted
        );
        assert_eq!(hub.offers.len(), 1);
        let stored = &hub.offers[&(device, file_id)].frame;
        assert_eq!(
            stored.from_device,
            Some(device),
            "the hub stamps the registered device"
        );
    }

    #[test]
    fn an_offer_already_expired_is_rejected() {
        let (mut hub, client, _) = hub_at(1_000);
        // Exactly now is already too late: the deadline is exclusive.
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 1_000)),
            OfferResult::BadExpiry
        );
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 999)),
            OfferResult::BadExpiry
        );
        assert!(hub.offers.is_empty());
    }

    #[test]
    fn an_offer_beyond_24h_is_rejected() {
        let (mut hub, client, _) = hub_at(1_000);
        assert_eq!(
            publish_offer(
                &mut hub,
                client,
                offer_at(Uuid::new_v4(), 1_000 + DAY_MS + 1)
            ),
            OfferResult::BadExpiry
        );
        assert!(hub.offers.is_empty());
    }

    #[test]
    fn an_offer_exactly_24h_out_is_accepted() {
        // The cap is inclusive: 24 h out is allowed, 24 h + 1 ms is not.
        let (mut hub, client, _) = hub_at(1_000);
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 1_000 + DAY_MS)),
            OfferResult::Accepted
        );
        assert_eq!(hub.offers.len(), 1);
    }

    #[test]
    fn an_absurd_expiry_does_not_overflow() {
        // Asserts what an ordinary clock makes checkable: an expiry far past the
        // 24 h window is refused, and the hub does not panic on it.
        //
        // It does NOT cover `checked_add`'s `None` branch — with `now = 1_000`
        // the sum is 86_401_000, so the guard never fires and the rejection comes
        // from the `expires_at_ms > limit` comparison. That branch is covered by
        // `a_clock_near_i64_max_rejects_every_offer_instead_of_wrapping`.
        let (mut hub, client, _) = hub_at(1_000);
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), i64::MAX)),
            OfferResult::BadExpiry
        );
        assert!(hub.offers.is_empty());
    }

    #[test]
    fn a_clock_near_i64_max_rejects_every_offer_instead_of_wrapping() {
        // Covers `checked_add`'s `None` branch, which a real clock cannot reach
        // but the injectable one can. Its purpose: if `limit` wrapped negative,
        // `expires_at_ms > limit` would pass for every input and no offer could
        // ever expire — a fail-open, so it must fail closed instead.
        let (mut hub, client, _) = hub_at(i64::MAX - 1_000);

        // Any expiry `validate()` accepts must still be refused: no instant
        // beyond this clock is representable.
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), i64::MAX)),
            OfferResult::BadExpiry
        );
        assert!(hub.offers.is_empty());

        // Crucially, the field check is not what rejected it. Without this the
        // test would also pass if `validate()` grew a new reason to refuse.
        let sample = offer_at(Uuid::new_v4(), i64::MAX);
        assert!(
            sample.validate().is_ok(),
            "the frame passes field validation, so the rejection is the clock rule"
        );
    }

    #[test]
    fn a_negative_now_cannot_be_reached_but_max_expiry_still_rejects() {
        // The clock never goes negative (`now_ms` saturates at 0), but the
        // *upper bound* arithmetic must still hold if it did: with a negative
        // `now`, `checked_add` succeeds and the comparison still rejects the
        // absurd value. This pins the arithmetic, not the clock.
        let (mut hub, client, _) = hub_at(-1_000);
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), i64::MAX)),
            OfferResult::BadExpiry
        );
        // A sane expiry relative to that clock is still accepted. It has to be
        // positive because `FileOfferFrame::validate` rejects a non-positive
        // timestamp before the clock comparison ever runs.
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 500)),
            OfferResult::Accepted
        );
    }

    #[test]
    fn per_device_cap_rejects_the_excess() {
        let (mut hub, client, device) = hub_at(1_000);
        for _ in 0..MAX_OFFERS_PER_DEVICE {
            assert_eq!(
                publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
                OfferResult::Accepted
            );
        }
        assert_eq!(hub.offers.len(), MAX_OFFERS_PER_DEVICE);

        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::PerDeviceCap
        );
        assert_eq!(
            hub.offers.len(),
            MAX_OFFERS_PER_DEVICE,
            "a refusal must not disturb the existing offers"
        );
        assert!(hub.offers.keys().all(|(owner, _)| *owner == device));
    }

    #[test]
    fn total_cap_rejects_without_evicting_others() {
        // One connection per device, so the hub must accept more than the
        // default 8 clients.
        let mut hub = Hub::new(MAX_OFFERS_TOTAL, None);
        hub.now_override = Some(1_000);
        // Fill the global cap from many devices, so the per-device cap is never
        // the reason for a rejection.
        let per_device = MAX_OFFERS_TOTAL / 64;
        let mut filled = 0usize;
        let mut first_device = None;
        while filled < MAX_OFFERS_TOTAL {
            let client = Uuid::new_v4();
            let device = Uuid::new_v4();
            hub.handle(register_msg(client, device));
            for _ in 0..per_device {
                assert_eq!(
                    publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
                    OfferResult::Accepted
                );
                filled += 1;
            }
            first_device.get_or_insert(device);
        }
        assert_eq!(hub.offers.len(), MAX_OFFERS_TOTAL);
        let first_device = first_device.expect("at least one device filled");

        let intruder_client = Uuid::new_v4();
        let intruder_device = Uuid::new_v4();
        hub.handle(register_msg(intruder_client, intruder_device));
        assert_eq!(
            publish_offer(&mut hub, intruder_client, offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::TotalCap
        );
        assert_eq!(
            hub.offers.len(),
            MAX_OFFERS_TOTAL,
            "the total cap rejects the newcomer, it does not evict anyone"
        );
        assert!(
            hub.offers
                .keys()
                .all(|(owner, _)| *owner != intruder_device),
            "the refused offer is absent"
        );
        assert!(
            hub.offers
                .keys()
                .filter(|(owner, _)| *owner == first_device)
                .count()
                == per_device,
            "a pre-existing device still holds every offer it had"
        );
    }

    #[test]
    fn re_advertising_does_not_extend_expiry() {
        let (mut hub, client, device) = hub_at(1_000);
        let file_id = Uuid::new_v4();
        let t1 = 1_000 + MINUTE_MS;
        let t2 = 1_000 + 2 * MINUTE_MS;
        publish_offer(&mut hub, client, offer_at(file_id, t1));
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(file_id, t2)),
            OfferResult::Accepted
        );
        assert_eq!(
            hub.offers[&(device, file_id)].frame.expires_at_ms,
            t1,
            "a reconnect must not buy the offer more time"
        );
    }

    #[test]
    fn re_advertising_can_shorten_expiry() {
        let (mut hub, client, device) = hub_at(1_000);
        let file_id = Uuid::new_v4();
        let t1 = 1_000 + MINUTE_MS;
        let t2 = 1_000 + 2 * MINUTE_MS;
        publish_offer(&mut hub, client, offer_at(file_id, t2));
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(file_id, t1)),
            OfferResult::Accepted
        );
        assert_eq!(
            hub.offers[&(device, file_id)].frame.expires_at_ms,
            t1,
            "the source may bring its own deadline in"
        );
    }

    #[test]
    fn two_devices_can_use_the_same_file_id() {
        // The reason the key is `(device, file_id)`: a bare `file_id` would let
        // the second device overwrite the first one's offer.
        let (mut hub, a_client, _a_device) = hub_at(1_000);
        let b_client = Uuid::new_v4();
        hub.handle(register_msg(b_client, Uuid::new_v4()));

        let shared_file_id = Uuid::new_v4();
        assert_eq!(
            publish_offer(&mut hub, a_client, offer_at(shared_file_id, 2_000)),
            OfferResult::Accepted
        );
        assert_eq!(
            publish_offer(&mut hub, b_client, offer_at(shared_file_id, 2_000)),
            OfferResult::Accepted
        );
        assert_eq!(hub.offers.len(), 2);
    }

    #[test]
    fn an_unregistered_sender_offer_is_rejected() {
        let (mut hub, _client, _device) = hub_at(1_000);
        assert_eq!(
            publish_offer(&mut hub, Uuid::new_v4(), offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::Invalid("sender is not registered".into())
        );
        assert!(hub.offers.is_empty());
    }

    #[test]
    fn a_never_valid_offer_does_not_close_the_connection() {
        // Field-shape failure is a verdict, not a fault: the sender stays
        // registered and can send a corrected offer. The socket-level half of
        // "does not close" is the ws reader's job (3.9 wires the route).
        let (mut hub, client, _device) = hub_at(1_000);
        let mut bad = offer_at(Uuid::new_v4(), 2_000);
        bad.name = String::new();

        match publish_offer(&mut hub, client, bad) {
            OfferResult::Invalid(detail) => {
                assert!(detail.contains("name"), "{detail}")
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
        assert!(hub.offers.is_empty());
        assert!(
            hub.clients.contains_key(&client),
            "the sender must still be registered"
        );
        // And a well-formed offer from the same connection still works.
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::Accepted
        );
    }

    // ---- Pull authorization (3.6) ----

    /// A `Hub` at a fixed clock with two registered, unrelated peers:
    /// `(source_client, source_device, puller_client, puller_device)`.
    fn hub_with_two_peers(now: i64) -> (Hub, Uuid, Uuid, Uuid, Uuid) {
        let mut hub = hub_direct();
        hub.now_override = Some(now);
        let source_client = Uuid::new_v4();
        let source_device = Uuid::new_v4();
        let puller_client = Uuid::new_v4();
        let puller_device = Uuid::new_v4();
        // Register with live receivers: a pull is *delivered* to the source now,
        // so a dropped receiver would turn `Forward` into `SourceGone`.
        let (s_chunks, s_frames) = register_capturing(&mut hub, source_client, source_device);
        let (p_chunks, p_frames) = register_capturing(&mut hub, puller_client, puller_device);
        keep_alive(s_chunks);
        keep_alive(s_frames);
        keep_alive(p_chunks);
        keep_alive(p_frames);
        (
            hub,
            source_client,
            source_device,
            puller_client,
            puller_device,
        )
    }

    /// A pull as the *client* sends it: `from_device` is absent, because only
    /// the hub may fill it. Tests that inspect the relayed frame assert the
    /// stamped value.
    fn pull_of(file_id: Uuid, to: Uuid) -> FilePullFrame {
        FilePullFrame {
            file_id,
            transfer_id: Uuid::new_v4(),
            to,
            chunk_index: 0,
            window: 4,
            from_device: None,
        }
    }

    /// Run a pull through `handle`, as the ws route will in 3.9.
    fn authorize(hub: &mut Hub, from: Uuid, pull: FilePullFrame) -> PullResult {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        hub.handle(HubMessage::PublishPull {
            from,
            pull,
            reply: reply_tx,
        });
        reply_rx.try_recv().expect("hub must answer every pull")
    }

    #[test]
    fn a_pull_for_a_live_offer_is_authorized() {
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );

        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::Forward { source_client }
        );
    }

    #[test]
    fn a_pull_naming_itself_as_source_is_unknown_offer() {
        // The puller has no offer of its own, so naming itself as the source
        // finds nothing. This is what a mistaken "puller == source" reading
        // would look like from the wire.
        let (mut hub, _source_client, _source_device, puller_client, puller_device) =
            hub_with_two_peers(1_000);
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_of(Uuid::new_v4(), puller_device)
            ),
            PullResult::UnknownOffer
        );
    }

    #[test]
    fn the_puller_need_not_own_the_offer() {
        // The regression this exists for: an early design draft required the
        // puller to be the offer's owner. `to` names the source, and the source
        // is never the puller, so that rule rejected every legitimate pull.
        let (mut hub, source_client, source_device, puller_client, puller_device) =
            hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );

        assert_ne!(puller_device, source_device);
        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::Forward { source_client },
            "an unrelated third party must still be able to pull"
        );
    }

    #[test]
    fn a_pull_for_an_unexpired_offer_still_works_near_the_boundary() {
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        // One millisecond of life left.
        publish_offer(&mut hub, source_client, offer_at(file_id, 1_000 + 1));
        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::Forward { source_client }
        );
        assert!(hub.offers.contains_key(&(source_device, file_id)));
    }

    #[test]
    fn a_pull_for_an_expired_offer_is_expired() {
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 2_000));
        assert_eq!(hub.offers.len(), 1);

        // The clock has moved past the deadline; the offer is still stored.
        hub.now_override = Some(2_000);

        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::Expired
        );
        assert!(
            !hub.offers.contains_key(&(source_device, file_id)),
            "an expired offer is dropped so it cannot answer twice"
        );
    }

    #[test]
    fn a_pull_for_an_missing_offer_is_unknown_offer() {
        let (mut hub, _source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_of(Uuid::new_v4(), source_device)
            ),
            PullResult::UnknownOffer
        );
    }

    #[test]
    fn a_pull_reports_source_gone_when_the_owner_is_offline() {
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );

        hub.handle(HubMessage::Deregister {
            id: source_client,
            device: source_device,
        });
        assert!(
            hub.offers.contains_key(&(source_device, file_id)),
            "the offer outlives the connection; the grace period is 3.8"
        );

        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::SourceGone
        );
    }

    #[test]
    fn a_pull_reports_source_gone_when_device_index_is_stale() {
        // Constructed directly: `device_index` still maps the device to a
        // client_id that `clients` no longer holds. `Deregister` cannot produce
        // this state (it clears both), but a crash between the two, or a future
        // code change, could — and relaying to a dead connection would silently
        // drop the pull.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );

        hub.clients.remove(&source_client);
        assert_eq!(hub.device_index.get(&source_device), Some(&source_client));

        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::SourceGone
        );
    }

    #[test]
    fn a_pull_with_an_invalid_window_is_invalid() {
        // `validate` runs before the lookup, so a bad window is reported as such
        // rather than as `unknown_offer` for an offer that does exist.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );

        // The typed error survives, which is what lets the caller answer
        // `window_too_large` instead of a generic refusal.
        for (window, expected) in [
            (0u32, ControlFrameError::EmptyWindow),
            (33u32, ControlFrameError::WindowTooLarge),
        ] {
            let mut pull = pull_of(file_id, source_device);
            pull.window = window;
            match authorize(&mut hub, puller_client, pull) {
                PullResult::Invalid(error) => assert_eq!(error, expected, "window={window}"),
                other => panic!("window={window} should be Invalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_pull_for_another_devices_offer_is_unknown_offer() {
        // Proves the key includes the owner: a bare `file_id` key would let a
        // puller name device C while using device A's file id and succeed.
        let (mut hub, a_client, a_device, b_client, _) = hub_with_two_peers(1_000);
        let c_client = Uuid::new_v4();
        let c_device = Uuid::new_v4();
        // C must have a live frames channel: a pull is delivered to the source,
        // so a dropped receiver would turn a legitimate `Forward` into
        // `SourceGone`.
        let c_frames = register_with_frames(&mut hub, c_client, c_device);
        keep_alive(c_frames);

        let shared_file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            a_client,
            offer_at(shared_file_id, 1_000 + MINUTE_MS),
        );
        publish_offer(
            &mut hub,
            c_client,
            offer_at(shared_file_id, 1_000 + MINUTE_MS),
        );
        assert_ne!(a_device, c_device);

        // A's file id, but C as the source: C *does* have that file id, so this
        // would falsely pass if the key were only `file_id`... except it would
        // also legitimately pass. The discriminating case is the next one.
        assert_eq!(
            authorize(&mut hub, b_client, pull_of(shared_file_id, c_device)),
            PullResult::Forward {
                source_client: c_client
            }
        );

        // A file id only A owns, addressed to C: `file_id` alone would find A's
        // offer and wrongly authorize C.
        let a_only = Uuid::new_v4();
        publish_offer(&mut hub, a_client, offer_at(a_only, 1_000 + MINUTE_MS));
        assert_eq!(
            authorize(&mut hub, b_client, pull_of(a_only, c_device)),
            PullResult::UnknownOffer,
            "C must not inherit A's offer just because the file_id is public"
        );
    }

    // ---- Welcome snapshot + peer_changed (3.9) ----

    /// Register a peer and return its routed-frame receiver.
    fn register_with_frames(hub: &mut Hub, id: Uuid, device: Uuid) -> mpsc::Receiver<Frame> {
        let (_chunks, frames) = register_capturing(hub, id, device);
        frames
    }

    #[test]
    fn welcome_lists_live_offers_and_peers() {
        let (mut hub, a_client, a_device) = hub_at(1_000);
        for _ in 0..2 {
            assert_eq!(
                publish_offer(&mut hub, a_client, offer_at(Uuid::new_v4(), 5_000)),
                OfferResult::Accepted
            );
        }

        let b_client = Uuid::new_v4();
        let (ack_tx, mut ack_rx) = oneshot::channel();
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow, _overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (frames, _frames_rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        hub.handle(HubMessage::Register {
            id: b_client,
            device: Uuid::new_v4(),
            caps: Caps::default(),
            outbound: clip,
            file_outbound: file,
            overflow,
            frames,
            ctl,
            ack: ack_tx,
        });

        match ack_rx.try_recv().expect("registration must be acked") {
            RegisterResult::Accepted { peers, offers, .. } => {
                assert_eq!(peers.len(), 1, "only the other peer, not the newcomer");
                assert_eq!(peers[0].0, a_device);
                assert_eq!(offers.len(), 2, "both live offers are listed");
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    #[test]
    fn welcome_does_not_include_an_expired_offer() {
        // The deadline passes while the offer is still in the map (only a pull
        // or a cap check sweeps lazily). Registration sweeps first, so a late
        // joiner is never handed a deadline that has already passed.
        let (mut hub, a_client, _a_device) = hub_at(1_000);
        assert_eq!(
            publish_offer(&mut hub, a_client, offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::Accepted
        );
        hub.now_override = Some(3_000);

        let b_client = Uuid::new_v4();
        let (ack_tx, mut ack_rx) = oneshot::channel();
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow, _overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (frames, _frames_rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        hub.handle(HubMessage::Register {
            id: b_client,
            device: Uuid::new_v4(),
            caps: Caps::default(),
            outbound: clip,
            file_outbound: file,
            overflow,
            frames,
            ctl,
            ack: ack_tx,
        });

        match ack_rx.try_recv().expect("registration must be acked") {
            RegisterResult::Accepted { offers, .. } => {
                assert!(offers.is_empty(), "an expired offer must not be listed");
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    #[test]
    fn registering_broadcasts_peer_joined_to_others() {
        let (mut hub, a_client, a_device) = hub_at(1_000);
        let mut a_frames = register_with_frames(&mut hub, a_client, a_device);

        let b_client = Uuid::new_v4();
        let b_device = Uuid::new_v4();
        let mut b_frames = register_with_frames(&mut hub, b_client, b_device);

        match a_frames.try_recv().expect("A must learn about B") {
            Frame::PeerChanged(frame) => {
                assert_eq!(frame.device, b_device);
                assert_eq!(frame.event, PeerEvent::Joined);
            }
            other => panic!("expected peer_changed, got {other:?}"),
        }
        assert!(
            b_frames.try_recv().is_err(),
            "a client must not be told about its own join"
        );
    }

    #[test]
    fn grace_expiry_broadcasts_peer_left() {
        let (mut hub, a_client, a_device) = hub_at(1_000);
        let a_caps = Caps::LAZY_FILE | Caps::FILE_RECV;
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow, _overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (a_frames_tx, mut a_frames) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: a_client,
            device: a_device,
            caps: a_caps,
            outbound: clip,
            file_outbound: file,
            overflow,
            frames: a_frames_tx,
            ctl,
            ack,
        });

        let b_client = Uuid::new_v4();
        let mut b_frames = register_with_frames(&mut hub, b_client, Uuid::new_v4());
        // Drain B's `joined` for A.
        b_frames.try_recv().ok();
        a_frames.try_recv().ok();

        hub.handle(HubMessage::Deregister {
            id: a_client,
            device: a_device,
        });
        hub.now_override = Some(1_000 + DEVICE_GRACE_MS);
        hub.handle(HubMessage::Tick);

        match b_frames.try_recv().expect("B must learn that A left") {
            Frame::PeerChanged(frame) => {
                assert_eq!(frame.device, a_device);
                assert_eq!(frame.event, PeerEvent::Left);
                assert_eq!(
                    frame.caps,
                    a_caps.tokens(),
                    "the departing device's capabilities are still reported"
                );
            }
            other => panic!("expected peer_changed, got {other:?}"),
        }
    }

    #[test]
    fn a_pull_for_a_live_offer_is_delivered_to_the_source() {
        let (mut hub, source_client, source_device, puller_client, puller_device) =
            hub_with_two_peers(1_000);
        // Re-register the source with a frames channel the test can read.
        let (frames_tx, mut source_frames) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow, _overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: source_client,
            device: source_device,
            caps: Caps::default(),
            outbound: clip,
            file_outbound: file,
            overflow,
            frames: frames_tx,
            ctl,
            ack,
        });

        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );
        // Drain the peer_changed from the re-registration.
        while source_frames.try_recv().is_ok() {}

        assert!(matches!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::Forward { source_client: sc } if sc == source_client
        ));

        match source_frames
            .try_recv()
            .expect("the source must receive the pull")
        {
            Frame::FilePull(pull) => {
                assert_eq!(pull.to, source_device);
                assert_eq!(pull.file_id, file_id);
                assert_eq!(pull.chunk_index, 0);
                assert_eq!(
                    pull.from_device,
                    Some(puller_device),
                    "the source needs the puller's device to address its reply"
                );
            }
            other => panic!("expected file_pull, got {other:?}"),
        }
    }

    #[test]
    fn a_relayed_pull_carries_the_pullers_device() {
        // The property the whole reply path rests on: chunks and session-scoped
        // aborts are addressed to `from_device`, so a pull relayed without it
        // cannot be served at all.
        let (mut hub, source_client, source_device, puller_client, puller_device) =
            hub_with_two_peers(1_000);
        let (frames_tx, mut source_frames) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow, _overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: source_client,
            device: source_device,
            caps: Caps::default(),
            outbound: clip,
            file_outbound: file,
            overflow,
            frames: frames_tx,
            ctl,
            ack,
        });

        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );
        while source_frames.try_recv().is_ok() {}

        // The client's own frame never carries this field.
        let mut pull = pull_of(file_id, source_device);
        assert_eq!(
            pull.from_device, None,
            "precondition: the client sends none"
        );
        pull.from_device = None;
        assert!(matches!(
            authorize(&mut hub, puller_client, pull),
            PullResult::Forward { .. }
        ));

        match source_frames
            .try_recv()
            .expect("the source must receive it")
        {
            Frame::FilePull(relayed) => {
                assert_eq!(relayed.from_device, Some(puller_device));
                assert_ne!(relayed.from_device, Some(source_device));
            }
            other => panic!("expected file_pull, got {other:?}"),
        }
    }

    #[test]
    fn a_client_supplied_from_device_on_a_pull_is_overwritten() {
        // Security, not hygiene: `from_device` decides where the file bytes go.
        // Trusting the client's value would let any authenticated peer aim a
        // pull's reply — and therefore the file contents — at a third device.
        let (mut hub, source_client, source_device, puller_client, puller_device) =
            hub_with_two_peers(1_000);
        let (frames_tx, mut source_frames) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow, _overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: source_client,
            device: source_device,
            caps: Caps::default(),
            outbound: clip,
            file_outbound: file,
            overflow,
            frames: frames_tx,
            ctl,
            ack,
        });

        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + MINUTE_MS),
        );
        while source_frames.try_recv().is_ok() {}

        let victim_device = Uuid::new_v4();
        assert_ne!(victim_device, puller_device);
        let mut pull = pull_of(file_id, source_device);
        pull.from_device = Some(victim_device);

        assert!(matches!(
            authorize(&mut hub, puller_client, pull),
            PullResult::Forward { .. }
        ));

        match source_frames
            .try_recv()
            .expect("the source must receive it")
        {
            Frame::FilePull(relayed) => {
                assert_eq!(
                    relayed.from_device,
                    Some(puller_device),
                    "the hub overwrites, never fills only when empty"
                );
                assert_ne!(
                    relayed.from_device,
                    Some(victim_device),
                    "a client must not be able to redirect the reply"
                );
            }
            other => panic!("expected file_pull, got {other:?}"),
        }
    }

    #[test]
    fn a_pull_for_an_expired_offer_returns_expired_and_contacts_nobody() {
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let (frames_tx, mut source_frames) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (clip, _clip_rx) = mpsc::channel(PER_CLIENT_CHANNEL_BUF);
        let (file, _file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow, _overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (ctl, _ctl_rx) = mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: source_client,
            device: source_device,
            caps: Caps::default(),
            outbound: clip,
            file_outbound: file,
            overflow,
            frames: frames_tx,
            ctl,
            ack,
        });

        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 2_000));
        while source_frames.try_recv().is_ok() {}
        hub.now_override = Some(3_000);

        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::Expired
        );
        assert!(
            source_frames.try_recv().is_err(),
            "a refused pull must not reach the source"
        );
    }

    // ---- TTL gates the start of a transfer, not its continuation ----

    /// A pull for `(index)` under a caller-chosen session.
    fn pull_at(file_id: Uuid, to: Uuid, transfer_id: Uuid, chunk_index: u32) -> FilePullFrame {
        FilePullFrame {
            file_id,
            transfer_id,
            to,
            chunk_index,
            window: 4,
            from_device: None,
        }
    }

    #[test]
    fn a_transfer_that_started_inside_the_ttl_continues_after_it() {
        // Spec, §Offer lifecycle, scenario "TTL expires during a transfer":
        // "WHEN an offer's TTL elapses while a pull is in progress THEN the
        // transfer continues and completes."
        //
        // Without this, a file big enough to need a second window half an hour
        // in is undeliverable -- and large files are the whole point.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 1_000 + 1_000));

        let transfer_id = Uuid::new_v4();
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 0)
            ),
            PullResult::Forward { source_client },
            "the first window starts inside the TTL"
        );

        // The offer's deadline passes while the transfer is mid-file.
        hub.now_override = Some(1_000 + 2_000);

        for index in 1..4 {
            assert_eq!(
                authorize(
                    &mut hub,
                    puller_client,
                    pull_at(file_id, source_device, transfer_id, index)
                ),
                PullResult::Forward { source_client },
                "window {index} must still be served after the TTL"
            );
        }
    }

    #[test]
    fn a_new_transfer_after_the_ttl_is_refused() {
        // The other half of the rule: expiry still gates the *start*. A
        // different `transfer_id` after the deadline is a new transfer, and the
        // cache of started sessions must not wave it through.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 1_000 + 1_000));

        let started = Uuid::new_v4();
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, started, 0)
            ),
            PullResult::Forward { source_client }
        );

        hub.now_override = Some(1_000 + 2_000);

        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, Uuid::new_v4(), 0)
            ),
            PullResult::Expired,
            "a transfer that never started is still gated by the TTL"
        );
    }

    #[test]
    fn the_offer_survives_for_a_continued_transfer() {
        // A continued transfer still needs the offer: later windows read
        // `file_size`, `name` and the source path through it. Dropping the
        // entry in the expired branch would turn a mid-file window into
        // `unknown_offer` -- a different failure with a different recovery.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 1_000 + 1_000));

        let transfer_id = Uuid::new_v4();
        assert!(matches!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 0)
            ),
            PullResult::Forward { .. }
        ));

        hub.now_override = Some(1_000 + 2_000);
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 1)
            ),
            PullResult::Forward { source_client }
        );

        assert!(
            hub.offers.contains_key(&(source_device, file_id)),
            "the offer a live transfer is still reading must not be swept"
        );
    }

    // ---- Bounding the transfer memory ----

    /// A hub whose source routed-frame channel the caller must drain, so
    /// `deliver_frame_to_device` keeps returning `Delivered`.
    ///
    /// Without the drain the channel holds `PER_CLIENT_FRAME_CHANNEL_BUF`
    /// frames and every pull after that is `Busy` — which never reaches the
    /// `active_transfers` insert, so a test of the bound would pass vacuously.
    ///
    /// The receiver is returned rather than drained by a spawned thread: a
    /// thread has to be scheduled before the first pull, and losing that race
    /// makes the test flaky rather than wrong.
    fn hub_with_drained_source(now: i64) -> (Hub, Uuid, Uuid, Uuid, Uuid, mpsc::Receiver<Frame>) {
        let mut hub = hub_direct();
        hub.now_override = Some(now);

        let source_client = Uuid::new_v4();
        let source_device = Uuid::new_v4();
        let puller_client = Uuid::new_v4();
        let puller_device = Uuid::new_v4();

        let (s_file, s_file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (s_frames, s_frames_rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (s_ack, _s_ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: source_client,
            device: source_device,
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: s_file,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames: s_frames,
            ctl: mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF).0,
            ack: s_ack,
        });
        let (p_file, p_file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (p_frames, p_frames_rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (p_ack, _p_ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: puller_client,
            device: puller_device,
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: p_file,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames: p_frames,
            ctl: mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF).0,
            ack: p_ack,
        });

        keep_alive(s_file_rx);
        keep_alive(p_file_rx);
        keep_alive(p_frames_rx);

        (
            hub,
            source_client,
            source_device,
            puller_client,
            puller_device,
            s_frames_rx,
        )
    }

    #[test]
    fn the_transfer_memory_is_bounded() {
        // The bound exists because nothing else limits the number of
        // `transfer_id`s a peer can mint against one live offer: every pull is
        // authorized, `Delivered` only means the frame reached the source's
        // channel, and each entry lives for `TRANSFER_MEMORY_MS`. A few hundred
        // pulls a second would grow the map for half an hour.
        let (mut hub, source_client, source_device, puller_client, _, mut source_frames) =
            hub_with_drained_source(1_000);
        let file_id = Uuid::new_v4();
        // Deadline far beyond the test so TTL never plays a part: the only
        // thing that can bound the map here is the cap.
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + 10 * MINUTE_MS),
        );

        let started = std::time::Instant::now();
        let attempts = MAX_ACTIVE_TRANSFERS + 100;
        for _ in 0..attempts {
            assert_eq!(
                authorize(
                    &mut hub,
                    puller_client,
                    pull_at(file_id, source_device, Uuid::new_v4(), 0)
                ),
                PullResult::Forward { source_client },
                "every pull is authorized; the cap bounds memory, not admission"
            );
            // Drain the source's relayed frames, or the buffer fills after
            // `PER_CLIENT_FRAME_CHANNEL_BUF` pulls and every later one is
            // `Busy` — which never reaches the insert under test.
            while source_frames.try_recv().is_ok() {}
        }
        let elapsed = started.elapsed();

        assert!(
            hub.active_transfers.len() <= MAX_ACTIVE_TRANSFERS,
            "the transfer memory grew past its bound: {} entries",
            hub.active_transfers.len()
        );
        // Exactly one eviction pass can have run here: the map reached the cap
        // once, a batch was freed, and the remaining inserts never got back to
        // it. So the length is inside `(cap - batch, cap]`.
        assert!(
            hub.active_transfers.len() > MAX_ACTIVE_TRANSFERS - EVICT_BATCH,
            "fewer entries than one eviction pass removes: {} means it ran twice",
            hub.active_transfers.len()
        );
        assert!(
            hub.active_transfers.len() < MAX_ACTIVE_TRANSFERS + 1,
            "the cap was exceeded"
        );
        println!(
            "{attempts} pulls in {elapsed:?} ({:.0} pulls/s)",
            attempts as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
        );
    }

    #[test]
    fn evicting_the_oldest_keeps_the_newest() {
        // `select_nth_unstable_by_key` must partition on the *oldest* entries.
        // Getting the pivot direction wrong would evict the newest sessions and
        // keep the coldest ones, which is exactly backwards.
        let mut hub = hub_direct();
        hub.now_override = Some(1_000);

        let mut ids = Vec::new();
        for i in 0..40u128 {
            let id = Uuid::from_u128(i + 1);
            ids.push(id);
            // Strictly increasing `last_pull`, so "oldest" is unambiguous.
            hub.active_transfers.insert(
                id,
                ActiveTransfer {
                    last_pull: 1_000 + i as i64,
                    offer: (Uuid::from_u128(i + 1), Uuid::from_u128(i + 1)),
                },
            );
        }

        let evicted = hub.evict_oldest_transfers(10);
        assert_eq!(evicted, 10);
        assert_eq!(hub.active_transfers.len(), 30);

        for id in &ids[..10] {
            assert!(
                !hub.active_transfers.contains_key(id),
                "the ten oldest must be the ten evicted"
            );
        }
        for id in &ids[10..] {
            assert!(
                hub.active_transfers.contains_key(id),
                "the newest sessions must survive"
            );
        }

        // A batch larger than the map is a full reset, not an overflow.
        assert_eq!(hub.evict_oldest_transfers(100), 30);
        assert!(hub.active_transfers.is_empty());
    }

    #[test]
    fn a_repeated_window_refreshes_the_transfer_age() {
        // The map is keyed by `transfer_id`, so forgetting to refresh
        // `last_pull` on a repeat window would let `sweep_active_transfers`
        // delete a session that is still being pulled — and its next window
        // would then be judged by the TTL again, mid-file.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + 10 * MINUTE_MS),
        );

        let transfer_id = Uuid::new_v4();
        for index in 0..3u32 {
            hub.now_override = Some(1_000 + i64::from(index) * 60_000);
            assert_eq!(
                authorize(
                    &mut hub,
                    puller_client,
                    pull_at(file_id, source_device, transfer_id, index)
                ),
                PullResult::Forward { source_client },
                "window {index}"
            );
            let active = hub
                .active_transfers
                .get(&transfer_id)
                .expect("the session is remembered");
            assert_eq!(
                active.last_pull,
                1_000 + i64::from(index) * 60_000,
                "every window must refresh the age"
            );
        }

        // Two minutes in, still inside the memory window: the sweep keeps it.
        hub.now_override = Some(1_000 + 2 * 60_000);
        assert_eq!(hub.sweep_active_transfers(), 0);
        assert!(hub.active_transfers.contains_key(&transfer_id));
    }

    #[test]
    fn a_forgotten_transfer_falls_back_to_the_ttl_gate() {
        // Eviction must fail closed. Forgetting a session only means its later
        // windows are judged by the offer's TTL again — the pre-existing
        // behaviour — never that they are waved through.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 1_000 + 1_000));

        let transfer_id = Uuid::new_v4();
        assert!(matches!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 0)
            ),
            PullResult::Forward { .. }
        ));
        assert!(hub.active_transfers.contains_key(&transfer_id));

        // Evicted before the offer's deadline passes: the window right after
        // eviction is inside the TTL, so it is still served — and re-recorded.
        assert_eq!(hub.evict_oldest_transfers(1), 1);
        assert!(!hub.active_transfers.contains_key(&transfer_id));
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 1)
            ),
            PullResult::Forward { source_client },
            "eviction alone must not break a transfer still inside the TTL"
        );

        // Now evict again and let the deadline pass. With no memory of the
        // session, the pull is refused by TTL — fail-closed.
        assert_eq!(hub.evict_oldest_transfers(1), 1);
        hub.now_override = Some(1_000 + 2_000);
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 2)
            ),
            PullResult::Expired,
            "a forgotten session is gated by TTL again, not waved through"
        );
    }

    #[test]
    fn a_registration_mid_transfer_does_not_sweep_the_offer_being_read() {
        // `sweep_expired_offers` runs on *every* registration, not just on the
        // pull path, so a peer joining a busy hub would delete the offer under
        // a running transfer and the next window would fail `unknown_offer` --
        // the same TTL bug through a different door. The offer a live transfer
        // started from is exempt, and only until the session is forgotten.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 1_000 + 1_000));

        let transfer_id = Uuid::new_v4();
        assert!(matches!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 0)
            ),
            PullResult::Forward { .. }
        ));

        // The deadline passes, then a third peer joins -- which sweeps.
        hub.now_override = Some(1_000 + 2_000);
        hub.handle(register_msg(Uuid::new_v4(), Uuid::new_v4()));

        assert!(
            hub.offers.contains_key(&(source_device, file_id)),
            "a registration must not sweep an offer a live transfer is reading"
        );
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 1)
            ),
            PullResult::Forward { source_client },
            "the transfer continues across a registration sweep"
        );

        // The exemption is not permanent: once the session is forgotten, the
        // same sweep may reap the corpse.
        hub.now_override = Some(1_000 + 2_000 + TRANSFER_MEMORY_MS + 1);
        hub.handle(HubMessage::Tick);
        hub.handle(register_msg(Uuid::new_v4(), Uuid::new_v4()));
        assert!(
            !hub.offers.contains_key(&(source_device, file_id)),
            "after the memory window the expired offer is swept normally"
        );
    }

    #[test]
    fn a_finished_transfer_is_forgotten_after_the_memory_window() {
        // The memory is bounded: a transfer nobody has pulled for
        // `TRANSFER_MEMORY_MS` is dropped, so a long-lived hub cannot accumulate
        // one entry per transfer forever. After that the TTL applies again,
        // which fails closed -- the worst outcome is a refusal that triggers a
        // receiver restart, never a stale acceptance.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        // Long enough that the offer outlives the memory window, so the only
        // thing that can expire this pull is the forgotten session.
        publish_offer(
            &mut hub,
            source_client,
            offer_at(file_id, 1_000 + 4 * TRANSFER_MEMORY_MS),
        );

        let transfer_id = Uuid::new_v4();
        assert!(matches!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 0)
            ),
            PullResult::Forward { .. }
        ));
        assert_eq!(hub.active_transfers.len(), 1);

        // Past the memory window: the tick forgets it.
        hub.now_override = Some(1_000 + TRANSFER_MEMORY_MS + 1);
        hub.handle(HubMessage::Tick);
        assert_eq!(
            hub.active_transfers.len(),
            0,
            "an idle transfer must not be remembered forever"
        );

        // A late window is now a *new* transfer as far as the TTL is concerned.
        // The clock is also past the offer's deadline here, so it is refused.
        hub.now_override = Some(1_000 + 4 * TRANSFER_MEMORY_MS + 1);
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, transfer_id, 1)
            ),
            PullResult::Expired,
            "once forgotten, an expired offer is judged by TTL again"
        );
    }

    #[test]
    fn an_offer_expired_before_any_transfer_is_still_removed() {
        // The pre-existing behaviour, kept: with no session to protect, an
        // expired offer is refused *and* swept. Leaving the corpse would let
        // the same dead offer answer again and would count against the caps.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, source_client, offer_at(file_id, 1_000 + 1_000));

        hub.now_override = Some(1_000 + 2_000);
        assert_eq!(
            authorize(
                &mut hub,
                puller_client,
                pull_at(file_id, source_device, Uuid::new_v4(), 0)
            ),
            PullResult::Expired
        );
        assert!(
            !hub.offers.contains_key(&(source_device, file_id)),
            "a dead offer nobody is transferring must be dropped"
        );
        assert!(
            hub.active_transfers.is_empty(),
            "a refused pull starts nothing"
        );
    }

    // ---- Deregistration grace period (3.8) ----

    /// A fresh offer that outlives the 60 s grace period by a wide margin.
    fn long_lived_offer() -> (Uuid, FileOfferFrame) {
        let file_id = Uuid::new_v4();
        (file_id, offer_at(file_id, 10 * 60_000))
    }

    #[test]
    fn deregister_schedules_a_sweep_instead_of_dropping_offers() {
        let (mut hub, client, device) = hub_at(1_000);
        let (file_id, offer) = long_lived_offer();
        assert_eq!(
            publish_offer(&mut hub, client, offer),
            OfferResult::Accepted
        );

        hub.handle(HubMessage::Deregister { id: client, device });

        assert!(
            hub.offers.contains_key(&(device, file_id)),
            "a disconnect must not invalidate the device's offers"
        );
        assert_eq!(
            hub.device_index.get(&device),
            None,
            "the device is unreachable immediately, even while its offers survive"
        );
        assert_eq!(
            hub.pending_sweep.get(&device).map(|p| p.deadline),
            Some(1_000 + DEVICE_GRACE_MS)
        );
    }

    #[test]
    fn the_grace_sweep_drops_offers_and_broadcasts_source_gone() {
        let (mut hub, client, device) = hub_at(1_000);
        let (file_id, offer) = long_lived_offer();
        publish_offer(&mut hub, client, offer);

        let watcher_client = Uuid::new_v4();
        let watcher_device = Uuid::new_v4();
        let (_chunks, mut watcher_frames) =
            register_capturing(&mut hub, watcher_client, watcher_device);

        hub.handle(HubMessage::Deregister { id: client, device });
        // Nothing yet: the grace period has not elapsed.
        hub.handle(HubMessage::Tick);
        assert!(!hub.offers.is_empty());
        assert!(watcher_frames.try_recv().is_err());

        hub.now_override = Some(1_000 + DEVICE_GRACE_MS);
        hub.handle(HubMessage::Tick);

        assert!(hub.offers.is_empty(), "the device's offers are gone");
        assert!(hub.pending_sweep.is_empty());

        match watcher_frames.try_recv().expect("watchers must be told") {
            Frame::FileAbort(abort) => {
                assert_eq!(abort.reason, "source_gone");
                assert_eq!(abort.to, Some(watcher_device));
                assert_eq!(abort.file_id, file_id);
                assert_eq!(abort.from_device, Some(device));
                assert_eq!(abort.transfer_id, None, "offer-scoped, not session");
            }
            other => panic!("expected file_abort, got {other:?}"),
        }
    }

    #[test]
    fn a_reconnect_cancels_the_pending_sweep() {
        let (mut hub, client, device) = hub_at(1_000);
        let (file_id, offer) = long_lived_offer();
        publish_offer(&mut hub, client, offer);
        hub.handle(HubMessage::Deregister { id: client, device });
        assert!(hub.pending_sweep.contains_key(&device));

        let new_client = Uuid::new_v4();
        hub.handle(register_msg(new_client, device));
        assert!(
            hub.pending_sweep.is_empty(),
            "a reconnect cancels the sweep for its own device"
        );

        // Even past the original deadline, the offer survives.
        hub.now_override = Some(1_000 + DEVICE_GRACE_MS * 2);
        hub.handle(HubMessage::Tick);
        assert!(hub.offers.contains_key(&(device, file_id)));
    }

    #[test]
    fn a_flapping_device_keeps_its_original_offer_expiry() {
        // The "flapping connection" scenario: a source that reconnects every
        // 30 s must not keep buying itself more time. While the offer is live,
        // each re-advertisement is a no-op for `expires_at_ms`.
        let (mut hub, client, device) = hub_at(1_000);
        let original_expiry = 1_000 + 5 * 60_000; // t = 301_000
        let file_id = Uuid::new_v4();
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(file_id, original_expiry)),
            OfferResult::Accepted
        );

        let mut current_client = client;
        // Nine reconnects at 30 s intervals: the last lands at t = 271_000,
        // still inside the original TTL.
        for step in 0..9 {
            let now = 1_000 + (step as i64 + 1) * 30_000;
            hub.now_override = Some(now);
            hub.handle(HubMessage::Deregister {
                id: current_client,
                device,
            });
            current_client = Uuid::new_v4();
            hub.handle(register_msg(current_client, device));
            // Re-advertise on every reconnect, as a real source would.
            assert_eq!(
                publish_offer(
                    &mut hub,
                    current_client,
                    offer_at(file_id, now + 5 * 60_000)
                ),
                OfferResult::Accepted
            );
            assert_eq!(
                hub.offers[&(device, file_id)].frame.expires_at_ms,
                original_expiry,
                "step {step}: flapping must not extend a live offer"
            );
        }

        // At the original deadline the offer is dead: a pull says so, and the
        // grace timer is irrelevant because the device is connected.
        hub.now_override = Some(original_expiry);
        assert_eq!(
            authorize(&mut hub, Uuid::new_v4(), pull_of(file_id, device)),
            PullResult::Expired
        );
        assert!(
            !hub.offers.contains_key(&(device, file_id)),
            "the expired entry is dropped by the lazy pull path"
        );

        // Re-advertising now creates a *new* offer rather than extending the
        // old one — that is not a violation, it is a fresh advertisement after
        // the previous one died.
        hub.now_override = Some(original_expiry);
        assert_eq!(
            publish_offer(
                &mut hub,
                current_client,
                offer_at(file_id, original_expiry + 60_000)
            ),
            OfferResult::Accepted
        );
        assert_eq!(
            hub.offers[&(device, file_id)].frame.expires_at_ms,
            original_expiry + 60_000
        );
    }

    #[test]
    fn a_late_deregister_does_not_schedule_a_sweep_for_the_new_connection() {
        let mut hub = hub_direct();
        hub.now_override = Some(1_000);
        let device = Uuid::new_v4();
        let old_client = Uuid::new_v4();
        hub.handle(register_msg(old_client, device));
        let (file_id, offer) = long_lived_offer();
        publish_offer(&mut hub, old_client, offer);

        // A newer connection takes the device; the old one is evicted.
        let new_client = Uuid::new_v4();
        hub.handle(register_msg(new_client, device));
        assert_eq!(hub.device_index.get(&device), Some(&new_client));

        // The superseded connection's deregister arrives only now.
        hub.handle(HubMessage::Deregister {
            id: old_client,
            device,
        });

        assert!(
            hub.pending_sweep.is_empty(),
            "the old connection must not schedule a sweep for a live device"
        );
        assert!(
            hub.offers.contains_key(&(device, file_id)),
            "the current connection's offers are untouched"
        );
        assert_eq!(hub.device_index.get(&device), Some(&new_client));
    }

    #[test]
    fn an_absent_device_mid_transfer_is_reported_as_source_gone_on_pull() {
        // Ties 3.6 to 3.8: the offer is still listed (it survives the grace
        // period) but the source cannot be reached, so a pull says so.
        let (mut hub, source_client, source_device, puller_client, _) = hub_with_two_peers(1_000);
        let (file_id, offer) = long_lived_offer();
        publish_offer(&mut hub, source_client, offer);

        hub.handle(HubMessage::Deregister {
            id: source_client,
            device: source_device,
        });
        assert!(
            hub.offers.contains_key(&(source_device, file_id)),
            "the offer survives the grace period"
        );

        assert_eq!(
            authorize(&mut hub, puller_client, pull_of(file_id, source_device)),
            PullResult::SourceGone
        );
    }

    // ---- Backpressure (3.7b) ----

    /// A peer whose data channels the test controls directly, so both levels
    /// can be filled deterministically.
    struct CapturedPeer {
        chunks: mpsc::Receiver<FileChunkFrame>,
        overflow: mpsc::Receiver<FileChunkFrame>,
        frames: mpsc::Receiver<Frame>,
        /// Held so the senders stay open while the test runs; dropping these
        /// would turn `Full` into `Closed`.
        _keep: (mpsc::Sender<FileChunkFrame>, mpsc::Sender<FileChunkFrame>),
    }

    fn register_captured(hub: &mut Hub, id: Uuid, device: Uuid) -> CapturedPeer {
        let (file_tx, chunks) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow_tx, overflow) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (frames_tx, frames) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id,
            device,
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: file_tx.clone(),
            overflow: overflow_tx.clone(),
            frames: frames_tx,
            ctl: mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF).0,
            ack,
        });
        CapturedPeer {
            chunks,
            overflow,
            frames,
            _keep: (file_tx, overflow_tx),
        }
    }

    /// A lazy chunk for an existing transfer, with a caller-chosen session.
    fn lazy_chunk_with(to: Uuid, transfer_id: Option<Uuid>, file_id: Uuid) -> FileChunkFrame {
        let mut chunk = chunk_from_client(None);
        chunk.to = Some(to);
        chunk.transfer_id = transfer_id;
        chunk.file_id = file_id;
        chunk.file_sha256 = None;
        chunk
    }

    /// Route one chunk through the hub's directed path.
    ///
    /// `PublishFile` has no reply channel — only digest/abort report a verdict —
    /// so these tests call the same function that arm calls. The arm itself is
    /// covered by the 3.7a delivery tests.
    fn hub_deliver(
        hub: &mut Hub,
        sender_client: Uuid,
        mut chunk: FileChunkFrame,
    ) -> DeliveryResult {
        // Mirror what `PublishFile` does before calling `deliver_to_device`:
        // stamp the sender's identity, then read the target. The arm itself is
        // covered by the 3.7a tests; this helper exists so queueing can be
        // driven directly.
        chunk.from_device = hub.clients.get(&sender_client).map(|p| p.device);
        let device = chunk.to.expect("directed chunk");
        hub.deliver_to_device(device, chunk)
    }

    #[test]
    fn a_chunk_goes_to_overflow_when_the_file_channel_is_full() {
        let mut hub = hub_direct();
        let sender = Uuid::new_v4();
        hub.handle(register_msg(sender, Uuid::new_v4()));
        let target_device = Uuid::new_v4();
        let mut peer = register_captured(&mut hub, Uuid::new_v4(), target_device);

        let transfer_id = Uuid::new_v4();
        let file_id = Uuid::new_v4();

        // Fill the primary channel exactly.
        for i in 0..PER_CLIENT_FILE_CHANNEL_BUF {
            let mut chunk = lazy_chunk_with(target_device, Some(transfer_id), file_id);
            chunk.chunk_index = i as u32;
            assert_eq!(
                hub_deliver(&mut hub, sender, chunk),
                DeliveryResult::Delivered
            );
        }
        // The next one must land in overflow rather than being dropped.
        let mut extra = lazy_chunk_with(target_device, Some(transfer_id), file_id);
        extra.chunk_index = PER_CLIENT_FILE_CHANNEL_BUF as u32;
        assert_eq!(
            hub_deliver(&mut hub, sender, extra.clone()),
            DeliveryResult::Delivered
        );

        let queued: Vec<u32> = std::iter::from_fn(|| peer.overflow.try_recv().ok())
            .map(|c| c.chunk_index)
            .collect();
        assert_eq!(
            queued,
            vec![PER_CLIENT_FILE_CHANNEL_BUF as u32],
            "the overflowed chunk is the one that did not fit"
        );
        // The primary channel still holds exactly its capacity.
        let primary: Vec<u32> = std::iter::from_fn(|| peer.chunks.try_recv().ok())
            .map(|c| c.chunk_index)
            .collect();
        assert_eq!(primary.len(), PER_CLIENT_FILE_CHANNEL_BUF);
    }

    #[test]
    fn both_buffers_full_aborts_with_busy() {
        let mut hub = hub_direct();
        let sender = Uuid::new_v4();
        let sender_device = Uuid::new_v4();
        hub.handle(register_msg(sender, sender_device));
        let target_device = Uuid::new_v4();
        let mut peer = register_captured(&mut hub, Uuid::new_v4(), target_device);

        let transfer_id = Uuid::new_v4();
        let file_id = Uuid::new_v4();

        // Capacity of both levels, then one more.
        let total = PER_CLIENT_FILE_CHANNEL_BUF + PER_CLIENT_FILE_OVERFLOW;
        for i in 0..total {
            let mut chunk = lazy_chunk_with(target_device, Some(transfer_id), file_id);
            chunk.chunk_index = i as u32;
            assert_eq!(
                hub_deliver(&mut hub, sender, chunk),
                DeliveryResult::Delivered,
                "chunk {i} should still queue"
            );
        }

        let mut overflowed = lazy_chunk_with(target_device, Some(transfer_id), file_id);
        overflowed.chunk_index = total as u32;
        assert_eq!(
            hub_deliver(&mut hub, sender, overflowed),
            DeliveryResult::Busy
        );

        match peer.frames.try_recv().expect("the receiver must be told") {
            Frame::FileAbort(abort) => {
                assert_eq!(abort.reason, "busy");
                assert_eq!(abort.to, Some(target_device));
                assert_eq!(abort.transfer_id, Some(transfer_id));
                assert_eq!(abort.file_id, file_id);
                assert_eq!(
                    abort.from_device,
                    Some(sender_device),
                    "the receiver needs to know which source was busy"
                );
            }
            other => panic!("expected file_abort, got {other:?}"),
        }
    }

    #[test]
    fn an_eager_chunk_that_cannot_be_queued_is_dropped_not_aborted() {
        // No session means no `transfer_id` to echo and no restart mechanism in
        // v0.3, so a `busy` abort would only confuse the receiver.
        let mut hub = hub_direct();
        let sender = Uuid::new_v4();
        hub.handle(register_msg(sender, Uuid::new_v4()));
        let target_device = Uuid::new_v4();
        let mut peer = register_captured(&mut hub, Uuid::new_v4(), target_device);

        let file_id = Uuid::new_v4();
        let total = PER_CLIENT_FILE_CHANNEL_BUF + PER_CLIENT_FILE_OVERFLOW;
        for i in 0..total {
            let mut chunk = lazy_chunk_with(target_device, None, file_id);
            chunk.chunk_index = i as u32;
            assert_eq!(
                hub_deliver(&mut hub, sender, chunk),
                DeliveryResult::Delivered
            );
        }

        let mut extra = lazy_chunk_with(target_device, None, file_id);
        extra.chunk_index = total as u32;
        assert_eq!(
            hub_deliver(&mut hub, sender, extra),
            DeliveryResult::Busy,
            "the transfer still cannot continue"
        );
        assert!(
            peer.frames.try_recv().is_err(),
            "an eager transfer must not receive a session-less busy abort"
        );
    }

    #[test]
    fn a_closed_channel_is_not_treated_as_busy() {
        // Closed is "the connection is gone", not "the peer is slow". Sending a
        // busy abort would address a session on a connection that no longer
        // exists.
        let mut hub = hub_direct();
        let sender = Uuid::new_v4();
        hub.handle(register_msg(sender, Uuid::new_v4()));
        let target_device = Uuid::new_v4();
        let target_client = Uuid::new_v4();

        let (file_tx, file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (overflow_tx, overflow_rx) = mpsc::channel(PER_CLIENT_FILE_OVERFLOW);
        let (frames_tx, mut frames_rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id: target_client,
            device: target_device,
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: file_tx,
            overflow: overflow_tx,
            frames: frames_tx,
            ctl: mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF).0,
            ack,
        });
        // Both data receivers vanish: the peer's task has exited.
        drop(file_rx);
        drop(overflow_rx);

        let chunk = lazy_chunk_with(target_device, Some(Uuid::new_v4()), Uuid::new_v4());
        assert_eq!(
            hub_deliver(&mut hub, sender, chunk),
            DeliveryResult::UnknownDevice
        );
        assert!(
            frames_rx.try_recv().is_err(),
            "a dead connection must not get a busy abort"
        );
    }

    // ---- Directed delivery (3.7a) ----

    /// Register `id` under `device` and hand back receivers for chunks and
    /// routed frames.
    fn register_capturing(
        hub: &mut Hub,
        id: Uuid,
        device: Uuid,
    ) -> (mpsc::Receiver<FileChunkFrame>, mpsc::Receiver<Frame>) {
        let (file_tx, file_rx) = mpsc::channel(PER_CLIENT_FILE_CHANNEL_BUF);
        let (frames_tx, frames_rx) = mpsc::channel(PER_CLIENT_FRAME_CHANNEL_BUF);
        let (ack, _ack_rx) = oneshot::channel();
        hub.handle(HubMessage::Register {
            id,
            device,
            caps: Caps::default(),
            outbound: mpsc::channel(PER_CLIENT_CHANNEL_BUF).0,
            file_outbound: file_tx,
            overflow: mpsc::channel(PER_CLIENT_FILE_OVERFLOW).0,
            frames: frames_tx,
            ctl: mpsc::channel(PER_CLIENT_CONTROL_CHANNEL_BUF).0,
            ack,
        });
        (file_rx, frames_rx)
    }

    /// A lazy chunk addressed to `to`, as the source would send it.
    fn lazy_chunk(to: Uuid) -> FileChunkFrame {
        let mut chunk = chunk_from_client(None);
        chunk.to = Some(to);
        chunk.transfer_id = Some(Uuid::new_v4());
        chunk.file_sha256 = None;
        chunk
    }

    fn digest_to(to: Uuid) -> FileDigestFrame {
        FileDigestFrame {
            file_id: Uuid::new_v4(),
            transfer_id: Uuid::new_v4(),
            to,
            from_device: None,
            file_sha256: "AA:BB".to_string(),
        }
    }

    fn session_abort(to: Option<Uuid>, transfer_id: Option<Uuid>) -> FileAbortFrame {
        FileAbortFrame {
            file_id: Uuid::new_v4(),
            transfer_id,
            to,
            from_device: None,
            reason: "user_cancel".to_string(),
        }
    }

    fn publish_chunk(hub: &mut Hub, from: Uuid, chunk: FileChunkFrame) {
        hub.handle(HubMessage::PublishFile { from, chunk });
    }

    fn publish_digest(hub: &mut Hub, from: Uuid, digest: FileDigestFrame) -> DeliveryResult {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        hub.handle(HubMessage::PublishDigest {
            from,
            digest,
            reply: reply_tx,
        });
        reply_rx.try_recv().expect("hub must answer every digest")
    }

    fn publish_abort(hub: &mut Hub, from: Uuid, abort: FileAbortFrame) -> DeliveryResult {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        hub.handle(HubMessage::PublishAbort {
            from,
            abort,
            reply: reply_tx,
        });
        reply_rx.try_recv().expect("hub must answer every abort")
    }

    #[test]
    fn a_directed_chunk_reaches_only_its_target() {
        let mut hub = hub_direct();
        let a_client = Uuid::new_v4();
        let b_client = Uuid::new_v4();
        let b_device = Uuid::new_v4();
        let c_client = Uuid::new_v4();
        let (_a_rx, _a_frames) = register_capturing(&mut hub, a_client, Uuid::new_v4());
        let (mut b_rx, _b_frames) = register_capturing(&mut hub, b_client, b_device);
        let (mut c_rx, _c_frames) = register_capturing(&mut hub, c_client, Uuid::new_v4());

        publish_chunk(&mut hub, a_client, lazy_chunk(b_device));

        let got = b_rx.try_recv().expect("the target must receive the chunk");
        assert_eq!(got.to, Some(b_device));
        assert!(
            c_rx.try_recv().is_err(),
            "an unrelated peer must not see it"
        );
    }

    #[test]
    fn chunks_from_different_transfers_do_not_cross_route() {
        // Two sessions in flight at once, sharing one sender. Routing is per
        // `to`, so each receiver must see only its own transfer's chunks — a
        // fan-out bug that leaked a chunk across devices would corrupt both
        // receivers' assemblies, and the transfer ids make the mix-up visible.
        let mut hub = hub_direct();
        let sender_client = Uuid::new_v4();
        let sender_device = Uuid::new_v4();
        hub.handle(register_msg(sender_client, sender_device));

        let b_device = Uuid::new_v4();
        let c_device = Uuid::new_v4();
        let (mut b_rx, _b_frames) = register_capturing(&mut hub, Uuid::new_v4(), b_device);
        let (mut c_rx, _c_frames) = register_capturing(&mut hub, Uuid::new_v4(), c_device);

        let transfer_to_b = Uuid::new_v4();
        let transfer_to_c = Uuid::new_v4();
        let file_to_b = Uuid::new_v4();
        let file_to_c = Uuid::new_v4();

        // Interleaved so a "last chunk decides the route" bug cannot hide.
        publish_chunk(
            &mut hub,
            sender_client,
            lazy_chunk_with(b_device, Some(transfer_to_b), file_to_b),
        );
        publish_chunk(
            &mut hub,
            sender_client,
            lazy_chunk_with(c_device, Some(transfer_to_c), file_to_c),
        );

        let at_b = b_rx.try_recv().expect("B must receive its chunk");
        assert_eq!(at_b.to, Some(b_device));
        assert_eq!(at_b.transfer_id, Some(transfer_to_b));
        assert_eq!(at_b.file_id, file_to_b);
        assert_eq!(
            at_b.from_device,
            Some(sender_device),
            "the sender is stamped on the directed path too"
        );
        assert!(b_rx.try_recv().is_err(), "B must not receive C's chunk");

        let at_c = c_rx.try_recv().expect("C must receive its chunk");
        assert_eq!(at_c.to, Some(c_device));
        assert_eq!(at_c.transfer_id, Some(transfer_to_c));
        assert_eq!(at_c.file_id, file_to_c);
        assert_eq!(at_c.from_device, Some(sender_device));
        assert!(c_rx.try_recv().is_err(), "C must not receive B's chunk");
    }

    #[tokio::test]
    async fn a_slow_peer_does_not_stall_other_peers() {
        // The spec promises *other peers' traffic continues* while one peer is
        // saturated. The two pieces (overflow, then busy) are covered
        // separately; this is the property those pieces exist for.
        let (mut hub, sender, sender_device) = hub_at(1_000);
        let slow_device = Uuid::new_v4();
        let mut slow = register_captured(&mut hub, Uuid::new_v4(), slow_device);
        let fast_device = Uuid::new_v4();
        let mut fast = register_captured(&mut hub, Uuid::new_v4(), fast_device);

        // Saturate the slow peer at both levels, through the hub's own path.
        let stalled_transfer = Uuid::new_v4();
        let stalled_file = Uuid::new_v4();
        let capacity = PER_CLIENT_FILE_CHANNEL_BUF + PER_CLIENT_FILE_OVERFLOW;
        for i in 0..capacity {
            let mut chunk = lazy_chunk_with(slow_device, Some(stalled_transfer), stalled_file);
            chunk.chunk_index = i as u32;
            assert_eq!(
                hub_deliver(&mut hub, sender, chunk),
                DeliveryResult::Delivered,
                "chunk {i} should still fit"
            );
        }
        let mut one_too_many = lazy_chunk_with(slow_device, Some(stalled_transfer), stalled_file);
        one_too_many.chunk_index = capacity as u32;
        assert_eq!(
            hub_deliver(&mut hub, sender, one_too_many),
            DeliveryResult::Busy,
            "precondition: the slow peer really is saturated"
        );

        // The fast peer is served while the slow one is stuck. `handle` is
        // synchronous, so the assertion is that the actor returns rather than
        // waiting on the slow peer's socket — if it ever awaited, the rim of
        // this timeout would never be reached.
        let fast_transfer = Uuid::new_v4();
        let fast_file = Uuid::new_v4();
        let delivered = tokio::time::timeout(Duration::from_millis(500), async {
            hub.handle(HubMessage::PublishFile {
                from: sender,
                chunk: lazy_chunk_with(fast_device, Some(fast_transfer), fast_file),
            });
        })
        .await;
        assert!(
            delivered.is_ok(),
            "the hub must not block on a saturated peer"
        );

        let at_fast = fast.chunks.try_recv().expect("the fast peer is served");
        assert_eq!(at_fast.to, Some(fast_device));
        assert_eq!(at_fast.transfer_id, Some(fast_transfer));
        assert_eq!(at_fast.from_device, Some(sender_device));

        // The slow peer's backlog is untouched: still exactly its capacity on
        // each level, and the fast chunk never leaked into it.
        let slow_chunks: Vec<Uuid> = std::iter::from_fn(|| slow.chunks.try_recv().ok())
            .map(|c| c.file_id)
            .collect();
        assert_eq!(slow_chunks.len(), PER_CLIENT_FILE_CHANNEL_BUF);
        assert!(slow_chunks.iter().all(|file_id| *file_id == stalled_file));
        let slow_overflow: Vec<Uuid> = std::iter::from_fn(|| slow.overflow.try_recv().ok())
            .map(|c| c.file_id)
            .collect();
        assert_eq!(slow_overflow.len(), PER_CLIENT_FILE_OVERFLOW);
        assert!(slow_overflow.iter().all(|file_id| *file_id == stalled_file));
    }

    #[test]
    fn an_eager_chunk_still_broadcasts() {
        // v0.3 compatibility: no `to` means fan-out, exactly as before.
        let mut hub = hub_direct();
        let a_client = Uuid::new_v4();
        let b_client = Uuid::new_v4();
        let c_client = Uuid::new_v4();
        let (_a_rx, _a_frames) = register_capturing(&mut hub, a_client, Uuid::new_v4());
        let (mut b_rx, _b_frames) = register_capturing(&mut hub, b_client, Uuid::new_v4());
        let (mut c_rx, _c_frames) = register_capturing(&mut hub, c_client, Uuid::new_v4());

        publish_chunk(&mut hub, a_client, chunk_from_client(None));

        assert!(b_rx.try_recv().is_ok());
        assert!(c_rx.try_recv().is_ok());
    }

    #[test]
    fn a_directed_chunk_goes_to_the_devices_new_connection() {
        // `to` names a device, not a connection. After the device reconnects
        // under a new client id, a chunk must follow the new connection; a
        // cached client id would deliver to a dead one.
        let mut hub = hub_direct();
        let sender = Uuid::new_v4();
        hub.handle(register_msg(sender, Uuid::new_v4()));

        let device = Uuid::new_v4();
        let old_client = Uuid::new_v4();
        let (mut old_rx, _old_frames) = register_capturing(&mut hub, old_client, device);
        hub.handle(HubMessage::Deregister {
            id: old_client,
            device,
        });

        let new_client = Uuid::new_v4();
        let (mut new_rx, _new_frames) = register_capturing(&mut hub, new_client, device);
        assert_eq!(hub.device_index.get(&device), Some(&new_client));

        publish_chunk(&mut hub, sender, lazy_chunk(device));

        assert!(
            new_rx.try_recv().is_ok(),
            "the chunk must reach the device's current connection"
        );
        assert!(
            old_rx.try_recv().is_err(),
            "the superseded connection must not receive it"
        );
    }

    #[test]
    fn a_directed_chunk_for_an_unknown_device_is_dropped() {
        let mut hub = hub_direct();
        let sender = Uuid::new_v4();
        let other = Uuid::new_v4();
        hub.handle(register_msg(sender, Uuid::new_v4()));
        let (mut other_rx, _other_frames) = register_capturing(&mut hub, other, Uuid::new_v4());

        // Must not panic, and must not fall back to broadcasting.
        publish_chunk(&mut hub, sender, lazy_chunk(Uuid::new_v4()));
        assert!(other_rx.try_recv().is_err());
    }

    #[test]
    fn a_directed_chunk_does_not_leak_from_device_to_others() {
        // The directed path must not bypass the 3.4b overwrite: a forged
        // `from_device` is replaced by the sender's registered device.
        let mut hub = hub_direct();
        let sender = Uuid::new_v4();
        let sender_device = Uuid::new_v4();
        let target = Uuid::new_v4();
        let target_device = Uuid::new_v4();
        hub.handle(register_msg(sender, sender_device));
        let (mut target_rx, _target_frames) = register_capturing(&mut hub, target, target_device);

        let mut chunk = lazy_chunk(target_device);
        let forged = Uuid::new_v4();
        chunk.from_device = Some(forged);
        publish_chunk(&mut hub, sender, chunk);

        let got = target_rx.try_recv().expect("target receives the chunk");
        assert_eq!(got.from_device, Some(sender_device));
        assert_ne!(got.from_device, Some(forged));
    }

    #[test]
    fn a_session_abort_reaches_only_its_target() {
        let mut hub = hub_direct();
        let a_client = Uuid::new_v4();
        let b_client = Uuid::new_v4();
        let b_device = Uuid::new_v4();
        let c_client = Uuid::new_v4();
        let (_a_rx, _a_frames) = register_capturing(&mut hub, a_client, Uuid::new_v4());
        let (_b_rx, mut b_frames) = register_capturing(&mut hub, b_client, b_device);
        let (_c_rx, mut c_frames) = register_capturing(&mut hub, c_client, Uuid::new_v4());

        let transcript = publish_abort(
            &mut hub,
            a_client,
            session_abort(Some(b_device), Some(Uuid::new_v4())),
        );
        assert_eq!(transcript, DeliveryResult::Delivered);

        match next_non_membership_frame(&mut b_frames) {
            Some(Frame::FileAbort(frame)) => assert_eq!(frame.to, Some(b_device)),
            other => panic!("B should receive the abort, got {other:?}"),
        }
        assert!(
            next_non_membership_frame(&mut c_frames).is_none(),
            "C must not receive it"
        );
    }

    #[test]
    fn an_offer_scoped_abort_broadcasts_to_everyone_but_the_sender() {
        let mut hub = hub_direct();
        let a_client = Uuid::new_v4();
        let a_device = Uuid::new_v4();
        let b_client = Uuid::new_v4();
        let b_device = Uuid::new_v4();
        let c_client = Uuid::new_v4();
        let c_device = Uuid::new_v4();
        let (_a_rx, mut a_frames) = register_capturing(&mut hub, a_client, a_device);
        let (_b_rx, mut b_frames) = register_capturing(&mut hub, b_client, b_device);
        let (_c_rx, mut c_frames) = register_capturing(&mut hub, c_client, c_device);

        // `to: None` and no `transfer_id`: offer-scoped, broadcast form.
        let result = publish_abort(&mut hub, a_client, session_abort(None, None));
        assert_eq!(result, DeliveryResult::Broadcast { recipients: 2 });

        // Each expanded copy names its own recipient. Asserting only "it is a
        // file_abort" is what let the `to: None` bug through in the first pass.
        match next_non_membership_frame(&mut b_frames).expect("B must receive the abort") {
            Frame::FileAbort(frame) => {
                assert_eq!(
                    frame.to,
                    Some(b_device),
                    "each expanded copy carries its own recipient's device in `to`"
                );
                assert_eq!(frame.transfer_id, None, "offer-scoped: no session");
                assert_eq!(
                    frame.from_device,
                    Some(a_device),
                    "sender identity is still stamped"
                );
            }
            other => panic!("expected file_abort for B, got {other:?}"),
        }
        match next_non_membership_frame(&mut c_frames).expect("C must receive the abort") {
            Frame::FileAbort(frame) => {
                assert_eq!(frame.to, Some(c_device), "C's copy must name C, not B");
                assert_eq!(frame.transfer_id, None);
                assert_eq!(frame.from_device, Some(a_device));
            }
            other => panic!("expected file_abort for C, got {other:?}"),
        }
        assert!(
            next_non_membership_frame(&mut a_frames).is_none(),
            "the sender must not get its own abort back"
        );
    }

    #[test]
    fn each_broadcast_abort_copy_is_addressed_to_its_recipient() {
        // Spec: "A receiver therefore always sees a frame with `to` set."
        // Three recipients — two would let an implementation that stamps every
        // copy with the *first* peer's device slip through one of the checks.
        let mut hub = hub_direct();
        let sender_client = Uuid::new_v4();
        let sender_device = Uuid::new_v4();
        hub.handle(register_msg(sender_client, sender_device));

        let mut devices = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..3 {
            let client = Uuid::new_v4();
            let device = Uuid::new_v4();
            let (_chunks, frames) = register_capturing(&mut hub, client, device);
            devices.push(device);
            receivers.push((client, frames));
        }
        // All distinct, so a mixed-up copy is visible.
        let mut sorted = devices.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 3);

        let result = publish_abort(&mut hub, sender_client, session_abort(None, None));
        assert_eq!(result, DeliveryResult::Broadcast { recipients: 3 });

        for (index, (client, frames)) in receivers.iter_mut().enumerate() {
            match next_non_membership_frame(frames)
                .unwrap_or_else(|| panic!("peer {index} got nothing"))
            {
                Frame::FileAbort(frame) => {
                    assert_eq!(
                        frame.to,
                        Some(devices[index]),
                        "copy {index} must name its own recipient"
                    );
                    assert_eq!(frame.from_device, Some(sender_device));
                    assert_ne!(devices[index], sender_device);
                    let _ = client;
                }
                other => panic!("peer {index} expected file_abort, got {other:?}"),
            }
        }

        // Every recipient got a *different* `to`: this fails if the hub stamps
        // one device onto all copies.
        let mut seen = devices.clone();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 3, "each copy must be addressed differently");
    }

    #[test]
    fn a_digest_reaches_only_its_target() {
        let mut hub = hub_direct();
        let a_client = Uuid::new_v4();
        let b_client = Uuid::new_v4();
        let b_device = Uuid::new_v4();
        let c_client = Uuid::new_v4();
        let (_a_rx, _a_frames) = register_capturing(&mut hub, a_client, Uuid::new_v4());
        let (_b_rx, mut b_frames) = register_capturing(&mut hub, b_client, b_device);
        let (_c_rx, mut c_frames) = register_capturing(&mut hub, c_client, Uuid::new_v4());

        assert_eq!(
            publish_digest(&mut hub, a_client, digest_to(b_device)),
            DeliveryResult::Delivered
        );
        match next_non_membership_frame(&mut b_frames) {
            Some(Frame::FileDigest(frame)) => {
                assert_eq!(frame.to, b_device);
                assert_eq!(frame.file_sha256, "AA:BB");
            }
            other => panic!("B should receive the digest, got {other:?}"),
        }
        assert!(
            next_non_membership_frame(&mut c_frames).is_none(),
            "C must not receive it"
        );
    }

    // ---- Expiry sweeping (3.5/3.6 fix) ----
    //
    // Dead offers used to sit in the map forever: only a pull removed one, and
    // a pull only happens for offers somebody wants. With a 30 min default TTL a
    // busy source filled its 256-offer budget with entries nobody could pull and
    // then could not advertise anything — an everyday failure, not an attack.

    #[test]
    fn expired_offers_do_not_consume_the_per_device_cap() {
        let (mut hub, client, device) = hub_at(1_000);
        for _ in 0..MAX_OFFERS_PER_DEVICE {
            assert_eq!(
                publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
                OfferResult::Accepted
            );
        }
        // While they are live, the cap is real.
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::PerDeviceCap
        );

        // Every deadline has passed; the budget must be reclaimed.
        hub.now_override = Some(3_000);
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 4_000)),
            OfferResult::Accepted,
            "expired entries must not deny a live peer its budget"
        );
        assert_eq!(
            hub.offers.len(),
            1,
            "the dead entries are gone, not merely bypassed"
        );
        assert!(hub.offers.keys().all(|(owner, _)| *owner == device));
    }

    #[test]
    fn a_dead_offer_can_be_re_advertised_with_a_live_deadline() {
        // Regression for a dead entry being mistaken for a live
        // re-advertisement. Without the alive-filter on `existing`, the dead
        // entry would (a) skip the cap check and (b) let the anti-extension rule
        // clamp the new deadline down to the old, already-past one — so
        // re-advertising a file could never revive it, and the source would
        // silently lose the ability to offer that file at all.
        let (mut hub, client, device) = hub_at(1_000);
        let file_id = Uuid::new_v4();

        assert_eq!(
            publish_offer(&mut hub, client, offer_at(file_id, 2_000)),
            OfferResult::Accepted
        );

        // Clock moves past the deadline; the entry is now a corpse.
        hub.now_override = Some(3_000);

        // Re-advertise the *same* file_id with a live deadline.
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(file_id, 9_000)),
            OfferResult::Accepted
        );

        let stored = hub
            .offers
            .get(&(device, file_id))
            .expect("the offer must be present");
        assert_eq!(
            stored.frame.expires_at_ms, 9_000,
            "a dead entry must not clamp the revived deadline"
        );
        assert_eq!(
            hub.offers.len(),
            1,
            "reviving replaces the dead entry, it does not add a second one"
        );
    }

    #[test]
    fn reviving_a_dead_file_id_reclaims_the_cap_it_held() {
        // Neighbour of the test above, at the cap boundary: the whole budget is
        // dead and the sender reuses one of those file ids. The sweep must run
        // and the count must be recomputed, otherwise a live offer is refused by
        // corpses that the same call is about to delete.
        let (mut hub, client, device) = hub_at(1_000);
        let mut file_ids = Vec::new();
        for _ in 0..MAX_OFFERS_PER_DEVICE {
            let file_id = Uuid::new_v4();
            assert_eq!(
                publish_offer(&mut hub, client, offer_at(file_id, 2_000)),
                OfferResult::Accepted
            );
            file_ids.push(file_id);
        }
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::PerDeviceCap
        );

        hub.now_override = Some(3_000);

        // Reuse a dead id: `existing` must be `None` here, so the cap path runs,
        // sweeps, and accepts.
        let revived = file_ids[0];
        assert_eq!(
            publish_offer(&mut hub, client, offer_at(revived, 9_000)),
            OfferResult::Accepted
        );
        assert_eq!(hub.offers.len(), 1);
        assert_eq!(
            hub.offers[&(device, revived)].frame.expires_at_ms,
            9_000,
            "the revived entry carries the new deadline"
        );
    }

    #[test]
    fn expired_offers_do_not_consume_the_total_cap() {
        // Same defect at the global cap. Filled from many devices so the
        // per-device cap is never the reason for a rejection.
        let mut hub = Hub::new(MAX_OFFERS_TOTAL, None);
        hub.now_override = Some(1_000);
        let per_device = MAX_OFFERS_TOTAL / 64;
        let mut filled = 0usize;
        while filled < MAX_OFFERS_TOTAL {
            let client = Uuid::new_v4();
            hub.handle(register_msg(client, Uuid::new_v4()));
            for _ in 0..per_device {
                assert_eq!(
                    publish_offer(&mut hub, client, offer_at(Uuid::new_v4(), 2_000)),
                    OfferResult::Accepted
                );
                filled += 1;
            }
        }
        assert_eq!(hub.offers.len(), MAX_OFFERS_TOTAL);

        let newcomer_client = Uuid::new_v4();
        let newcomer_device = Uuid::new_v4();
        hub.handle(register_msg(newcomer_client, newcomer_device));
        assert_eq!(
            publish_offer(&mut hub, newcomer_client, offer_at(Uuid::new_v4(), 2_000)),
            OfferResult::TotalCap,
            "while the existing offers are live the cap still holds"
        );

        hub.now_override = Some(3_000);
        assert_eq!(
            publish_offer(&mut hub, newcomer_client, offer_at(Uuid::new_v4(), 4_000)),
            OfferResult::Accepted,
            "the total cap must not be permanently exhausted by dead entries"
        );
        assert_eq!(hub.offers.len(), 1);
        assert!(hub
            .offers
            .keys()
            .all(|(owner, _)| *owner == newcomer_device));
    }

    #[test]
    fn sweeping_keeps_unexpired_offers() {
        // Guards the comparison direction: `retain(|_, o| o.expires > now)` the
        // wrong way round would delete every live offer. (a) and (b) would still
        // pass under some clocks if nobody else is watching this.
        let mut hub = hub_direct();
        hub.now_override = Some(1_000);
        let live_client = Uuid::new_v4();
        let live_device = Uuid::new_v4();
        let dead_client = Uuid::new_v4();
        let dead_device = Uuid::new_v4();
        hub.handle(register_msg(live_client, live_device));
        hub.handle(register_msg(dead_client, dead_device));

        let live_file = Uuid::new_v4();
        let dead_file = Uuid::new_v4();
        publish_offer(&mut hub, live_client, offer_at(live_file, 5_000));
        publish_offer(&mut hub, dead_client, offer_at(dead_file, 1_500));

        hub.now_override = Some(2_000);
        assert_eq!(hub.sweep_expired_offers(), 1);

        assert!(
            hub.offers.contains_key(&(live_device, live_file)),
            "a live offer must survive the sweep"
        );
        assert!(
            !hub.offers.contains_key(&(dead_device, dead_file)),
            "an expired offer must be dropped"
        );
        assert_eq!(hub.offers.len(), 1);
    }

    #[test]
    fn an_offer_is_not_swept_early() {
        // The deadline is exclusive on both sides: `expires_at_ms == now` is
        // dead, one millisecond later is alive. This is the same boundary
        // `authorize_pull` uses, so a pull and a sweep cannot disagree.
        let (mut hub, client, device) = hub_at(1_000);
        let file_id = Uuid::new_v4();
        publish_offer(&mut hub, client, offer_at(file_id, 1_001));

        assert_eq!(hub.sweep_expired_offers(), 0, "not dead yet");
        assert!(hub.offers.contains_key(&(device, file_id)));

        hub.now_override = Some(1_001);
        assert_eq!(hub.sweep_expired_offers(), 1, "now == expiry is dead");
        assert!(!hub.offers.contains_key(&(device, file_id)));
    }
}

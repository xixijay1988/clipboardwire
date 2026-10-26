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

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::protocol::{ClipFrame, FileChunkFrame};

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

/// Capacity of the hub's own inbox. Held by every connection task plus the
/// supervisor; sized generously since the hub drains synchronously.
const HUB_INBOX_BUF: usize = 1024;

#[derive(Debug)]
pub enum HubMessage {
    Register {
        id: Uuid,
        outbound: mpsc::Sender<ClipFrame>,
        file_outbound: mpsc::Sender<FileChunkFrame>,
        ack: oneshot::Sender<RegisterResult>,
    },
    Deregister {
        id: Uuid,
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
}

#[derive(Debug)]
pub enum RegisterResult {
    Accepted { last_clip: Option<ClipFrame> },
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
    pub async fn register(
        &self,
        id: Uuid,
        outbound: mpsc::Sender<ClipFrame>,
        file_outbound: mpsc::Sender<FileChunkFrame>,
    ) -> anyhow::Result<RegisterResult> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(HubMessage::Register {
                id,
                outbound,
                file_outbound,
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
    pub async fn deregister(&self, id: Uuid) {
        let _ = self.tx.send(HubMessage::Deregister { id }).await;
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

/// Per-client fan-out channels. Paired so we can deliver both
/// `clip` and `file_chunk` traffic to the same peer connection.
struct ClientChannels {
    clip: mpsc::Sender<ClipFrame>,
    file: mpsc::Sender<FileChunkFrame>,
}

pub(crate) struct Hub {
    clients: HashMap<Uuid, ClientChannels>,
    last_clip: Option<ClipFrame>,
    max_clients: usize,
    stats: Option<HubStatsSink>,
}

impl Hub {
    pub(crate) fn new(max_clients: usize, stats: Option<HubStatsSink>) -> Self {
        Self {
            clients: HashMap::new(),
            last_clip: None,
            max_clients,
            stats,
        }
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
                outbound,
                file_outbound,
                ack,
            } => {
                if self.clients.len() >= self.max_clients {
                    warn!(client = %id, "rejecting registration: at capacity");
                    let _ = ack.send(RegisterResult::AtCapacity);
                    return;
                }
                self.clients.insert(
                    id,
                    ClientChannels {
                        clip: outbound,
                        file: file_outbound,
                    },
                );
                if let Some(s) = &self.stats {
                    s.count.fetch_add(1, Ordering::Relaxed);
                }
                let snapshot = self.last_clip.clone();
                info!(client = %id, total = self.clients.len(), "client registered with hub");
                let _ = ack.send(RegisterResult::Accepted {
                    last_clip: snapshot,
                });
            }
            HubMessage::Deregister { id } => {
                if self.clients.remove(&id).is_some() {
                    if let Some(s) = &self.stats {
                        s.count.fetch_sub(1, Ordering::Relaxed);
                    }
                    info!(client = %id, total = self.clients.len(), "client deregistered from hub");
                }
            }
            HubMessage::Publish { from, mut clip } => {
                clip.from = Some(from);
                self.last_clip = Some(clip.clone());

                for (peer_id, channels) in self.clients.iter() {
                    if *peer_id == from {
                        continue;
                    }
                    match channels.clip.try_send(clip.clone()) {
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
                let mut delivered = 0usize;
                for (peer_id, channels) in self.clients.iter() {
                    if *peer_id == from {
                        continue;
                    }
                    match channels.file.try_send(chunk.clone()) {
                        Ok(()) => delivered += 1,
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            // File transfers tolerate a missed chunk
                            // poorly (the assembled file would be
                            // corrupt). We could close the peer here;
                            // for v0.4 first cut we just drop and log.
                            // TODO: surface to receiver as a transfer
                            // abort marker.
                            warn!(
                                client = %peer_id,
                                file = %chunk.name,
                                index = chunk.chunk_index,
                                "outbound file buffer full; dropping chunk \
                                 (receiver's transfer will be incomplete)"
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
                if delivered == 0 {
                    // A sender relaying into a hub with no other
                    // registered clients is a silent data hole — this
                    // is exactly what a leaked/zombie hub looks like.
                    warn!(
                        file = %chunk.name,
                        index = chunk.chunk_index,
                        total = self.clients.len(),
                        "file_chunk relayed to zero peers"
                    );
                }
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
        let id = Uuid::new_v4();
        assert!(matches!(
            hub.register(id, clip_tx, file_tx).await.unwrap(),
            RegisterResult::Accepted { .. }
        ));
        join.abort();
        assert!(clip_rx.recv().await.is_none());
        assert!(file_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn publish_fans_out_to_peers_but_not_sender() {
        let (hub, _join) = spawn(8);

        let id_a = Uuid::new_v4();
        let (tx_a, mut rx_a) = mpsc::channel(8);
        assert!(matches!(
            hub.register(id_a, tx_a, dummy_file_tx()).await.unwrap(),
            RegisterResult::Accepted { last_clip: None }
        ));

        let id_b = Uuid::new_v4();
        let (tx_b, mut rx_b) = mpsc::channel(8);
        hub.register(id_b, tx_b, dummy_file_tx()).await.unwrap();

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
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(id_a, tx_a, dummy_file_tx()).await.unwrap();
        hub.publish(id_a, dummy_clip("cached")).await.unwrap();

        // Give the hub a tick to apply the publish.
        tokio::task::yield_now().await;

        let id_b = Uuid::new_v4();
        let (tx_b, _rx_b) = mpsc::channel(8);
        let result = hub.register(id_b, tx_b, dummy_file_tx()).await.unwrap();
        match result {
            RegisterResult::Accepted { last_clip } => {
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
                hub.register(Uuid::new_v4(), tx, dummy_file_tx())
                    .await
                    .unwrap(),
                RegisterResult::Accepted { .. }
            ));
        }

        let (tx, _rx) = mpsc::channel(8);
        let third = hub
            .register(Uuid::new_v4(), tx, dummy_file_tx())
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
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(id_a, tx_a, dummy_file_tx()).await.unwrap();
        assert_eq!(stats.current(), 1);

        let id_b = Uuid::new_v4();
        let (tx_b, _rx_b) = mpsc::channel(8);
        hub.register(id_b, tx_b, dummy_file_tx()).await.unwrap();
        assert_eq!(stats.current(), 2);

        hub.deregister(id_a).await;
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
        hub.deregister(Uuid::new_v4()).await;
        tokio::task::yield_now().await;
        assert_eq!(stats.current(), 0);
    }

    #[tokio::test]
    async fn deregister_frees_capacity() {
        let (hub, _join) = spawn(1);

        let id_a = Uuid::new_v4();
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(id_a, tx_a, dummy_file_tx()).await.unwrap();

        // Second registration is rejected.
        let (tx_b, _rx_b) = mpsc::channel(8);
        assert!(matches!(
            hub.register(Uuid::new_v4(), tx_b, dummy_file_tx())
                .await
                .unwrap(),
            RegisterResult::AtCapacity
        ));

        hub.deregister(id_a).await;
        tokio::task::yield_now().await;

        // Now there is room again.
        let (tx_c, _rx_c) = mpsc::channel(8);
        assert!(matches!(
            hub.register(Uuid::new_v4(), tx_c, dummy_file_tx())
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
        let (tx_a, _rx_a) = mpsc::channel(1);
        hub.register(id_a, tx_a, dummy_file_tx()).await.unwrap();

        // Fill A's buffer by sending from someone else first.
        let id_pub = Uuid::new_v4();
        let (tx_pub, _rx_pub) = mpsc::channel(8);
        hub.register(id_pub, tx_pub, dummy_file_tx()).await.unwrap();

        let id_b = Uuid::new_v4();
        let (tx_b, mut rx_b) = mpsc::channel(8);
        hub.register(id_b, tx_b, dummy_file_tx()).await.unwrap();

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
        let (tx_a, _rx_a) = mpsc::channel(8);
        hub.register(id_a, tx_a, dummy_file_tx()).await.unwrap();

        let mut clip = dummy_clip("x");
        clip.from = None;
        hub.publish(id_a, clip).await.unwrap();
        tokio::task::yield_now().await;

        let id_b = Uuid::new_v4();
        let (tx_b, _rx_b) = mpsc::channel(8);
        let r = hub.register(id_b, tx_b, dummy_file_tx()).await.unwrap();
        if let RegisterResult::Accepted {
            last_clip: Some(cached),
        } = r
        {
            assert_eq!(cached.from, Some(id_a));
        } else {
            panic!("expected cached clip with from set");
        }
    }
}

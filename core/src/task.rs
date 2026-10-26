// SPDX-License-Identifier: GPL-3.0-or-later

//! Task-lifecycle helpers shared between the client supervisor and the
//! hub server.

use tokio::task::JoinHandle;

/// Aborts the wrapped task when the guard drops — including when the
/// owning future is itself aborted (e.g. the tray aborting a supervisor
/// on config reload). `JoinHandle::abort` on the *outer* task does NOT
/// cancel tasks it spawned, so without a guard the child (transport
/// run_loop, hub actor) detaches and keeps running: the transport
/// reconnects forever as a phantom client, and the hub actor keeps
/// serving connections accepted before a reload — split-brain with the
/// freshly-spawned hub. Dropping the guard aborts the child alongside
/// its owner.
pub(crate) struct AbortOnDrop(pub JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Dropping the guard's owner (e.g. an aborted supervisor future)
    /// must kill the spawned child too — this is what stops orphaned
    /// transports and hub actors from churning after a config reload.
    #[tokio::test]
    async fn abort_on_drop_kills_spawned_child() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let outer = tokio::spawn(async move {
            let child = tokio::spawn(async move {
                let _tx = tx;
                std::future::pending::<()>().await
            });
            let _guard = AbortOnDrop(child);
            std::future::pending::<()>().await
        });
        // Let the outer task reach its pending point before aborting.
        tokio::task::yield_now().await;
        outer.abort();
        // The guard drops with the future and aborts the child; the
        // child drops its sender, so rx resolves with a cancellation.
        assert!(rx.await.is_err());
    }
}

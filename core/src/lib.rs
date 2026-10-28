// SPDX-License-Identifier: GPL-3.0-or-later

//! clipboardwire core library.
//!
//! Wire protocol types, hub server, and clipboard client supervisor live here.
//! The `clipboardwire` binary in `cli/` is a thin clap wrapper around these.
//!
//! See `PROTOCOL.md` and `ARCHITECTURE.md` in the repository root.

pub mod client;
pub mod identity;
pub mod logging;
pub mod protocol;
pub mod server;
pub(crate) mod task;

// The module file carries `#![cfg(test)]`; registering it unconditionally here
// would require a second `#[cfg(test)]` for the same purpose.
pub(crate) mod env_lock;

// SPDX-License-Identifier: GPL-3.0-or-later

//! Crate-wide serialisation for tests that touch process-global state.
//!
//! The environment block is shared by every thread in a test binary, and
//! `std::env::set_var` / `remove_var` racing with any concurrent `env::var`
//! is undefined behaviour — regardless of whether the two calls touch the
//! same variable. Rust's harness runs a crate's unit tests in one process on
//! many threads by default, so every module that mutates the environment must
//! take the **same** lock. Per-module locks do not help: two modules each
//! holding their own mutex still race each other.

#![cfg(test)]

use std::sync::{Mutex, MutexGuard};

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Serialise a test that reads or mutates the process environment.
///
/// The returned guard must be held for the whole test body.
///
/// A poisoned lock is recovered rather than propagated: one test panicking
/// while it holds the guard must not cascade into dozens of unrelated
/// environment tests failing on `.unwrap()`. The invariant the lock protects is
/// "no two tests touch the environment at once", and that still holds after a
/// panic — the panicking test has already unwound.
pub(crate) fn lock_env() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

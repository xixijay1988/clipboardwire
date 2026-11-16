# Implementation report — macOS `receive_mode = "paste"` via NSFilePresenter

Builder session, 2026-11-16. Scope: phases 1–2 of `tasks.md`, the mockable
parts of 3.5, and docs. No commits made.

## What changed

| File | Change |
| ---- | ------ |
| `core/src/client/paste.rs` | Module doc rewritten for two backends. `SpoolFrontier::wait()` added (blocks on the existing condvar until `outcome` is set). `PasteSource` is now per-platform: `cmd` channel on Windows/stubs, `Mutex<imp::MacPaste>` on macOS — `arm()` runs inline, no command thread (per design §2: presenter callbacks arrive on each presenter's own `NSOperationQueue`). `paste::spawn` cfg arms: `any(windows, macos)` → `imp::spawn`, else stub. New platform-neutral helpers: `armed_dir()`, `is_armed_placeholder()` (pub — used by `clipboard.rs`), `placeholder_path()` (`armed/<file_id>/<sanitized>`), `render_placeholder()` (fill-or-unlink decision, `#[cfg(any(macos, test))]`). New `#[cfg(target_os = "macos")] mod imp`: `CWPastePresenter` `define_class!` NSObject subclass (URL + per-file `NSOperationQueue` `maxConcurrentOperationCount = 1`, `relinquish…ToReader:`, `…ToWriter:`, `accommodatePresentedItemDeletion…`, `presentedItemDidMoveToURL:`, `presentedItemDidChange`), `MacPaste` arm/disarm registry, `imp::spawn`. 10 unit tests appended. |
| `core/src/client/mod.rs` | `ReceivePolicy::resolved_mode()`: `Paste` degrades to `Pickup` only on `!(windows \|\| macos)`. Doc comments updated (`pulls_offers_automatically`, the paste-channel comment). `auto` untouched — `ReceiveMode::resolved()` still maps macOS `Auto` → `Pickup`. New test `paste_resolves_where_a_paste_hook_exists`. |
| `core/src/client/clipboard.rs` | `decide_files` now filters out every path under `paste::armed_dir()` before it can reach `seen` — an armed-only read degrades to `NoFiles`. New test `armed_placeholders_are_never_published`. |
| `core/src/client/file.rs` | `sanitize_filename` → `pub(crate)` so placeholders use the identical naming rule as landed files. |
| `core/Cargo.toml` | `block2 = "0.6"` added to the macOS target deps; `objc2-foundation` gains `block2`, `NSError`, `NSFileCoordinator`, `NSFilePresenter`, `NSOperation`. |
| `Cargo.lock` | `block2 0.6.2` recorded as a direct dep of `clipboardwire-core`. |
| `ARCHITECTURE.md` | macOS presenter paragraph replaces the stale "no equivalent" note; documents placeholders, coordinated read, -43 failure surface, no byte progress, `auto` stays `pickup`. |
| `CHANGELOG.md` | `Unreleased` entry for macOS paste mode. |
| `MANUAL_SMOKE.md` | macOS paste recipe (items 10–15): placeholders visible under `armed/`, Cmd+V → preparing dialog, re-paste from spool, -43 failure path, cancel-from-Preparing check, echo check. |
| `tasks.md` | Phases 1–2 and 3.2–3.6 ticked with notes; real-Finder verification left for the lead. |

## Behaviour against the spec delta

- Offer → `<spool>/armed/<file_id>/<sanitized-name>` (0 bytes) + one
  registered presenter per file + `file://` URLs on the general pasteboard
  via `files_clipboard::write_files` (the same `clearContents` +
  `writeObjects` the apply path uses — one writer, one place).
- `relinquishPresentedItemToReader:` → `PasteEvent::Pull` → block on
  `reply.recv()` → `frontier.wait()` → `Ok`: `std::fs::copy` verified
  partial → placeholder → `reader(reacquirer)`; `Err` or failed copy:
  unlink → `reader(reacquirer)`. Runs on the presenter queue — blocking it
  is the relinquish contract.
- Nested reply signature `reader(reacquirer)` copied verbatim from the
  spike (`*mut DynBlock<dyn Fn()>` argument), reacquirer block forgotten
  after the call as leak-insurance — the proven shape.
- `arm(vec![])` / drop: `removeFilePresenter:` all + `remove_dir_all(armed)`;
  the pasteboard is cleared only while it still holds exactly our armed
  URLs (spec: "clear the clipboard only if it still owns it").
- Repeat paste: `ready` hit → completed frontier → copy again, no re-pull.
- No wire changes; `auto` unchanged; Windows path untouched (all its code
  sits behind `#[cfg(windows)]` as before — not compile-checked on this
  host, but also not edited).

## Deviations from design.md

1. **Echo safety took the "extend suppression" branch**, not `apply_tx`.
   `decide_files` filters armed paths out of every files read — the exact
   analogue of Windows, whose `OleSetClipboard` promise is never a
   readable file list. The `apply_tx`/`mark_applied` route would still
   have leaked: `mark_applied` snapshots file identity, and a paste
   filling the placeholder changes it → the "same list, stale" branch
   would re-emit `ClipChange::Files(armed_paths)`. Filtering is also
   strictly better than `OleSetClipboard` parity because `poll_clipboard`
   never even *reads* image/text while file URLs occupy the clipboard.
2. **`PasteSource::arm` mutates the registry inline** behind a `Mutex`
   rather than sending a command to a consumer. Design's "no dedicated
   thread" is honoured literally — the mutex never contends (only the
   supervisor calls `arm`; presenter callbacks never lock it).
3. **Disarm clears the pasteboard when it's still ours.** Design didn't
   specify; the spec's "clear only if it still owns it" does. Compared by
   `files_clipboard::read_files() == armed_paths`; anything else on the
   clipboard (a user copy that landed later) keeps it.
4. **`ready` entries do not evict on abort** (task 2.2's "confirm"). They
   didn't on Windows either — `paste_sink.fail` only ends `live` frontiers.
   A repeat paste after `file_abort` serves the already-verified spool.
   Same semantics both platforms; recorded as a finding, not a change.

## Verification (all run, none claimed)

```
$ cargo fmt --check            → clean
$ cargo clippy --all-targets -- -D warnings
                               → Finished dev profile, 0 warnings
$ cargo build -p clipboardwire-core --target aarch64-apple-darwin
                               → Finished dev profile
$ cargo test                   → 382 passed; 0 failed (lib)
                               → all integration/doc-test targets pass
```

New tests (12): `frontier_wait_*` ×3, `placeholder_path_*` /
`same_named_offers_never_collide` / `the_pull_partial_is_not_an_armed_placeholder`,
`render_placeholder_*` ×5 (fill, wait-for-frontier, pull-Err unlink,
frontier-Err unlink, copy-fail unlink), `armed_placeholders_are_never_published`,
`paste_resolves_where_a_paste_hook_exists`.

One test initially failed during development and was corrected before this
report: `armed_placeholders_are_never_published` fed an `image`/`text` read
into `decide` that `poll_clipboard` can never produce while file URLs
occupy the clipboard (it early-returns). Fixed the test to match real
inputs — the filter itself was already correct.

## Could not verify (handed to lead)

- Real Finder Cmd+V end-to-end, the -43 dialog, and cancel-from-Preparing —
  spike 0.1 proves the mechanism; the smoke recipe is in `MANUAL_SMOKE.md`
  items 10–15.
- Windows build — code untouched and still `#[cfg(windows)]`-gated; no
  Windows toolchain on this host.
- ~~A mid-pull disconnect that neither aborts nor finishes the frontier is
  bounded by the existing TailStall/restart-budget machinery~~ — **wrong**:
  the stall checks are skipped entirely while `connected` is false. The
  review caught this; round 2 bounds the render explicitly.

## Round 2 — review fixes (all in `core/src/client/paste.rs` + docs)

1. **Unbounded frontier wait during disconnect → bounded.** Added
   `SpoolFrontier::wait_timeout(d)` (same condvar, deadline loop) and
   `PASTE_RENDER_TIMEOUT = 180 s` — sized above the machinery's own
   connected failure bound (`TAIL_STALL_TIMEOUT_MS` 15 s ×
   `MAX_RESTARTS+1` ≈ 1 min) with margin for a large healthy pull.
   `render_placeholder` now delegates to `render_placeholder_within`
   (deadline as a parameter, for tests). Timeout → unlink →
   `reader(reacquirer)` → Finder -43, same surface as every other
   failure. `wait()` is unchanged for its other callers.
   New test: `render_placeholder_gives_up_and_unlinks_on_timeout`
   (never-finishing frontier, 50 ms deadline → elapsed ≥ 50 ms,
   placeholder gone).
2. **`mem::forget(reacquirer)` removed** from both relinquish callbacks.
   Verified by API contract (reasoning + docs, not instrumentation):
   Foundation invokes the reacquirer *after* `reader.call` returns —
   after the coordinated read finishes — so it must hold its own
   reference; the documented ObjC usage passes a *stack-block literal*
   (`reader(^{ … })`), which could not survive the callback unless the
   system copied it. Dropping our `RcBlock` on return is therefore safe
   and reclaims the per-paste leak. Comments updated on both callbacks.
3. **Re-arm killing an in-flight paste is now logged.** `Ivars` gains a
   shared `Arc<AtomicBool> pending`; the reader callback sets it for the
   render's duration, `Presenter::new` hands a copy back, `MacPaste`
   keeps them parallel to `presenters`, and `disarm` `warn!`s
   (`count = N, "re-arm killed an in-flight paste render"`) before
   unregistering. Behaviour unchanged — the kill is correct (the new
   copy replaces the clipboard) — it just leaves a trace now.
4. **`is_armed_placeholder` compares both TMPDIR spellings.** The
   pasteboard can hand back `/private/var/folders/...` while
   `temp_dir()` reports `/var/folders/...` (verified on this host:
   `TMPDIR=/var/folders/…/T`, `realpath=/private/var/folders/…/T`).
   New `canonical_armed_dir()` canonicalizes `temp_dir` — which always
   exists, so it can't fail the way canonicalizing a dead placeholder
   could — cached in a `OnceLock`. The check is
   `starts_with(armed_dir()) || starts_with(canonical_armed_dir())`.
   New test: `placeholder_filter_recognizes_the_canonical_tmp_spelling`
   (on this Mac the `/private` spelling fails the raw check and matches
   only via the canonical root; on Linux it reduces to the raw check).
5. **Non-coordinating readers documented.** `MANUAL_SMOKE.md` macOS
   section notes `cp`/Mail/Slack-attachment readers copy the 0-byte
   placeholder verbatim (silent, no pull, no error) — Finder Cmd+V is
   the supported surface, an empty copy elsewhere is the known boundary.
   `ARCHITECTURE.md` states the same in the presenter paragraph, and now
   also records the bounded render.

Round-2 gates (all run):

```
$ cargo fmt --check            → clean
$ cargo clippy --all-targets -- -D warnings
                               → Finished dev profile, 0 warnings
$ cargo test                   → 384 passed; 0 failed (lib, +2 new)
                               → all integration targets pass
$ cargo build -p clipboardwire-core --target aarch64-apple-darwin
                               → Finished dev profile
```

Verified vs reasoning-only: items 1, 3, 4 are test-verified (timeout
unlink, canonical-spelling filter) or compile/logic-inspected (pending
flag → warn). Item 2 is reasoning + Apple API-contract verification —
the reacquirer MUST be retained by the system to fire after the read,
and the documented call shape proves it copies — but it has no unit
test; the real-Finder smoke run exercises it. Item 5 is docs.

## Round 3 — deadline semantics fixes (all in `core/src/client/paste.rs` + docs)

1. **Total → idle deadline.** `wait_timeout(d)` replaced by
   `wait_idle_while(idle, poll, still_valid)`: each iteration re-checks
   `committed` (bumped + `notify_all`'d by `update` on every chunk) and
   resets the deadline when it grew. A 180 s *total* deadline would have
   killed any healthy pull over 3 min (~360 MB at 2 MB/s); "no progress
   for N seconds" catches the same disconnect hang (a dead transport
   commits nothing) while letting arbitrarily large files render.
   `PASTE_RENDER_TIMEOUT` → `PASTE_RENDER_IDLE_TIMEOUT = 120 s` — ~8×
   the per-attempt stall budget (`TAIL_STALL_TIMEOUT_MS` = 15 s), so a
   link idle that long is functionally dead while a healthy pull rarely
   goes one `TAIL_STALL` without a commit. `wait()` still unchanged.
   - `render_placeholder_gives_up_and_unlinks_on_timeout` updated for
     the new signature (verdict never lands → -43 path after idle).
   - New: `render_placeholder_stays_alive_while_progress_arrives` —
     280 ms of `update()`s spaced under a 100 ms idle window, then
     `finish(Ok)` → the render *waits past* the old wall-clock deadline
     and fills the placeholder.
2. **Render fails fast when its placeholder is deleted.** `disarm`
   removes `armed/` mid-render; the render now polls
   `placeholder.exists()` via `still_valid` inside the wait — checked
   before every wait slice (and before the outcome, since a verdict is
   only renderable while the destination exists). Wait slices are capped
   at `PASTE_RENDER_GONE_POLL = 500 ms` so a gone placeholder surfaces
   in ≤0.5 s instead of waiting out the pull for a doomed copy.
   `render_placeholder_within` now takes `(idle, poll)` so tests don't
   wait out real durations.
   - New: `render_placeholder_fails_fast_when_the_placeholder_is_deleted`
     — placeholder unlinked 30 ms into a never-resolving pull, 10 ms
     poll, 60 s idle → returns promptly (≪5 s, measured ~40 ms) and the
     copy never happens (placeholder stays absent).
3. Docs: `ARCHITECTURE.md` and `MANUAL_SMOKE.md` updated from "bounded
   (~3 min)" to "~2 min without forward progress" + the fail-fast note;
   smoke item 13 now explains the idle semantics explicitly.

Round-3 gates (all run):

```
$ cargo fmt --check            → clean
$ cargo clippy --all-targets -- -D warnings
                               → Finished dev profile, 0 warnings
$ cargo test                   → 386 passed; 0 failed (lib, +2 new)
                               → all integration targets pass
$ cargo build -p clipboardwire-core --target aarch64-apple-darwin
                               → Finished dev profile
```

Both items are test-verified end-to-end through `render_placeholder`
(the same code path the presenter callback runs); the only unverifiable
piece remains the real-Finder surface itself.

## Round 4 — restart re-download liveness (`core/src/client/paste.rs`)

The r3 idle deadline watched `committed` grow — but `committed` is a
`max()` over contiguous verified bytes, and a pull restarted from index
0 re-downloads already-committed bytes without growing it. `update()`
still fires per accepted chunk, so the waiter woke, saw no growth, and
kept its deadline: a healthy re-download lasting >120 s would have
failed the paste -43.

- `SpoolInner` gains `activity: u64` — a monotonic counter bumped on
  **every** `update()` call, inside the same lock, before the `max`.
  `wait_idle_while` resets its deadline when `activity` changes.
  Semantics are now "no accepted chunk for `idle`": a disconnected
  transport calls `update` zero times → still trips; `Ignored` chunks
  never reach `advance` → don't count; restart loops can't pin it
  forever because `MAX_RESTARTS` ends them in `finish(Err)`.
  `committed` is unchanged — it still means contiguous bytes for the
  `Read` path.
- `PASTE_RENDER_IDLE_TIMEOUT` comment rewritten to match: "no bytes
  arriving", 120 s ≈ 8× `TAIL_STALL_TIMEOUT_MS`.
- `wait_idle_while` doc rewritten: explains the activity epoch and why
  re-downloads count.
- New test: `render_placeholder_stays_alive_through_a_restart_redownload`
  — `update(8192)` sets the high-water mark, then ~280 ms of
  `update(1024)` re-download ticks under a 100 ms idle window, then
  `finish(Ok)` → placeholder fills; `committed` never moved.
  The silent-frontier timeout case stays covered by
  `render_placeholder_gives_up_and_unlinks_on_timeout`.
- `MANUAL_SMOKE.md` item 13: the idle-clock clause now notes that
  re-downloaded bytes after a restart count as arriving.

Round-4 gates (all run):

```
$ cargo fmt --check            → clean
$ cargo clippy --all-targets -- -D warnings
                               → Finished dev profile, 0 warnings
$ cargo test                   → 387 passed; 0 failed (lib, +1 new)
                               → all integration targets pass
                               → paste::tests additionally run 3× clean
$ cargo build -p clipboardwire-core --target aarch64-apple-darwin
                               → Finished dev profile
```

Test-verified: the re-download liveness path, the idle timeout, and the
deleted-placeholder fast-fail — all through `render_placeholder` on the
same code path the presenter callback runs. Reasoning-only: that
`PasteSink::advance` → `frontier.update` per accepted chunk is the real
caller (verified by code read + reviewer's path check), and that
`Ignored` outcomes skip it — both consistent with the counter
semantics.

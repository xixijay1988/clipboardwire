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
- A mid-pull disconnect that neither aborts nor finishes the frontier is
  bounded by the existing TailStall/restart-budget machinery, same as
  Windows' blocked `GetData`.

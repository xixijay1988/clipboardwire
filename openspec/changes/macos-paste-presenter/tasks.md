# Tasks — macOS paste path via `NSFilePresenter` placeholders

Phase 0 is **done**: `spike-0.1-result.md` (60 s wait tolerated, multi-file
sequential pulls, -43 failure surface, no GUI runloop needed, nested-block
reply shape verified).

## Phase 1 — presenter backend in `core/src/client/paste.rs`

- [x] 1.1 `#[cfg(target_os = "macos")] mod imp`: `SpikePresenter`-shaped
      `define_class!` NSObject subclass with `presentedItemURL`,
      `presentedItemOperationQueue` (one queue per file, `maxConcurrentOperationCount = 1`),
      `relinquishPresentedItemToReader:`, `…ToWriter:`,
      `accommodatePresentedItemDeletionWithCompletionHandler:`,
      `presentedItemDidMoveToURL:`, `presentedItemDidChange`.
- [x] 1.2 Reply shape per spike: `reader(reacquirer)` — the nested signature.
      Reacquirer block logs only. Copy the incoming reply block to the heap
      before moving it off the callback.
- [x] 1.3 `PasteSource::arm` for macOS: wipe previous `armed/` registry
      (`removeFilePresenter:` + delete dirs), create
      `<spool>/armed/<file_id>/<sanitized-name>` placeholders, register
      presenters, `writeObjects` the URLs to the general pasteboard. Keep
      `Retained` presenters in the registry until replaced.
- [x] 1.4 `relinquish…ToReader:` → send `PasteEvent::Pull`, block on
      `reply.recv()`, then block on `SpoolFrontier` outcome:
      `Ok` → `std::fs::copy(SpoolStream.path → placeholder)` →
      `reader(reacquirer)`; `Err` → unlink placeholder → `reader(reacquirer)`.
      Everything runs on the presenter queue — blocking it is correct.
- [x] 1.5 Add `SpoolFrontier::wait()` (condvar already exists): returns the
      `Result<(), String>` once `outcome` is set. Used by the macOS backend;
      also useful for tests.
- [x] 1.6 `paste::spawn` cfg arms: macOS impl builds the registry + event
      channel inline (no thread); keep `#[cfg(not(any(windows, macos)))]` stub.
- [x] 1.7 `resolved_mode()`: `Paste` degrades to `Pickup` only on
      `!(windows || macos)`; `ReceiveMode::resolved()` (`auto`) unchanged —
      macOS `auto` stays `Pickup`.
- [x] 1.8 `objc2-foundation` features: `NSFileCoordinator`, `NSFilePresenter`,
      `NSOperation`, `NSError`, `block2`.

## Phase 2 — echo safety & lifecycle

- [x] 2.1 Arming must not re-publish placeholder paths as offers. Either route
      the pasteboard write through `mark_applied`/`apply_tx` or extend file
      suppression to self-armed lists — pick whichever matches how Windows'
      `OleSetClipboard` avoids the same trap, and write a test for it.
      → Chose the read-side filter: `decide_files` drops every path under
      `paste::armed_dir()` before it can reach `seen` — matching Windows,
      whose promise simply is not a readable file list. The `apply_tx` route
      would still have leaked on the post-fill identity change.
      Test: `armed_placeholders_are_never_published`.
- [x] 2.2 `file_abort`/TTL/`superseded` while armed → `paste_sink.fail`
      already ends the frontier with `Err`; confirm the waiting presenter
      takes the unlink path. Also confirm `ready` entries evict.
      → Confirmed by inspection + `render_placeholder_unlinks_when_the_*`
      tests: `paste_sink.fail` → `frontier.finish(Err)` → `wait()` → unlink
      → -43. `ready` entries do **not** evict — a repeat paste serves the
      already-verified spool even after abort, same as Windows. Recorded as
      a finding, not changed.
- [x] 2.3 Repeat paste of a `ready` file: `SpoolStream.path` is the verified
      partial — copy into placeholder again (placeholder may have been
      cleaned or partially overwritten), reply, no re-pull.
- [x] 2.4 Empty `arm(vec![])`/shutdown: `removeFilePresenter:` all,
      delete `armed/`.

## Phase 3 — spec, docs, verification

- [ ] 3.1 Spec delta applied to `openspec/specs/file-transfer/spec.md`
      (archive step).
- [x] 3.2 `ARCHITECTURE.md`: macOS presenter paragraph next to the Windows
      paste-mode section; note "no byte progress, indeterminate Preparing".
- [x] 3.3 `MANUAL_SMOKE.md`: Finder Cmd+V from a remote offer, incl. pull
      failure → -43 dialog, and cancel-from-Preparing behaviour check.
- [x] 3.4 `CHANGELOG.md` entry.
- [x] 3.5 Unit tests: `SpoolFrontier::wait`; sanitize/dedup of armed names;
      `resolved_mode` mapping on macOS. Integration tests for the Pull →
      spool → frontier flow where mockable without AppKit.
      → The supervisor's `PasteEvent` channel is not injectable, so the
      mockable surface is `render_placeholder` + `SpoolFrontier::wait` —
      fill/wait/unlink covered by unit tests (see `impl-report.md`).
- [x] 3.6 `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test` — all green
      (382 lib + integration, macOS arm64 host build).
      Real-Finder verification is manual (spike harness proves the shape);
      record it in this file when done.
      → **Done, 2026-11-16, lead.** End-to-end against the real hub: isolated
      `receive_mode = "paste"` client (`--config` + `HOME` override → fresh
      device_id) + the live instance as lazy sender + real Finder Cmd+C/Cmd+V.
      Verified: single-file paste (pull → SHA-256 verify → bytes copied into
      placeholder → Finder completes), multi-file arm (`count=2`, Finder reads
      serially), repeat-paste from `ready` spool (no second pull session —
      presenter's Pull answered instantly), and the failure path end-to-end:
      a `source_changed` abort → retries exhausted → placeholder unlinked →
      read released (Finder -43 surface). Echo observed: an OLD-build peer
      re-publishes our armed placeholder URLs as offers — expected, gone once
      senders also run this build (the read-side filter is what they lack).

## Out of scope

- Flipping `auto` to `paste` on macOS (follow-up after soak).
- Byte-level progress / streaming into the copy (impossible with this
  mechanism — Finder waits for the reply before reading).
- `NSFileProvider`-extension alternative (Option C from the analysis):
  strictly more correct for non-Finder readers, far heavier packaging.

# Spike 0.1 (redo) — macOS paste-time pull via `NSFilePresenter` placeholder

**Date:** 2026-11-16 · **Platform:** macOS 15 (Darwin 24.6, arm64) ·
**Harness:** `tools/spike-macos-paste/` (standalone crate, not a workspace member)

## Verdict: PASS

Finder's Cmd+V **does** wait on a lazily-filled file, via a different mechanism
than the file-promise (`NSFilePromiseProvider`) path the archived change
evaluated: an **empty placeholder file + `NSFilePresenter`**. This is the
technique Royal TSX / Devolutions RDM ship in production for RDP file
copy-paste (lemonmojo/RemoteFilesVsPasteboard is the reference demo).

The archived Phase 0 note "Finder does not invoke a file-promise provider on
Cmd+V" was about the wrong hook and was never actually spiked.

## Mechanism tested

1. Create a 0-byte placeholder at `$TMPDIR/cw-paste-spike/<name>`.
2. `NSFileCoordinator addFilePresenter:` with `presentedItemURL` → placeholder,
   `presentedItemOperationQueue` → own `NSOperationQueue`.
3. Put the `file://` URL on the general pasteboard (`NSPasteboard.writeObjects`,
   same write as `files_clipboard::write_files`).
4. On Cmd+V in Finder → `relinquishPresentedItemToReader:` fires on the
   presenter queue. Do the pull, then reply.
5. Finder copies the (now filled) file to the destination.

## Results

| Test | Outcome |
| ---- | ------- |
| GUI-free coordinated read (`NSFileCoordinator` reader process) | Callback fired; reader's accessor blocked for the full simulated delay, then read the filled bytes. |
| Finder Cmd+V, 8 s simulated pull | `"正在准备拷贝"spike-file-1.txt""` progress dialog shown while waiting; file landed byte-exact. |
| Multi-file paste (2 placeholders, 3 s) | Works — Finder issues the coordinated reads **sequentially** (file 2's relinquish starts only after file 1's read completes). |
| Finder wait limit, 60 s pull | No timeout — Finder waited the full 60 s and copied correctly. Longer delays untested. |
| Failure path: unlink placeholder before replying | Finder shows error dialog `无法完成此操作…（错误代码-43）` (fnfErr) — **a failed pull surfaces as a paste error**, better than the predicted "copies an empty file". Close to Windows `E_ABORT` semantics. |
| Non-coordinated readers | Do not fire the callback (presenter callbacks route only through filecoordinationd). URL-only readers (clipboard managers, our own poller) are confirmed safe — they never touch the bytes. |
| Process without GUI runloop | Works — presenter callbacks arrive on the declared `NSOperationQueue`. `CFRunLoopRun` used as insurance in the spike. |

## Trap that cost a debugging round: the reply is nested-block

`relinquishPresentedItemToReader:` is **not** `void (^)(void)`. Its real
signature takes a reacquirer block:

```objc
- (void)relinquishPresentedItemToReader:
    (void (^)(void (^ _Nullable reacquirer)(void)))reader;
```

Reply = `reader(reacquirer)` where `reacquirer` is a block **we** supply;
Foundation calls it when the read finishes (observed: `reacquired after read`
fires ~15 ms after the reply). The nested shape is precisely what makes an
asynchronous reply legal. Calling the outer block with no argument produced
`NSInternalInconsistencyException: didRelinquish called, but there are no
replies` — after the copy had already succeeded. Same nested shape applies to
`relinquishPresentedItemToWriter:`. Rust binding type (matches the generated
`objc2-foundation` protocol signature):

```rust
&block2::DynBlock<dyn Fn(*mut block2::DynBlock<dyn Fn()>)>
```

## Observed side effects / open questions

- **The armed URL occupies the real clipboard.** During the run, an inbound
  text clip (synced by the running clipboardwire client) replaced the armed
  URLs and the next Cmd+V pasted that text. Inherent to single-slot
  clipboards; same exposure Windows `OleSetClipboard` has. Worth noting for UX
  ("arm on offer" clobbers the user's clipboard the same way `pickup` mode's
  write already does).
- **`presentedItemDidChange` fires after the copy** — likely Finder touching
  the file post-copy; harmless, but the real impl should not misread it as the
  source changing.
- Second paste would re-fire relinquish — real impl decides serve-from-spool
  vs re-pull (Windows spool semantics apply: pull once, serve every paste).
- Untested: delays >60 s; non-Finder targets (Mail, Terminal `cp` — expected
  empty file per Devolutions' notes); arm→paste across a reboot; multiple
  concurrent pastes of the same offer.

## Implementation notes for Phase 7 (macOS paste path)

- `resolved_mode()` must stop degrading `Paste` → `Pickup` on macOS.
- One presenter per offer file, or one on the spool directory (untested;
  per-file proven here). Spool analog of `%TEMP%\clipboardwire-paste` →
  `$TMPDIR/clipboardwire-paste`.
- Digest-verify before replying: fill placeholder under a temp name, rename on
  verify — or on failure, unlink before `reader(reacquirer)` to surface -43.
- The pull is synchronous-from-the-reader's-view: multi-file pastes serialize
  pull-by-pull (matches the Windows `MAX_ACTIVE_PULLS` queueing reality).
- objc2 plumbing needed in core: `objc2-foundation` features `NSFilePresenter`,
  `NSFileCoordinator`, `NSOperation` + `block2` (for `addOperationWithBlock`,
  though a plain presenter needs none of that — only the protocol object and
  `addFilePresenter:`).

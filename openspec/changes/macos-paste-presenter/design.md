# Design — macOS paste path via `NSFilePresenter` placeholders

Spike evidence: `spike-0.1-result.md` (macOS 15, arm64, real Finder Cmd+V).

## 1. Mechanism

Windows' paste path works because OLE `IDataObject` gives the shell a
*callback* (`GetData`) at paste time. macOS has no pasteboard-level callback
Finder honours — but it has a **filesystem-level** one: `NSFileCoordinator`.
When a process coordinates a read of a file another process presents, the
presenter is asked to relinquish the item first, and the reader blocks until
the presenter replies. Finder's Cmd+V copy **does** coordinate the read of a
`file://` URL from the pasteboard (spike-verified).

So the macOS "promise" is:

```
offer arrives → create <spool>/armed/<file_id>/<name> (0 bytes)
              → register NSFilePresenter(presentedItemURL = placeholder)
              → put file:// URL of placeholder on general pasteboard

Cmd+V in Finder → coordinated read → relinquishPresentedItemToReader:
                → PasteEvent::Pull → supervisor runs normal lazy pull
                → digest verified → copy verified bytes INTO placeholder
                → reply reader(reacquirer) → Finder copies placeholder
```

Pull failure → unlink the placeholder before replying → Finder shows
error -43 ("item not found") — a visible copy failure instead of an empty
file, matching the spec's failure table for `paste` on Windows (`E_ABORT`).

## 2. Integration points (existing code reused)

The supervisor's paste pipeline is already platform-shaped; the macOS backend
plugs into the same surface `paste.rs` defines:

| Existing piece | Reuse |
| --- | --- |
| `paste::spawn(events_tx) -> PasteSource` | Add a `#[cfg(target_os = "macos")]` `imp` module; it needs **no dedicated thread** — presenter callbacks arrive on the presenter's own `NSOperationQueue` (GCD-backed, works headless; spike ran CFRunLoop only as insurance). |
| `PasteSource::arm(Vec<PromiseFile>)` | On macOS: create placeholders, register presenters (kept in a registry), write the URLs to the pasteboard via the same `writeObjects` call `files_clipboard::write_files` uses. A new `arm` replaces the previous registry: `removeFilePresenter:` + delete stale `armed/` dirs. |
| `PasteEvent::Pull { file_id, reply }` | Same event from `relinquish…ToReader`. The callback sends the event, `recv`s the `SpoolStream`, then waits. |
| `paste_sink.live` / `paste_sink.ready` / `SpoolFrontier` | Unchanged. `ready` hits serve repeat pastes with no re-pull. Needs one addition: a blocking `wait()` on `SpoolFrontier` that returns when `outcome` is set (condvar is already there). |
| `finish_ready_transfer` paste_render branch | Unchanged — partial stays at `<file_id>.partial`, verified in place, graduates to `ready`. |
| `promise_files` / `PROMISE_BATCH_WINDOW` / offer-batch arming | Unchanged. |

What is **not** reused: the `IStream` frontier streaming. Finder cannot read
the placeholder until the reply, so bytes are copied in whole after verify —
no `Read` loop exists on this backend.

## 3. Decisions

- **Placeholder layout: `<spool>/armed/<file_id>/<name>`**, where `spool` is
  the existing `paste::spool_dir()` (`$TMPDIR/clipboardwire-paste`) and
  `<name>` is the same sanitization used for landing. Per-`file_id`
  directories make name collisions impossible, which matters because the
  verified file lives at `<spool>/<file_id>.partial` — a flat `<spool>/<name>`
  placeholder could never coincide with it and two same-named offers would
  collide with each other.
- **Fill the placeholder by copy, not rename.** The verified spool file must
  stay at `<file_id>.partial` — `paste_sink.ready` holds that path and repeat
  pastes re-read it. `std::fs::copy` on APFS goes through `fcopyfile`/clone
  and is effectively free. The presenter therefore: waits on the frontier →
  `Ok` → copy `SpoolStream.path` → placeholder → `reader(reacquirer)`;
  `Err` → unlink placeholder → `reader(reacquirer)`.
- **Reply is a nested-block call: `reader(reacquirer)`.**
  `relinquishPresentedItemToReader:`'s parameter has the signature
  `void (^)(void (^ _Nullable reacquirer)(void))` — you hand it a block of
  yours; Foundation calls it when the read completes. The spike proved that
  calling it argument-less corrupts bookkeeping (`didRelinquish … no replies`
  crash *after* a successful copy). The reacquirer block does logging only.
- **Writer/deletion/move callbacks**: `…ToWriter:` is answered immediately
  with an empty reacquirer (nothing else should write; if it does, let it);
  `accommodatePresentedItemDeletion:` unlinks the placeholder and replies
  nil; `presentedItemDidMoveToURL:`/`presentedItemDidChange` log.
- **`resolved_mode()`**: drop `Paste → Pickup` for macOS only — i.e. degrade
  on `!(windows || macos)`; Linux still rejects `paste` at config load.
  `ReceiveMode::resolved()` (`auto`) keeps macOS → `Pickup`.
- **Echo/loop safety**: arming puts `file://` URLs of *our* placeholders on
  the clipboard → the clipboard thread sees `ClipChange::Files`. The existing
  `mark_applied` file-list suppression does **not** cover this (it covers
  supervisor-applied inbound clips; arming happens outside that path, same
  caveat as Windows' `OleSetClipboard` — which the X11/Win backends handle by
  the same suppression on the apply path). Verify: the arm must go through
  `clipboard.apply_tx`/`mark_applied` equivalent or extend suppression so a
  macOS arm is not re-published as an offer for the placeholder paths. This
  is a **must-test**, not an assumption.
- **Offer lifecycle while armed**: `file_abort`/TTL expiry calls
  `paste_sink.fail(file_id, …)` → the waiting `relinquish` sees `Err` →
  unlink → -43. `superseded` same path.
- **Headless**: works — no runloop, no STA. `pickup_auto` irrelevant here.
- **Cancellation**: untested (Finder's "Preparing" dialog has a stop button).
  Expected worst case: the claim is revoked, our pull completes into spool
  harmlessly. Task list requires a manual check.

## 4. Dependencies

`objc2-foundation` gains features `NSFileCoordinator`, `NSFilePresenter`,
`NSOperation`, `NSError`, `block2` (already used for `NSArray` etc.).
`objc2-app-kit` unchanged. No new crates beyond the existing objc2/block2 set.

## 5. Spec delta summary

`Paste-time clipboard entries`: replace the "macOS … only if the spike proves
Finder honours a promise" clause — spike **passed** via the presenter
mechanism; describe placeholder + coordinated-read semantics, the -43 failure
surface, and that macOS shows indeterminate "Preparing" progress rather than
byte progress.

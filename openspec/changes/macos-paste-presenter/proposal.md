# macOS paste path via `NSFilePresenter` placeholders

## Why

`receive_mode = "paste"` resolves to `pickup` on macOS, and `auto` resolves to
`pickup` too — so every file offer a macOS client sees is pulled the moment it
arrives and dropped into `~/Downloads/clipboardwire/`. That means every Ctrl+C
on a remote peer — including copies the remote user only ever pastes locally —
lands full-size on the Mac's disk, unprompted. It is the single biggest
behavioural gap between the two desktop platforms.

The archived lazy-transfer change closed macOS `paste` on the verdict "Finder
does not invoke a file-promise provider on Cmd+V". That verdict was about
`NSFilePromiseProvider` only and was recorded without running a spike. A new
spike (this change, `spike-0.1-result.md`) proves a different mechanism works:

- An **empty placeholder file + `NSFilePresenter`** makes Finder's Cmd+V wait:
  the paste coordinates a *read* on the placeholder, which fires
  `relinquishPresentedItemToReader:` on the presenter's operation queue.
- Finder shows its own "正在准备拷贝" progress dialog while it waits, waited
  the full 60 s in testing with no timeout observed, handles multi-file pastes
  (sequentially), and reports a copy error (-43) when the placeholder is
  unlinked instead of filled — the macOS analogue of Windows' `E_ABORT`.
- This is the same technique Royal TSX and Devolutions RDM ship in production
  for RDP file copy/paste.

## What changes

On macOS, `receive_mode = "paste"` becomes a real mode instead of silently
degrading to `pickup`:

- Offer arrival → create one placeholder per offered file
  (`<spool>/armed/<file_id>/<name>`, 0 bytes), register an `NSFilePresenter`
  for it, and put its `file://` URL on the general pasteboard — replacing the
  OLE `IDataObject` promise Windows uses, with the same observable contract.
- Paste → `relinquishPresentedItemToReader:` sends `PasteEvent::Pull` to the
  supervisor; the pull runs into the normal spool, verifies SHA-256, then the
  verified bytes are copied into the placeholder and the read is released.
- Pull failure → the placeholder is unlinked before the reply, so Finder
  reports error -43 rather than copying an empty file.

## Boundaries

- **No wire changes.** `file_offer`/`file_pull`/`file_chunk`/`file_digest` are
  untouched; a macOS `paste` receiver is indistinguishable from a Windows one
  on the network.
- **`auto` stays `pickup` on macOS.** `paste` is opt-in this release; flipping
  `auto` is a follow-up decision once the path soaks.
- **Send side untouched.** macOS already sends lazily.
- **No progress streaming.** Unlike Windows' `IStream`, the placeholder cannot
  show byte progress — Finder shows indeterminate "Preparing" for the whole
  pull, then a normal copy. Accepted: correctness first.

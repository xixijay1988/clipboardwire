# Paste-on-demand (receive_mode = "paste") — feasibility & design

Status: SPIKE VERIFIED (toolchain). Design proposed, not implemented.
Date: 2026-11-03. Author: lead session, from live XS-OFFICE evidence.

## Verdict

**Feasible on Windows.** Every load-bearing piece is proven:

- `windows` 0.61 + `windows-core` 0.61 crates compile `#[implement(IDataObject)]`
  and `#[implement(IStream)]` for `x86_64-pc-windows-gnu` — the toolchain we
  already ship (verified in /tmp/ole-spike, compile+link clean).
- The tray app already runs a real STA message pump (tao event loop).
- The pull machinery (offer → ReceiveEngine session → file_pull → file_chunk →
  file_digest → sha verify) already exists and is trigger-agnostic: today the
  trigger is "offer arrived", for paste mode it becomes "Explorer asked for
  FILECONTENTS".
- `FileOfferFrame` already carries exactly what `FILEDESCRIPTORW` needs:
  `name`, `file_size`, `expires_at_ms`, `from_device`.
- Proven mechanism in the wild: VirtualBox shared clipboard, Windows zip
  folders, FTP shell folders all use FILEDESCRIPTOR+FILECONTENTS delayed
  rendering.

**macOS paste-on-demand is not implementable** via public APIs:
`NSFilesPromisePboardType`/`provideDataForType:` fires on Finder *drags*, not
Cmd+V. So `receive_mode="paste"` is Windows-only; macOS targets keep
`pickup`. This matches the user's topology anyway — the paste target is
XS-OFFICE (Windows); the Mac only ever sources offers, which needs no
promise machinery.

## Architecture

```
offer arrives ─┬─ pickup (today): pull now → save → CF_HDROP
               └─ paste  (new):  PromiseDataObject → OleSetClipboard

Explorer Ctrl+V
  → OleGetClipboard → EnumFormatEtc sees FILEDESCRIPTORW + FILECONTENTS
  → GetData(FILEDESCRIPTORW) → FILEGROUPDESCRIPTORW (names, sizes, flags)
  → GetData(FILECONTENTS, lindex=i, TYMED_ISTREAM)
       → STA thread signals tokio supervisor: pull(file_id, from_device)
       → ReceiveEngine session → chunks → temp spool file
       → sha256 verify → IStream serves spool bytes to Explorer
  → Explorer writes to paste destination
```

Threading: a **dedicated STA thread** owns OleInitialize + OleSetClipboard +
a minimal GetMessage loop. NOT the tao loop — a blocking GetData on the tao
thread would freeze the tray and all OLE callbacks for the duration of a
large pull.

Echo suppression is free: a promise object is not CF_HDROP, so our own
clipboard watcher never sees it as a file copy. No rebroadcast, no
mark_applied needed.

## The verify-vs-stream decision

Lazy chunks can't be verified until the digest arrives (whole-file sha), but
an IStream has already handed bytes to Explorer by then. Options:

- **(chosen) Spool-first**: pull → `%TEMP%\clipboardwire\…` → sha256 → serve
  stream over the verified file. Honest about correctness; temp spool is
  invisible and GC'd, unlike today's Downloads cache. Paste blocks for the
  full pull duration — same wall time as today, minus the visible folder.
- Speculative streaming (bytes flow during pull): rejected for v1 — a
  sha mismatch leaves a corrupt file at the destination with no way to
  retract.
- `IDataObjectAsyncCapability` (+`IAsyncOperation`): v2 — lets Explorer show
  real copy progress while the pull runs instead of a busy cursor.

## Failure surface (v1 accepts honest errors)

- Paste after `expires_at_ms` or source offline → GetData returns
  DV_E_*/E_FAIL → Explorer "cannot paste". No retry in v1.
- Pull abort mid-render → stream EOF early or GetData failure.
- Paste target requests TYMED_IStorage (folders) or TYMED_HGLOBAL →
  unsupported in v1 → fail the specific GetDataHere, paste aborts cleanly.
- Non-Explorer targets (Office apps ask FILECONTENTS too — works via the
  same path, bonus).
- Directory Opus (present on XS-OFFICE): also consumes
  FILEDESCRIPTOR/FILECONTENTS — should work, needs a live test.

## Phasing

1. **Spike A** (prove the paste path without clipboardwire behind it):
   standalone exe — OleSetClipboard a PromiseDataObject describing a static
   file; verify Explorer Ctrl+V produces the file. De-risks FORMATETC/
   FILEDESCRIPTOR details that docs get wrong. ~1 day equivalent.
2. **Integration**: `receive_mode="paste"` resolves on Windows only
   (config.rs already rejects paste on other OSes); FileOffer arm routes to
   PromiseDataObject via the STA thread; chunk bridge via std mpsc; spool +
   verify; FileOffer's batch semantics (batch_id) → multi-file descriptor.
3. **Polish**: IAsyncOperation progress; Shell IDList Array if Opus still
   misbehaves; GC of stale spools; offer-refresh at paste time.

## Open questions flagged for testing on XS-OFFICE

- Does Explorer request FILECONTENTS per lindex sequentially? (assumed yes)
- Does Opus consume FILECONTENTS correctly? (it originates them for its own
  copies, so likely yes)
- Paste into apps that want HGLOBAL instead of IStream → decide HGLOBAL
  support by file-size threshold.

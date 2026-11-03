# Lazy file transfer (paste-time pull)

## Why

Today a file copy is **eager**: the client notices new files on the OS
clipboard, hashes the whole file, and streams every chunk to the hub. Every
connected peer receives the bytes; the receiver writes a real file to
`~/Downloads/clipboardwire/` and only then sets its clipboard to those paths.

Consequences:

1. **Transfer starts on copy, not on paste.** In Windows Remote Desktop the
   copy is instant and the bytes move when you paste on the target.
2. **Every connected peer receives every file**, including peers that never
   paste.
3. **No progress anywhere.** A 2 GiB copy is an invisible multi-minute stall,
   and a partial transfer leaves a `.partial` file with no user state.
4. The whole-file SHA-256 pass before the first chunk means the first byte of
   a large file waits for a full disk read.

## What changes

Files become **offers** redeemed on demand:

- Copy → publish a small `file_offer` descriptor. No bytes move.
- A receiving client asks for bytes → the source serves chunks to that one
  peer, in windows the receiver controls.
- Progress is reported to the tray; on Windows the shell draws its own
  copy-progress UI because we hand it a lazy `IStream`.

Eager transfer stays a first-class path: v0.3 peers, `clipboardwire send`,
Linux, and `send_mode = "eager"` keep today's behaviour byte-for-byte.

## Two switches, not one

Sending and receiving are separate capabilities and get separate settings.
The first draft folded them into one `file_mode`, which made macOS — whose
*receive* path is limited — silently fall back to eager *sending* as well.
That contradicted the whole point of the change.

- `send_mode = "lazy" | "eager"` (ships as `eager`, flips to `lazy` once
  every phase has landed): does a copy publish offers or push bytes?
- `receive_mode = "paste" | "pickup" | "eager" | "auto"` (ships as `eager`,
  flips to `auto` at the same time): how do we hand received files to the
  user?

Any client that can consume `file_offer` advertises `lazy-file`, whatever its
`receive_mode`. So macOS sending stays lazy even when it receives via
`pickup`.

## Platform reality

Both receive paths rest on an OS mechanism we have not verified on this
codebase. They are gated, in this order.

| Direction | Paste-time pull | Gate |
| --- | --- | --- |
| Windows receives | `CFSTR_FILECONTENTS` + `IStream`; Explorer draws progress | **spike 0.2** |
| macOS receives | `NSFilePromiseProvider` on the general pasteboard | **spike 0.1** |
| macOS sends → Windows pulls | sending is lazy everywhere; the Windows side needs spike 0.2 to receive | spike 0.2 |
| Windows sends → Windows pulls | sending is lazy everywhere; receive needs spike 0.2 | spike 0.2 |
| Either direction, any platform | `pickup`: tray action fetches, then paste a real file | none |

If a spike fails, the receive path on that platform drops to `pickup` and the
corresponding Tier 2 item is deleted from this change. That decision is
recorded in `design.md` §2, not left as a silent gap.

**Out of scope:** Linux and Wayland paste-time pull. There is no OS file-promise
mechanism; FreeRDP's maintainers found that even RDP needs FUSE there. Linux
keeps eager transfer and gains progress reporting.

## Mixed versions are not auto-bridged

A sender does **not** silently fall back to eager for peers that lack
`lazy-file`. Real deployments contain v0.3 desktops and Android clients that
never advertise anything, and broadcasting a full file to everyone whenever
one such peer is online would defeat the change entirely.

Instead:

- Peers that advertise `lazy-file` get offers.
- Peers that do not are logged and surfaced in the tray as "cannot receive
  lazy files".
- A mixed deployment sets `send_mode = "eager"` and gets today's behaviour.

This is written down as a decision because the alternative — automatic
per-peer eager — requires directional eager chunks plus peer join/leave
notifications, and still cannot distinguish a v0.3 desktop from an Android
client that has no file support at all.

## Impact

| Area | Change |
| --- | --- |
| `PROTOCOL.md` | v0.4; new frames; the file section v0.3 never documented; §7 renegotiated |
| `core/src/protocol.rs` | new frames, optional fields, split validation |
| `core/src/server/ws.rs` | `?caps=`/`?device=` parsing, unknown-type tolerance, per-peer directed channel |
| `core/src/server/hub.rs` | offer registry, device routing, directed chunks, caps-aware welcome |
| `core/src/client/transport.rs` | capability + device advertisement, control channel, offer list from welcome |
| `core/src/client/file.rs` | sender split into offer registry + pull server |
| `core/src/client/files_clipboard.rs` | Windows `IDataObject`/`IStream`; macOS promise (gated) |
| `core/src/client/mod.rs` | supervisor plumb-through, progress events |
| `core/src/client/config.rs` | `send_mode`, `receive_mode`, TTL/window/timeout knobs (**not** `device_id`) |
| `core/src/identity.rs` | new: durable `device_id` in the state dir |
| `cli/src/tray.rs` | progress, pending-offer menu, incompatible-peer warning |
| New deps | `windows` (COM), `crossbeam-channel` |

## Success criteria

Applies to an **all-v0.4 deployment**; mixed versions are covered by the
`send_mode = "eager"` escape hatch above.

Tier 1 — must ship:

- Copying a 1 GiB file sends no `file_chunk` while no peer pulls.
- A peer that never fetches never receives file bytes.
- `pickup` delivers byte-exact files on Windows and macOS, with tray progress.
- A transfer interrupted by an abort restarts from index 0 under a new
  `transfer_id`, so it survives a reconnect rather than resuming mid-file.
- `clipboardwire send <FILE>` is byte-identical to today.
- A v0.4 client against a v0.3 hub stays eager and works.

Tier 2 — conditional on the spikes:

- 0.2 passes: pasting in Explorer pulls at paste time with Explorer's own
  progress bar (Windows→Windows and macOS→Windows).
- 0.1 passes: pasting in Finder pulls at paste time with Finder's progress bar.

A failed spike deletes its Tier 2 line and downgrades that platform's
`receive_mode` default to `pickup`.

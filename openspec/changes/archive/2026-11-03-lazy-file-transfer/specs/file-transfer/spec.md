# file-transfer

## MODIFIED Requirements

### Requirement: Chunked file transmission

The sender SHALL split a file into `file_chunk` frames of `FILE_CHUNK_BYTES`
(4 MiB) raw bytes each and base64-encode each chunk into `payload_b64`.

Every chunk of one file SHALL carry the same `file_id`, `name`,
`content_type`, `total_chunks` and `file_size`.

A chunk MAY carry `to`, `transfer_id` and `from_device`. All three SHALL be
omitted from serialisation when absent, so an eager chunk is byte-identical to
v0.3.

Chunks SHALL be produced either eagerly (immediately after a copy) or lazily
(in response to `file_pull`).

#### Scenario: Single-chunk file

- **WHEN** a 100 KiB file is transmitted
- **THEN** exactly one `file_chunk` frame is produced with `chunk_index = 0`
  and `total_chunks = 1`

#### Scenario: Multi-chunk file

- **WHEN** a 9 MiB file is transmitted
- **THEN** three `file_chunk` frames are produced with `chunk_index` 0, 1, 2
  and `total_chunks = 3`

#### Scenario: Chunk size stays inside the frame limit

- **WHEN** a chunk is base64-encoded
- **THEN** the resulting JSON frame is smaller than `MAX_FRAME_BYTES` (50 MiB)

#### Scenario: Addressed chunk

- **WHEN** a chunk carries `to` and `transfer_id`
- **THEN** only that peer receives it

#### Scenario: Eager chunk is byte-identical to v0.3

- **WHEN** a chunk is produced by the eager path or by `clipboardwire send`
- **THEN** its JSON contains `file_sha256` and contains none of `to`,
  `transfer_id`, `from_device`

### Requirement: Whole-file hash computed before the first chunk

The eager sender and `clipboardwire send` SHALL continue to compute the
SHA-256 of the entire file before enqueueing the first chunk and SHALL put it
in every chunk's `file_sha256`.

The lazy sender SHALL NOT perform this pre-pass; it SHALL emit `file_digest`
for the session instead, as specified in "SHA-256 verification before
publish".

#### Scenario: Eager pre-pass retained

- **WHEN** a file is sent with `send_mode = "eager"` or via
  `clipboardwire send`
- **THEN** the file is read end-to-end for hashing before any chunk is
  enqueued

#### Scenario: Lazy sender does not pre-read

- **WHEN** a file is offered with `send_mode = "lazy"`
- **THEN** no read of the file occurs until the first `file_pull`

### Requirement: SHA-256 verified before the file is published

The receiver SHALL verify the full byte stream before the file becomes visible
to the user.

| `receive_mode` | Byte sink | Check point | Failure result |
| --- | --- | --- | --- |
| `eager` | our `.partial` | before renaming into place | partial deleted |
| `pickup` | our `.partial` | before renaming into place | partial deleted |
| `paste` (Windows) | the shell's target | the final bytes and EOF are withheld until the digest verifies | the stream returns `E_ABORT` and the shell reports a failed copy |
| `paste` (macOS) | the shell's target | written under a temp name, verified, then renamed | temp file removed |

Eager transfers SHALL carry the expected digest in every chunk's
`file_sha256`. Pulled transfers SHALL carry it in `file_digest`.

If a pulled transfer has every chunk but no `file_digest` within `DIGEST_WAIT`,
the transfer SHALL fail and all partial state SHALL be discarded. Waiting for
the digest SHALL count as progress, so the inactivity timeout cannot expire
first.

#### Scenario: Tampered payload

- **WHEN** any chunk's payload differs from the hashed byte stream
- **THEN** the transfer fails and no file is published

#### Scenario: Digest missing

- **WHEN** every chunk arrives but no `file_digest` follows within 10 s
- **THEN** the transfer fails and partial state is discarded

#### Scenario: Source file modified during the transfer

- **WHEN** the file's pinned identity changes between served windows
- **THEN** the sender aborts with `source_changed` rather than serving a torn
  stream that would still hash consistently

### Requirement: Zero-byte files are refused

Every transfer path — eager, lazy offer, and `clipboardwire send` — SHALL
reject a zero-byte file with an error rather than emitting a transfer with no
payload.

A directory in a copied selection SHALL be skipped with a warning rather than
turned into an offer.

#### Scenario: Empty file

- **WHEN** the user copies a 0-byte file in any mode
- **THEN** no offer and no chunk is produced, and an error is logged

### Requirement: Hub relays chunks without caching

The hub SHALL fan every valid `file_chunk` out to all other currently
registered clients and SHALL NOT store it for late joiners.

When a chunk carries `to`, the hub SHALL deliver it only to that device.

The hub SHALL stamp `from_device` and SHALL validate the chunk's name, NUL
bytes, `total_chunks`, `chunk_index`, `payload_b64`, and the digest-field rules
appropriate to its mode.

#### Scenario: Two peers connected

- **WHEN** client A sends an unaddressed chunk while B is connected
- **THEN** B receives it with `from_device` set to A's device, and A does not
  receive it

#### Scenario: Peer connects after the transfer

- **WHEN** a chunk was relayed while no peer was connected
- **THEN** that chunk is lost; a later-connecting peer cannot recover it

### Requirement: Receiver assembles into a partial file

In `eager` and `pickup` modes the receiver SHALL lazily create
`<save_dir>/<file_id>.partial` on the first chunk, pre-allocate it to
`file_size`, and seek-write each chunk at `chunk_index * FILE_CHUNK_BYTES`.

Chunks MAY arrive out of order. A duplicate `chunk_index` SHALL be ignored
without error. Inconsistent `total_chunks`, digest or `file_size` across
chunks of the same `file_id` SHALL abort that transfer.

In `paste` mode the receiver SHALL NOT write to `save_dir`.

#### Scenario: Out-of-order arrival

- **WHEN** chunks arrive in reverse order
- **THEN** the assembled file matches the original bytes

#### Scenario: Duplicate chunk

- **WHEN** the same `chunk_index` arrives twice
- **THEN** the second copy is ignored and the transfer still completes

#### Scenario: Paste mode does not touch the save dir

- **WHEN** the user pastes in Explorer with `receive_mode = "paste"`
- **THEN** no file appears under `~/Downloads/clipboardwire/`

### Requirement: Multi-file clipboard batching on receive

In `eager` and `pickup` modes, files completing within `RECEIVE_BATCH_WINDOW`
(500 ms) of each other SHALL be applied to the OS clipboard as one list.

Offers from one copy SHALL share a `batch_id`. Pulls belonging to one
`batch_id` SHALL be queued locally rather than counted against the
cross-batch concurrency limit.

A stream queued behind another stream of the same `batch_id` SHALL be treated
as making progress for the purpose of the inactivity timeout.

#### Scenario: Five files copied together

- **WHEN** five files finish transferring within the batch window
- **THEN** the OS clipboard is set once with all five paths

#### Scenario: Five-file paste does not trip the pull limit

- **WHEN** the user pastes five files at once
- **THEN** all five transfer, and none is rejected with `busy` or times out
  while queued

### Requirement: Default save location

Received files SHALL be saved under the platform downloads directory in a
`clipboardwire/` subdirectory, created on first use. If no downloads directory
can be resolved the receiver SHALL fall back to `<home>/Downloads/clipboardwire`.

This applies to `eager` and `pickup` modes. In `paste` mode the shell owns the
destination and the save location SHALL NOT be used.

#### Scenario: Fresh install

- **WHEN** the first file arrives in `eager` or `pickup` mode
- **THEN** `~/Downloads/clipboardwire/` is created and the file lands there

#### Scenario: Paste mode

- **WHEN** a file is pasted in Explorer with `receive_mode = "paste"`
- **THEN** nothing is written under the save location

### Requirement: Received filename cannot escape the save dir

Filenames from a peer SHALL be sanitised on **every** receive path, including
the promise path.

Path separators and control characters SHALL be replaced.

`cFileName` in a Windows descriptor SHALL be truncated to `MAX_PATH` measured
in UTF-16 code units, **preserving the extension**.

Names that sanitise to empty, `.` or `..` SHALL become `received-file`.
Collisions SHALL be resolved with a ` (n)` suffix.

#### Scenario: Traversal attempt

- **WHEN** the sender sets `name` to `../etc/passwd`
- **THEN** the file is written inside the destination as `.._etc_passwd`

#### Scenario: Backslash in a paste descriptor

- **WHEN** the sender's filename contains `\`
- **THEN** it is replaced before reaching `cFileName`

#### Scenario: Long name

- **WHEN** the name exceeds `MAX_PATH`
- **THEN** it is truncated while keeping its extension

### Requirement: One-shot `send` subcommand

`clipboardwire send <FILE>` SHALL connect with the existing client config,
publish the file as an eager chunk stream including `file_sha256`, flush all
queued chunks, and exit 0.

It SHALL NOT reconnect, SHALL NOT accept inbound transfers, SHALL use an
ephemeral device identity so it does not evict the running tray client, and
its wire output SHALL remain byte-compatible with v0.3.

#### Scenario: Scripted transfer

- **WHEN** `clipboardwire send ./report.pdf` runs while a tray client is
  connected
- **THEN** the tray client saves the file, the tray's own session is not
  disconnected, and the command exits 0

## ADDED Requirements

### Requirement: Durable device identity

The client SHALL persist a `device_id` in the state directory, separate from
`config.toml`, and SHALL report it on the WebSocket upgrade request.

The identity SHALL be accompanied by a host fingerprint. If the fingerprint
does not match at startup — a cloned VM image or a restored backup — the
client SHALL log a warning and generate a new `device_id` rather than sharing
an identity with another machine.

All relayed frames SHALL identify peers by `device_id`. The hub SHALL
translate between `device_id` and the per-connection `client_id` internally and
SHALL NOT expose `client_id` in new frame types.

#### Scenario: Settings dialog round-trip

- **WHEN** the user saves the settings dialog
- **THEN** the device identity is unchanged, because it does not live in the
  config file

#### Scenario: Cloned machine

- **WHEN** a VM image containing the state dir is booted on a second host
- **THEN** the client detects the fingerprint mismatch, warns, and takes a new
  identity instead of evicting the original machine in a loop

### Requirement: Duplicate device sessions

When a second connection presents a `device_id` that is already live, the hub
SHALL close the older connection with `4005 duplicate_session` and keep the
newer one.

The `send` subcommand SHALL use an ephemeral device identity that is not
persisted.

A connection's device identity SHALL be fixed at upgrade time and SHALL NOT be
changeable by any later frame. Because the hub stamps the identity itself,
frame types SHALL NOT carry a client-declared sender identity.

#### Scenario: Send while the tray runs

- **WHEN** `clipboardwire send` connects with the same config as a running
  tray client
- **THEN** the tray session survives and the send completes

#### Scenario: Connecting as another device

- **WHEN** a peer connects with a `device_id` already in use
- **THEN** the newer connection takes over and the older is closed with
  `4005 duplicate_session`; this is recorded as a credential-level risk, not
  treated as prevented

### Requirement: Device parameter compatibility

A connection that presents **no** `device` parameter SHALL be accepted and
assigned a hub-generated ephemeral identity for the lifetime of that
connection only. This is the pre-0.4 behaviour, and refusing it would lock
every already-deployed client out of an upgraded hub.

A `device` parameter that is present but not a valid UUID SHALL reject the
upgrade with HTTP `400 Bad Request` rather than falling back to an ephemeral
identity: a client that meant to be durable but sent garbage would otherwise
silently lose its offers across reconnects, which is harder to diagnose than a
refused connection.

This rejection happens **before** the WebSocket handshake, so the `4002
bad_frame` close code cannot be used: no socket exists yet to carry it. `4002`
applies only to frames rejected after the upgrade. Any client-visible
documentation SHALL describe the pre-upgrade case as an HTTP failure.

A `caps` value SHALL be parsed via the guarded entry point, so a value over
512 bytes is ignored wholesale rather than partially parsed.

#### Scenario: Pre-0.4 client connects to a 0.4 hub

- **WHEN** a client that predates the `device` parameter upgrades
- **THEN** the hub accepts it and assigns an ephemeral identity
- **AND** that client's frames are relayed as before

#### Scenario: Malformed device parameter

- **WHEN** a client sends `?device=not-a-uuid`
- **THEN** the hub refuses the upgrade with HTTP `400 Bad Request`
- **AND** no WebSocket is established, so no close code is involved

#### Scenario: Oversized caps

- **WHEN** a client sends a `caps` value longer than 512 bytes
- **THEN** the hub treats it as advertising nothing and still accepts the
  connection

### Requirement: Deregistration grace period

A disconnecting connection SHALL NOT immediately invalidate its device's
offers.

Offers SHALL be dropped and `file_abort{source_gone}` emitted only after the
device has been absent for the grace period, or when the offer's TTL expires,
whichever comes first.

A late `Deregister` from a superseded connection SHALL NOT remove offers owned
by the device's current connection.

Re-advertising offers after a reconnect SHALL NOT extend their expiry: the
source SHALL re-send the original `ts` and the original absolute expiry, so
the deadline is a property of the offer rather than of the hub's memory. This
also makes a hub restart harmless — the source restores the same deadline
from its own state.

#### Scenario: Flapping connection

- **WHEN** a source reconnects every 30 s for an hour
- **THEN** its offers still expire on the original TTL

#### Scenario: Stale connection closes late

- **WHEN** an old connection deregisters after the device has reconnected
- **THEN** the new connection's offers remain valid

### Requirement: Capability negotiation

The client SHALL advertise capabilities on the upgrade request as a
comma-separated list of lowercase tokens.

Only recognised tokens SHALL be retained; empty, malformed, and unknown tokens
SHALL be ignored and SHALL NOT consume any budget. Token matching SHALL be
exact: surrounding whitespace SHALL NOT be trimmed, because a lenient parser
and a strict one would then disagree about the same input.

There SHALL be no token-count cap, because any such cap would contradict the
invariant below once the known set grew past it.

A hub SHALL ignore a `caps` value longer than 512 bytes rather than parse it,
as a transport guard.

The resulting capability set SHALL depend only on which known tokens appear in
the input, so two implementations — and two protocol versions — always agree.

`welcome` SHALL report the hub's capabilities and the connected peers, and
SHALL include live offers.

The hub SHALL broadcast `peer_changed` when a device joins or leaves.

A sender SHALL publish offers only when the hub advertises `lazy-file`, and
SHALL NOT send them to peers that do not.

#### Scenario: New client, old hub

- **WHEN** a v0.4 client connects to a v0.3 hub with no `caps`
- **THEN** the client uses eager sending and transfers behave as in v0.5.4

#### Scenario: Peer joins after the sender

- **WHEN** a peer connects after a sender has already decided who can receive
  offers
- **THEN** the sender learns about it via `peer_changed`

#### Scenario: Mixed deployment is surfaced

- **WHEN** a connected peer does not advertise `lazy-file`
- **THEN** the tray warns that the peer cannot receive lazy files, and the
  sender does not downgrade every other peer

### Requirement: Unknown frame types are ignored

The hub's reader SHALL ignore any frame whose `type` it does not recognise,
including frame types added in a later protocol revision, logging at DEBUG and
keeping the connection open.

#### Scenario: Future frame type

- **WHEN** a hub receives a frame type added by a later protocol revision
- **THEN** it logs at DEBUG and continues, leaving the connection open

### Requirement: Hub restart

The hub holds no persistent state, so an offer only exists while the source is
connected and re-advertises it.

Offer expiry SHALL be an absolute deadline carried by the offer, not a timer
owned solely by the hub, so that a source which reconnects after a hub restart
restores the same deadline rather than restarting the clock.

#### Scenario: Hub restarts between copy and paste

- **WHEN** the hub restarts after an offer was published and before any peer
  pulls it
- **THEN** the offer is absent until the source re-advertises it, and its
  expiry is unchanged by the restart

### Requirement: Paste-time pull

When `send_mode` is `lazy`, copying files SHALL publish one `file_offer` per
file and SHALL NOT transmit file bytes.

An offer SHALL carry `file_id`, `batch_id`, `ts`, `name`, `content_type`,
`file_size` and an absolute `expires_at_ms`.

Bytes SHALL move only in response to a `file_pull`.

A `file_pull` SHALL carry the puller's `device_id` in `from_device`, stamped by
the hub from the connection rather than claimed by the client. The source needs
it to address the `file_chunk` and `file_digest` frames it produces: every
lazy chunk carries `to: <puller>`, and a source that had to guess would deliver
file contents to an arbitrary device.

This mirrors `file_offer`: the hub is the only writer of `from_device`, and a
client-supplied value is discarded before relay.

#### Scenario: Source learns who asked

- **WHEN** B pulls a file that A offered
- **THEN** A receives the pull with `from_device` set to B's device
- **AND** the chunks A emits carry `to` set to that same device

#### Scenario: Copy without a paste

- **WHEN** the user copies a 1 GiB file and no peer pulls it
- **THEN** the source sends no `file_chunk` frame

#### Scenario: Copy then paste

- **WHEN** the user copies a file and a peer pastes it before the TTL expires
- **THEN** that peer pulls the chunks and receives the complete file

#### Scenario: Re-copy of the same path

- **WHEN** the user copies the same unchanged path again
- **THEN** the existing offer is reused, or superseded with
  `file_abort{superseded}` for the old `file_id`

### Requirement: Transfer sessions

The receiver SHALL generate a `transfer_id`, and the first `file_pull` SHALL
create the session at `chunk_index = 0`.

`transfer_id` SHALL be the sole session key; per-session state SHALL be keyed
on it alone.

A **receiver** session SHALL end on digest delivery, on abort, or on idle
timeout.

A **source** session SHALL remain answerable after its digest until the idle
timeout, and a repeated `file_pull` for a completed session SHALL return the
same digest again with no chunks. The two lifetimes differ for one reason: a
`file_digest` can be dropped in transit (the receiver's control buffer is
bounded, and the hub drops rather than blocks), and the receiver's recovery is
to re-request the final window. A source that had already forgotten the session
would answer `invalid_start`, and the receiver would discard a fully
transferred file and start over. Repeating a digest is idempotent; re-sending
the bytes is not.

A source SHALL re-check the offer's identity before repeating a digest, and
SHALL answer `source_changed` instead if the file no longer matches — the
digest describes bytes the receiver already holds.

A `file_abort`'s **scope SHALL be decided by whether it carries
`transfer_id`**, not by its reason — the reason set is open and a receiver
cannot be expected to classify unknown reasons correctly:

| Carries `transfer_id` | Scope | Effect |
| --- | --- | --- |
| yes | that session only | the named session ends; other sessions for the same `file_id` continue |
| no | the offer | the offer is invalidated, **and every in-flight session for that `file_id` owned by the same source ends too** |

The offer-scoped form exists because some aborts happen before any session
exists (`source_gone` before a paste, `superseded` from a second copy,
`expired` from the TTL sweep). It is **not** a way to leave in-flight transfers
running: a source that disappears mid-paste takes its sessions down with it.

Replies to a specific `file_pull` (`unknown_offer`, `expired`, `busy`,
`window_too_large`, `invalid_start`) SHALL echo that pull's `transfer_id`, so a
receiver running two concurrent pastes of the same file can tell which one
failed.

A broadcast abort — one whose outbound `to` is absent — SHALL be expanded by
the hub into one directed copy per recipient, each carrying that recipient's
`to`. A receiver therefore always sees a frame with `to` set.

A session-scoped abort (one carrying `transfer_id`) SHALL name its `to`; it is
never broadcast.

A `file_offer` SHALL carry a `name` no longer than 1024 bytes and a
`content_type` no longer than 255 bytes, so a single offer cannot push
`welcome` past the frame limit for later joiners.

Frames with an unknown `transfer_id` SHALL be dropped, except a `file_pull`,
which creates the session.

Two peers pulling the same file SHALL have independent sessions.

#### Scenario: Concurrent pulls

- **WHEN** two peers pull the same file simultaneously
- **THEN** both receive correct files and neither aborts the other

#### Scenario: One of two concurrent sessions fails

- **WHEN** a receiver has two sessions open for the same `file_id` and one
  receives `file_abort{busy}` carrying its `transfer_id`
- **THEN** only that session ends and the other continues

#### Scenario: Source vanishes mid-paste

- **WHEN** the source disconnects while a session is in flight
- **THEN** the offer is invalidated and the in-flight session is also ended, so
  the shell reports failure instead of waiting for a timeout

#### Scenario: Restart after abort

- **WHEN** a session is aborted for any reason
- **THEN** the receiver starts a new session at index 0, so the single
  `file_digest` still covers the whole file

#### Scenario: Cross-batch concurrency limit

- **WHEN** pulls from different batches exceed the per-device limit
- **THEN** only the excess receives `file_abort{busy}`

### Requirement: Source identity pinning

At offer time the sender SHALL record the file's identity: on Windows
`(volume_serial, file_index, size, mtime)`, on macOS `(device, inode, size,
mtime_ns)`.

The sender SHALL re-stat and compare before the first served chunk, before
every served window, and immediately before emitting `file_digest`, aborting
with `source_changed` on any difference.

#### Scenario: File replaced between copy and paste

- **WHEN** the copied file is overwritten after the offer and before the pull
- **THEN** the sender aborts with `source_changed` and serves no bytes

#### Scenario: File modified mid-transfer

- **WHEN** the file changes after the first window was served
- **THEN** the sender aborts before serving further bytes, and no torn file
  can complete verification

### Requirement: Bounded pull window and loss recovery

A `file_pull` SHALL name a starting `chunk_index` and a `window`.

**The first pull of a session SHALL start at `chunk_index = 0`**, because the
single `file_digest` covers the whole file and a session that began mid-file
could never validate it. The sender SHALL reject a *session-opening* pull with
any other starting index using `file_abort{invalid_start}`.

Subsequent pulls **within the same `transfer_id`** SHALL be free to continue at
any index, so a file larger than one window is transferred by a sequence of
windows that advance through it.

The sender SHALL reject `window > 32` or `chunk_index >= total_chunks` with a
transfer-scoped `file_abort`.

After any abort, the receiver SHALL discard the partial session and restart
from index 0 with a new `transfer_id`, overwriting bytes already written in
place.

The receiver SHALL detect gaps by index, because chunks from one source arrive
FIFO. A detected gap SHALL end the session (`file_abort`) and restart from
index 0 with a new `transfer_id`, up to three restart attempts per file before
the transfer fails visibly.

A stalled tail SHALL be detected by an inactivity timer rather than a fixed
per-read deadline.

#### Scenario: Oversized window

- **WHEN** a peer requests `window = 1000`
- **THEN** the sender aborts only that transfer and the connection stays open

#### Scenario: Dropped chunk

- **WHEN** one chunk never arrives
- **THEN** the receiver restarts the session from index 0 and completes the
  transfer

#### Scenario: Slow chunk is not a gap

- **WHEN** a single chunk takes several seconds to arrive
- **THEN** the receiver does not restart the transfer

#### Scenario: Unrecoverable gap

- **WHEN** restart attempts are exhausted
- **THEN** the transfer fails visibly and partial state is discarded

### Requirement: Backpressure without blocking the hub

The hub SHALL NOT await a slow peer inside its single message-handling task.

When a directed delivery cannot be enqueued, the hub SHALL buffer it in a
bounded per-peer overflow queue drained by that peer's writer.

If the overflow queue is exhausted, the hub SHALL abort that transfer with
`file_abort{busy}` so the receiver can restart it, and SHALL NOT drop the chunk
silently.

#### Scenario: Slow receiver

- **WHEN** a receiver cannot keep up with a pull
- **THEN** other peers' traffic continues, and the transfer either completes or
  fails with a recoverable abort

### Requirement: Pull authorization and source binding

The hub SHALL forward a `file_pull` only if the named `file_id` has a live,
unexpired offer owned by the requested device.

`to` names the **source**, so a legitimate pull always names a device other
than the sender; the hub SHALL NOT require the puller to be the offer's owner.

The source SHALL serve only paths present in its own offer registry; a pull
SHALL NOT cause it to read an arbitrary path.

A receiver SHALL accept chunks and digests only for a `transfer_id` it
initiated and only from the offer's owner.

A receiver SHALL accept a **session-scoped** `file_abort` (one carrying
`transfer_id`) only for a session it initiated and only from the offer's
owner. It SHALL accept an **offer-scoped** `file_abort` (no `transfer_id`)
only from the offer's owner, and SHALL apply it to every local session for
that `file_id`.

#### Scenario: Pull for an unknown offer

- **WHEN** a peer pulls a `file_id` that was never offered
- **THEN** the hub replies `unknown_offer` and no source is contacted

#### Scenario: Injected chunk

- **WHEN** a third peer sends a chunk for someone else's `transfer_id`
- **THEN** the receiver drops it and logs a warning

### Requirement: Offer limits

The hub SHALL reject an offer whose `expires_at_ms` is not in the future or is
more than 24 hours away, SHALL compare against the current time without
overflowing, and SHALL cap both the per-device and total number of live
offers.

Exceeding the total cap SHALL reject the new offer; it SHALL NOT evict other
clients' offers.

Rejecting an offer SHALL NOT close the sender's connection.

#### Scenario: Absurd expiry

- **WHEN** a peer sends `expires_at_ms = 18446744073709551615`
- **THEN** the offer is rejected and the hub keeps running

#### Scenario: Offer flood

- **WHEN** one peer exceeds the per-device offer cap
- **THEN** its excess offers are rejected and other clients' offers survive

### Requirement: Offer lifecycle

Offers SHALL expire after `offer_ttl_minutes` (default 30). Expiry SHALL gate
only the *start* of a pull; an in-flight transfer SHALL continue until it
finishes or hits the idle timeout.

`welcome` SHALL include live offers.

A received offer SHALL be placed on the clipboard only if its `file_id` is new
and its `ts` is newer than the last value this client applied.

#### Scenario: Source disconnects

- **WHEN** a source device has been absent past the grace period
- **THEN** every peer receives `file_abort{source_gone}` for its offers

#### Scenario: Late joiner

- **WHEN** a peer connects while an offer is still live
- **THEN** its `welcome` lists that offer and it can pull it

#### Scenario: TTL expires during a transfer

- **WHEN** an offer's TTL elapses while a pull is in progress
- **THEN** the transfer continues and completes

#### Scenario: Stale offer does not clobber the clipboard

- **WHEN** a re-advertised offer is older than what the clipboard currently
  holds
- **THEN** the clipboard is not overwritten and the offer stays in the tray
  list

### Requirement: Paste-time clipboard entries

On Windows, when `receive_mode` is `paste`, the receiver SHALL expose offerable
files through `CFSTR_FILEDESCRIPTORW` + `CFSTR_FILECONTENTS`, backed by an
`IStream` that pulls bytes on demand while the message pump keeps running.

`FILEDESCRIPTOR` entries SHALL set `FD_FILESIZE` and `FD_PROGRESSUI`.

`Read` SHALL return `S_OK` with zero bytes read at EOF, SHALL implement `Stat`
and `Seek`, and SHALL use an inactivity timeout that treats digest waiting as
progress.

On shutdown the receiver SHALL clear the clipboard only if it still owns it,
and SHALL NOT flush a promise-based object.

On macOS, paste-time entries SHALL be used only if the spike proves Finder
honours a promise on the general pasteboard; otherwise `pickup` SHALL be used
and recorded as an explicit decision.

#### Scenario: Paste into Explorer

- **WHEN** the user pastes on Windows with `receive_mode = "paste"`
- **THEN** Explorer shows a copy-progress dialog and the file appears with
  bytes pulled from the source

#### Scenario: Source vanished before paste

- **WHEN** the source is offline when the paste starts
- **THEN** the read fails and the shell reports a copy failure

#### Scenario: Digest failure during paste

- **WHEN** the digest does not match at the end of a paste
- **THEN** the final bytes and EOF are withheld, so the shell reports failure
  instead of saving a corrupt file

### Requirement: Pickup mode

In `pickup` mode the receiver SHALL fetch on explicit user action from the
tray, save the file under the save dir, and then place the real path on the
clipboard.

With `pickup_auto` enabled, offers SHALL be fetched automatically on arrival,
which reproduces eager behaviour for headless clients.

#### Scenario: macOS without a promise

- **WHEN** the user chooses to fetch a received offer from the tray
- **THEN** the file lands in the save dir and the clipboard holds its path

#### Scenario: Headless pickup

- **WHEN** a headless client runs with `receive_mode = "pickup"` and
  `pickup_auto = true`
- **THEN** offers are fetched on arrival with no UI required

### Requirement: Send and receive modes are separate

`send_mode` SHALL accept `"lazy"` and `"eager"`.
`receive_mode` SHALL accept `"auto"`, `"paste"`, `"pickup"` and `"eager"`.

`auto` SHALL resolve by platform and spike outcome, not by configuration
guess.

A client SHALL advertise `lazy-file` whenever it can consume offers,
regardless of its `receive_mode`.

Headless clients SHALL default to `receive_mode = "eager"`.

An explicit mode the platform cannot support SHALL fail config validation with
an actionable message.

Until the lazy path is fully shipped, both modes SHALL default to `"eager"`;
they SHALL flip to `"lazy"` and `"auto"` in the same release that completes the
feature, so no intermediate build ships half a path.

#### Scenario: macOS sends lazily

- **WHEN** a macOS client receives via `pickup` but sends with
  `send_mode = "lazy"`
- **THEN** its copies still produce offers and peers can pull them

#### Scenario: Linux with auto

- **WHEN** the client runs on Linux with default settings
- **THEN** files are transferred eagerly as in v0.5.4

#### Scenario: Explicit paste on Linux

- **WHEN** the config sets `receive_mode = "paste"` on Linux
- **THEN** the client refuses to start and explains why

### Requirement: Transfer progress reporting

The client SHALL report progress for every transfer in every mode, including
the eager pre-hash pass.

Progress SHALL report direction, peer device, name, bytes done, bytes total,
and a state covering hashing, offering, pulling, verifying, done, failed and
aborted.

The sender of a transfer SHALL NOT report a verifying state.

The tray SHALL present an aggregate summary within its tooltip length limit
and SHALL expose per-transfer detail through a menu item.

#### Scenario: Eager large file

- **WHEN** a 2 GiB file is hashed and sent eagerly
- **THEN** progress advances through hashing, pulling and done

#### Scenario: Tooltip length

- **WHEN** five transfers run at once
- **THEN** the tooltip stays within the platform limit and the menu lists each
  transfer

### Requirement: File name redaction in logs

File names SHALL be redacted at INFO and above on both the client and the hub
unless explicitly enabled by configuration.

#### Scenario: Default logging

- **WHEN** a file is transferred with default settings
- **THEN** INFO logs on both sides contain a redacted identifier rather than
  the file name

## Notes

No baseline requirement is removed by this change. `PROTOCOL.md` §7's "refuse a
mismatched version" rule was never implemented by the shipped client
(`transport.rs` only logs the version), so there is nothing to strike from the
baseline spec; task 8.5 rewrites that section of `PROTOCOL.md` to describe
capability downgrade.

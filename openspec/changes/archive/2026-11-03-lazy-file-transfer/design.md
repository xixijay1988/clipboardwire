# Design — lazy file transfer

## 1. What the current code assumes

| Place | Assumption |
| --- | --- |
| [mod.rs:113](core/src/client/mod.rs#L113) | `ClipChange::Files` means "start sending now" |
| [file.rs:59](core/src/client/file.rs#L59) | the whole file can be read and hashed before the first chunk |
| [hub.rs:235](core/src/server/hub.rs#L235) | chunks are broadcast and never cached |
| [files_clipboard.rs:275](core/src/client/files_clipboard.rs#L275) | the clipboard holds a path to a complete file |

Two further constraints shape everything below:

- The hub reader treats an unknown frame type as `bad_frame`
  ([ws.rs:272](core/src/server/ws.rs#L272)) and closes the connection
  ([ws.rs:158](core/src/server/ws.rs#L158)). This contradicts `PROTOCOL.md` §3
  and means **any** new frame type breaks an un-upgraded hub. It must be fixed
  before new frames exist.
- `Hub::handle` is a synchronous function on a single actor task
  ([hub.rs:170](core/src/server/hub.rs#L170)). Nothing inside it may `await`
  on a slow receiver.

## 2. Phase 0 — gates, with pass/fail criteria

Each spike is a throwaway binary with a written checklist. **Anything short of
all checks passing is a fail**, and a fail removes that receive path from the
change.

### 2.1 Spike 0.1 — macOS promise on the general pasteboard

Question: does Finder `Cmd+V` honour `NSFilePromiseProvider` placed on
`NSPasteboard.general`?

Checklist (all required):

1. A Paste menu item appears in Finder's context menu.
2. `writePromiseToURL:completionHandler:` fires.
3. The bytes arrive intact.
4. Finder shows a copying progress UI.
5. Works for one file, several files, and a ~1 GB file.
6. Cancelling the copy in Finder stops the delegate, and our process stays
   responsive.
7. Our app quitting while the promise is live does not hang the system (this
   is the failure mode reported with general-pasteboard promises).
8. Works with our app not focused, and after a clipboard-manager read.

Record: macOS versions tested, and whether the result holds with the app
running headless (no tray).

If step 2 passes and step 4 fails, that is a **fail** — Finder progress is a
stated success criterion, and a promise without progress is not better than
`pickup`.

### 2.2 Spike 0.2 — Windows `IStream` promise

Checklist (all required):

1. Explorer shows a Paste item and a copy-progress dialog
   (`FD_PROGRESSUI`).
2. A `Read` that blocks for 10 s does not surface as an unresponsive app and
   does not make Explorer time out.
3. The STA message pump keeps running during a blocked `Read`, and another
   process can still read the clipboard.
4. A multi-file paste: Explorer's `GetData`/`Read` ordering does not trip the
   per-client pull limit (see §5.4 — pulls for one batch are queued locally,
   so this is a design requirement as much as a check).
5. Returning `E_ABORT` mid-copy makes Explorer report a failure and clean up
   its partial file. **Verify, do not assume** — the design depends on it.
6. A cross-process `IStream::Release` reaches us while a `Read` is pending, so
   cancel detection is real.
7. Two candidate stream placements both work end to end: (a) stream object
   marshalled to the MTA so `Read` runs off the apartment thread, (b) stream in
   the STA waiting with `CoWaitForMultipleHandles`. Pick whichever passes and
   delete the other from the code.

If 0.2 fails, Windows `receive_mode` defaults to `pickup`.

### 2.3 What is still unverified after Phase 0

Anything not listed above stays an assumption. In particular the *sending*
side (offer registry, pull server, hash-as-you-serve) has no platform-specific
risk and is testable without either spike.

## 3. Protocol v0.4

`PROTOCOL_VERSION` becomes `clipboardwire/0.4.0`.

### 3.1 Identity: `device_id`

`client_id` is `Uuid::new_v4()` per connection
([ws.rs:101](core/src/server/ws.rs#L101)), so it cannot identify a source
across a reconnect. The first draft tried to fix this by putting `device_id`
in `welcome`, which is the wrong direction — `welcome` is hub→client, so the
hub would never learn anything.

The working shape:

```
GET /sync?caps=lazy-file&device=<uuid> HTTP/1.1
```

- The client generates `device_id` on first run and persists it in the
  **state dir** (`state_dir/device_id`), never in `config.toml`: the settings
  dialog rewrites the whole config file from its form fields
  ([settings.rs:557](cli/src/settings.rs#L557)), which would erase it and
  mint a new identity on every save.
- The hub records `device_id` per connection at upgrade time.
- **All frames that cross the wire use `device_id`.** The hub stamps
  `from_device` on relayed frames; `to` is always a `device_id`. `client_id`
  stays an internal routing key and never appears in a new frame.
- Lookup is `device_index: HashMap<DeviceId, Uuid>` (device → current
  connection).

**Duplicate `device_id`.** Two live connections with the same device id happen
in practice: `clipboardwire send` reads the same config as the tray
([main.rs:200](cli/src/main.rs#L200)), and users copy config files between
machines. Policy, written down rather than left implicit:

- The **newest** connection wins. The older one is closed with
  `4005 duplicate_session` and told why.
- `send` uses an **ephemeral** device id (a fresh random UUID, not persisted).
- A connection's device is fixed at upgrade time and cannot be changed
  afterwards. A second connection presenting a live `device_id` evicts the
  first (§3.1), so there is no ambiguity about who owns a device.


**Deregister with a grace period.** Closing the socket must not immediately
invalidate offers: the old connection can linger until `READ_TIMEOUT`
([ws.rs:50](core/src/server/ws.rs#L50)), and its late `Deregister` would wipe
the offers the *new* connection just re-advertised. Therefore:

- `Deregister` only takes effect if `device_index[device] == this connection`.
- Offers are dropped and `file_abort{source_gone}` emitted only after the
  device has been absent for `DEVICE_GRACE` (60 s), or when the offer's own TTL
  expires — whichever comes first.
- Re-advertising after a reconnect refreshes reachability but **does not
  extend `expires_at`**. A device that flaps every minute cannot keep an offer
  alive forever.

### 3.2 Capabilities

`caps` is a comma-separated list of lowercase tokens:

```
caps = token *( "," token )
token = 1*( "a".."z" / "0".."9" / "-" )
```

**The capability set depends only on which recognised tokens appear.** The
known set is closed and small, so:

1. Split the query value on `,`.
2. A token is accepted only if it matches the grammar exactly — no surrounding
   whitespace is trimmed, because a lenient parser and a strict one would
   disagree about the same input.
3. Ignore empty, malformed, and unrecognised tokens. They cost nothing.
4. Retain every recognised token.

Because a version that knows fewer tokens simply ignores the ones it does not
know, the resulting set is identical across implementations *and* across
protocol versions: **caps ≙ { t ∈ known(v) : t ∈ input }**. A future client
sending `some-long-new-token,lazy-file` still yields `lazy-file` on an older
parser, which is what forward compatibility requires.

There is deliberately **no token-count cap**: a cap would contradict the
invariant above as soon as the known set grew past it, because two versions
would then retain different subsets of the same input. The real bound is that
`Caps` is a `u8` — adding a ninth capability is a compile error, not a silent
truncation.

A `caps` value longer than 512 bytes is a **transport guard**, not a parsing
rule: a hub SHALL ignore the whole value rather than parse it, so there is no
truncation point that two implementations could compute differently.

Defined values:

| Token | Meaning |
| --- | --- |
| `lazy-file` | can consume `file_offer` and pull chunks |
| `file-recv` | can receive file bytes at all |
| `transfer-id` | understands `transfer_id` as the session key |

`file-recv` exists to separate "old desktop that can still take a file" from
"Android client that cannot take one at all". Today neither advertises
anything, so this token only helps once v0.4 clients are on both sides; it
does not make v0.3 automatic.

`welcome` gains optional `caps`, `peers`, and `offers`:

```jsonc
{ "type": "welcome",
  "server": "clipboardwire/0.4.0",
  "client_id": "…",
  "caps": ["lazy-file", "file-recv", "transfer-id"],
  "peers": [{ "device": "…", "caps": ["lazy-file"] }],
  "offers": [ /* live offers, see §3.3 */ ],
  "last_clip": null }
```

`peers` is a snapshot at connect time and is therefore **not** sufficient on
its own — see §4.4 for join/leave notifications.

### 3.3 Frames

```jsonc
{ "type": "file_offer",
  "file_id": "…", "batch_id": "…", "ts": 1716163200000,
  "name": "report.pdf", "content_type": "application/pdf",
  "file_size": 1073741824,
  "expires_at_ms": 1716165000000,
  "from_device": "…" }

{ "type": "file_pull",
  "file_id": "…", "transfer_id": "…",
  "to": "…",              // source device_id
  "from_device": "…",     // puller device_id, stamped by the hub
  "chunk_index": 0, "window": 3 }

{ "type": "file_chunk",
  "file_id": "…", "transfer_id": "…",
  "to": "…", "from_device": "…",
  "name": "report.pdf", "content_type": "application/pdf",
  "chunk_index": 3, "total_chunks": 256, "file_size": 1073741824,
  "payload_b64": "…" }

{ "type": "file_digest",
  "file_id": "…", "transfer_id": "…", "to": "…", "from_device": "…",
  "file_sha256": "AA:BB:…" }

{ "type": "file_abort",
  "file_id": "…",
  "transfer_id": "…",     // present ⇒ session-scoped; absent ⇒ offer-scoped
  "to": "…",              // absent ⇒ broadcast; the hub expands it per recipient
  "from_device": "…",
  "reason": "…" }

{ "type": "peer_changed",
  "device": "…", "caps": ["lazy-file"], "event": "joined" | "left" }
```

### 3.4 `transfer_id` is the only session key

The first draft used `(file_id, to, transfer_id)` in some places and
`(file_id, device_id, transfer_id)` in others. One key, one definition:

- The **receiver** generates `transfer_id` and creates the session with its
  first `file_pull`, always at `chunk_index = 0`.
- Every subsequent frame for that session carries `transfer_id`; `file_id` is
  metadata for diagnostics and for locating the offer.
- The session ends on `file_digest` delivery, on `file_abort`, or on
  `pull_idle_timeout_secs` of silence.
- Frames with an unknown `transfer_id` are dropped **except** a `file_pull`,
  which creates the session.

Because a session cannot start mid-file, an interrupted transfer is a *new*
session from index 0, not a continuation. That is what keeps a single
`file_digest` able to cover the whole file (§5.5).

### 3.5 Chunk compatibility and validation

`file_chunk` keeps every v0.3 field. Changes, all wire-additive:

| Field | Change |
| --- | --- |
| `to`, `transfer_id`, `from_device` | new, `skip_serializing_if = "Option::is_none"` |
| `file_sha256` | `Option<String>`, `skip_serializing_if`; required for eager, forbidden for lazy |

Validation splits on presence of `transfer_id`:

- **Eager chunk** (no `transfer_id`): `file_sha256` required, `to` forbidden,
  `from_device` optional. Must serialise byte-identically to v0.3 so
  `clipboardwire send` is unchanged.
- **Lazy chunk**: `to` and `transfer_id` required, `file_sha256` forbidden.

`file_abort` scope is decided by **presence of `transfer_id`**, never by the
reason string — the reason set is open, so classifying by reason would force
every implementation to track the set in lockstep.

- With `transfer_id`: that session ends; sibling sessions for the same
  `file_id` continue.
- Without: the offer is invalidated **and every in-flight session for that
  `file_id` from the same source ends**. The session-less form exists for
  aborts that happen before a session exists (`source_gone` before a paste,
  `superseded` from a second copy, `expired` from the TTL sweep) — it is not a
  way to leave transfers running after the source dies.

Replies to a specific pull (`unknown_offer`, `expired`, `busy`,
`window_too_large`, `invalid_start`) echo that pull's `transfer_id`, so two
concurrent pastes of one file remain distinguishable.

The hub expands a broadcast abort — one whose outbound `to` is absent — into
one directed copy per recipient, so a receiver never sees an abort without
`to`. A session-scoped abort (one carrying `transfer_id`) MUST name its `to`
and is never broadcast.

Recipients are bounded so a single offer cannot poison `welcome` for later
joiners: `name` is capped at 1024 bytes (a name longer than any filesystem's
limit is not a real file), and `content_type` at 255 bytes.

## 4. Hub

```rust
struct Hub {
    clients: HashMap<Uuid, PeerEntry>,        // client_id → connection    device_index: HashMap<Uuid, Uuid>,        // device_id → client_id
    last_clip: Option<ClipFrame>,
    offers: HashMap<Uuid, OfferEntry>,
    pending_abort: HashMap<Uuid, Instant>,    // device_id → grace deadline
}

struct PeerEntry {
    device: Uuid,
    caps: Caps,
    clip: mpsc::Sender<ClipFrame>,
    file: mpsc::Sender<FileChunkFrame>,
    ctl: mpsc::Sender<ControlFrame>,
}
```

### 4.1 Routing

- `PublishFileOffer` — validate (§4.3), stamp `from_device`, insert, broadcast.
- `PublishFilePull` — forward only if `offers[file_id]` exists, is unexpired,
  and its owner equals `pull.to`. Return `file_abort{unknown_offer|expired}`
  otherwise. Checking the offer registry is what makes the hub's TTL
  meaningful.

  Note what is deliberately **not** checked: the puller is not required to be
  the offer's owner. The `to` field names the *source* device, so a legitimate
  pull always names a device other than the sender. `from_device` is stamped by
  the hub from the connection, not claimed by the client, so there is no
  spoofable identity field in a new frame at all.

  `from_device` on a `file_pull` is load-bearing, not decorative: the source
  answers with chunks carrying `to: <puller>`, so without it the source cannot
  address its reply and would have to guess a device. This was missing from
  the first draft of the frame, which made the pull unusable.
- `PublishFileChunk` — directed delivery when `to` is set, broadcast when not.
- `PublishFileDigest`, `PublishFileAbort` — routed by `to`.
- `Deregister` — apply the grace rule from §3.1.

### 4.2 Backpressure belongs in the ws writer, not the hub

The first draft said "use `send().await` instead of `try_send` in the hub".
That cannot work: `handle` is synchronous and single-actor, so awaiting one
slow peer stalls every other peer's traffic.

Instead:

1. Hub handlers stay **synchronous** and always `try_send`.
2. On `Full`, the hub records the chunk in a per-peer **overflow queue**
   (bounded, `PER_CLIENT_FILE_OVERFLOW = 8`). The queue is owned by the hub;
   the peer's writer task drains it between socket writes and reports
   completions back through a `ctl` message, which the hub uses to release the
   slot. Ownership never moves — only draining is delegated.

If the overflow queue is also full, the chunk is **not** silently dropped: the
hub emits `file_abort{busy}` for that `transfer_id`, and the receiver restarts
the transfer from index 0 under a new `transfer_id` (§5.5). Silent loss is what
made the earlier design unrecoverable.

The "window 3 < channel depth 4" argument from the previous draft is
withdrawn: one peer has a single file channel shared by all its transfers and
by any broadcast traffic, so channel depth is not a bound on a single
transfer. Window size is now tuned for memory and latency only.

### 4.3 Hard caps

| Bound | Limit | On violation |
| --- | --- | --- |
| `expires_at_ms` | must be within `now..=now + 86_400_000` | reject offer, `abort{...}` to sender |
| expiry bound | `now + 86_400_000` computed without overflow | reject offer |
| offers per device | 256 | reject offer |
| total offers | 4096 | reject offer |
| `window` | `1..=32` | `file_abort{window_too_large}` |
| `caps` value length | > 512 bytes | ignore the whole value (transport guard) |
| token grammar | exact match, no trimming | malformed tokens ignored |

`Instant + Duration::from_millis(u64::MAX)` panics; unchecked expiry lets any
authenticated peer kill the hub task and every connection with it. The offer
carries an absolute `expires_at_ms`, so the sender — not the hub — owns the
deadline and can restore it after a hub restart (§"Hub restart").

Global eviction is **rejected rather than "drop the oldest"**: with 64
connections × 256 offers the theoretical maximum is 16× the global cap, so
oldest-wins lets one peer evict everyone else's offers. Rejecting makes the
failure local and visible.

Control-frame rejections are always transfer-scoped and never close the
connection.

### 4.4 Peer join/leave

`welcome.peers` is a snapshot; a peer that connects five minutes later is
invisible to a sender that decided at connect time. The hub therefore
broadcasts `peer_changed{event: "joined"|"left"}` whenever a device registers
or expires. Clients keep a live view and can warn when a mixed deployment
appears.

### 4.5 Supersede

The hub cannot key offers by path — offers carry only `name`. Supersession is
the **sender's** job:

- A second copy of the same path reuses the previous `file_id` if the pinned
  identity is unchanged, or publishes a new offer and immediately sends
  `file_abort{reason: "superseded"}` for the old `file_id`.

(`superseded` is a valid open-set reason.)

### 4.6 Unknown frame types

`ws.rs` logs at DEBUG and continues instead of `bad_frame` + close. This also
covers the new `Frame` variants falling into the existing `Ok(_)` arm
([ws.rs:262](core/src/server/ws.rs#L262)), which today would close the
connection. **This must ship before any new frame type reaches a hub that may
be older.**

`PROTOCOL.md` §7 says clients must refuse a server whose major.minor differs.
That rule is incompatible with a v0.4 client talking to a v0.3 hub, so it is
renegotiated: version differences downgrade capability, they do not refuse the
connection.

## 5. Sender

### 5.1 Offer registry

`core/src/client/offer.rs`. At copy time: stat, generate `file_id`, pin
identity, publish an offer. Cost is O(files), not O(bytes).

Zero-byte files are rejected; a directory in the selection is skipped with a
warning.

### 5.2 Pinning, checked repeatedly

At offer time record `(volume_serial, file_index, size, mtime)` on Windows and
`(device, inode, size, mtime_ns)` on macOS.

Re-stat and compare:

1. before the first served chunk,
2. **before every served window**, and
3. **immediately before emitting `file_digest`**.

Without (2) and (3) a file modified mid-transfer produces a stream whose first
half is old and second half is new — and the running hash covers exactly that
stream, so verification **passes** on a torn file. This is the failure the
previous draft's single pre-check could not catch.

### 5.3 Pull server

One task per `transfer_id`:

1. Validate `chunk_index < total_chunks`, `window <= 32`.
2. Identity check (§5.2).
3. Read the window, push chunks with `to` + `transfer_id`.
4. Feed the sequential hasher, deduping by index.
5. Release the handle after `pull_idle_timeout_secs`.

### 5.4 Concurrency

`MAX_ACTIVE_PULLS` = 4 per device, but **pulls belonging to one `batch_id`
queue locally instead of being rejected**. Explorer's multi-file paste may
open several streams at once; giving the third one `busy` would fail a routine
five-file paste. Only pulls from *different* batches compete for the limit.

A stream that is *queued* behind another in the same batch counts as making
progress, so the inactivity timeout cannot fire while it waits. Without that,
a `Read` for file #5 would time out during file #1's transfer.

**Cloned state dirs.** `device_id` lives in the state dir, which people clone
with VM images and restore from backups. Two machines sharing a `device_id`
will evict each other in a loop under the newest-wins rule. The client
therefore records a per-host fingerprint next to the id; on mismatch it logs a
warning at startup and regenerates the id rather than fighting the other
machine.

### 5.5 Hash

Sequential: hash-as-you-serve keyed on `transfer_id`. That covers the served
byte stream by construction.

**A session starts at `chunk_index = 0`.** This is required for the digest to
be meaningful: the receiver verifies the *whole* file, so a session that began
at index *k* would produce a digest over `[k, N)` that can never validate
`[0, N)`.

That constrains only the **first** pull of a session. Later windows inside the
same `transfer_id` advance freely through the file — otherwise a file bigger
than one window could never be fetched at all. The sender rejects a
session-opening pull with `chunk_index != 0` using
`file_abort{invalid_start}`.

- After any abort (`busy`, `source_gone`, stall), the receiver discards the
  partial session and **restarts from index 0 with a new `transfer_id`**.
  Bytes already written to a `.partial` file are overwritten in place, so the
  only cost is re-transferring them.
- There is therefore no parallel hash pass and no mid-session resume. A
  resumption is a new session, not a continuation of an old one; within a
  session the windows simply advance.

This trades bandwidth for correctness: it is the only shape in which a single
`file_digest` can cover exactly what the receiver will publish.

## 6. Receiver

### 6.1 Modes

| `receive_mode` | Byte sink | Verification point | Failure result |
| --- | --- | --- | --- |
| `eager` | our `.partial` | before `rename` | partial deleted |
| `pickup` | our `.partial` | before `rename` | partial deleted |
| `paste` (Win) | Explorer's target | last bytes + EOF withheld until digest verifies | `Read` returns `E_ABORT`; Explorer reports failure and cleans up (verified by spike 0.2 item 5) |
| `paste` (mac) | Finder's URL | temp name in destination, verify, rename | temp removed |

### 6.2 Pull engine

- Requests `pull_window_chunks` at a time; tracks `received: Vec<bool>`.
- **Gap detection is index-based, not timer-based.** Chunks from one source
  arrive FIFO through the hub, so a gap is known as soon as a higher index
  lands. Only a missing *tail* needs a timer
  (`TAIL_STALL_TIMEOUT`, default 15 s). The previous draft's "500 ms of
  silence means re-request" would misfire on any chunk that takes longer than
  500 ms to arrive, which is every chunk on a slow link.
- `MAX_RESTARTS` = 3 per file: a detected gap ends the session and the receiver
  restarts from index 0 (§5.5). Per-index retry is not possible under the
  single-digest rule.
- Binds strictly to `(transfer_id, from_device == offer.from_device)`.
- Looks up `file_id` → offer only to validate the owner; all routing uses
  `transfer_id`.
- Sends `file_abort{user_cancel}` when the stream is released early or a
  `pickup` is dismissed.

### 6.3 Timeouts

`PASTE_INACTIVITY_TIMEOUT` = 30 s of no bytes **and** no progress.
`DIGEST_WAIT` = 10 s. Waiting for a digest counts as progress, so the two
cannot race: the inactivity timer is strictly larger and is reset by the
digest arriving.

A fixed per-`Read` cap is gone. A 5.5 MiB chunk needs ~9 Mbps to arrive in 5 s,
so any fixed small cap fails on real links.

### 6.4 Headless

Headless clients have no tray, so `pickup` has no trigger. Policy, fixed in one
place:

- Headless **receiving** is `eager` (today's behaviour) — no regression.
- Headless **sending** is still `lazy` if the peer supports it: sending needs
  no UI.
- A headless client that must receive lazily sets
  `receive_mode = "pickup"` plus `pickup_auto = true`, which fetches offers
  automatically as they arrive (the old eager behaviour with better
  bookkeeping) and is documented as such.

### 6.5 Offers entering the clipboard

`welcome.offers` and re-advertised offers must not clobber the user's local
clipboard. A received offer is written to the clipboard only if:

- its `file_id` has not been seen before, **and**
- its `ts` is newer than the `ts` of the last value this client applied.

Otherwise it stays in the tray's pending list.

## 7. Windows

As gated by spike 0.2:

- `OleInitialize` on `clipboardwire-ole`; `GetMessage` pump; owns the
  `IDataObject` for process lifetime.
- `FileGroupDescriptorW` from the offer list; names go through
  `sanitize_filename`, then truncate to `MAX_PATH` **UTF-16 code units while
  preserving the extension**.
- `FD_FILESIZE | FD_PROGRESSUI`.
- `IStream`: implement `Stat` and `Seek`; `S_OK` + `cb_read == 0` at EOF;
  hand-off via `crossbeam-channel`, never `block_on`.
- Stream placement decided by the spike, not assumed.
- Shutdown: `OleIsCurrentClipboard` → `OleSetClipboard(NULL)`. Never
  `OleFlushClipboard`, which force-renders every format and leaves dead
  descriptors.
- No `CF_HDROP` paste-time fallback: there is no local file to put in one.

## 8. macOS (only if spike 0.1 passes)

- `NSFilePromiseProvider` per file; override `operationQueue(for:)` so the
  delegate is not on the main queue.
- The earlier claim "`NSPasteboard` writes must happen on the main thread" is
  **not** supported: the shipped code writes from
  `clipboardwire-clipboard`
  ([clipboard.rs:349](core/src/client/clipboard.rs#L349) →
  [files_clipboard.rs:359](core/src/client/files_clipboard.rs#L359)). What
  actually requires a run loop is *serving data lazily*: the delegate must be
  called while an AppKit loop is alive. The requirement is therefore
  "AppKit loop present", detected at runtime, not "write on main thread".
- Headless → `eager` (§6.4), not `pickup`.

## 9. Progress

```rust
pub struct TransferProgress {
    pub file_id: Uuid,
    pub transfer_id: Option<Uuid>,
    pub direction: Direction,       // Send | Receive
    pub peer: Option<Uuid>,         // device_id
    pub name: String,               // redacted unless log_file_names
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub state: ProgressState,
}

pub enum ProgressState {
    Hashing, Offering, Pulling, Verifying, Done, Failed, Aborted,
}
```

Send side reaches `Pulling`/`Done`; it never reports `Verifying` (the receiver
verifies). The first draft's scenario asking the sender to show `verifying`
was wrong.

The `broadcast` sender is created by the caller and passed into
`run_with_status`, mirroring `status_tx` — the tray spawns the supervisor
itself ([tray.rs:490](cli/src/tray.rs#L490)), so an internally created channel
would have no receiver.

Tray: one aggregate tooltip line (`NOTIFYICONDATAW.szTip` is 128 chars and the
tooltip already carries the URL and peer count), plus a menu section listing
active transfers.

## 10. Configuration

```toml
# Both default to "eager" until Phase 8 flips them (see tasks.md 4.2 / 8.1),
# so no intermediate release ships half a feature.
send_mode    = "eager"       # "lazy" | "eager"
receive_mode = "eager"       # "auto" | "paste" | "pickup" | "eager"
pickup_auto  = false         # headless helper; see §6.4
offer_ttl_minutes = 30
pull_window_chunks = 3
pull_idle_timeout_secs = 30
log_file_names = false
```

- Phase 8.1 changes the shipped defaults to `send_mode = "lazy"` and
  `receive_mode = "auto"`.
- `auto` resolves by platform and spike result, not by guess:
  Windows → `paste` if spike 0.2 passed else `pickup`; macOS → `paste` if 0.1
  passed else `pickup`; Linux and headless → `eager`.
- `device_id` lives in the state dir, **not** here (§3.1).
- `ClientConfig` has `deny_unknown_fields`
  ([config.rs:41](core/src/client/config.rs#L41)). A config written by a v0.4
  client therefore fails to parse on an older binary **as soon as any v0.4
  setting is written — including automatically on first run**, not only after
  opening the settings dialog. The CHANGELOG must say so.

## 11. Security

- **`from` cannot be forged.** The hub resets it and stamps `from_device` from
  the connection's registered device ([ws.rs:241](core/src/server/ws.rs#L241)).
- **`to` cannot be used to read arbitrary paths.** A pull names a `file_id`;
  the source serves only paths in its own registry. A pull cannot name a path.
- **Peer identity is bound to the connection, but not to a person.** The hub
  stamps `from_device` from the connection's upgrade parameter, so a frame
  cannot claim a device it was not connected as. What this does **not**
  prevent: any holder of valid credentials can connect with someone else's
  `device_id`, evict them (newest-wins, §3.1) and then answer pulls as them.
  That is inside the existing threat model — credentials are full access — and
  is recorded here rather than described as closed. Mitigation is credential
  hygiene, not protocol.
- **Filenames are sanitised on every path**, promise path included, and
  `cFileName` is truncated in UTF-16 while keeping the extension — a backslash
  there can read as a relative subdirectory in Explorer.
- **New exposure surface.** `welcome.offers` plus `peer_changed` let a
  late-joining peer enumerate and pull anything copied within the TTL. Add it
  to the `PROTOCOL.md` §4 table.
- **File names are logged today.** `file.rs:89`, `file.rs:288`, `hub.rs:251`,
  `hub.rs:266` log names or paths at INFO/WARN. `log_file_names` (client) and
  `CLIPBOARDWIRE_LOG_FILE_NAMES` (hub) control redaction. The previous draft
  claimed names were already DEBUG-only; they are not.

## 12. Failure modes

| Failure | Detection | Result |
| --- | --- | --- |
| Source offline at paste | device absent past grace | `file_abort{source_gone}`; shell reports failure |
| Source quits mid-paste | grace expiry | same |
| Either side reconnects | same `device_id`, new connection | offers stay valid; an interrupted transfer restarts from index 0 under a new `transfer_id` |
| File changed mid-transfer | identity check per window | `file_abort{source_changed}` |
| Peer floods pulls | window cap, batch-queued limit | `file_abort{busy\|window_too_large}`, connection survives |
| Paste cancelled | stream released / dismissed | `file_abort{user_cancel}` |
| Offer expired before pull | TTL check at pull | `file_abort{expired}` |
| TTL expires mid-transfer | not applied — TTL gates pull start only | transfer continues |
| Re-copy of the same path | sender reuses or supersedes | old offer aborted with `superseded` |
| Hub restarted | offers are memory-only; clients cannot tell a hub restart from a connection loss | offers are lost, and a re-advertisement after reconnecting starts a fresh TTL — clients should treat the hub as authoritative rather than assume the old expiry survives |
| Mixed versions | `peer_changed` / missing `lazy-file` | tray warning; offers not delivered to that peer |
| Old hub, new client | no `caps` in `welcome` | sender stays eager |

## 13. Alternatives considered

**One `file_mode` switch.** Simpler config, but it couples sending and
receiving, which is exactly wrong for macOS. Rejected in favour of two
switches (§"Two switches" in the proposal).

**Automatic per-peer eager fallback.** Needs directional eager chunks, a
`file-recv` capability, and join/leave notifications — and still cannot
distinguish a v0.3 desktop from an Android client. Rejected for an explicit
`send_mode = "eager"` escape hatch.

**Session key `(file_id, peer)`.** Fewer fields, but forces the receiver to
merge two concurrent pastes of the same file into one engine and leaves
cancel-then-repaste ambiguous. `transfer_id` as the sole key is simpler to
reason about.

**Hub-buffered staging.** Removes the "source must stay online" constraint at
the cost of the hub storing user bytes, contradicting `PROTOCOL.md` §4.
Rejected.

**FUSE / FSKit.** The only mechanism FreeRDP found to work for RDP→Finder.
Needs a kernel driver or a signed File Provider extension, so it is deferred,
not dismissed — it is the named fallback if spike 0.1 fails *and* `pickup`
proves insufficient.

**Blocking `text/uri-list` download on X11.** File managers time out in
seconds and misreport. Not a promise equivalent.

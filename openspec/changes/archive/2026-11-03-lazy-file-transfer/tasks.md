# Tasks — lazy file transfer

Each phase is independently reviewable. Phase 0 decides whether later phases
exist; do not start Phase 6/7 before its gate clears.

## Outcome (archived 2026-11-03)

**Shipped.** All of Phases 1–5 plus the Windows paste path are live on
`fix/file-copy-shadow-clips` (commits `cdc50f0`…`2f5344a`), verified
Mac↔Windows on real hardware.

- **Phase 0**: 0.2 (Windows OLE spike) passed — `OleSetClipboard` +
  `IDataObject`/`IStream` rendered a 5 MiB promise byte-exact under real
  Explorer. 0.1 (macOS promise) was closed without a spike: Finder does not
  invoke a file-promise provider on Cmd+V, so macOS resolves `paste` to
  `pickup` — task 7.4's fallback is the shipped answer.
- **Phase 6**: implemented as `core/src/client/paste.rs` — a dedicated STA
  thread owns the promise object (not named `clipboardwire-ole`); `GetData`
  bridges to the tokio supervisor, pulls into `%TEMP%\clipboardwire-paste`,
  verifies the digest, then serves `IStream`. `IDataObjectAsyncCapability` is
  implemented so Explorer shows a real progress dialog. Items 6.4–6.6 are
  open follow-ups (no inactivity timeout on render, no `file_abort` on
  promise release — promise loss is natural clipboard takeover).
- **Phase 7**: closed by the 0.1 gate verdict — no AppKit work.
- **Phase 8**: 8.2–8.5 done in `PROTOCOL.md` (v0.4). 8.7 partially (this
  archive summary). 8.6 done in `ARCHITECTURE.md`. 8.8 verified against a
  real hub. 8.9/8.10 not exercised on v0.3 peers — accepted risk.

Original checklist below, preserved verbatim.


## Phase 0 — gates with written pass/fail criteria

- [ ] 0.1 **macOS promise spike.** Checklist (all must pass): Paste menu item
      in Finder; `writePromiseToURL:completionHandler:` fires; bytes intact;
      **Finder shows copying progress**; single file, multiple files, ~1 GB;
      cancel works and the app stays responsive; app quit while a promise is
      live does not hang the system; works unfocused and after a clipboard
      manager read. Record macOS versions tested. Delegate-fires-but-no-progress
      counts as **fail**.
- [ ] 0.2 **Windows `IStream` spike.** Checklist (all must pass): Explorer
      shows Paste + progress dialog; a 10 s blocked `Read` is tolerated;
      STA pump keeps running and other apps can still read the clipboard;
      multi-file paste ordering does not trip pull limits; **`E_ABORT` makes
      Explorer report failure and clean up its partial file (verify, do not
      assume)**; cross-process `Release` reaches us while `Read` is pending;
      one of the two stream placements works (MTA-marshalled, or STA +
      `CoWaitForMultipleHandles`) — pick one, delete the other from the design.
- [ ] 0.3 Record both outcomes in `design.md` §2 and delete the losing Tier 2
      line from `proposal.md`.
- [ ] 0.4 Regression fixture: confirm against a v0.3 hub binary that an
      unknown frame type currently closes the connection.

## Phase 1 — progress only (no protocol change, shippable alone)

- [x] 1.1 `TransferProgress` + `ProgressState` (including `Hashing`) in
      `core/src/client/mod.rs`.
- [x] 1.2 Emit from the eager path in `core/src/client/file.rs`: `Hashing`
      before the pre-pass, `Pulling` per chunk, `Verifying`, `Done`/`Failed`.
      Sender never emits `Verifying`; only the receiver does.
- [x] 1.3 Replace `info!("file sent")` with the `Done` event.
- [x] 1.4 `broadcast::Sender<TransferProgress>` is **created by the caller**
      and passed into `run_with_status`, mirroring `status_tx` (the tray spawns
      the supervisor itself, so an internal channel has no receiver).
- [x] 1.5 Tray: one aggregate tooltip line within the 128-char `szTip` limit,
      plus a menu section listing active transfers.
- [x] 1.6 Headless: one stderr line per second per transfer.
- [x] 1.7 `log_file_names` (client) + `CLIPBOARDWIRE_LOG_FILE_NAMES` (hub) and
      redaction at INFO+ on both sides.
- [x] 1.8 Tests: progress reaches a subscriber for a multi-chunk eager
      transfer; no events after `Done`; redaction hides names on both sides.

> **Phase 1 status: complete, code-reviewed, gates green.** 218 tests pass
> workspace-wide (3 consecutive runs), clippy clean, fmt clean. Additions the
> spec did not spell out but review required:
>
> - `TransferProgress.generation` so the tray can drop events from a retired
>   supervisor (`abort_running` bumps the generation).
> - `is_live_supervisor_event` — one predicate for all three supervisor-scoped
>   events; it tests generation **and** liveness, because the window between
>   `abort_running` and the replacement has no supervisor at all.
> - `next_transfer_deadline()` with `Hashing` scaled by file size (1 MiB/s
>   floor, clamped to 60 s..`TRANSFER_STALL_MAX`). A fixed 60 s window would
>   evict a *healthy* large-file hash, where the percentage is the progress.
> - `Failed`/`Aborted` get their own verbs and no percentage; the header counts
>   done and failed separately. Without this a failure rendered as
>   `sending <name> 0%` and vanished silently.
> - Redaction is a **salted** per-process hash (64-bit), so a log cannot be
>   dictionary-reversed; non-UTF-8 names stay distinct.
> - `core/src/env_lock.rs`: one crate-wide lock for tests mutating the process
>   environment. Per-module locks do not help — `set_var` rewrites the whole
>   environment block, so it races an `env::var` in another module too.

## Phase 2 — protocol types

- [x] 2.1 `FileOfferFrame`, `FilePullFrame`, `FileDigestFrame`,
      `FileAbortFrame`, `PeerChangedFrame`; add the `Frame` variants.
- [x] 2.2 `FileChunkFrame`: `file_sha256` → `Option<String>`; add `to`,
      `transfer_id`, `from_device`; all four
      `#[serde(default, skip_serializing_if = "Option::is_none")]`.
- [x] 2.3 Split validation on presence of `transfer_id`: eager requires
      `file_sha256` and forbids `to`/`transfer_id`; lazy requires
      `to` + `transfer_id` and forbids `file_sha256`.
- [x] 2.4 `file_abort.reason` is an open string set; unknown reasons map to
      `failed` with a DEBUG log. Known: `source_gone`, `source_changed`,
      `user_cancel`, `unknown_offer`, `expired`, `busy`, `window_too_large`,
      `invalid_start`,
      `superseded`.
- [x] 2.5 Caps: recognise only known lowercase tokens, matched exactly (no
      trimming); empty/malformed/unknown tokens ignored and cost no budget;
      **no token-count cap**; a `caps` value over 512 bytes is ignored
      wholesale as a transport guard. Surface exactly one parser to the hub.
- [x] 2.5a `FileOfferFrame::validate` caps `name` at 1024 bytes and
      `content_type` at 255 bytes, so one offer cannot bloat `welcome` past
      the frame limit for later joiners.
- [x] 2.5b `FileAbortFrame.to` is `Option<Uuid>`: session-scoped aborts
      (carrying `transfer_id`) must name `to`; offer-scoped ones may omit it
      and are expanded per recipient by the hub.
- [x] 2.5c Control-frame `validate()` returns a typed error (not a string) so
      the hub can map a rejection to the right `AbortReason` without matching
      on message text.
- [x] 2.5d `PeerChangedFrame` filters malformed caps tokens instead of
      rejecting the whole frame, so a future hub that widens the token grammar
      does not make older clients blind to peer join/leave.
- [x] 2.6 Add a `validate()` to each new frame, matching
      `FileChunkFrame::validate`'s style. Protocol-level checks only (no
      clock): `file_offer` — non-empty `name`, no NUL, `file_size > 0`;
      `file_pull` — `window ∈ 1..=32`; `file_digest` — non-empty
      `file_sha256`; `file_abort` — non-empty `reason`; `peer_changed` —
      known `event`. Time-dependent bounds (`expires_at_ms` vs now) stay in
      the hub.
- [x] 2.7 Tests: round-trip each frame; **one rejection test per invalid
      field on every frame**, not just chunks; **assert an eager chunk
      serialises byte-identically to the v0.3 fixture**; unknown abort reason
      is tolerated.
- [x] 2.8 Do **not** bump `PROTOCOL_VERSION` yet.

> **Phase 2 status: complete; two code-review rounds returned no blocker, a
> third could not run (reviewer session failed twice with `TARGET_FAILED`).**
> 218 tests pass workspace-wide; clippy clean; fmt clean.
>
> The byte-compat guard was verified independently, not taken on trust: with
> the diff stashed, the original v0.3 code serialises an eager chunk to exactly
> the bytes of `V03_EAGER_CHUNK_JSON`.
>
> Known gaps carried into later phases:
>
> - 2.7's fixture is built from a hand-written struct, so it never passes
>   through `send_file_through`. If production code ever fills `from_device` on
>   an eager chunk, the fixture still passes. Worth a real end-to-end variant
>   before Phase 8.
> - `Caps::parse` vs `parse_guarded` — the hub must call the guarded one;
>   Phase 3.2 makes this an explicit test.
> - Nothing committed yet, so review has only mtime to attribute changes. A
>   commit here would give Phase 3 a fixed baseline.

## Phase 3 — hub: identity, routing, caps, unknown types

- [x] 3.1 `ws.rs`: unknown frame type logs at DEBUG and continues — including
      the existing `Ok(_)` arm that new `Frame` variants will land in.
      Before/after regression test.
      → `classify_inbound(text) -> ReaderAction`; unknown `type` is `Ignore`
        (continue), unreadable `type` stays `Reject`; the reader's match now
        lists all nine variants explicitly (verified: adding a `Probe` variant
        fails to compile in **two** places, ws.rs and transport.rs).
- [x] 3.2 `ws.rs`: parse `?caps=` (**via the guarded entry point only**, so an
      oversized value is ignored rather than parsed — add a ws-level test for
      that) and `?device=`; store `device_id` per connection.
      → `parse_upgrade_params`; absent `device` mints a per-connection id
        (pre-0.4 clients keep working), malformed `device` is HTTP 400 before
        the handshake, so `4002` cannot apply there. A test asserts
        `Caps::parse` would still see the 513-byte value, pinning the guard.
- [x] 3.3 Duplicate `device_id`: newest wins, older closed with
      `4005 duplicate_session`. Test both orders.
      → `evict()` puts `Close{4005}` on the superseded connection's control
        channel with `try_send` (the hub is synchronous). The device slot moves
        to the newer connection *before* eviction, so a lost message cannot
        leave the device unowned. The old row stays in `clients` until its own
        deregister — asserted, because removing it early would make the later
        `Deregister` take the "unknown client" branch.
- [x] 3.4a `hub.rs`: `PeerEntry { device, caps, clip, file, ctl }`;
      `device_index`; a `ControlMsg::Close` channel the connection's writer
      consumes on the first `biased` branch (the hub is synchronous and must
      never await a peer, so it cannot close a socket itself). `Deregister`
      carries the device and clears the slot only when it still points at this
      connection — a superseded connection's late deregister must not erase
      the newer mapping.
- [x] 3.4b `hub.rs`: stamp `from_device` on relayed frames; never expose
      `client_id` in new frames.
      → `chunk.from_device = self.clients.get(&from).map(|p| p.device)` — an
        unconditional overwrite, not fill-if-empty. `from_device` is
        client-suppliable, so filling it only when absent would let any peer
        attribute a file to any device. An unregistered `from` (racing
        deregister) stamps `None` rather than echoing the claim. The reader
        clears it first, so the hub is the only writer; a single call site
        (`publish_file`) means no path bypasses that.
- [x] 3.5 `hub.rs`: offer registry keyed on absolute `expires_at_ms`, validated
      against `now..=now + 24 h` without overflow, plus the cap table
      (256/device, 4096 total, reject-don't-evict).
      → keyed on `(owner_device, file_id)`, not `file_id`: the latter is
        client-chosen and broadcast to every peer, so a bare key would let one
        peer overwrite another's offer. Expiry is three checks (past / beyond
        now+24h / `checked_add` returning `None`). Re-advertising takes the
        **smaller** expiry: a reconnect cannot renew an offer forever, but the
        source may shorten it. The clock is injectable under `cfg(test)`, so
        every TTL test is deterministic — no sleeps.
      → `checked_add`'s `None` branch is security-critical and initially had
        zero coverage: at a clock near `i64::MAX`, `saturating_add` would let
        `i64::MAX > limit` be false and **accept** an offer dated ~292 million
        years out. Verified by mutation: replacing the branch with
        `unreachable!()` fails the test, and replacing `checked_add` with
        `saturating_add` fails it too (`left: Accepted, right: BadExpiry`).
- [x] 3.6 `hub.rs`: pull authorization — the offer exists, is unexpired, and
      its owner == `to`. Do **not** require the puller to be the offer's
      owner: `to` names the source, so a legitimate pull always names another
      device.
      → `authorize_pull` has exactly three conditions and says so: validate,
        `(to, file_id)` exists and is live, source reachable — the last checked
        in **both** `device_index` and `clients`, because an index entry can
        outlive its row while a deregister is in flight. `from` is kept but
        deliberately unused for authorization; 3.7 binds chunks with it.
      → Two defects found by review rather than by the task text: (a) expired
        offers were only dropped on pull while both caps counted them, so a
        normally-operating source would fill its 256-offer budget with corpses
        after ~30 minutes and could advertise nothing — fixed with
        `sweep_expired_offers` on the cap-rejection path; (b) a dead entry was
        mistaken for a live re-advertisement, skipping the cap check and
        clamping the new deadline to the old past one, so re-offering a file
        silently produced something unpullable — fixed by filtering `existing`
        to live entries. Both fixes were proved by mutation (`left: 2000 /
        right: 9000` and `left: 256 / right: 1`).
- [x] 3.7a `hub.rs`: directed delivery. `file_chunk` with `to` goes to exactly
      one device; without `to` it still broadcasts (v0.3 compatibility).
      `file_digest` and `file_abort` route by `to`, and a broadcast abort is
      expanded into **one copy per recipient, each carrying that recipient's
      own `to`** so a receiver never has to infer whether an abort is for it.
      → `PeerEntry.frames: Sender<Frame>` (option a) rather than reusing `ctl`
        or widening `file`: `ctl` must stay a high-priority close channel, and
        `file`'s full-buffer semantics are tuned for bulk bytes, so mixing them
        would make "chunk lost" and "abort lost" inseparable.
      → Delivery always looks up `device_index`, never a cached client id, so a
        device that reconnects still receives its pull.
- [x] 3.7b `hub.rs`: bounded per-peer overflow queue
      (`PER_CLIENT_FILE_OVERFLOW = 8`) owned by the hub and drained by the ws
      writer, completions reported back through `ctl`. Hub handlers stay
      synchronous; never `await` a peer. Queue exhaustion → `file_abort{busy}`.
      → Two levels in the hub (`file` then `overflow`) with
        `QueueOutcome::{Queued, Full, Gone}`; `Gone` is distinct from `Full` so
        a dead connection is not reported as a slow one. Both full →
        `file_abort{busy}` to the **receiver** on the `frames` buffer (the two
        data levels are saturated by definition). An eager chunk has no session
        to abort, so it is dropped with a warning.
      → "The hub owns the queue" is implemented as a dedicated bounded channel
        whose *capacity is the limit*: only the hub decides what is pushed, and
        the writer only drains. The design's literal `VecDeque` cannot exist in
        a single-actor hub, since handing an element to another task requires a
        channel anyway.
      → **Ordering is enforced in the writer, not between the channels.**
        Draining `file` before an overflowed chunk is not just insufficient, it
        is backwards: one successful write frees a slot, so the hub can put a
        *newer* chunk back into `file`, which the drain rule would then emit
        before the older overflowed one. `ordered_ready` tracks
        `next_expected` per `(file_id, transfer_id)` and holds out-of-order
        chunks in a bounded `BTreeMap`; duplicates are dropped; the hold is
        capped and degrades by releasing the oldest. Eager chunks bypass it.
      → Proved by mutation: making `ordered_ready` a pass-through fails
        `left: [10] right: [2]` and the index-order test.
- [x] 3.8 `hub.rs`: deregistration grace period (60 s) with
      `device_index`-guarded deregister; `source_gone` only after grace or TTL.
      Re-advertisement does not extend expiry.
      → grace deadlines are absolute `i64` ms on the same clock as `now_ms()`,
        so tests drive them with `now_override` and never sleep. A ticker task
        outside the actor sends `HubMessage::Tick` every `DEVICE_GRACE_MS / 4`
        (15 s), which is the only way time-driven cleanup can happen while the
        hub is idle.
      → `device_index` is still cleared **immediately** while the offers
        survive the grace period. These are separate facts: "unreachable" is
        answered by `device_index` (so a pull returns `SourceGone` at once
        instead of vanishing into a dead channel), "still advertised" by
        `offers` (so a reconnecting source's files stay pullable).
      → A reconnect cancels the pending sweep and never touches
        `expires_at_ms`, which is what keeps a flapping source on its original
        TTL. Re-advertising *after* the deadline is a fresh offer, not an
        extension — asserted separately, since the two are easy to conflate.
- [x] 3.9 `hub.rs` + `ws.rs`: `welcome` gains `caps`, `peers`, `offers`;
      broadcast `peer_changed` on register and on grace expiry.
      → `WelcomeFrame` gains three **optional** fields (an older client must
        still parse a newer hub's welcome). Snapshotting sweeps first, so a
        late joiner never receives a deadline that already passed.
      → `PendingSweep` now also stores the device's `caps`: the `left` event
        fires after the client row is gone, and reporting an empty capability
        list would read as "this peer lost its capabilities" rather than "it
        left".
      → The five control frames became real routes. Rejecting an offer replies
        with an `error` and **continues** — spec: rejecting an offer must not
        close the sender. Only `Welcome`/`Error` remain in the reject arm, with
        no `_` fallback, so a future frame variant still fails to compile.
      → `pull_refusal_abort` is a pure function, which finally made the refusal
        frame testable without a socket. Two defects surfaced there: `to` named
        the **source** (`pull.to`) instead of the puller, so a receiver
        filtering on `to == me` — exactly what the spec tells it to expect —
        would discard the refusal and wait for a timeout; and `transfer_id` was
        `None` although the spec requires it echoed, on a claim that
        `FilePullFrame.transfer_id` is optional when it is a required `Uuid`.
      → A window-out-of-range pull answered `invalid_start` instead of
        `window_too_large`: `authorize_pull` flattened `ControlFrameError` to a
        `String`, discarding a mapping `abort_reason()` already had. `Invalid`
        now carries the typed error. `invalid_start` remains as the fallback
        for a future session-level index check.
- [x] 3.10 Bump `PROTOCOL_VERSION` to `clipboardwire/0.4.0`, only after 3.1.
      (The real protection is the caps gate — an already-deployed v0.5.4 hub
      cannot be changed.)
      → the accompanying test was renamed: `..._is_not_bumped_in_phase_two`
        became a false description once 0.4 shipped.
- [x] 3.11 Tests: routing reaches only `to`; unknown/expired offer rejected;
      absurd `expires_at_ms` rejected without killing the hub; total-cap
      rejection does
      not evict other devices; late deregister does not remove the new
      connection's offers; flapping device cannot extend TTL; late joiner sees
      live offers; slow peer does not stall others.
      → audited first: six of the eight were already covered, so only two were
        added. Both target a property rather than a part:
        `chunks_from_different_transfers_do_not_cross_route` (two concurrent
        sessions must not swap frames) and
        `a_slow_peer_does_not_stall_other_peers` (the promise the overflow and
        busy pieces exist to keep).
      → the two mutations are deliberately disjoint — broadcasting everything
        fails the cross-route test but not the isolation test; leaking a
        saturated peer's backlog to everyone fails the isolation test but not
        the cross-route one. Running only one would have made the other look
        redundant.

## Phase 4 — config, identity, transport wiring

- [x] 4.1 `core/src/identity.rs`: `device_id` + host fingerprint in the state
      dir, generated on first run, **never** in `config.toml`; regenerated with
      a warning if the fingerprint mismatches (cloned VM / restored backup).
      → two plain-text lines (id, fingerprint), written atomically via a temp
        file in the same directory. A trailing third line is ignored rather
        than rejected, so a future format can add fields without making this
        build churn identities.
      → the fingerprint is `os`, `arch`, `machine-id` (file only, never
        shelling out), `COMPUTERNAME`, `hostname` — **no time, pid or salt**,
        because an unstable fingerprint would mint a new id on every launch,
        which is worse than having none. Missing components are skipped rather
        than substituted with an empty string, so "no machine-id" cannot hash
        the same as "machine-id equals the next label".
      → a fingerprint mismatch **overwrites** the stored id: keeping it would
        preserve exactly the two-machines-evicting-each-other failure the
        fingerprint exists to break.
      → a corrupt or unreadable file warns and continues with an ephemeral id;
        losing an identity only makes offers look new, while failing startup
        makes the client unusable.
- [x] 4.2 `ClientConfig`: `send_mode` (default `eager` in Phases 4–7, flipped
      to `lazy` in 8.1 so no phase ships half a feature), `receive_mode`
      (default `eager` until 8.1, then `auto`), `pickup_auto`,
      `offer_ttl_minutes`, `pull_window_chunks`, `pull_idle_timeout_secs`,
      `log_file_names`.
      → every new field carries `skip_serializing_if` for its default. That is
        a **compatibility** measure, not tidiness: `ClientConfig` has
        `deny_unknown_fields`, so writing `send_mode = "eager"` into the file
        would make an older binary unable to read it at all. Verified by
        mutation — removing the skip makes
        `default_values_are_not_serialised` fail with
        `send_mode leaked into: ...`.
- [x] 4.3 Validation: unsupported explicit modes fail with an actionable
      message.
      → only **explicit** modes fail; `auto` always resolves to something
        workable so a default config never refuses to start. The message names
        the alternatives (`pickup`, `eager`) and the platform, and a test
        asserts all three substrings.
      → `validate_modes` is called from `ClientConfig::validate()`, so every
        load path gets it. A dedicated test deletes that call and checks it
        fails, because unit tests on the function alone would not notice the
        wiring being missing.
- [x] 4.4 `transport.rs`: send `?caps=` and `?device=` on upgrade; parse
      `caps`/`peers`/`offers` from `welcome`; add the control channel for
      pulls/digests/aborts/peer_changed.
      → `upgrade_url` inserts the query before any fragment and uses `&` when
        the URL already has one. Six similar channels became a `Links` struct
        instead of `#[allow(clippy::too_many_arguments)]`: six positional
        `Sender`/`Receiver` arguments of four similar types can be swapped
        without a compile error.
      → `client_caps` still returns nothing, with a Phase 5 TODO and a test
        pinning it — advertising `lazy-file` before the pull path exists would
        make a v0.4 hub send offers this build can only drop.
      → **This shipped a flaky regression and a single green run hid it.**
        Adding a third outbound channel to the `select!` meant three arms
        observe a closed channel at once, and only the clip arm drained what
        the others still held. `core/tests/file_transfer.rs` then timed out
        roughly once in four full runs. Fixed with one `drain_and_finish`
        used by every arm, ordered by consequence: chunks (loss corrupts a
        file), then control (a lost `file_pull` stalls to the idle timeout),
        then clips (loss is recoverable on the next poll).
      → The stability test needed six queued clips, not one: with a single
        clip the files-arm mutation still passed, because that clip was
        already on the wire before the drop. Finding that required trying the
        mutation, not reading the test.
- [x] 4.5 `send` subcommand uses an ephemeral device id.
      → `send` mints a **fresh** device id per process. Not a shortcut: two
        live connections sharing one id evict each other under newest-wins, so
        a one-shot send would kick the user's own tray offline mid-copy.
      → `send_spawn_options()` takes no config or state dir, so it *cannot*
        read the persisted identity; the test asserts two calls differ and are
        non-nil, which is all that structure permits. Giving it a `&ClientConfig`
        later should read as a warning sign, not a refactor.

- [x] 4.6 CHANGELOG: `deny_unknown_fields` means a v0.4 config is unreadable by
      an older binary **as soon as any v0.4 setting is written, including on
      first run** — not only after opening the settings dialog.
      → CHANGELOG states the hazard plainly and, importantly, does **not**
        claim lazy transfers work — both modes still default to eager and the
        client advertises no capabilities until the receive path exists.

- [x] 4.7 Settings GUI: `send_mode` + `receive_mode`; the writer must not drop
      unrelated keys (today it rewrites from form fields only).

      → `log_file_names` was missing from both `FormState` and
        `WritableConfig`, so every save silently reset it — pre-existing data
        loss on an already-shipped setting, fixed here.
      → default-valued keys are omitted from the written file, matching
        `ClientConfig`, so saving an untouched config keeps it readable by
        v0.5.4.
      → `hub` stays last in `WritableConfig` because TOML serialises struct
        fields in order and a `[hub]` table would otherwise absorb every key
        written after it.

## Phase 5 — pull path and `pickup` (independently verifiable)

- [x] 5.1 `core/src/client/offer.rs`: registry with pinned identity
      (Windows `volume_serial`/`file_index`/`mtime`; macOS `inode`/`mtime_ns`).
      → one Unix path covers macOS (`dev`/`ino`/`mtime_ns`); Windows uses
        `volume_serial_number`/`file_index`, degrading to size+mtime where the
        filesystem cannot supply them (some network shares).
      → re-advertising an unchanged path reuses its `file_id` and **keeps the
        original `ts`** while refreshing the deadline. `ts` is what a receiver
        consults before putting an offer on its clipboard, and only a genuinely
        new `file_id` is meant to count as new — bumping `ts` on a reused id
        would make the two rules disagree about what "new" means.
- [x] 5.2 `ClipChange::Files` publishes offers when `send_mode = "lazy"`;
      zero-byte rejected; directories skipped.
      → `publish_offers` sends its own frames so the test can assert the control
        channel receives offers while the files channel stays empty.
      → the mutation that made lazy mode also send chunks eagerly was caught by
        **only** the supervisor-level test; the direct `publish_offers` test
        passes under it because it never goes through the branch. Worth the
        extra test.
- [x] 5.3 Supersession: reuse `file_id` for an unchanged path, else publish new
      and abort the old with `superseded`.
      → the abort is emitted **before** the new offer in the same batch, so a
        receiver never sees two live offers for one path.
- [x] 5.4 Pull server, one task per `transfer_id`; enforce window caps.
      → one task per `transfer_id`; a session-opening pull with a non-zero
        index is refused with `invalid_start` **before any disk read**, so a
        hostile pull cannot force I/O.
      → `MAX_ACTIVE_PULLS = 4`, but pulls sharing a `batch_id` queue rather
        than being refused: Explorer's multi-file paste opens several streams
        and busy-ing the third would break a routine five-file paste.

- [x] 5.5 Identity re-check before every window and before `file_digest`.
      → identity is re-checked before **every** window, not once at offer
        time: a file edited mid-pull yields a stream whose halves disagree,
        and the running hash covers exactly that torn stream, so the receiver's
        verification would *pass* on corrupt output.

- [x] 5.6 Sequential hash keyed on `transfer_id`, dedupe by index; reject a
      session-opening pull whose `chunk_index != 0` with `invalid_start`, but
      allow later windows of the same session to start anywhere.
      → sequential hashing keyed on `transfer_id`, deduped with
        `highest_hashed`. Without the dedup a retransmitted chunk is hashed
        twice and the digest can never match, so the receiver restarts
        forever.

- [x] 5.7 `MAX_ACTIVE_PULLS = 4` across batches; pulls sharing a `batch_id`
      queue locally instead of taking a `busy`.
      → concurrency cap with same-batch queueing; a queued stream counts as
        making progress so the idle timer cannot fire while it waits.

- [x] 5.8 Idle timeout releases the handle.
      → sessions are swept after `pull_idle_timeout_secs` of silence.

- [x] 5.9 Receiver pull engine: windowed requests from index 0, index-based gap
      detection, restart-from-zero with a new `transfer_id` on any abort,
      3 restarts per file, tail stall timer, `DIGEST_WAIT`, digest-wait counts
      as progress.
      → `receive.rs`: index-based gap detection (chunks from one source arrive
        FIFO, so a higher index landing *is* the gap signal), `MAX_RESTARTS = 3`,
        and two timers that cannot race: `DIGEST_WAIT` (10 s) is strictly below
        `TAIL_STALL` (15 s), and awaiting a digest counts as progress.
      → two tests were initially self-referential — they drove their loops and
        probe times from the very constants under test, so raising
        `MAX_RESTARTS` or `DIGEST_WAIT` changed the test alongside the code and
        both stayed green. Only mutation exposed it; both now use literals plus
        an explicit assertion on the constant's value.

- [x] 5.10 Receiver binds to `(transfer_id, from_device == offer owner)`.
      → a session binds to `(transfer_id, from_device == offer owner)`; a chunk
        from any other device is ignored rather than treated as an error,
        because a concurrent pull by a third party is not this session's
        problem.

- [x] 5.11 `pickup` mode + `pickup_auto`; batch application with the 500 ms
      window and `batch_id` grouping.
      → engine plus landing path only; the tray trigger for `pickup` is
        deferred. `paste` is deliberately not half-implemented: it needs a
        platform clipboard hook, and a stub would look like it worked on
        platforms that lack one.

- [x] 5.12 Offer-to-clipboard guard: only if `file_id` is new and `ts` is
      newer than the last applied value.
      → applied only when the `file_id` is new and its `ts` is newer than the
        last applied value.

- [x] 5.13 `file_abort{user_cancel}` on dismissal.
      → `file_abort{user_cancel}` when a stream is released early or a pickup
        is dismissed.

- [x] 5.14 Tests: offer created, no chunks sent; multi-chunk pull byte-exact;
      concurrent pulls independent; cross-batch limit; same-batch queueing;
      identity change mid-transfer aborts; dropped chunk recovered by a
      restart-from-zero; slow chunk is not treated as a gap; `invalid_start`
      rejected; TTL gates start but not an in-flight transfer;
      re-advertisement does not clobber the clipboard.

      → audited first: eight of the eleven were already covered, so only four
        tests were added (three integration + one that also covers the tray).
      → `lazy_transfer.rs` round-trips `PullServer` into `ReceiveEngine`
        directly, without a socket. Every prior test was segmented — offers,
        pull serving, and chunk assembly each verified alone — and nothing had
        proved the two ends fit together. Three separate defects this session
        were of exactly that shape.
      → **TTL was enforced by the hub on every pull frame**, not just the
        first, which contradicts the spec's "expiry SHALL gate only the start"
        and would have killed any transfer outliving the 30-minute default —
        the large-file case the feature exists for. The hub now remembers
        started transfers (`active_transfers`, keyed by `transfer_id`) and also
        protects their offers from the expiry sweep, so a live transfer is
        never left reading a deleted entry.
      → the tray keyed transfers by `file_id` alone; under lazy, the same file
        pasted twice is two `transfer_id`s, so the second row overwrote the
        first and the first `Done` cleared both. Now keyed on
        `(file_id, transfer_id)`.


## Phase 6 — Windows paste path (only if 0.2 passed)

- [ ] 6.1 `clipboardwire-ole` thread: `OleInitialize`, `GetMessage` pump, owns
      the `IDataObject` for process lifetime.
- [ ] 6.2 `FileGroupDescriptorW` from the offer list; `sanitize_filename` then
      truncate to `MAX_PATH` in UTF-16 preserving the extension;
      `FD_FILESIZE | FD_PROGRESSUI`.
- [ ] 6.3 `IStream`: `Stat`, `Seek`, `S_OK` + `cb_read == 0` at EOF,
      `crossbeam-channel` hand-off, placement chosen by the spike.
- [ ] 6.4 Inactivity timeout counts digest waiting as progress; withhold the
      final bytes and EOF until the digest verifies.
- [ ] 6.5 `OleSetClipboard` sequencing including re-set on clipboard change;
      shutdown via `OleIsCurrentClipboard` → `OleSetClipboard(NULL)`, never
      `OleFlushClipboard`.
- [ ] 6.6 Send `file_abort{user_cancel}` on cross-process `Release` / early
      stream teardown.
- [ ] 6.7 No `CF_HDROP` paste-time fallback.
- [ ] 6.8 `windows` crate dependency; update the Impact table.
- [ ] 6.9 `MANUAL_SMOKE.md`: paste from an offer, verify progress and bytes;
      cancel mid-paste and verify the source stops.

## Phase 7 — macOS paste path (only if 0.1 passed)

- [ ] 7.1 `NSFilePromiseProvider` + delegate; override `operationQueue(for:)`
      so the callback is not on the main queue.
- [ ] 7.2 Require a live AppKit loop at runtime; headless falls back to
      `eager` (not `pickup`, which needs a tray).
- [ ] 7.3 Delegate writes under a temp name in the destination, verifies, then
      renames.
- [ ] 7.4 If 0.1 failed: close the change with `pickup` as the macOS answer;
      record the decision and the FUSE/FSKit follow-up in `design.md` §13.
- [ ] 7.5 Manual smoke both directions: Windows copy → macOS paste, macOS copy
      → Windows paste.

## Phase 8 — defaults, docs, integration

- [ ] 8.1 Resolve `auto` from spike outcomes (this is code, not a guess).
- [ ] 8.2 `PROTOCOL.md`: it has **no `file_chunk` section at all** and its
      header still says v0.2. Write the file-transfer section for both eager
      and lazy shapes.
- [ ] 8.3 `PROTOCOL.md` §3: unknown frame types must be ignored (matching the
      new hub behaviour).
- [ ] 8.4 `PROTOCOL.md` §4: digest covers the served byte stream; promise
      extends the exposure window to the TTL; late joiners can enumerate and
      pull anything copied within the TTL.
- [ ] 8.5 `PROTOCOL.md` §7: replace "refuse a mismatched version" with
      capability downgrade.
- [ ] 8.6 `ARCHITECTURE.md`: OLE thread and stream placement, macOS AppKit
      loop requirement, device identity, ws-writer overflow queue.
- [ ] 8.7 CHANGELOG.
- [ ] 8.8 Integration: v0.4 lazy sender → v0.4 receiver pull over a real hub,
      byte-exact.
- [ ] 8.9 Regression: eager mode and `send` unchanged, using the 2.6 fixture.
- [ ] 8.10 Compatibility matrix: v0.4 client + v0.3 hub; v0.4 sender + v0.3
      peer; v0.4 client + Android peer.

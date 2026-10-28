# Code review record — lazy-file-transfer Phase 2

Phase 2 = protocol types (tasks.md 2.1–2.8). Reviewer role: Lody Reviewer
(`c2a6446a-cc75-4742-b6c8-3f329687b427`), session `7bc64b4e-7a11-4f9b-8c6e-79de0af7a05b`.

## Round 1 — verdict: no Blocker, mergeable, M1 to fix first

Findings and disposition:

| Finding | Disposition |
| --- | --- |
| **M1** — the five new frames had no `validate()`; `window: 0`, empty `name`, empty digest, empty `reason` all parsed clean | Fixed: `validate()` on all five, protocol-level checks only, no clock access |
| **M2** — the abort `transfer_id` rule I had just written conflicted with 5 other clauses (session-less aborts would be dropped; in-flight transfers would wait 15–30 s instead of failing) | Spec fixed: scope is decided by presence of `transfer_id`; offer-scoped aborts also end in-flight sessions |
| Minor 1 — client-supplied `from_device` passes validation and is forwarded verbatim | Deferred to Phase 3 (hub must clear + stamp), added as acceptance criterion |
| Minor 2 — hub accepts and broadcasts lazy chunks (previously they failed parse) | Deferred to Phase 3 |
| Minor 3 — the byte-compat fixture uses a hand-built struct, not `send_file_through` | Open |
| Minor 4 — `Caps::parse` truncation rule was ambiguous and could evict known tokens | Fixed twice, see round 2 |
| Minor 5 — `AbortReason::Failed` was serialisable as `"failed"` despite a "not a wire value" comment | Fixed: `to_wire() -> Option<&'static str>` |
| Minor 6 — `logging.rs` uses unsalted SHA-256 prefix (dictionary-reversible) | Open, Phase 1 |

## Round 2 — verdict: no Blocker, mergeable; M1 + Minors before Phase 3

| Finding | Disposition |
| --- | --- |
| **M1** — `FileAbortFrame.to` was still required `Uuid`, so a broadcast abort could not be expressed and would get the sender disconnected | Fixed: `to: Option<Uuid>`; session-scoped aborts must name `to`, offer-scoped may omit |
| Minor 1 — `Caps::parse` and `parse_guarded` both public, hub likely to call the unguarded one | Tasks 3.2 now mandates the guarded entry + a ws-level test |
| Minor 2 — `trim()` disagreed with the grammar; `Caps::parse` was lenient while `PeerChangedFrame` was strict | Fixed: no trimming; grammar is exact |
| Minor 3 — `MAX_TOKENS = 16` is unreachable, and would contradict the "set depends only on known tokens" invariant once the known set exceeded it | Fixed: cap deleted; `u8` width is the real bound, asserted at compile time |
| Minor 4 — `PeerChangedFrame` rejected the whole frame over one malformed cap token | Fixed: filters instead, so a future hub that widens the grammar does not blind older clients |
| Minor 5 — the known-token table existed twice (`token_for` and `tokens()`) and could drift | Fixed: `Caps::KNOWN` single table + round-trip test over all 8 combinations |
| Minor 7 — `validate()` returned `&'static str`, so the hub had to match message text to pick an abort reason | Fixed: typed `ControlFrameError` + `abort_reason()` |
| Minor 8 — offer `name`/`content_type` unbounded, so one offer could bloat `welcome` past the frame limit | Fixed: 1024 / 255 byte caps |

## Round 3 — did not run

Two dispatch attempts both returned `TARGET_FAILED` (non-retryable) within 8–18 s.
The session reported idle with the machine online, and the prompt was in fact
delivered, so this is a session-side runtime fault — plausibly context
exhaustion from the two prior full-diff reviews — not a problem with the diff.

Round 3's three questions were therefore verified locally instead:

1. **Blockers** — none found. `cargo fmt --all -- --check`, `cargo clippy
   --workspace --all-targets -- -D warnings`, and `cargo test --workspace`
   (172 passed / 0 failed / 5 ignored, core lib 138) all pass; the 5 ignored
   are pre-existing platform gates and the diff adds no `#[ignore]`.
2. **Judgment A** ( `abort_reason()` returns `Option<AbortReason>` ) — holds.
   `AbortReason::Failed.to_wire()` is `None`, so returning `Failed` would leave
   the hub with no legal value to serialise.
3. **Judgment B** ( `PeerEvent::is_known()` is a tautology ) — holds, empirically:
   an unknown event was rejected at deserialisation with
   `unknown variant 'reconnected', expected 'joined' or 'left'`.
   The redundant check is harmless.
4. **New contradictions** — none. `ControlFrameError` is not yet consumed by
   `ws.rs` (Phase 3 wires it, as planned). `known_caps()` and `validate()` do
   not overlap: one parses caps, the other checks the event. All six
   `FileAbortFrame` construction sites respect the new `to` rule.

**Status: Phase 2 is complete and verified locally, but its final review round
is unconfirmed by the reviewer.**

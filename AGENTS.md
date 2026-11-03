# Agent rules

## Delegating to another Lody session

`lody_session_chat` / `lody_session_create` are **turn-boundary operations**, not
blocking calls. The result is delivered as a new inbound turn only after the
current turn has been released. Measured behaviour: a delegation that finished at
02:44 was delivered at 03:21:31 — four seconds after the caller's turn finally
ended, not at completion time.

So for every delegation:

1. **End the turn immediately after dispatching.** Once the dispatch tool returns
   an `operationId`, the only permitted follow-up work is writing the state file
   below. Then emit a one-line status and stop the turn. Do not keep doing
   unrelated work in the same turn.

2. **Never wait for a result inside the same turn.**
   - Do not poll `lody_operation_get`, `lody_operation_list`, or
     `lody_operation_batch_get`. They are for reading one snapshot at the start
     of a turn, never for waiting in a loop.
   - Do not use `sleep`, and do not use the CLI `wait` tool to await an MCP
     operation. `wait` only awaits local background tasks; an MCP operation is
     not a wake source, so the wait always runs to its full timeout and then
     tells you nothing.
   - Do not read operation rows straight out of
     `~/.lody/orchestration/*/operations.sqlite3`. That bypasses delivery and
     produces results the harness does not know were consumed.

   This rule was broken in the session that produced the v0.4 test builds: after
   dispatching the Windows-compile fix the lead called `wait` five times in a
   row, the last few after the child had already finished, and the user had to
   interrupt. Each `wait` burned a full timeout and returned nothing. The
   dispatch call itself is the *last* action of the turn; anything after it
   except the state-file write is a mistake.
   - Corollary: if a dispatch call fails (`INTERNAL_ERROR`, `fetch failed`), do
     not fall back to waiting or polling. Retry the dispatch, or stop and report.
     A failed dispatch is not a pending one.

3. **Persist loop state outside the conversation** so the next inbound turn can
   resume. Use `openspec/changes/<change-id>/.delegation.json`:

   ```json
   {
     "round": 2,
     "total_rounds": 3,
     "pending_operation": "cw-openspec-review-r2",
     "last_result": "openspec/changes/<change-id>/review-r1.md",
     "next_action": "revise-then-dispatch"
   }
   ```

   Write it before ending the turn; read it at the start of the inbound turn.
   Keep the child's output in the repo too (e.g. `review-rN.md`) so the record
   does not exist only in one delivery payload.

4. **The inbound turn drives the follow-up work.** When the result arrives, that
   turn consumes it, decides what changes are needed, dispatches whoever should
   make them (see "Who implements" below), updates the state file, and ends
   again. One round therefore costs at least two turns. That is the intended
   shape, not an inefficiency.
   - "Performs the revision" means **commissioning** it, not hand-editing. If a
     builder role is in play, the lead dispatches the revision to the builder.

5. **Honour the round cap.** When `round >= total_rounds`, stop delegating and
   report the outcome, including "still blocked". Do not silently start another
   round to finish the work you wanted to do.

## Who implements

Once the user has designated a builder Agent Role for a task, **only that
session writes source code**. The lead session reads, verifies, reviews
outcomes, and dispatches. This is not ceremony: a lead who patches "just this
one small thing" breaks a test the builder would not have broken, because the
builder holds the whole change in its head and the lead only sees the hunk.

**Lead may do (verification):**
- read files, `rg`/`grep`, `git diff`, `git status`
- run `cargo build` / `test` / `clippy` / `fmt`
- edit the spec and process docs under `openspec/` and this file
- hand-edit only when the user explicitly says "you do it"

**Lead must not do once a builder role exists:**
- edit `core/src/**`, `cli/src/**`, `core/tests/**`, `cli/tests/**`
- "quick fixes" while waiting on a build or a review

**Self-check before every edit.** If the path lands in either list below and
the user did not tell you to edit it, stop and dispatch instead:
`core/src/`, `cli/src/`, `core/tests/`, `cli/tests/`.

"Too small to be worth a round trip" is the exact moment the boundary erodes.
Send it anyway — with the failing assertion, the test name, and what you
already verified, so the builder starts from evidence rather than from your
guess.

Corollary: when a review addresses *you* by name ("your change is correct"),
that reflects who wrote the code last, not who should write it next.

## When a delegated result looks wrong

Verify before reporting, and never take the child's word for gate results:
re-run `fmt`, `clippy`, and `test` yourself, and stress the suite a few times —
a builder once reported "all green" while a nanosecond-suffixed temp path made
one test fail roughly one run in eight.

If a test is red, hand it to the builder with the exact `left:`/`right:`
assertion output. Do not "just" fix it.

## When the loop genuinely must stay in one turn

If a workflow cannot span turns, do not reach for the Lody MCP — use the CLI
`agent` tool instead. Its result arrives as a `<notification>` inside the running
turn, so no turn release is needed. The trade-off is that it runs locally rather
than as a separate Lody session with its own role and config.

## Dispatch permission mode

The user asked that delegated sessions on XS-Office run without interactive
permission prompts. `lody_session_chat` cannot change a session's permission
mode — mode is fixed at creation. Use `lody_session_create` with
`machineId` + `agentConfigId` (from `lody_session_create_options`) and
`modeId: "bypass"` for diagnostic sessions there.

# Project context

## What this is

`clipboardwire` mirrors text, images and files between trusted devices. A dumb
relay ("hub") fans frames out over WebSocket; each desktop runs a client that
polls the OS clipboard and applies inbound values.

- `core/` — library: wire protocol, hub actor, client transport, clipboard adapter.
- `cli/` — thin binary: clap subcommands (`serve`, `host`, `connect`,
  `settings`, `send`), system tray, settings GUI.
- `PROTOCOL.md` — the only contract between the two halves; language-agnostic.
- `ARCHITECTURE.md` — how the Rust implementation is laid out.

## Active deployment targets

Windows and macOS are the primary platforms. Linux (X11/Wayland) is supported
but is explicitly out of scope for the lazy file-transfer work described in
`changes/lazy-file-transfer/`.

## Conventions

- Rust 2021, MSRV 1.89, no nightly.
- Wire version lives in `core/src/protocol.rs` (`PROTOCOL_VERSION`).
- Every non-trivial change lands with a test; tests are the contract.
- `cargo fmt --all -- --check` and
  `cargo clippy --workspace --all-targets -- -D warnings` must pass.
- Security posture (`PROTOCOL.md` §4): transport-only confidentiality, the hub
  is inside the trust boundary, no application-layer crypto.

## OpenSpec layout

```
openspec/
  project.md
  specs/<capability>/spec.md          # shipped truth
  changes/<change-id>/                # one in-flight change
    proposal.md
    design.md
    tasks.md
    specs/<capability>/spec.md        # ADDED / MODIFIED / REMOVED deltas
```

`specs/` holds shipped behaviour only. Everything under `changes/` is proposed
until archived.

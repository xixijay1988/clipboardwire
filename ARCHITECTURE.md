# clipboardwire — implementation architecture

Companion to `PROTOCOL.md`. This document covers the **Rust implementation** of
the server and the desktop client: how the code is laid out, which crates are
used and why, and the deployment shape. The protocol itself is language-agnostic
and lives in `PROTOCOL.md`.

## 1. Cargo workspace

A single workspace with one library crate and one binary crate:

```
clipboardwire/
├── Cargo.toml                  # [workspace] members = ["core", "cli"]
├── core/
│   ├── Cargo.toml              # name = "clipboardwire-core" (library)
│   └── src/
├── cli/
│   ├── Cargo.toml              # name = "clipboardwire" (binary)
│   └── src/
└── packaging/                  # cargo-deb metadata, rpm spec, PKGBUILD, systemd unit
```

- **`clipboardwire-core`** owns *all* protocol and runtime code: frame types,
  the hub task, the WebSocket server endpoint, the clipboard poll loop, the
  client supervisor. No `main`.
- **`clipboardwire`** is a thin binary that uses `clap` to dispatch one of three
  subcommands into the library:
  - `serve` → start hub only (env-var config).
  - `host` → start hub *and* a local client connecting to it on loopback (env
    vars for the hub + config file for the client).
  - `connect` (default) → start client only (config file).

The `host` mode deliberately uses real WebSocket-on-loopback rather than an
in-memory shortcut. It keeps every code path the same as a multi-machine
deployment, costing only a few μs of localhost overhead per frame.

Rust edition: **2021**. MSRV: latest stable; no nightly features. Workspace
pins for shared deps (`tokio`, `serde`, `serde_json`, `tracing`, `uuid`, `rand`).

## 2. Hub server (`clipboardwire-core::server`)

The server-side code lives in `clipboardwire-core` and is invoked by the CLI's
`serve` subcommand (or as one half of `host`).

### 2.1 Module layout

```
core/src/
├── lib.rs           # re-exports
├── protocol.rs      # ClipFrame, WelcomeFrame, ErrorFrame, ClientId, version const
├── server/
│   ├── mod.rs       # `pub async fn run(config: ServerConfig) -> Result<()>`
│   ├── config.rs    # ServerConfig struct + env parser
│   ├── auth.rs      # parse Authorization header, constant-time compare
│   ├── hub.rs       # central fan-out hub + last_clip cache
│   ├── ws.rs        # /sync WebSocket handler
│   └── error.rs     # AppError, IntoResponse impl
└── client/          # see §3
```

### 2.2 Dependencies (rationale)

| Crate                 | Purpose                                    | Why this one                                                                                       |
| --------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `axum` 0.7            | HTTP routing + WebSocket upgrade           | Best-supported Rust HTTP framework; built-in WS extractor, plays well with tokio.                  |
| `tokio` (full)        | Async runtime                              | The only realistic choice for an axum + tungstenite stack.                                         |
| `tokio-tungstenite`   | Pulled in transitively by axum WS         | Not used directly.                                                                                 |
| `serde` / `serde_json`| Frame (de)serialization                    | Standard.                                                                                          |
| `tracing` + `tracing-subscriber` | Logging                          | Async-aware structured logging.                                                                    |
| `uuid`                | Generate per-connection client ids         | v4.                                                                                                |
| `subtle`              | Constant-time comparison for Basic auth    | Resists timing oracles.                                                                            |
| `tower-http`          | Request limits, optional access log layer  | Plugs into axum.                                                                                   |
| `anyhow` / `thiserror`| Error plumbing                             | `thiserror` for typed library errors, `anyhow` only at the top of `main`.                          |

TLS comes from `axum-server` with the `tls-rustls` feature, plus `rustls`
and `rustls-pemfile` for cert loading. **Not** used: any database crate, any
session/cookie crate, any templating engine, any application-layer
cryptography crate (no E2EE). The server has zero persistent state.

### 2.3 Concurrency model

One **hub task** owns the canonical state:

```rust
struct Hub {
    clients: HashMap<ClientId, mpsc::Sender<ClipFrame>>,
    last_clip: Option<ClipFrame>,
}
```

The hub is driven by a single inbound `mpsc::Receiver<HubMessage>` where
`HubMessage` is one of `{ Register, Deregister, Publish }`. Each WebSocket
connection task:

1. Performs the auth + upgrade in `ws.rs`.
2. Sends `Register { client_id, tx }` to the hub and awaits the snapshot
   `last_clip` so it can emit the `welcome` frame.
3. Spawns a *reader* task that pulls WS frames and forwards parsed `clip`s as
   `Publish { from, frame }` to the hub.
4. Spawns a *writer* task that pulls from its mpsc and writes to the WS sink.
5. On disconnect, sends `Deregister { client_id }`.

Single-owner design keeps `last_clip` consistent without locks. The hub never
blocks: each per-client mpsc has a small bounded buffer (e.g. 32); if a slow
peer fills it, the hub drops the frame for that one peer (preferring liveness
over delivery — clipboard is last-write-wins anyway).

### 2.4 Configuration

All config from environment variables, no config file:

| Variable                    | Default                          | Notes                                  |
| --------------------------- | -------------------------------- | -------------------------------------- |
| `CLIPBOARDWIRE_BIND`        | `0.0.0.0:8484`                   | `host:port` to listen on               |
| `CLIPBOARDWIRE_USER`        | *required*                       | Basic-auth username                    |
| `CLIPBOARDWIRE_PASSWORD`    | *required\**                     | Basic-auth password.                    |
| `CLIPBOARDWIRE_PASSWORD_FILE` | unset                          | Path to a file containing the password (trailing newline trimmed). Mutually exclusive with `CLIPBOARDWIRE_PASSWORD`; one of the two must be set. Docker-secret friendly. |
| `CLIPBOARDWIRE_MAX_CONNS`   | `64`                             | Cap (see `PROTOCOL.md` §5)             |
| `CLIPBOARDWIRE_MAX_FRAME`   | `10485760`                       | Bytes                                  |
| `CLIPBOARDWIRE_TLS_CERT_FILE` | unset                          | PEM cert file. When set together with `CLIPBOARDWIRE_TLS_KEY_FILE`, the server speaks `wss://`. |
| `CLIPBOARDWIRE_TLS_KEY_FILE`  | unset                          | PEM private key file. Required iff `CLIPBOARDWIRE_TLS_CERT_FILE` is set. |
| `RUST_LOG`                  | `clipboardwire_server=info`      | `tracing-subscriber` env filter        |

Refusing to start without `USER`/`PASSWORD` is intentional — no "default
admin/admin".

### 2.5 Deployment

- **Static binary.** Build with `--release`. Final size: ~7 MiB stripped on
  Linux, ~7 MiB on Windows (rustls + arboard + axum included).
- **Native distro packages.** Built via `cargo-deb` (Debian, Ubuntu, Mint),
  `cargo-generate-rpm` (Fedora, RHEL, openSUSE) and a hand-written `PKGBUILD`
  for Arch. Each ships the binary plus a `clipboardwire.service` systemd unit
  with `ExecStart=/usr/bin/clipboardwire serve`, `DynamicUser=yes`,
  `ProtectSystem=strict`, `ProtectHome=yes`, `NoNewPrivileges=yes`, and a
  `/etc/clipboardwire/clipboardwire.env` for the user/password env vars.
  Recipes live in `packaging/`.
- **Windows.** Cross-compiled from Linux via the `x86_64-pc-windows-gnu`
  target. Produces a single ~7 MiB stripped `.exe` with no DLL dependencies
  beyond what ships with Windows. Local cross-build:
  ```sh
  rustup target add x86_64-pc-windows-gnu
  sudo apt install -y gcc-mingw-w64-x86-64   # one-time
  cargo build --release --target x86_64-pc-windows-gnu
  ```
  An MSI installer is produced by `cargo wix` on the windows-latest CI
  runner; the WiX 3 source lives in `cli/wix/main.wxs`. The MSI installs to
  `%ProgramFiles%\clipboardwire\`, drops a license file alongside the
  binary, and prepends that directory to the system `PATH`. The
  `UpgradeCode` is stable so future versions upgrade in place.
- **macOS.** Expected to fall out of the same source tree (`arboard` and the
  axum stack both target macOS); will be wired up alongside the macOS client
  phase.
- **No Docker image.** The binary is small enough that direct install via a
  distro package is the supported deployment path.

## 3. Clipboard client (`clipboardwire-core::client`)

The client-side code lives in `clipboardwire-core` and is invoked by the CLI's
`connect` subcommand (or as the other half of `host`). Without `--tray` it runs
headless as a foreground process that logs to stderr and exits non-zero on fatal
errors; with `--tray` (the default for a desktop launch) the CLI owns the tray
icon and settings dialog, both of which live in `cli/` rather than here.

### 3.1 Module layout

```
core/src/client/
├── mod.rs           # supervisor: `run_with_status`, SendPolicy/ReceivePolicy
├── config.rs        # ClientConfig struct + TOML loader
├── transport.rs     # WS connect, auth, reconnect with backoff, framed I/O
├── clipboard.rs     # arboard wrapper, polling loop, dedup of self-set values
├── file.rs          # eager chunk writer (`FileReceiver`) + `finish_lazy_transfer`
├── offer.rs         # source-side offer registry, pinned identity (§3.9)
├── pull.rs          # source-side pull server: windows, hashing, digest (§3.9)
├── receive.rs       # receiver-side engine + `LazyAssembler` (§3.9)
└── tls.rs           # custom root store / insecure verifier for wss://
```

### 3.2 Dependencies (rationale)

| Crate                  | Purpose                                  | Why this one                                                       |
| ---------------------- | ---------------------------------------- | ------------------------------------------------------------------ |
| `tokio` (full)         | Async runtime                            | Symmetry with the server.                                          |
| `tokio-tungstenite`    | WebSocket client                         | Mature, idiomatic, works on all our targets.                       |
| `http`                 | Build the `Authorization` header         | Used by tungstenite handshake.                                     |
| `arboard`              | Read/write the platform clipboard        | Single API across Linux (X11/Wayland), Windows, macOS.             |
| `rand`                 | UUID randomness, jitter                  | CSPRNG via `OsRng`.                                                |
| `uuid`                 | Generate frame ids                       | v4.                                                                |
| `serde` / `serde_json` | Frame (de)serialization                  | Same shape as server.                                              |
| `toml`                 | Config                                   | Light, deterministic, friendly for hand-editing.                   |
| `clap` (derive)        | CLI parsing                              | `--config`, `--server`, `--user`, etc.                             |
| `tracing` + `tracing-subscriber` | Logging                        | Same stack as server.                                              |
| `directories`          | Locate `~/.config/clipboardwire/`        | XDG/AppData-aware across platforms.                                |
| `rustls` + `rustls-pemfile` + `webpki-roots` | `wss://` support       | Custom root store for self-signed certs, plus an opt-in "skip verification" verifier for trusted-LAN deployments. |
| `anyhow` / `thiserror` | Error plumbing                           | Same pattern.                                                      |

### 3.3 Config file

`~/.config/clipboardwire/config.toml` (or platform equivalent):

```toml
# WebSocket endpoint; use wss:// when fronted by a TLS proxy.
server = "ws://nas.lan:8484/sync"

# HTTP Basic credentials shared with the server.
user     = "alice"
password = "..."

# Optional: polling interval for clipboard changes.
poll_ms = 300

# Optional: TLS trust configuration.
# tls_ca_file  = "/etc/clipboardwire/ca.crt"   # extra root CA to trust
# tls_insecure = false                          # set to true on LAN/VPN only
```

Permissions: client refuses to start if the file is world-readable on Unix
(mode `0o077`-clean check). The Basic-auth password is the only secret in
the file; OS-keyring integration is a possible Phase-3 follow-up.

### 3.4 Clipboard plumbing

`arboard` is a polling API: the client samples the clipboard at `poll_ms`
intervals. Two failure modes to handle:

- **Inbound echo (from the server).** The hub never echoes our own publishes
  back — `PROTOCOL.md` §3.1 stamps every relayed frame with `from`, and the
  hub skips the originating connection during fan-out. The client confirms
  this defensively by dropping any frame where `from == self.client_id` (paranoia
  against future server bugs).
- **Outbound echo (from our own write).** When we apply an incoming clip, the
  next clipboard poll will read it back. Fix: hash the value we just wrote and
  suppress the next matching poll result.
- **Read flap during a copy.** On some platforms `arboard` briefly fails while
  another app holds the clipboard. Treat transient errors as "no change".

Pseudocode:

```rust
let mut last_seen: Option<Hash> = None;
loop {
    interval.tick().await;
    let Ok(text) = clipboard.get_text() else { continue };
    let h = blake3::hash(text.as_bytes());
    if Some(h) == last_seen { continue }
    last_seen = Some(h);
    if Some(h) == self_set_hash { continue }  // we wrote this
    publish(text).await?;
}
```

For incoming frames the writer task does the symmetric thing:

```rust
let text = &frame.content;
let h = blake3::hash(text.as_bytes());
self_set_hash = Some(h);
clipboard.set_text(text)?;
last_seen = Some(h);  // suppress immediate poll echo
```

(`blake3` is a candidate hash dep; `siphash` or even a `u64` of `xxhash` works
too. Settle on one in implementation.)

### 3.5 Platform notes

- **Linux/X11.** `arboard` works. Note that X11 clipboard data lives in the app
  that copied it; if that app exits, the clipboard goes empty. Acceptable for
  our use case — we only need to *observe* changes while we run.
- **Linux/Wayland.** `arboard` uses Wayland's clipboard protocols (when built
  with the relevant feature). Polling works on most compositors, but some
  restrict clipboard access to focused windows, which prevents true background
  polling. Document as a known limitation; a "headless companion" foreground
  window is a possible workaround.
- **Windows.** `arboard` uses the Win32 clipboard API. Polling is fine. A future
  improvement is `AddClipboardFormatListener` for push events, but it requires
  a real HWND.
- **macOS.** `arboard` uses NSPasteboard. Polling against `changeCount` is
  cheap. Should work without changes.

### 3.6 Tray UI (cross-platform)

A thin wrapper around the headless supervisor in `cli/src/tray.rs`. Builds
a `tray_icon::TrayIcon` with a placeholder icon (drawn programmatically —
no asset shipped) and a single "Quit" menu item, then spawns `client::run`
as a tokio task and multiplexes three sources via `tokio::select!`:

1. The supervisor's `JoinHandle` (returns when the client exits).
2. `MenuEvent::receiver()` (polled every 100 ms — the receiver is a
   `crossbeam_channel` so we can't await it directly).
3. `tokio::signal::ctrl_c()` for clean shutdown from a terminal.

Backends per platform (via `tray-icon` 0.22):

- **Windows.** Win32 directly. No external runtime deps.
- **Linux.** `libayatana-appindicator3` (GNOME/KDE-compatible). Requires
  `libgtk-3-0`, `libayatana-appindicator3-1`, and `libxdo3` at runtime;
  the distro packages declare these so `apt`/`dnf` install pulls them in.
  On a truly headless server the binary won't even start because it's
  dynamically linked against libgtk — see below for that escape hatch.
- **macOS.** AppKit. No extra deps (will be tested when the macOS build
  pipeline is added).

**Headless-server build:** the tray dep is currently mandatory. If a user
needs a binary with no GTK runtime deps (a Linux server with nothing X11
installed) they can build from source — a `tray` feature flag (default
on) is on the v0.3 backlog so distro maintainers can ship a separate
`clipboardwire-server` package that omits the tray.

### 3.7 Supervisor

`client::run` owns a small supervisor:

1. The clipboard poll task.
2. The transport task (connect → run reader+writer → exit on error/close).
3. A `JoinSet` so one task's failure tears the rest down.

On transport exit it sleeps the backoff (`PROTOCOL.md` §2.5) and retries. The
clipboard task keeps running across reconnects; outbound frames are dropped
while disconnected (clipboard is last-write-wins, no queueing needed).

The supervisor also owns the state that must outlive a single connection: the
offer registry, the pull server, and the receive engine. A reconnect creates a
new socket but not a new engine, because a transfer's sessions are keyed by
`transfer_id` and by definition span multiple frames.

### 3.8 Device identity

A peer has two ids and they are not interchangeable (wire spec §2.1):

- `client_id` — minted by the hub per connection. Dies with the socket.
- `device_id` — minted by the client, stable for the machine's lifetime.

Only the second can be addressed. `peer_changed`, `file_offer.from_device`,
`file_pull.from_device` and `file_abort.to` all carry device ids, because a
frame that named a connection would become undeliverable the moment that
connection dropped.

**`device_id` lives in the state directory, not in `config.toml`.** The settings
dialog rewrites the config file wholesale on Save; anything stored there and not
modelled by the form would be erased by a user editing the poll interval. The
state dir (`identity::load_or_create`) is never rewritten from the UI, so the id
survives. A failure to load it is **not** fatal — an unwritable state dir costs
a stable id, not the connection — and the fallback is an ephemeral `Uuid::new_v4()`
logged at WARN, which looks exactly like a first run.

Because the id is persisted in a file, copying a machine image clones it. Two
hosts then present the same `device_id`, and the hub's duplicate-session rule
(`4005`) makes them evict each other in a loop. The state file therefore records
a host fingerprint alongside the id; a mismatch is treated as "new machine" and
a fresh id is minted. This is detection, not prevention: it cannot tell a cloned
image from a legitimately moved disk, and it deliberately errs toward a new id,
since a spurious identity change costs an offer refresh while a duplicate id
costs the connection.

**`from_device` is written only by the hub.** Every control frame that carries it
arrives from a client with that field either absent or a lie. The hub overwrites
it from its own registry on relay (`ws.rs` clears it on the way in; the hub sets
it on the way out). A source that trusted a client-supplied value would let any
peer redirect a file's bytes to a device of its choosing.

### 3.9 Offer registry and pull server

The lazy path splits into three pieces with one job each:

| Piece | Question it answers | State |
| ----- | ------------------- | ----- |
| `offer::OfferRegistry` (source) | *Which files am I willing to serve, and from which path?* | `file_id → (path, pinned identity)`, plus a reverse index by path |
| `pull::PullServer` (source) | *What does this pull get?* | `transfer_id → session` (hasher, high-water mark, admission) |
| `receive::ReceiveEngine` (receiver) | *Do I accept this chunk, and am I done?* | `transfer_id → session` (bitmap, contiguous run, in-flight window) |

**Offers are keyed by `(owner device, file_id)`, not by `file_id`.** `file_id` is
chosen by the client, so a bare key would let one peer overwrite another peer's
offer simply by reusing its id. The pair is the identity of an offer.

**Identity is re-checked repeatedly, not once.** A `PinnedIdentity` is captured
when the offer is built — `file_key` (`(dev, ino)` on Unix, `(volume, index)` on
Windows, `None` where the platform cannot supply one), `size`, and `mtime_ns` —
and compared again:

- before every window is read — a file rewritten mid-transfer would otherwise
  splice two versions into one hash stream;
- before the digest is emitted — the last chance to notice the file changed
  between the final chunk and the hash.

A mismatch answers `file_abort{source_changed}`. There is no resume: the
receiver restarts from index 0 under a new `transfer_id`, because a single
digest can only vouch for the whole file and a partial restart would have no
value to verify against.

**Sessions start at index 0 and restart from there.** A `transfer_id` the source
has not seen must arrive with `chunk_index == 0`; anything else is
`invalid_start` before any disk I/O happens. That rule is what makes "the
receiver restarts from zero" a safe recovery rather than a guess.

The pull server retains a completed session past its digest, up to an idle
timeout. That is not an optimisation: a `file_digest` can be dropped by a full
peer buffer, and the receiver's recovery is to re-ask for the final window. A
source that forgot the session the instant it answered would turn every dropped
digest into a full re-download.

### 3.10 Hub delivery, and why there are two queues

The hub is a single synchronous actor. It must never `await` a peer, because
one slow receiver would then stall every other client's traffic — the exact
failure the backpressure requirement exists to prevent.

Each connection therefore has **two** outbound paths for bulk data:

| Queue | Capacity | Purpose |
| ----- | -------- | ------- |
| `file` (`PER_CLIENT_FILE_CHANNEL_BUF`) | 4 chunks | The primary path, drained by the peer's writer |
| `overflow` (`PER_CLIENT_FILE_OVERFLOW`) | 8 chunks | Second chance when the primary is full |

`try_send` on the primary, then `try_send` on the overflow. If both are full the
hub **aborts that one transfer** with `file_abort{busy}` rather than dropping a
chunk silently: a missing chunk inside a session is unrecoverable without a
restart anyway, so the honest answer is the one that lets the receiver restart
immediately. Eager chunks have no session to abort, so they are dropped with a
warning — there is nothing to name.

Control frames take a third, separate lane (`frames`, capacity 8). Keeping them
apart means a close instruction, a digest, or a busy abort can still get through
while the data path is saturated.

Two limits are easy to confuse and bound different things:

| Limit | Bounds | Effect when hit |
| ----- | ------ | --------------- |
| `MAX_ACTIVE_PULLS` (4) | How many transfers one **source** reads at once | Further sessions queue behind batch-mates or get `busy`; bounds open file handles and 4 MiB read buffers per source |
| `MAX_ACTIVE_TRANSFERS` (16 384) | How many session ids the **hub** remembers | The hub forgets the oldest batch so it can keep applying TTL per transfer; nothing is refused |

`MAX_ACTIVE_TRANSFERS` is a memory bound, not a concurrency limit. It exists
because the hub must answer "has this transfer started?" for a pull that arrives
after the offer's TTL — and the set of answers grows once per accepted pull.

### 3.11 Why the digest covers the served stream, not the file

`file_digest` (and an eager chunk's `file_sha256`) is the hash of the bytes the
source actually read out of the file, accumulated as they were sent. It is
**not** a re-hash of the file on disk at completion time.

That distinction matters because it makes one class of corruption invisible: if
the file is rewritten while it is being served, the hasher sees a mixture of old
and new bytes, the receiver receives exactly that mixture, and the digest
matches. Verification passes on a file neither side would have chosen.

The protocol does not try to prevent that with the digest — it cannot, since the
digest is computed from the same stream. It prevents it with the identity
re-checks described in §3.9: each window re-stats the file, so a change between
windows is caught by something other than the hash.

What remains is the gap *between* a stat and the reads that follow it. It is
bounded by one window rather than by the whole file, but "one window" is not
small: `MAX_PULL_WINDOW` is 32 chunks of 4 MiB, so a single window can be 128 MiB
and on a slow disk can take tens of seconds to read. A writer that rewrites the
file during that span can still produce a stream that hashes consistently. The
bound is a bound on the *span*, not a claim that the span is short; closing it
further would need a per-chunk identity check, which costs a stat per chunk on
the read path.

### Windows paste mode (`receive_mode = "paste"`, `core/src/client/paste.rs`)

Offers never touch Downloads in this mode. The supervisor registers the
offer's shape, batches the copy into one `FILEGROUPDESCRIPTORW`, and a
dedicated STA thread arms an OLE `IDataObject` on the clipboard via
`OleSetClipboard`. A paste target (Explorer, Office, a file dialog) asking for
`FileContents` is what starts the pull: `GetData` blocks on the STA thread
while the supervisor runs the normal lazy pull into `%TEMP%\clipboardwire-paste`,
verifies the SHA-256, then answers with an `IStream` over the verified file.
A failed pull fails the paste — unverified bytes can never reach the target.
The object also implements `IDataObjectAsyncCapability`, which is what makes
Explorer wrap the paste in its own async progress UI rather than freezing on
the synchronous `GetData`.

macOS deliberately has no equivalent: Finder does not invoke a file-promise
provider on Cmd+V, so `receive_mode = "paste"` resolves to `pickup` there.

The spool is invisible cache — pulled once, served to every later paste of
the same `file_id` without pulling again; `%TEMP%` reclaims it.

## 4. The `clipboardwire` binary (`cli/`)

A thin wrapper around the library. `cli/src/main.rs` parses arguments with
`clap` (derive macros) into one of three subcommands:

```
clipboardwire serve           # hub only
clipboardwire host            # hub + local client (same process)
clipboardwire connect         # client only (also the default if no subcommand)
clipboardwire [global flags]  # --config <path>, --log-level <lvl>
```

Each subcommand:

- `serve` — load `ServerConfig` from env vars; call `core::server::run(cfg)`.
- `connect` — load `ClientConfig` from `--config` or the platform default path;
  call `core::client::run(cfg)`.
- `host` — load both. Spawn `core::server::run(server_cfg)` and, after the
  listener is up, spawn `core::client::run(client_cfg)` pointed at
  `ws://127.0.0.1:<bound_port>/sync`. Use a `JoinSet`; if either task exits,
  shut the other down and exit the process.

### 4.1 CLI dependencies

| Crate                            | Purpose                  |
| -------------------------------- | ------------------------ |
| `clap` (derive)                  | subcommand routing       |
| `tokio` (full)                   | runtime                  |
| `tracing` + `tracing-subscriber` | logging setup            |
| `anyhow`                         | top-level error plumbing |
| `clipboardwire-core` (path dep)  | everything substantive   |

## 5. Cross-cutting concerns

### 5.1 Tests

- **Server unit tests** — hub register/publish/deregister, last-clip cache,
  frame size enforcement, auth parsing.
- **Server integration test** — spin up the axum app with `tower::ServiceExt`
  and drive a fake WebSocket pair.
- **Client unit tests** — config parsing, echo-loop suppression logic,
  frame (de)serialization round-trip.
- **End-to-end test** (gated behind a feature flag because it needs a real
  network port) — server + two clients on loopback, assert clip flows A→B.

### 5.2 CI

Two GitHub Actions workflows live in `.github/workflows/`:

- **`ci.yml`** — runs on every push to `main` and on pull requests.
  - `lint` job (ubuntu): `cargo fmt --all -- --check` and
    `cargo clippy --workspace --all-targets -- -D warnings`.
  - `test` job (matrix: `ubuntu-latest`, `windows-latest`):
    `cargo test --workspace`.
- **`release.yml`** — runs on tagged pushes (`v*`).
  - Linux job builds the release binary, then `cargo deb` and
    `cargo generate-rpm`, and uploads `.deb` / `.rpm` / raw binary to the
    GitHub release.
  - Windows job builds the release `.exe` and uploads it.

Both workflows use `Swatinem/rust-cache` for compile-time caching.

### 5.3 Observability

- `tracing` everywhere with INFO default. Each WebSocket connection gets a
  `client_id` span; every published frame logs `id`, `len`, `from` at DEBUG.
- The server logs frame *metadata* (id, length, content_type, from) at DEBUG and
  the `content` itself only at TRACE — operators who don't want clipboard text
  in their logs can simply leave `RUST_LOG` at INFO (the default).
- A `/healthz` endpoint on the server returns 200 OK with a tiny body for
  Docker healthchecks.

## 6. Build & run targets (cheatsheet)

```sh
# Workspace build
cargo build --release

# Hub only (headless / Docker target)
CLIPBOARDWIRE_USER=alice CLIPBOARDWIRE_PASSWORD=hunter2 \
  ./target/release/clipboardwire serve

# Hub + local clipboard client (one machine bootstrapping the network)
CLIPBOARDWIRE_USER=alice CLIPBOARDWIRE_PASSWORD=hunter2 \
  ./target/release/clipboardwire host \
    --config ~/.config/clipboardwire/config.toml

# Client connecting to a remote hub
./target/release/clipboardwire connect \
  --config ~/.config/clipboardwire/config.toml
# (or just `./target/release/clipboardwire` — `connect` is the default)

# Native packages (deb / rpm)
cargo deb -p clipboardwire                                  # → target/debian/*.deb
cargo generate-rpm -p clipboardwire                         # → target/generate-rpm/*.rpm
# After install:
sudo systemctl enable --now clipboardwire
```

## 7. Out of scope (intentionally)

Each of these is a deliberate boundary, not an unfinished task. The section
name used to say "for v0.1"; the boundaries that have since moved are marked.

- Cross-platform tray UI on **Linux** — deferred due to libgtk/libappindicator
  runtime deps that would either split the Linux package or force a GTK dep on
  headless servers. *(Windows and macOS both ship a tray; this entry is now
  Linux-only.)*
- Image clipboard payloads (protocol bump required). *(Files are no longer in
  this list: v0.4 transfers them, eagerly or lazily — see §3.8–§3.11.)*
- Multi-user / per-room separation.
- TLS terminated inside the server, rather than by a reverse proxy. *(The server
  can now serve TLS directly with `tls_cert_file`/`tls_key_file` and generates a
  self-signed certificate when none is configured; a reverse proxy remains the
  recommended shape for a public deployment.)*
- Application-layer encryption / TOFU device pairing (see `PROTOCOL.md` §4).
- OS keyring integration for storing the Basic-auth password.
- Persistent `last_clip` across restarts — the hub keeps it in memory only.

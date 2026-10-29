// SPDX-License-Identifier: GPL-3.0-or-later

// On Windows release builds, use the "windows" subsystem so launching the
// binary from a Start Menu shortcut, the Desktop icon, or the autostart
// HKCU\…\Run entry doesn't pop a console window. The
// `attach_parent_console` shim re-attaches us to the calling terminal
// when the user ran us from cmd / PowerShell, so logs still appear
// inline there.
#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

#[cfg(windows)]
mod autostart_win;
mod instance;
mod settings;
mod tray;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use clipboardwire_core::client::ClientConfig;
use clipboardwire_core::server::ServerConfig;

#[derive(Parser, Debug)]
#[command(version, about = "Clipboard sync over WebSocket", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to the client config file (used by `connect` and `host`).
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Show a system-tray icon while running.
    #[arg(long, global = true)]
    tray: bool,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Run as a relay hub only (headless / systemd / NAS).
    Serve,
    /// Run as a hub *and* as a local clipboard client on the same machine.
    Host,
    /// Connect to an existing hub as a clipboard client (default).
    Connect,
    /// Open the GUI settings dialog for the client config file.
    Settings,
    /// Send a file through the configured hub to every other connected
    /// peer. One-shot: opens a fresh connection, publishes the file in
    /// 4-MiB chunks, then exits. The receivers' tray supervisors save
    /// each file under their `~/Downloads/clipboardwire/`.
    Send {
        /// Path to the file to send.
        file: PathBuf,
    },
}

fn main() -> Result<()> {
    #[cfg(target_os = "windows")]
    attach_parent_console();

    init_tracing();
    let cli = Cli::parse();
    let no_subcommand = cli.command.is_none();
    let cmd = cli.command.clone().unwrap_or(Command::Connect);

    // Settings mode owns the main thread for its own eframe/winit event
    // loop. No tokio runtime needed — the dialog is a pure GUI tool.
    if matches!(cmd, Command::Settings) {
        let path = match cli.config.clone() {
            Some(p) => p,
            None => {
                ClientConfig::default_path().context("could not determine default config path")?
            }
        };
        return settings::run(path);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    // Tray policy: tray mode is the default for desktop launches
    // (`clipboardwire` with no subcommand, or a Start Menu shortcut
    // launch). Explicit `--tray` keeps the old behavior. `serve` never
    // gets a tray.
    let needs_tray = matches!(cmd, Command::Connect | Command::Host) && (cli.tray || no_subcommand);
    if needs_tray {
        run_with_tray(runtime, cli, cmd)
    } else {
        runtime.block_on(run_headless(cli, cmd))
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "clipboardwire=info,clipboardwire_core=info".into());

    if let Some(log_dir) = log_dir() {
        let file_appender = tracing_appender::rolling::daily(&log_dir, "clipboardwire.log");
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        // Leak the guard so the writer lives for the whole process.
        std::mem::forget(_guard);

        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false),
            )
            .init();

        install_panic_hook(Some(log_dir));
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
        install_panic_hook(None);
    }
}

fn log_dir() -> Option<PathBuf> {
    ClientConfig::default_path()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
}

fn install_panic_hook(log_dir: Option<PathBuf>) {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let msg = format!("PANIC: {info}");
        // Surface via tracing so it reaches the log file.
        tracing::error!("{msg}");
        // Also write a standalone crash file as a belt-and-suspenders
        // measure in case the tracing layer hasn't flushed yet.
        if let Some(dir) = &log_dir {
            let crash_path = dir.join("crash.log");
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let backtrace = std::backtrace::Backtrace::force_capture();
            let body = format!("{timestamp} {msg}\n{backtrace}\n");
            let _ = std::fs::write(&crash_path, body);
        }
        default_hook(info);
    }));
}

/// Best-effort: re-attach to the parent console so `cargo run`, PowerShell,
/// and cmd.exe still surface our stderr logs even with the "windows"
/// subsystem. If there's no parent console (e.g. launched from Start Menu
/// or autostart), this is a no-op and we stay silent.
#[cfg(target_os = "windows")]
fn attach_parent_console() {
    unsafe extern "system" {
        fn AttachConsole(pid: u32) -> i32;
    }
    const ATTACH_PARENT_PROCESS: u32 = 0xFFFF_FFFF;
    unsafe {
        let _ = AttachConsole(ATTACH_PARENT_PROCESS);
    }
}

async fn run_headless(cli: Cli, cmd: Command) -> Result<()> {
    match cmd {
        Command::Serve => {
            if cli.tray {
                tracing::warn!("`--tray` is ignored in serve mode");
            }
            let cfg = resolve_server_config(cli.config.as_deref())?;
            clipboardwire_core::server::run(cfg).await
        }
        Command::Connect => {
            let cfg = load_client_config_or_bail(cli.config.as_deref())?;
            run_client_headless(cfg).await
        }
        Command::Host => run_host_headless(cli.config.as_deref()).await,
        Command::Settings => unreachable!("settings handled before runtime build"),
        Command::Send { file } => run_send_once(cli.config.as_deref(), &file).await,
    }
}

/// Transport options for the one-shot `send` subcommand.
///
/// The device id is **ephemeral by design**, and that is not a shortcut:
///
/// - Two live connections sharing one device id evict each other, because the
///   hub's rule is newest-wins. A persisted id here would make `send` kick the
///   user's own tray offline mid-copy — the transfer would fail *and* the
///   clipboard would stop syncing.
/// - A fresh id per process is also what lets several sends run concurrently,
///   and what keeps a `send` on a read-only or unwritable state dir working.
///
/// The persistent identity in the state dir belongs to the long-lived client.
/// `send` is a separate, short-lived participant and says so on the wire.
fn send_spawn_options() -> clipboardwire_core::client::transport::SpawnOptions {
    clipboardwire_core::client::transport::SpawnOptions {
        status_tx: None,
        receive_files: false,
        one_shot: true,
        // `TransferId` is core's re-export of `uuid::Uuid`, so the CLI does
        // not need its own uuid dependency.
        device: Some(clipboardwire_core::client::TransferId::new_v4()),
        // No capabilities: this build cannot consume offers (Phase 5), so
        // advertising `lazy-file` would invite offers it can only drop.
        caps: clipboardwire_core::protocol::Caps::default(),
    }
}

/// `clipboardwire send <file>` — one-shot connect, publish the file as
/// a sequence of `file_chunk` frames, exit. Reuses the client config
/// (server URL + creds + TLS settings) the user already has.
async fn run_send_once(
    config_path: Option<&std::path::Path>,
    file: &std::path::Path,
) -> Result<()> {
    use clipboardwire_core::client::transport::spawn_with_options;

    let cfg = load_client_config_or_bail(config_path)?;
    let (transport, join) = spawn_with_options(cfg, send_spawn_options());

    let send_result =
        clipboardwire_core::client::file::send_file_through(file, &transport.outbound_files_tx)
            .await;

    // Consume the `Transport` so every sender really is dropped, then wait for
    // the task to flush what is still queued and close the socket
    // (`one_shot = true` makes it skip the reconnect retry).
    //
    // This used to be `let _ = transport.outbound_tx;`, which does **not**
    // drop anything: `let _ = expr` discards the place, not the value, so the
    // sender stayed alive inside `transport`, the transport's `select!` never
    // saw its channel close, and `clipboardwire send` hung after the file had
    // been delivered. `close_outbound` takes `self`, so the drop cannot be
    // skipped.
    transport
        .close_outbound(join)
        .await
        .context("transport task panicked")?;

    send_result
}

/// Pick the directory the singleton-lock file lives in. We co-locate
/// it with the config file (or its platform default) so users find
/// "what is this `clipboardwire.lock` file" right where they expect.
fn singleton_lock_dir(override_path: Option<&std::path::Path>) -> Result<PathBuf> {
    let config_path = match override_path {
        Some(p) => p.to_path_buf(),
        None => ClientConfig::default_path()
            .context("could not determine the default config path for the singleton lock")?,
    };
    Ok(config_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(".")))
}

/// Build the server config used by `clipboardwire serve`.
///
/// Sources, in order of precedence:
/// 1. Env vars (`CLIPBOARDWIRE_*`) — always win.
/// 2. The `[hub]` section of the TOML at `--config` (or the platform
///    default if `--config` is not supplied and the file exists).
/// 3. Built-in defaults from [`ServerConfig::from_env`].
///
/// The TOML's parent directory becomes the default `state_dir` so the
/// auto-generated self-signed cert lands next to the config file rather
/// than in a far-off platform data dir.
fn resolve_server_config(config_path: Option<&std::path::Path>) -> Result<ServerConfig> {
    let toml_path = match config_path {
        Some(p) => Some(p.to_path_buf()),
        None => ClientConfig::default_path().ok().filter(|p| p.exists()),
    };

    let base = if let Some(ref p) = toml_path {
        match ClientConfig::load(p) {
            Ok(client_cfg) => client_cfg.hub.map(|h| {
                let mut sc = h.to_server_config();
                if sc.state_dir.is_none() {
                    sc.state_dir = p.parent().map(|d| d.to_path_buf());
                }
                sc
            }),
            Err(e) => {
                tracing::warn!(
                    path = %p.display(),
                    error = %format!("{e:#}"),
                    "config file present but unreadable; falling back to env-only server config"
                );
                None
            }
        }
    } else {
        None
    };

    ServerConfig::from_env_layered(base)
}

async fn run_client_headless(cfg: ClientConfig) -> Result<()> {
    // Headless has no tray, so progress goes to stderr — throttled to one line
    // per second per transfer (task 1.6). Without the throttle a multi-chunk
    // transfer would emit a line per 4 MiB chunk, which buries the log.
    let (progress_tx, progress_rx) = tokio::sync::broadcast::channel(64);
    tokio::spawn(log_progress_to_stderr(progress_rx));

    tokio::select! {
        res = clipboardwire_core::client::run_with_status(cfg, None, Some(progress_tx)) => res,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            Ok(())
        }
    }
}

/// Print at most one progress line per second per transfer to stderr.
///
/// State transitions (`Hashing`, `Verifying`, terminal states) always print,
/// because those are the interesting moments; only the per-chunk `Pulling`
/// stream is throttled. A long transfer therefore shows start, one line a
/// second, and the end, with no chunk-level spam.
/// Last line printed for one transfer, and when.
#[cfg_attr(test, derive(Debug))]
#[derive(Clone, Copy)]
struct LastLogLine {
    at: std::time::Instant,
    state: clipboardwire_core::client::ProgressState,
}

/// At most one stderr line per second per transfer, regardless of state.
const PROGRESS_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Whether this progress event deserves a stderr line.
///
/// Throttled **by transfer**, not by state: throttling only `Pulling` let the
/// `Hashing` stream through untouched, which is ~101 lines per file — exactly
/// the spam this is meant to prevent.
///
/// Two events always print regardless of the interval: a state change (the
/// interesting moments are hashing→pulling→done, not the percentages between
/// them) and a terminal state (the user must see how it ended).
fn should_log_progress(
    previous: Option<LastLogLine>,
    event: &clipboardwire_core::client::TransferProgress,
    now: std::time::Instant,
) -> bool {
    let Some(previous) = previous else {
        return true;
    };
    if previous.state != event.state {
        return true;
    }
    if event.state.is_terminal() {
        return true;
    }
    now.duration_since(previous.at) >= PROGRESS_LOG_INTERVAL
}

async fn log_progress_to_stderr(
    mut rx: tokio::sync::broadcast::Receiver<clipboardwire_core::client::TransferProgress>,
) {
    use clipboardwire_core::client::TransferId;
    use std::collections::HashMap;

    let mut last_print: HashMap<TransferId, LastLogLine> = HashMap::new();
    loop {
        let event = match rx.recv().await {
            Ok(e) => e,
            // Dropping behind is fine: the next event carries the same
            // cumulative `bytes_done`, so nothing is lost but intermediate
            // percentages.
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };
        let now = std::time::Instant::now();
        if !should_log_progress(last_print.get(&event.file_id).copied(), &event, now) {
            continue;
        }
        last_print.insert(
            event.file_id,
            LastLogLine {
                at: now,
                state: event.state,
            },
        );
        // stderr, not tracing: the requirement is a line per second on the
        // terminal, and the tracing layer already writes INFO to stderr with
        // a timestamp prefix that would make these lines unskimmable.
        eprintln!(
            "clipboardwire: {} ({})",
            event.summary_line(64),
            event.state.as_str()
        );
        if event.state.is_terminal() {
            last_print.remove(&event.file_id);
        }
    }
}

/// Headless config loader: hard-errors if the file is missing. When the
/// user is at a terminal (no `--tray`) the diagnostic message is the right
/// thing to surface, and we write a template at the default path so they
/// have something to edit.
fn load_client_config_or_bail(override_path: Option<&std::path::Path>) -> Result<ClientConfig> {
    let (path, using_default) = match override_path {
        Some(p) => (p.to_path_buf(), false),
        None => (
            ClientConfig::default_path()
                .context("could not determine the default client config path")?,
            true,
        ),
    };

    if using_default && !path.exists() {
        ClientConfig::write_template(&path)
            .with_context(|| format!("writing template config at {}", path.display()))?;
        tracing::warn!(
            "no client config found; wrote a template to {} — edit it (set the server URL \
             and password) and re-run",
            path.display()
        );
        anyhow::bail!(
            "config missing; template written to {} — edit it and re-run",
            path.display()
        );
    }

    ClientConfig::load(&path)
        .with_context(|| format!("loading client config at {}", path.display()))
}

/// `host` mode without tray. The tray-mode variant is below in
/// `run_with_tray` because it has to share the runtime with the event loop.
async fn run_host_headless(client_config_path: Option<&std::path::Path>) -> Result<()> {
    let server_cfg = ServerConfig::from_env()?;
    let (listener, addr) = clipboardwire_core::server::bind(&server_cfg).await?;
    tracing::info!(addr = %addr, "hub listening (host mode)");

    let client_cfg = build_host_client_config(&server_cfg, addr.port(), client_config_path)?;

    let server_task = tokio::spawn(async move {
        clipboardwire_core::server::serve(listener, server_cfg, std::future::pending()).await
    });

    tokio::select! {
        res = server_task => { res.context("server task panicked")??; }
        res = run_client_headless(client_cfg) => { res?; }
        _ = tokio::signal::ctrl_c() => { tracing::info!("shutting down"); }
    }
    Ok(())
}

fn build_host_client_config(
    server_cfg: &ServerConfig,
    port: u16,
    override_path: Option<&std::path::Path>,
) -> Result<ClientConfig> {
    let scheme = if server_cfg.tls_enabled() {
        "wss"
    } else {
        "ws"
    };
    let loopback_url = format!("{scheme}://127.0.0.1:{port}/sync");

    // Build the in-process hop's config from the override file when there is
    // one, then overwrite only the fields the hop itself dictates. Everything
    // else — the transfer modes, the TTL, the pull window — is the user's
    // choice, not a property of the loopback hop; dropping them here would
    // make `host` mode quietly ignore a valid `config.toml`.
    //
    // `ClientConfig::load` has already run `validate()` (including
    // `validate_modes`), so those values are known-good.
    let mut cfg = match override_path {
        Some(p) => ClientConfig::load(p).with_context(|| format!("loading {}", p.display()))?,
        None => ClientConfig {
            server: String::new(),
            user: server_cfg.user.clone(),
            password: server_cfg.password.clone(),
            poll_ms: 300,
            tls_ca_file: None,
            tls_insecure: false,
            hub: None,
            log_file_names: false,
            send_mode: Default::default(),
            receive_mode: Default::default(),
            pickup_auto: false,
            offer_ttl_minutes: 30,
            pull_window_chunks: 3,
            pull_idle_timeout_secs: 30,
        },
    };

    cfg.server = loopback_url;
    cfg.hub = None;
    // Loopback in host mode often pairs with a public-cert TLS hub; the cert's
    // SAN won't cover 127.0.0.1. Skip cert verification on this in-process hop
    // only, and ignore any CA bundle aimed at the real server.
    cfg.tls_insecure = server_cfg.tls_enabled();
    cfg.tls_ca_file = None;
    Ok(cfg)
}

/// Tray mode: the main thread is given to tao's event loop so the OS
/// dispatches right-click, menu activation, etc. The tokio runtime stays
/// alive on its worker threads for the duration of the loop.
fn run_with_tray(runtime: tokio::runtime::Runtime, cli: Cli, cmd: Command) -> Result<()> {
    // Take the singleton lock *first*, before bringing up the tray or
    // the embedded hub. Two trays would race on the hub port and the
    // arboard X11 connection — and previously did, manifesting as
    // "client cycles connect/disconnect every few seconds." See
    // `instance::acquire_or_fail`.
    let lock_dir = singleton_lock_dir(cli.config.as_deref())?;
    if let Err(e) = instance::acquire_or_fail(&lock_dir) {
        // Emit BOTH a tracing diagnostic (for log-collectors / journald)
        // AND an `eprintln!` (so the message reaches stderr even when
        // tracing's fmt writer is line-buffered and the process exits
        // before its flush — see `singleton_lock` integration test).
        let msg = format!("another clipboardwire instance is already running: {e:#}");
        tracing::error!("{msg}");
        eprintln!("clipboardwire: {msg}");
        return Ok(());
    }

    let (config_path, initial_cfg, host_hub_handle, auto_open_settings) = match cmd {
        Command::Connect => {
            let (path, cfg, auto_open) = prepare_connect_tray_args(cli.config.as_deref())?;
            (path, cfg, None, auto_open)
        }
        Command::Host => {
            let (path, cfg, hub) = prepare_host_tray_args(&runtime, cli.config.as_deref())?;
            (path, Some(cfg), Some(hub), false)
        }
        Command::Serve | Command::Settings | Command::Send { .. } => {
            unreachable!("non-tray subcommand")
        }
    };

    tray::run(
        runtime,
        config_path,
        initial_cfg,
        host_hub_handle,
        auto_open_settings,
    )
}

/// Returns (config path, optional loaded config, auto-open-settings).
/// The tray handles embedded-hub startup internally on the new flow.
fn prepare_connect_tray_args(
    override_path: Option<&std::path::Path>,
) -> Result<(PathBuf, Option<ClientConfig>, bool)> {
    let path = match override_path {
        Some(p) => p.to_path_buf(),
        None => ClientConfig::default_path()
            .context("could not determine the default client config path")?,
    };

    let template_written = if override_path.is_none() && !path.exists() {
        match ClientConfig::write_template(&path) {
            Ok(()) => {
                tracing::info!(
                    "no client config found; wrote a placeholder at {} — the settings \
                     dialog will open so you can fill it in",
                    path.display()
                );
                true
            }
            Err(e) => {
                tracing::warn!(
                    error = %format!("{e:#}"),
                    "could not write template config at {}",
                    path.display()
                );
                false
            }
        }
    } else {
        false
    };

    let (cfg, load_failed) = match ClientConfig::load(&path) {
        Ok(c) => (Some(c), false),
        Err(e) => {
            tracing::warn!(
                error = %format!("{e:#}"),
                "no usable client config yet; tray will auto-open the settings dialog"
            );
            (None, true)
        }
    };

    // Auto-open the settings dialog at startup if we don't have a
    // usable config — that's the only path for a fresh install to
    // become useful without the user discovering the tray menu.
    let auto_open_settings = template_written || load_failed;

    Ok((path, cfg, auto_open_settings))
}

/// Returns (a stand-in path for the tray's Edit-config menu, client_cfg,
/// server JoinHandle to keep alive).
fn prepare_host_tray_args(
    runtime: &tokio::runtime::Runtime,
    override_path: Option<&std::path::Path>,
) -> Result<(PathBuf, ClientConfig, tokio::task::JoinHandle<Result<()>>)> {
    let server_cfg = ServerConfig::from_env()?;
    let (listener, addr) = runtime.block_on(clipboardwire_core::server::bind(&server_cfg))?;
    tracing::info!(addr = %addr, "hub listening (host mode)");

    let client_cfg = build_host_client_config(&server_cfg, addr.port(), override_path)?;

    let path = override_path.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        ClientConfig::default_path().unwrap_or_else(|_| PathBuf::from("config.toml"))
    });

    let hub_handle = runtime.spawn(async move {
        clipboardwire_core::server::serve(listener, server_cfg, std::future::pending()).await
    });

    Ok((path, client_cfg, hub_handle))
}

#[cfg(test)]
mod progress_log_tests {
    use super::*;
    use clipboardwire_core::client::{Direction, ProgressState, TransferId, TransferProgress};
    use std::time::{Duration, Instant};

    fn event(state: ProgressState, done: u64) -> TransferProgress {
        TransferProgress {
            file_id: TransferId::from_u128(1),
            transfer_id: None,
            direction: Direction::Send,
            peer: None,
            name: "a.bin".to_string(),
            bytes_done: done,
            bytes_total: 100,
            state,
        }
    }

    fn last(at: Instant, state: ProgressState) -> LastLogLine {
        LastLogLine { at, state }
    }

    #[test]
    fn the_send_subcommand_does_not_reuse_the_persisted_identity() {
        // A one-shot send that adopted the tray's device id would evict the
        // tray: the hub's rule is newest-wins, so the second connection kicks
        // the first. The transfer would fail and clipboard sync would stop.
        //
        // What this test can prove: the id is present, not the nil sentinel,
        // and freshly chosen per call. What it cannot prove: that the id is
        // not read from disk — `send_spawn_options` takes no config and no
        // state dir, so it has no way to read one. That is structural, and a
        // future change that adds a `&ClientConfig` parameter here should be
        // read as a red flag rather than a refactor.
        let options = send_spawn_options();
        let device = options.device.expect("send must always name a device");
        assert!(!device.is_nil(), "a nil device id is not an identity");

        let second = send_spawn_options();
        assert_ne!(
            second.device,
            Some(device),
            "each send gets its own ephemeral id, or concurrent sends collide"
        );
    }

    #[test]
    fn the_send_subcommand_advertises_no_capabilities() {
        // Same rule as the long-lived client (4.4): no pull path yet, so
        // claiming `lazy-file` would invite offers this build drops.
        let options = send_spawn_options();
        assert!(options.caps.is_empty(), "{:?}", options.caps.tokens());
    }

    #[test]
    fn throttle_is_per_file_not_per_state() {
        // The bug this replaces: `Hashing` was never throttled, so a large file
        // produced ~101 lines. Now every state shares one interval per file.
        let now = Instant::now();
        let e = event(ProgressState::Hashing, 5);
        assert!(
            !should_log_progress(Some(last(now, ProgressState::Hashing)), &e, now),
            "a second Hashing event inside the interval must be dropped"
        );
        assert!(
            should_log_progress(
                Some(last(now - Duration::from_secs(1), ProgressState::Hashing)),
                &e,
                now
            ),
            "one line per second is allowed"
        );
    }

    #[test]
    fn first_event_always_logs() {
        let e = event(ProgressState::Pulling, 1);
        assert!(should_log_progress(None, &e, Instant::now()));
    }

    #[test]
    fn state_change_logs_immediately() {
        let now = Instant::now();
        let e = event(ProgressState::Pulling, 50);
        assert!(
            should_log_progress(Some(last(now, ProgressState::Hashing)), &e, now),
            "hashing → pulling is the interesting moment, not a percentage"
        );
    }

    #[test]
    fn terminal_states_always_log() {
        let now = Instant::now();
        for state in [
            ProgressState::Done,
            ProgressState::Failed,
            ProgressState::Aborted,
        ] {
            let e = event(state, 100);
            assert!(
                should_log_progress(Some(last(now, state)), &e, now),
                "{state:?} must always print, even twice in a row"
            );
        }
    }

    #[test]
    fn a_sub_second_hashing_burst_collapses_to_one_line() {
        // Sanity: the decision function is pure, so 101 Hashing events inside
        // one second collapse to a single line.
        let start = Instant::now();
        let mut printed = 0;
        let mut previous: Option<LastLogLine> = None;
        for i in 0..101u64 {
            let now = start + Duration::from_millis(i * 5); // 505 ms total
            let e = event(ProgressState::Hashing, i);
            if should_log_progress(previous, &e, now) {
                printed += 1;
                previous = Some(last(now, ProgressState::Hashing));
            }
        }
        assert_eq!(printed, 1, "a sub-second Hashing burst is one line");
    }
}

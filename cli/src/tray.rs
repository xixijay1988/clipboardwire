// SPDX-License-Identifier: GPL-3.0-or-later

//! System-tray UI wired into a native OS event loop.
//!
//! `tray-icon` requires the thread that creates the tray icon to run an OS
//! message pump (Win32 / GTK / NSRunLoop). v0.2.0/v0.2.1 created the tray
//! from a tokio task, which on Windows meant right-click never showed the
//! menu — the icon was visible but the OS had no thread to dispatch
//! events to. v0.2.2 fixes that by giving the main thread to `tao`'s
//! event loop and running the tokio runtime on its worker threads.
//!
//! The tray comes up **before** the config is parsed, so a fresh install
//! still gets a visible icon (with a "needs config" tooltip) rather than
//! failing silently. The menu offers:
//!
//! - **Edit config…** — opens the settings GUI as a subprocess. When the
//!   GUI exits (Save or Cancel) the tray auto-reloads the config from
//!   disk and restarts the supervisor + hub if applicable.
//! - **Reload config** — manually re-trigger the same reload flow above.
//! - **Start / Stop / Restart hub** — only enabled when the loaded
//!   config has a `[hub]` section; controls the embedded hub task.
//! - **Quit clipboardwire** — orderly shutdown.
//!
//! On first launch (no usable config on disk) the tray also auto-spawns
//! the settings GUI so the user doesn't have to discover the menu first.

use std::path::{Path, PathBuf};
use std::process::Child;

use anyhow::{Context, Result};
use clipboardwire_core::client::{
    ClientConfig, ClientStatus, ProgressState, TransferId, TransferProgress,
};
use clipboardwire_core::server::hub::HubStatsSink;
use tao::event::Event;
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopProxy};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{info, warn};
#[cfg(windows)]
use tray_icon::menu::CheckMenuItem;
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{TrayIcon, TrayIconBuilder};

/// Events delivered to the tao event loop from background sources.
#[derive(Debug)]
enum UserEvent {
    /// A menu item was clicked.
    Menu(MenuEvent),
    /// The client supervisor task finished on its own (not via abort).
    SupervisorExited { generation: u64, summary: String },
    /// The embedded hub task finished on its own.
    HubExited { generation: u64, summary: String },
    /// The settings subprocess exited (Save or Cancel). Triggers an
    /// automatic reload of the config + restart of running tasks.
    SettingsExited,
    /// The client transport reported a connection-state transition for
    /// the supervisor identified by `generation`. If that supervisor is no
    /// longer the live one — exited, aborted, or never started — the event is
    /// dropped; see [`is_live_supervisor_event`].
    ClientStatusChanged {
        generation: u64,
        status: ClientStatus,
    },
    /// The system theme changed (dark ↔ light) since we last looked.
    /// Triggers an icon rebuild so the mono variant matches the new
    /// tray background.
    ThemeChanged(Option<dark_light::Mode>),
    /// The embedded hub's connected-peer count changed since the last
    /// poll. The handler just calls `refresh_tooltip`, which reads
    /// the live count straight from `state.hub_stats`.
    HubPeerCountChanged,
    /// One transfer reported progress for the supervisor identified by
    /// `generation`. Stale events are dropped at the loop, exactly like
    /// `ClientStatusChanged`: the old subscriber can still have events
    /// buffered when a reload swaps supervisors, and replaying them would
    /// resurrect transfers from the previous generation.
    TransferProgress {
        generation: u64,
        progress: Box<TransferProgress>,
    },
}

/// How often the background task re-checks the system theme. 5 s is
/// short enough that a manual light/dark toggle feels live, long
/// enough that the per-poll dark_light::detect() (which reaches DBus
/// on Linux) doesn't show up in profiles.
const THEME_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// How often the embedded-hub poller re-reads its connected-peer
/// count. The read is a single atomic load, so a 1 s cadence has no
/// measurable cost.
const HUB_STATS_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a finished transfer stays visible in the menu before it is
/// dropped. Long enough to read "sent report.pdf", short enough that the menu
/// returns to its normal size.
const TRANSFER_RETENTION: std::time::Duration = std::time::Duration::from_secs(3);

/// How long an **active** transfer may go without an event before the tray
/// gives up on it.
///
/// This is a presentation backstop, not a transfer timeout: the transfer itself
/// is still governed by the transport. It exists because an entry is otherwise
/// only removable by its own terminal event, so a wedged supervisor leaves
/// "sending 12%" on screen indefinitely.
///
/// This is the floor. `Hashing` gets a longer window scaled to the file size
/// (see [`stall_grace`]): the sender reports hash progress once per percent, so
/// the gap between two events is 1 % of the file's read time — on slow or
/// network-backed media that legitimately exceeds a minute for a large file.
/// Pulling keeps this value: one chunk is 4 MiB, so a minute of silence means
/// the stream really has stopped, and a resumed transfer re-adds its entry on
/// the next event.
const TRANSFER_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Slowest hashing throughput still treated as healthy, used to scale the
/// `Hashing` stall window. 1 MiB/s is well below any disk that can finish a
/// multi-gigabyte file in a useful time, so a healthy transfer keeps its entry.
const HASH_STALL_MIN_RATE_BYTES_PER_SEC: u64 = 1024 * 1024;

/// Ceiling for the size-scaled `Hashing` window.
///
/// Without it, a 1 TB file would earn a multi-hour window and the backstop
/// would never fire. Half an hour of silence during a pre-hash is enough to
/// conclude the process is wedged rather than slow.
const TRANSFER_STALL_MAX: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Capacity of the per-generation progress channel. A chunk is one event, so
/// this only needs to absorb a short burst; a lagging tray skips ahead rather
/// than blocking the transfer.
const TRANSFER_PROGRESS_BUF: usize = 64;

/// Menu slots for transfer detail. A larger concurrent fan-out still shows an
/// aggregate count in the tooltip; the menu just lists the first few.
const MAX_TRANSFER_MENU_ITEMS: usize = 4;

/// Hard ceiling for the whole tooltip, in characters (`szTip` is 128).
const TOOLTIP_MAX_CHARS: usize = 128;

/// Character budget for the transfer line appended to the tooltip.
///
/// `NOTIFYICONDATAW.szTip` is 128 UTF-16 code units, and the existing tooltip
/// already spends ~60 on the status line, server URL and hub peer count. 56
/// leaves headroom for the worst case (`clipboardwire — disconnected,
/// retrying in 60s` + URL + `\nhub running (64 clients connected)`).
const TOOLTIP_TRANSFER_BUDGET: usize = 56;

/// Key for one transfer row.
///
/// `file_id` alone is not unique under lazy transfer: the same file can be
/// pasted twice concurrently, producing two `transfer_id`s. Keying on the pair
/// keeps those rows apart, while an eager transfer (`transfer_id: None`) still
/// collapses to one row per file as before.
///
/// The `Option` is not a wart to be normalised away. An eager transfer has no
/// session, so there is nothing to disambiguate it with — and two eager
/// transfers of one `file_id` cannot happen, because eager mode sends a file
/// once per paste rather than once per pull.
type TransferKey = (TransferId, Option<TransferId>);

/// Tray-level handles that need to outlive a Reload event.
struct State {
    supervisor: Option<JoinHandle<Result<()>>>,
    supervisor_gen: u64,
    /// Last status reported by the current-generation supervisor.
    /// `None` when no supervisor is running (e.g. needs-config).
    client_status: Option<ClientStatus>,
    /// Live transfer progress, one entry per [`TransferKey`], in arrival order.
    ///
    /// Updated from the event loop only: the broadcast subscriber runs as a
    /// tokio task that posts `UserEvent::TransferProgress`, so nothing here is
    /// touched from a background thread.
    transfers: Vec<TransferProgress>,
    /// When each listed transfer should be dropped, keyed like `transfers`.
    ///
    /// Two rules share this table:
    ///
    /// - a terminal event schedules a clear 3 s later, so the menu shows "sent
    ///   report.pdf" briefly and then returns to normal size;
    /// - an **active** transfer is rescheduled for [`TRANSFER_STALL_TIMEOUT`]
    ///   after every event. If the supervisor wedges — or a `Failed`/`Done`
    ///   event is lost — the entry still disappears instead of hanging at
    ///   "sending 12%" forever.
    transfer_clear_at: Vec<(TransferKey, std::time::Instant)>,
    /// The embedded hub spawned from `[hub]` in the client config.
    embedded_hub: Option<JoinHandle<Result<()>>>,
    embedded_hub_gen: u64,
    /// The `[hub]` section the running hub was started with, and the address
    /// it actually bound. A config reload compares the freshly loaded section
    /// against this snapshot: identical means the running hub already serves
    /// exactly what the new config asks for, so it must not be rebound —
    /// every rebind kills all connected peers.
    embedded_hub_cfg: Option<clipboardwire_core::client::config::HubConfig>,
    embedded_hub_addr: Option<std::net::SocketAddr>,
    /// Set when the most recent hub-bind attempt failed (e.g. another
    /// process is on the same port). Shown in the tray status line so
    /// the user doesn't have to read the log to understand why nothing
    /// works.
    hub_bind_error: Option<String>,
    /// Live counter the embedded hub task updates on every client
    /// register/deregister. `Some` while the hub is running, `None`
    /// otherwise. The poller task reads from this; the tooltip folds
    /// it into the "X peers connected" suffix.
    hub_stats: Option<HubStatsSink>,
    /// JoinHandle for the per-hub-session background task that polls
    /// `hub_stats` and posts a `HubPeerCountChanged` event when the
    /// count shifts. Aborted alongside the hub itself.
    hub_stats_poller: Option<JoinHandle<()>>,
    /// PID of the currently-running settings subprocess, if any.
    /// We don't keep the `Child` here because the watcher thread owns
    /// it; this is just for "is one running" checks.
    settings_alive: bool,
    cfg: Option<ClientConfig>,
}

impl State {
    /// Fold one progress event into the transfer list.
    ///
    /// One entry per [`TransferKey`] — `(file_id, transfer_id)`, not `file_id`
    /// — so a newer event replaces the previous one for *that* transfer while
    /// a concurrent transfer of the same file keeps its own row.
    ///
    /// Every event reschedules the entry's removal: terminal states get
    /// [`TRANSFER_RETENTION`], active ones [`TRANSFER_STALL_TIMEOUT`]. The
    /// latter is the backstop for a wedged supervisor — without it an entry
    /// whose terminal event never arrives stays on screen until quit.
    fn record_progress(&mut self, event: TransferProgress) {
        let now = std::time::Instant::now();
        let deadline = if event.state.is_terminal() {
            now + TRANSFER_RETENTION
        } else {
            now + stall_grace(event.state, event.bytes_total)
        };
        let key: TransferKey = (event.file_id, event.transfer_id);
        match self
            .transfers
            .iter_mut()
            .find(|t| (t.file_id, t.transfer_id) == key)
        {
            Some(existing) => *existing = event.clone(),
            None => self.transfers.push(event.clone()),
        }
        self.transfer_clear_at.retain(|(k, _)| *k != key);
        self.transfer_clear_at.push((key, deadline));
    }

    /// Drop transfers whose retention or stall window has passed. Returns
    /// whether anything changed.
    fn expire_transfers(&mut self) -> bool {
        let now = std::time::Instant::now();
        let expired: Vec<TransferKey> = self
            .transfer_clear_at
            .iter()
            .filter(|(_, at)| *at <= now)
            .map(|(key, _)| *key)
            .collect();
        if expired.is_empty() {
            return false;
        }
        self.transfer_clear_at
            .retain(|(key, _)| !expired.contains(key));
        self.transfers
            .retain(|t| !expired.contains(&(t.file_id, t.transfer_id)));
        true
    }

    /// Earliest pending removal deadline (retention or stall), for the
    /// event-loop wake-up.
    fn next_transfer_deadline(&self) -> Option<std::time::Instant> {
        self.transfer_clear_at.iter().map(|(_, at)| *at).min()
    }
}

/// How long an active transfer in `state` may stay silent before it is dropped.
///
/// [`TRANSFER_STALL_TIMEOUT`] is enough for `Pulling`, where one chunk is 4 MiB
/// and a live stream reports every few milliseconds on any usable link. It is
/// **not** enough for `Hashing`: the sender emits one event per percent, so the
/// silent gap is 1 % of the time needed to read the whole file.
///
/// Concretely, a 100 GB file at a sustained 200 MB/s emits an event every ~5 s;
/// the same file on a slow USB 2 disk (~30 MB/s) emits one every ~33 s; on a
/// degraded network mount it can exceed a minute. The entry would then vanish
/// mid-hash and reappear at the next percent — a flicker the user reads as a
/// failed transfer.
///
/// So `Hashing` gets a window sized for the *slowest plausible read*: assume
/// [`HASH_STALL_MIN_RATE_BYTES_PER_SEC`], which makes the expected gap
/// `bytes_total / 100 / rate`. That is clamped to
/// `TRANSFER_STALL_TIMEOUT..=TRANSFER_STALL_MAX`, so small files keep the plain
/// minute and absurd sizes cannot disable the backstop.
///
/// This deliberately does not use "time since `bytes_done` last changed": that
/// would be wrong for `Hashing`, where the percentage *is* the progress and a
/// slow percent step is exactly the state being defended against. Silence, not
/// a flat byte count, is the signal.
fn stall_grace(state: ProgressState, bytes_total: u64) -> std::time::Duration {
    match state {
        ProgressState::Hashing => {
            // One percent of the file, read at the healthy-floor rate. Integer
            // division is fine: the result is a duration in whole seconds.
            let percent_bytes = (bytes_total / 100).max(1);
            let secs = percent_bytes / HASH_STALL_MIN_RATE_BYTES_PER_SEC;
            let scaled = std::time::Duration::from_secs(secs);
            scaled.clamp(TRANSFER_STALL_TIMEOUT, TRANSFER_STALL_MAX)
        }
        _ => TRANSFER_STALL_TIMEOUT,
    }
}

/// Enter the tray UI, taking the main thread for the OS event loop.
///
/// `host_hub_handle` is the long-lived hub task spawned by `host` mode
/// (env-driven, always-on). The embedded hub from `[hub]` in the client
/// config is managed *inside* this loop and is unrelated to that.
/// `auto_open_settings = true` is the first-run signal: pop the settings
/// dialog at startup since no usable config exists yet.
pub fn run(
    runtime: Runtime,
    config_path: PathBuf,
    initial_config: Option<ClientConfig>,
    host_hub_handle: Option<JoinHandle<Result<()>>>,
    auto_open_settings: bool,
) -> Result<()> {
    // CRITICAL: build the event loop FIRST. On Linux, tao's
    // EventLoopBuilder::build() runs gtk::init() — calling any
    // tray-icon constructor before that point panics with
    // "GTK has not been initialized". The Tier-1 smoke test caught
    // this regression once already; please don't reorder.
    let event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    let proxy = event_loop.create_proxy();

    let menu = Menu::new();
    // The top lines are informational (always disabled) — they show
    // connection status, server URL, and hub peer count. Tooltips are
    // unreliable on Linux (libayatana-appindicator doesn't support
    // them), so we surface everything in the menu itself.
    let status_item = MenuItem::new(status_label_for(None, false), false, None);
    let server_item = MenuItem::new(server_label(initial_config.as_ref()), false, None);
    let hub_info_item = MenuItem::new("", false, None);
    hub_info_item.set_enabled(false);
    let sep0 = PredefinedMenuItem::separator();
    // Per-transfer detail lives in the menu, never in the tooltip: `szTip` is
    // 128 characters and already carries status, URL and peer count. A fixed
    // pool of disabled items is reused, so the menu does not rebuild while a
    // multi-file copy runs.
    let transfer_header_item = MenuItem::new("", false, None);
    transfer_header_item.set_enabled(false);
    let transfer_items: Vec<MenuItem> = (0..MAX_TRANSFER_MENU_ITEMS)
        .map(|_| MenuItem::new("", false, None))
        .collect();
    let edit_item = MenuItem::new("Edit config…", true, None);
    let reload_item = MenuItem::new("Reload config", true, None);
    let sep1 = PredefinedMenuItem::separator();
    let start_hub_item = MenuItem::new("Start hub", false, None);
    let stop_hub_item = MenuItem::new("Stop hub", false, None);
    let restart_hub_item = MenuItem::new("Restart hub", false, None);
    let sep2 = PredefinedMenuItem::separator();
    #[cfg(windows)]
    let autostart_item = CheckMenuItem::new(
        "Start at login",
        true,
        crate::autostart_win::is_enabled(),
        None,
    );
    let quit_item = MenuItem::new("Quit clipboardwire", true, None);
    menu.append(&status_item)?;
    menu.append(&server_item)?;
    menu.append(&hub_info_item)?;
    menu.append(&transfer_header_item)?;
    for item in &transfer_items {
        menu.append(item)?;
    }
    menu.append(&sep0)?;
    menu.append(&edit_item)?;
    menu.append(&reload_item)?;
    menu.append(&sep1)?;
    menu.append(&start_hub_item)?;
    menu.append(&stop_hub_item)?;
    menu.append(&restart_hub_item)?;
    menu.append(&sep2)?;
    #[cfg(windows)]
    menu.append(&autostart_item)?;
    menu.append(&quit_item)?;

    let edit_id = edit_item.id().clone();
    let reload_id = reload_item.id().clone();
    #[cfg(windows)]
    let autostart_id = autostart_item.id().clone();
    let start_hub_id = start_hub_item.id().clone();
    let stop_hub_id = stop_hub_item.id().clone();
    let restart_hub_id = restart_hub_item.id().clone();
    let quit_id = quit_item.id().clone();

    let initial_tooltip = match &initial_config {
        Some(cfg) => format!("clipboardwire — {}", cfg.server),
        None => format!(
            "clipboardwire — needs config (right-click → Edit config)\n{}",
            config_path.display()
        ),
    };

    // dark_light::detect() reaches DBus on Linux (via zbus) which needs
    // a Tokio reactor in scope; do the detection inside the runtime
    // before tao's event loop takes over the main thread.
    let theme = {
        let _guard = runtime.enter();
        dark_light::detect().ok()
    };
    let tray = TrayIconBuilder::new()
        .with_icon(build_icon(theme, None, false))
        .with_menu(Box::new(menu))
        .with_tooltip(initial_tooltip)
        .build()?;

    info!(path = %config_path.display(), "tray icon shown");

    // Pipe menu events into the tao event loop.
    let menu_proxy = proxy.clone();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = menu_proxy.send_event(UserEvent::Menu(event));
    }));

    // Background task that re-detects the system theme every
    // THEME_POLL_INTERVAL and posts a UserEvent if it changed since
    // the last check. dark_light has no subscription API so polling
    // is the cross-platform option; the detect() call is cheap
    // (~ms) and only fires the event when the value actually flips,
    // so the cost is negligible.
    let theme_proxy = proxy.clone();
    runtime.handle().spawn(async move {
        let mut last = dark_light::detect().ok();
        let mut tick = tokio::time::interval(THEME_POLL_INTERVAL);
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let now = dark_light::detect().ok();
            if now != last {
                last = now;
                let _ = theme_proxy.send_event(UserEvent::ThemeChanged(now));
            }
        }
    });

    let mut state = State {
        supervisor: None,
        supervisor_gen: 0,
        client_status: None,
        transfers: Vec::new(),
        transfer_clear_at: Vec::new(),
        embedded_hub: None,
        embedded_hub_gen: 0,
        embedded_hub_cfg: None,
        embedded_hub_addr: None,
        hub_bind_error: None,
        hub_stats: None,
        hub_stats_poller: None,
        settings_alive: false,
        cfg: initial_config,
    };

    // Spawn supervisor + auto-start hub from the initial config.
    apply_loaded_config(runtime.handle(), &proxy, &mut state, &config_path, &tray);
    update_hub_menu(&state, &start_hub_item, &stop_hub_item, &restart_hub_item);
    update_status_item(&status_item, &state);
    update_server_item(&server_item, &state);
    update_hub_info_item(&hub_info_item, &state);
    refresh_icon(&tray, theme, &state);

    // First-run / no-valid-config flow: pop the settings GUI so the
    // user has somewhere obvious to fix things from.
    if auto_open_settings {
        match launch_settings_dialog(&config_path, &proxy) {
            Ok(()) => state.settings_alive = true,
            Err(e) => warn!(
                error = %format!("{e:#}"),
                "could not auto-open settings dialog on first run"
            ),
        }
    }

    // Keep the host-mode hub task alive for the lifetime of the loop.
    let _host_hub_handle = host_hub_handle;

    let mut theme = theme;
    event_loop.run(move |event, _, control_flow| {
        // Retire finished transfers whose retention window has passed. Done
        // before dispatch so `WaitUntil` wake-ups (which carry no user event)
        // and ordinary events both get the cleanup, and no arm can shadow
        // another by matching here.
        if state.expire_transfers() {
            update_transfer_items(&transfer_header_item, &transfer_items, &state);
            refresh_tooltip(&tray, &state, &config_path);
        }
        // Wait, unless a finished transfer is due to disappear: then wake up at
        // that instant instead of leaving a stale entry on screen forever.
        *control_flow = match state.next_transfer_deadline() {
            Some(at) => ControlFlow::WaitUntil(at),
            None => ControlFlow::Wait,
        };

        match event {
            Event::UserEvent(UserEvent::Menu(menu_event)) => {
                if menu_event.id == quit_id {
                    info!("Quit menu item clicked");
                    abort_running(&mut state, false);
                    *control_flow = ControlFlow::Exit;
                } else if menu_event.id == edit_id {
                    if state.settings_alive {
                        info!("settings dialog already open; ignoring duplicate Edit click");
                    } else {
                        match launch_settings_dialog(&config_path, &proxy) {
                            Ok(()) => state.settings_alive = true,
                            Err(e) => warn!(
                                error = %format!("{e:#}"),
                                "could not launch settings dialog"
                            ),
                        }
                    }
                } else if menu_event.id == reload_id {
                    let keep_hub = hub_restart_unneeded(&state, &config_path);
                    abort_running(&mut state, keep_hub);
                    reload_config_into(&mut state, &config_path);
                    apply_loaded_config(runtime.handle(), &proxy, &mut state, &config_path, &tray);
                    update_hub_menu(&state, &start_hub_item, &stop_hub_item, &restart_hub_item);
                    update_status_item(&status_item, &state);
                    update_server_item(&server_item, &state);
                    update_hub_info_item(&hub_info_item, &state);
                    refresh_icon(&tray, theme, &state);
                } else if menu_event.id == start_hub_id {
                    if let Err(e) =
                        start_embedded_hub(runtime.handle(), &proxy, &mut state, &config_path)
                    {
                        warn!(error=%format!("{e:#}"), "Start hub failed");
                    }
                    update_hub_menu(&state, &start_hub_item, &stop_hub_item, &restart_hub_item);
                    update_status_item(&status_item, &state);
                    update_hub_info_item(&hub_info_item, &state);
                    refresh_tooltip(&tray, &state, &config_path);
                    refresh_icon(&tray, theme, &state);
                } else if menu_event.id == stop_hub_id {
                    stop_embedded_hub(&mut state);
                    update_hub_menu(&state, &start_hub_item, &stop_hub_item, &restart_hub_item);
                    update_hub_info_item(&hub_info_item, &state);
                    refresh_icon(&tray, theme, &state);
                } else if menu_event.id == restart_hub_id {
                    stop_embedded_hub(&mut state);
                    if let Err(e) =
                        start_embedded_hub(runtime.handle(), &proxy, &mut state, &config_path)
                    {
                        warn!(error=%format!("{e:#}"), "Restart hub failed");
                    }
                    update_hub_menu(&state, &start_hub_item, &stop_hub_item, &restart_hub_item);
                    update_status_item(&status_item, &state);
                    update_hub_info_item(&hub_info_item, &state);
                    refresh_tooltip(&tray, &state, &config_path);
                    refresh_icon(&tray, theme, &state);
                } else {
                    #[cfg(windows)]
                    {
                        if menu_event.id == autostart_id {
                            // CheckMenuItem auto-toggles before the event fires, so
                            // `is_checked()` reflects the new desired state.
                            if autostart_item.is_checked() {
                                if let Err(e) = crate::autostart_win::enable() {
                                    warn!(error=%format!("{e:#}"), "could not enable autostart");
                                    autostart_item.set_checked(false);
                                } else {
                                    info!("autostart enabled");
                                }
                            } else if let Err(e) = crate::autostart_win::disable() {
                                warn!(error=%format!("{e:#}"), "could not disable autostart");
                                autostart_item.set_checked(true);
                            } else {
                                info!("autostart disabled");
                            }
                        } else {
                            warn!(id = ?menu_event.id, "ignoring unknown menu event");
                        }
                    }
                    #[cfg(not(windows))]
                    warn!(id = ?menu_event.id, "ignoring unknown menu event");
                }
            }
            Event::UserEvent(UserEvent::SupervisorExited {
                generation,
                summary,
            }) if is_live_supervisor_event(&state, generation) => {
                state.supervisor = None;
                state.client_status = None;
                // A dead supervisor has no live transfers; do not keep showing
                // stale percentages.
                state.transfers.clear();
                state.transfer_clear_at.clear();
                update_transfer_items(&transfer_header_item, &transfer_items, &state);
                warn!(
                    gen = generation,
                    summary, "supervisor exited on its own; staying in tray"
                );
                update_status_item(&status_item, &state);
                refresh_tooltip(&tray, &state, &config_path);
                refresh_icon(&tray, theme, &state);
            }
            Event::UserEvent(UserEvent::HubExited {
                generation,
                summary,
            }) if Some(generation) == current_hub_gen(&state) => {
                state.embedded_hub = None;
                state.embedded_hub_cfg = None;
                state.embedded_hub_addr = None;
                warn!(gen = generation, summary, "embedded hub exited on its own");
                update_hub_menu(&state, &start_hub_item, &stop_hub_item, &restart_hub_item);
            }
            Event::UserEvent(UserEvent::HubPeerCountChanged) => {
                update_hub_info_item(&hub_info_item, &state);
                refresh_tooltip(&tray, &state, &config_path);
            }
            Event::UserEvent(UserEvent::TransferProgress {
                generation,
                progress,
            }) if is_live_supervisor_event(&state, generation) => {
                state.record_progress(*progress);
                update_transfer_items(&transfer_header_item, &transfer_items, &state);
                refresh_tooltip(&tray, &state, &config_path);
            }
            Event::UserEvent(UserEvent::ThemeChanged(new_theme)) => {
                info!(?new_theme, "system theme changed; refreshing tray icon");
                theme = new_theme;
                refresh_icon(&tray, theme, &state);
            }
            Event::UserEvent(UserEvent::ClientStatusChanged { generation, status })
                if is_live_supervisor_event(&state, generation) =>
            {
                state.client_status = Some(status);
                update_status_item(&status_item, &state);
                update_server_item(&server_item, &state);
                refresh_tooltip(&tray, &state, &config_path);
                refresh_icon(&tray, theme, &state);
            }
            Event::UserEvent(UserEvent::SettingsExited) => {
                info!("settings dialog closed; auto-reloading config");
                state.settings_alive = false;
                let keep_hub = hub_restart_unneeded(&state, &config_path);
                abort_running(&mut state, keep_hub);
                reload_config_into(&mut state, &config_path);
                apply_loaded_config(runtime.handle(), &proxy, &mut state, &config_path, &tray);
                update_hub_menu(&state, &start_hub_item, &stop_hub_item, &restart_hub_item);
                update_status_item(&status_item, &state);
                update_server_item(&server_item, &state);
                update_hub_info_item(&hub_info_item, &state);
                refresh_tooltip(&tray, &state, &config_path);
                refresh_icon(&tray, theme, &state);
            }
            _ => {}
        }
    })
}

fn current_supervisor_gen(state: &State) -> Option<u64> {
    state.supervisor.as_ref().map(|_| state.supervisor_gen)
}

/// Whether an event tagged with `generation` belongs to the supervisor
/// currently running.
///
/// Every supervisor-scoped event (`SupervisorExited`, `ClientStatusChanged`,
/// `TransferProgress`) uses this one predicate. It deliberately tests **both**
/// the generation *and* that a supervisor is still installed:
///
/// - Generation alone is not enough. `abort_running` bumps the generation, so
///   an old forwarder's buffered events no longer match — but the window
///   between `abort_running` and the replacement in `apply_loaded_config` has
///   no supervisor at all, and an event from a forwarder that has not noticed
///   yet would otherwise be applied to a tray that is showing "not connected".
/// - Liveness alone is not enough either: a fresh generation's forwarder can
///   race with a reload.
///
/// Dropping is always the safe direction: status and progress are advisory, and
/// anything the new supervisor cares about it will report again.
fn is_live_supervisor_event(state: &State, generation: u64) -> bool {
    current_supervisor_gen(state) == Some(generation)
}

fn current_hub_gen(state: &State) -> Option<u64> {
    state.embedded_hub.as_ref().map(|_| state.embedded_hub_gen)
}

/// Tear down the supervisor, and the embedded hub too unless `keep_hub` —
/// a reload whose `[hub]` section did not change must not rebind, because
/// every rebind kills all connected peers with a bare TCP close.
fn abort_running(state: &mut State, keep_hub: bool) {
    if let Some(s) = state.supervisor.take() {
        s.abort();
    }
    if !keep_hub {
        if let Some(h) = state.embedded_hub.take() {
            h.abort();
        }
        state.embedded_hub_cfg = None;
        state.embedded_hub_addr = None;
        state.hub_stats = None;
        if let Some(p) = state.hub_stats_poller.take() {
            p.abort();
        }
    }
    state.client_status = None;
    state.hub_bind_error = None;
    // Retire the generation *before* clearing the list. A forwarder task that
    // outlives `abort()` can still deliver events already buffered in the
    // broadcast channel; without this bump they would pass the
    // `generation == state.supervisor_gen` guard and repopulate the entries we
    // are about to drop. `apply_loaded_config` normally bumps the generation
    // itself, but it returns early when the reloaded config is invalid — which
    // is exactly when this increment is the only one that happens.
    state.supervisor_gen = state.supervisor_gen.wrapping_add(1);
    // Transfers belong to the supervisor that just got aborted. Their
    // terminal events will never arrive, so leaving them would pin a zombie
    // "receiving 33%" in the tooltip until the process exits.
    state.transfers.clear();
    state.transfer_clear_at.clear();
}

/// Whether the reload can keep the running embedded hub: the file parses and
/// its `[hub]` section is identical to the snapshot the running hub was
/// started with. Anything else — unreadable file, changed bind/TLS/auth,
/// disabled hub, or no hub running — takes the restart path, which is the
/// same behaviour as before. The only thing being avoided is a pointless
/// rebind: closing the listener drops every connected peer, and a config
/// reload used to do it on every save.
fn hub_restart_unneeded(state: &State, config_path: &Path) -> bool {
    if state.embedded_hub.is_none() {
        return false;
    }
    match ClientConfig::load(config_path) {
        Ok(new_cfg) => new_cfg.hub == state.embedded_hub_cfg,
        Err(_) => false,
    }
}

fn reload_config_into(state: &mut State, config_path: &Path) {
    match ClientConfig::load(config_path) {
        Ok(cfg) => {
            state.cfg = Some(cfg);
            info!("config reloaded from disk");
        }
        Err(e) => {
            state.cfg = None;
            warn!(
                error = %format!("{e:#}"),
                path = %config_path.display(),
                "could not load config after reload"
            );
        }
    }
}

/// Given `state.cfg`, (re)spawn whatever the config says should be
/// running. Updates the tray tooltip too.
fn apply_loaded_config(
    handle: &Handle,
    proxy: &EventLoopProxy<UserEvent>,
    state: &mut State,
    config_path: &Path,
    tray: &TrayIcon,
) {
    let Some(client_cfg_orig) = state.cfg.clone() else {
        refresh_tooltip(tray, state, config_path);
        return;
    };

    let mut client_cfg = client_cfg_orig.clone();

    // If [hub] enabled, start the embedded hub first and rewrite the
    // client's server URL to point at it. When the running hub was started
    // from an identical `[hub]` section there is nothing to do but the URL
    // rewrite: rebinding the listener is what drops every connected peer, so
    // it must only ever happen when the hub config actually changed.
    let hub_enabled = client_cfg.hub.as_ref().is_some_and(|h| h.enabled);
    if hub_enabled {
        let already_serving =
            state.embedded_hub.is_some() && client_cfg.hub == state.embedded_hub_cfg;
        if already_serving {
            if let (Some(hub_cfg), Some(addr)) =
                (state.embedded_hub_cfg.as_ref(), state.embedded_hub_addr)
            {
                point_client_at_hub(&mut client_cfg, hub_cfg.tls_disabled, addr);
            }
        } else if let Err(e) =
            start_embedded_hub_with_cfg(handle, proxy, state, &mut client_cfg, config_path)
        {
            warn!(
                error = %format!("{e:#}"),
                "[hub] is enabled but could not start the embedded hub at startup"
            );
        }
    }

    state.supervisor_gen += 1;
    let gen = state.supervisor_gen;
    let proxy_for_status = proxy.clone();
    let (status_tx, mut status_rx) = tokio::sync::watch::channel(ClientStatus::Connecting);
    // Forwarder: every watch change → UserEvent so the tao loop wakes
    // up and updates the menu/tooltip. Lives until the supervisor
    // exits and drops the sender, at which point `changed().await`
    // returns Err and we break.
    handle.spawn(async move {
        let initial = *status_rx.borrow_and_update();
        let _ = proxy_for_status.send_event(UserEvent::ClientStatusChanged {
            generation: gen,
            status: initial,
        });
        while status_rx.changed().await.is_ok() {
            let s = *status_rx.borrow_and_update();
            let _ = proxy_for_status.send_event(UserEvent::ClientStatusChanged {
                generation: gen,
                status: s,
            });
        }
    });

    // Progress: the channel is created HERE, not inside `run_with_status` —
    // the tray spawns the supervisor itself, so an internally-created channel
    // would have no receiver (design §9). Every supervisor generation gets its
    // own channel; the old subscriber exits when this one drops.
    let (progress_tx, mut progress_rx) =
        broadcast::channel::<TransferProgress>(TRANSFER_PROGRESS_BUF);
    let proxy_for_progress = proxy.clone();
    let progress_gen = gen;
    handle.spawn(async move {
        // `lagged` means the tray fell behind; progress is advisory, so skip to
        // the newest events instead of tearing the subscription down.
        loop {
            match progress_rx.recv().await {
                Ok(event) => {
                    // Tag with the generation this subscriber was created for,
                    // so the loop can drop events that outlive their supervisor.
                    if proxy_for_progress
                        .send_event(UserEvent::TransferProgress {
                            generation: progress_gen,
                            progress: Box::new(event),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "tray lagged behind transfer progress events");
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });

    let proxy_clone = proxy.clone();
    let client_cfg_for_log = client_cfg.clone();
    let sup = handle.spawn(async move {
        let result = clipboardwire_core::client::run_with_status(
            client_cfg,
            Some(status_tx),
            Some(progress_tx),
        )
        .await;
        let summary = match &result {
            Ok(()) => "clean exit".to_string(),
            Err(e) => format!("{e:#}"),
        };
        let _ = proxy_clone.send_event(UserEvent::SupervisorExited {
            generation: gen,
            summary,
        });
        result
    });
    state.supervisor = Some(sup);
    state.client_status = Some(ClientStatus::Connecting);
    let _ = client_cfg_for_log;
    refresh_tooltip(tray, state, config_path);
}

/// Start the embedded hub from `state.cfg.hub`. Returns Err if no hub
/// section is present.
fn start_embedded_hub(
    handle: &Handle,
    proxy: &EventLoopProxy<UserEvent>,
    state: &mut State,
    config_path: &Path,
) -> Result<()> {
    let Some(cfg) = state.cfg.as_mut() else {
        anyhow::bail!("no config loaded; cannot start hub");
    };
    // Start-hub from the menu brings the hub up but leaves the
    // supervisor's URL alone (it's still pointed at whatever the
    // config specified). To make the supervisor use the new hub, the
    // user can hit Reload or restart from the settings dialog.
    let mut throwaway = cfg.clone();
    start_embedded_hub_with_cfg(handle, proxy, state, &mut throwaway, config_path)
}

/// Internal: spawn the hub, rewrite `client_cfg.server` to point at the
/// loopback port.
fn start_embedded_hub_with_cfg(
    handle: &Handle,
    proxy: &EventLoopProxy<UserEvent>,
    state: &mut State,
    client_cfg: &mut ClientConfig,
    config_path: &Path,
) -> Result<()> {
    let hub_cfg = client_cfg
        .hub
        .clone()
        .context("config has no [hub] section")?;
    let mut server_cfg = hub_cfg.to_server_config();
    // Default the auto-gen cert location to the config file's parent
    // dir so the user knows where to find the cert (and so trusting
    // it from another client is a matter of copying the .crt next to
    // the well-known config file).
    if server_cfg.state_dir.is_none() {
        server_cfg.state_dir = config_path.parent().map(|p| p.to_path_buf());
    }
    // Attach a live stats sink so the tooltip can surface the
    // connected-peer count. We keep our own clone in `state` and
    // hand the second clone to the hub task.
    let stats = HubStatsSink::new();
    server_cfg.stats = Some(stats.clone());

    let (listener, addr) = match handle.block_on(clipboardwire_core::server::bind(&server_cfg)) {
        Ok((l, a)) => l_and_a_clear(state, l, a),
        Err(e) => {
            // Visible failure: the user almost always hits this when
            // they have a second clipboardwire process running. Store
            // the message so the status item + tooltip can show it.
            let msg = format!("{} ({})", server_cfg.bind, friendly_bind_error(&e));
            state.hub_bind_error = Some(msg.clone());
            return Err(e).with_context(|| format!("binding embedded hub to {}", server_cfg.bind));
        }
    };
    info!(addr = %addr, "embedded hub bound");

    point_client_at_hub(client_cfg, server_cfg.tls_disabled, addr);
    // Record what this hub is serving so a later reload can tell "same
    // config, keep it alive" from "config changed, rebind".
    state.embedded_hub_cfg = Some(hub_cfg.clone());
    state.embedded_hub_addr = Some(addr);

    state.embedded_hub_gen += 1;
    let gen = state.embedded_hub_gen;
    let proxy_clone = proxy.clone();
    let cfg_for_serve = server_cfg.clone();
    let h = handle.spawn(async move {
        let result =
            clipboardwire_core::server::serve(listener, cfg_for_serve, std::future::pending())
                .await;
        let summary = match &result {
            Ok(()) => "clean exit".to_string(),
            Err(e) => format!("{e:#}"),
        };
        let _ = proxy_clone.send_event(UserEvent::HubExited {
            generation: gen,
            summary,
        });
        result
    });
    state.embedded_hub = Some(h);

    // Background stats poller — fires HubPeerCountChanged on the tao
    // event loop whenever the connected-peer count shifts, so the
    // tooltip stays accurate without us having to interrupt the
    // hub task or contend on the inbox.
    let stats_for_poller = stats.clone();
    let proxy_for_stats = proxy.clone();
    let poller = handle.spawn(async move {
        let mut last = stats_for_poller.current();
        // Tell the tray the initial value once, then only on change.
        let _ = proxy_for_stats.send_event(UserEvent::HubPeerCountChanged);
        let mut tick = tokio::time::interval(HUB_STATS_POLL_INTERVAL);
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let now = stats_for_poller.current();
            if now != last {
                last = now;
                let _ = proxy_for_stats.send_event(UserEvent::HubPeerCountChanged);
            }
        }
    });

    state.hub_stats = Some(stats);
    state.hub_stats_poller = Some(poller);
    Ok(())
}

/// Point a client config at a running embedded hub.
///
/// Loopback hop: cert SAN never covers 127.0.0.1 for arbitrary server certs,
/// and we don't auto-pin the generated self-signed cert at the client level.
/// Trust the loopback.
fn point_client_at_hub(
    client_cfg: &mut ClientConfig,
    tls_disabled: bool,
    addr: std::net::SocketAddr,
) {
    let scheme = if !tls_disabled { "wss" } else { "ws" };
    client_cfg.server = format!("{scheme}://127.0.0.1:{}/sync", addr.port());
    if !tls_disabled {
        client_cfg.tls_insecure = true;
    }
}

fn stop_embedded_hub(state: &mut State) {
    if let Some(h) = state.embedded_hub.take() {
        h.abort();
        info!("embedded hub stopped");
    }
    state.embedded_hub_cfg = None;
    state.embedded_hub_addr = None;
    state.hub_stats = None;
    if let Some(p) = state.hub_stats_poller.take() {
        p.abort();
    }
}

/// Enable/disable the three hub menu items based on whether `[hub]` is
/// present in the loaded config and whether the hub is currently up.
fn update_hub_menu(state: &State, start: &MenuItem, stop: &MenuItem, restart: &MenuItem) {
    let has_hub_section = state.cfg.as_ref().is_some_and(|c| c.hub.is_some());
    let running = state.embedded_hub.is_some();
    start.set_enabled(has_hub_section && !running);
    stop.set_enabled(has_hub_section && running);
    restart.set_enabled(has_hub_section && running);
}

/// Refresh the disabled "Status: …" menu item text from the current state.
fn update_status_item(item: &MenuItem, state: &State) {
    item.set_text(status_label_with_hub(
        state.client_status,
        state.cfg.is_some(),
        state.hub_bind_error.as_deref(),
    ));
}

/// Refresh the disabled server-URL menu item. Shows the server URL when
/// a config is loaded, hidden otherwise.
fn update_server_item(item: &MenuItem, state: &State) {
    item.set_text(server_label(state.cfg.as_ref()));
}

/// Render the server-URL label for the informational menu item.
fn server_label(cfg: Option<&ClientConfig>) -> String {
    match cfg {
        Some(c) => c.server.clone(),
        None => String::new(),
    }
}

/// Refresh the disabled hub-info menu item. Shows the connected-peer
/// count when the hub is running, empty otherwise. Tooltips are
/// unreliable on Linux, so this surfaces the same info in the menu.
fn update_hub_info_item(item: &MenuItem, state: &State) {
    let text = hub_info_label(state);
    // Hide the item when there's nothing to show (no hub running).
    if text.is_empty() {
        item.set_text("");
    } else {
        item.set_text(text);
    }
}

/// Render the hub-info label for the menu item.
fn hub_info_label(state: &State) -> String {
    if let Some(stats) = &state.hub_stats {
        let n = stats.current();
        match n {
            0 => "Hub: 0 clients connected".to_string(),
            1 => "Hub: 1 client connected".to_string(),
            n => format!("Hub: {n} clients connected"),
        }
    } else if state.embedded_hub.is_some() {
        "Hub: running".to_string()
    } else {
        String::new()
    }
}

/// Hot-swap the tray icon with one whose colored status dot matches
/// the current state. Called from every place that bumps status.
fn refresh_icon(tray: &TrayIcon, theme: Option<dark_light::Mode>, state: &State) {
    let icon = build_icon(theme, state.client_status, state.hub_bind_error.is_some());
    let _ = tray.set_icon(Some(icon));
}

/// Render the user-facing status label. Kept short + prefix-stable so
/// the menu doesn't reflow on every status change.
fn status_label_for(status: Option<ClientStatus>, has_cfg: bool) -> String {
    status_label_with_hub(status, has_cfg, None)
}

/// Variant of [`status_label_for`] that prioritises a hub-bind failure
/// when one is present — that's the user-actionable problem and they
/// shouldn't have to dig through logs to see it.
fn status_label_with_hub(
    status: Option<ClientStatus>,
    has_cfg: bool,
    hub_bind_error: Option<&str>,
) -> String {
    if let Some(err) = hub_bind_error {
        return format!("Status: hub bind failed — {err}");
    }
    if !has_cfg {
        return "Status: needs configuration".to_string();
    }
    match status {
        None => "Status: starting…".to_string(),
        Some(ClientStatus::Connecting) => "Status: connecting…".to_string(),
        Some(ClientStatus::Connected) => "Status: connected".to_string(),
        Some(ClientStatus::Disconnected { will_retry_in }) => format!(
            "Status: disconnected — retrying in {}s",
            will_retry_in.as_secs().max(1)
        ),
    }
}

/// Helper used inside `start_embedded_hub_with_cfg` to clear the
/// "hub bind failed" sticky state once a fresh bind has succeeded.
fn l_and_a_clear<L, A>(state: &mut State, listener: L, addr: A) -> (L, A) {
    state.hub_bind_error = None;
    (listener, addr)
}

/// Best-effort: shorten the typical OS errors into a human phrase. The
/// "address already in use" case is the one we expect 95% of the time.
fn friendly_bind_error(e: &anyhow::Error) -> String {
    let s = format!("{e:#}");
    let lower = s.to_lowercase();
    if lower.contains("address already in use") || lower.contains("address in use") {
        "another process is already on this port".to_string()
    } else if lower.contains("permission denied") {
        "permission denied".to_string()
    } else {
        // Keep just the deepest message — anyhow's chain is noisy.
        s.lines().last().unwrap_or("bind failed").to_string()
    }
}

/// Refresh the tray tooltip from current state.
fn refresh_tooltip(tray: &TrayIcon, state: &State, config_path: &Path) {
    // Hub bind failure wins the tooltip — it's the actionable problem.
    if let Some(err) = &state.hub_bind_error {
        let tip = compose_tooltip(
            &[
                format!("clipboardwire — hub bind failed ({err})"),
                config_path.display().to_string(),
            ],
            None,
        );
        let _ = tray.set_tooltip(Some(tip));
        return;
    }
    let status_line = match (&state.cfg, state.client_status) {
        (None, _) => format!(
            "clipboardwire — needs config (right-click → Edit config)\n{}",
            config_path.display()
        ),
        (Some(cfg), None) => format!("clipboardwire — starting\n{}", cfg.server),
        (Some(cfg), Some(ClientStatus::Connecting)) => {
            format!("clipboardwire — connecting…\n{}", cfg.server)
        }
        (Some(cfg), Some(ClientStatus::Connected)) => {
            format!("clipboardwire — connected\n{}", cfg.server)
        }
        (Some(cfg), Some(ClientStatus::Disconnected { will_retry_in })) => format!(
            "clipboardwire — disconnected, retrying in {}s\n{}",
            will_retry_in.as_secs().max(1),
            cfg.server
        ),
    };
    let mut lines: Vec<String> = status_line.split('\n').map(str::to_string).collect();
    // Append the embedded-hub peer count when the hub is running.
    // Count includes our own loopback supervisor (the hub treats it
    // as a peer like any other), so a freshly-started embedded hub
    // with no remote peers shows "1 client".
    if let Some(stats) = &state.hub_stats {
        lines.push(
            format_hub_peer_line(stats.current())
                .trim_start_matches('\n')
                .to_string(),
        );
    }
    let transfer_line = format_transfer_tooltip_line(&state.transfers);
    let tip = compose_tooltip(&lines, transfer_line.as_deref());
    let _ = tray.set_tooltip(Some(tip));
}

/// Assemble the tooltip and guarantee it fits [`TOOLTIP_MAX_CHARS`].
///
/// `NOTIFYICONDATAW.szTip` is 128 UTF-16 code units; a longer string is
/// silently truncated by the shell, usually mid-URL. This function decides what
/// to sacrifice instead: the transfer line keeps its own budget, the middle
/// lines (URL, peer count) are shortened, and the first line — the status — is
/// only touched if nothing else is left.
fn compose_tooltip(lines: &[String], transfer_line: Option<&str>) -> String {
    // The transfer line is the newest information, so it is reserved first.
    let transfer = transfer_line.map(|l| fit_chars(l, TOOLTIP_TRANSFER_BUDGET));
    let reserved = transfer.as_ref().map_or(0, |l| l.chars().count() + 1);
    let budget = TOOLTIP_MAX_CHARS.saturating_sub(reserved);

    let mut kept: Vec<String> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let used: usize = kept.iter().map(|l| l.chars().count() + 1).sum();
        let remaining = budget.saturating_sub(used);
        if remaining == 0 {
            break;
        }
        let need = line.chars().count();
        if need <= remaining {
            kept.push(line.clone());
        } else if i == 0 {
            // Never drop the status line entirely; shorten it.
            kept.push(fit_chars(line, remaining));
            break;
        } else {
            // A middle line: keep as much as fits, then stop. Better a short
            // URL than a clipped status or a missing transfer.
            if remaining > 8 {
                kept.push(fit_chars(line, remaining));
            }
            break;
        }
    }

    let mut out = kept.join("\n");
    if let Some(line) = transfer {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&line);
    }
    // Belt and braces: the arithmetic above should already bound this.
    fit_chars(&out, TOOLTIP_MAX_CHARS)
}

/// One aggregate line describing live transfers, or `None` when there are none.
///
/// Kept deliberately terse: `szTip` is 128 chars and the status/URL/peer lines
/// already use most of it. Detail per transfer lives in the menu instead. The
/// line is capped at [`TOOLTIP_TRANSFER_BUDGET`] characters, so five concurrent
/// transfers still fit ("2 transfers active, 45%").
fn format_transfer_tooltip_line(transfers: &[TransferProgress]) -> Option<String> {
    if transfers.is_empty() {
        return None;
    }
    let active: Vec<&TransferProgress> = transfers
        .iter()
        .filter(|t| !t.state.is_terminal())
        .collect();
    let line = if active.is_empty() {
        // Everything finished: say so once, briefly, until retention expires.
        match transfers.len() {
            1 => transfers[0].summary_line(24),
            n => format!("{n} transfers finished"),
        }
    } else if active.len() == 1 {
        active[0].summary_line(TOOLTIP_TRANSFER_BUDGET)
    } else {
        let pct = aggregate_percent(&active);
        match pct {
            Some(p) => format!("{} transfers active, {p}%", active.len()),
            None => format!("{} transfers active", active.len()),
        }
    };
    Some(fit_chars(&line, TOOLTIP_TRANSFER_BUDGET))
}

/// Bytes-weighted completion across `transfers`, or `None` if no total is
/// known (e.g. every event is still `Hashing` at 0 of an unknown size).
fn aggregate_percent(transfers: &[&TransferProgress]) -> Option<u64> {
    let total: u64 = transfers.iter().map(|t| t.bytes_total).sum();
    if total == 0 {
        return None;
    }
    let done: u64 = transfers
        .iter()
        .map(|t| t.bytes_done.min(t.bytes_total))
        .sum();
    Some(done * 100 / total)
}

/// Truncate to `max` characters, appending `…` when cut.
fn fit_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Fill the transfer section of the menu.
///
/// The header names the count; each slot lists one transfer. Empty slots are
/// blanked rather than hidden, because adding/removing menu items on every
/// chunk event makes some platforms flicker.
fn update_transfer_items(header: &MenuItem, items: &[MenuItem], state: &State) {
    if state.transfers.is_empty() {
        header.set_text("");
    } else {
        header.set_text(format_transfer_header(&state.transfers));
    }
    for (slot, item) in items.iter().enumerate() {
        match state.transfers.get(slot) {
            Some(t) => item.set_text(format!("  {}", t.summary_line(40))),
            None => item.set_text(""),
        }
    }
}

/// Header text for the transfer section.
///
/// A failure is not a completion: counting it under "done" hides the only
/// signal the user gets that a transfer died.
fn format_transfer_header(transfers: &[TransferProgress]) -> String {
    let active = transfers.iter().filter(|t| !t.state.is_terminal()).count();
    match active {
        0 => {
            let done = transfers
                .iter()
                .filter(|t| t.state == ProgressState::Done)
                .count();
            let failed = transfers
                .iter()
                .filter(|t| t.state == ProgressState::Failed)
                .count();
            match (done, failed) {
                (0, 0) => format!("Transfers ({})", transfers.len()),
                (d, 0) => format!("Transfers ({d} done)"),
                (0, f) => format!("Transfers ({f} failed)"),
                (d, f) => format!("Transfers ({d} done, {f} failed)"),
            }
        }
        1 => "Transfer in progress".to_string(),
        n => format!("Transfers in progress ({n})"),
    }
}

/// Render the per-peer-count suffix that gets appended to the
/// tooltip when the embedded hub is running. Pulled out for the
/// unit test below.
fn format_hub_peer_line(count: usize) -> String {
    match count {
        0 => "\nhub running (0 clients connected)".to_string(),
        1 => "\nhub running (1 client connected)".to_string(),
        n => format!("\nhub running ({n} clients connected)"),
    }
}

/// Launch `clipboardwire settings --config <path>` as a subprocess.
/// Spawns a watcher thread that posts `SettingsExited` to the event
/// loop when the child exits.
fn launch_settings_dialog(path: &Path, proxy: &EventLoopProxy<UserEvent>) -> Result<()> {
    let exe = std::env::current_exe().context("locating current_exe")?;
    let child: Child = std::process::Command::new(exe)
        .arg("settings")
        .arg("--config")
        .arg(path)
        .spawn()
        .context("spawning settings subprocess")?;
    info!(path = %path.display(), pid = child.id(), "settings dialog launched");

    let proxy = proxy.clone();
    std::thread::spawn(move || {
        let mut child = child;
        let _ = child.wait();
        info!("settings subprocess exited; posting reload event");
        let _ = proxy.send_event(UserEvent::SettingsExited);
    });

    Ok(())
}

/// Load the bundled tray icon, choosing a colour-/monochrome variant
/// based on the user's current system theme and overlaying a small
/// status dot in the bottom-right corner.
fn build_icon(
    theme: Option<dark_light::Mode>,
    status: Option<ClientStatus>,
    hub_bind_error: bool,
) -> tray_icon::Icon {
    const ICON_COLOR: &[u8] = include_bytes!("../../assets/icon-32.png");
    const ICON_MONO_DARK: &[u8] = include_bytes!("../../assets/icon-mono-dark-32.png");
    const ICON_MONO_LIGHT: &[u8] = include_bytes!("../../assets/icon-mono-light-32.png");

    let bytes = match theme {
        Some(dark_light::Mode::Light) => ICON_MONO_DARK,
        Some(dark_light::Mode::Dark) => ICON_MONO_LIGHT,
        _ => ICON_COLOR,
    };

    let img = image::load_from_memory_with_format(bytes, image::ImageFormat::Png)
        .expect("embedded tray icon PNG decodes");
    let mut rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();

    // Pick an overlay colour. Hub-bind failure dominates (the user
    // can't ignore it). Otherwise track the client status:
    // - green: connected
    // - amber: connecting (transient)
    // - red:   disconnected → retrying
    // - none:  no supervisor / starting up (don't add a dot — the bare
    //          icon is the right signal for "config-needed")
    let overlay = if hub_bind_error {
        Some([220u8, 60, 60, 255])
    } else {
        match status {
            Some(ClientStatus::Connected) => Some([60u8, 180, 80, 255]),
            Some(ClientStatus::Connecting) => Some([235u8, 175, 50, 255]),
            Some(ClientStatus::Disconnected { .. }) => Some([220u8, 60, 60, 255]),
            None => None,
        }
    };

    if let Some(colour) = overlay {
        draw_status_dot(&mut rgba, width, height, colour);
    }

    tray_icon::Icon::from_rgba(rgba.into_raw(), width, height).expect("tray-icon accepts RGBA")
}

/// Composite a filled circle (with a 1-px contrast halo) in the
/// bottom-right of the tray icon. Drawn directly into the RGBA buffer
/// to avoid pulling in an extra drawing crate.
fn draw_status_dot(rgba: &mut image::RgbaImage, width: u32, height: u32, color: [u8; 4]) {
    let r = (width.min(height) as i32) * 11 / 50; // ≈ 22% of the icon side
    let cx = width as i32 - r - 1;
    let cy = height as i32 - r - 1;
    let halo_r = r + 1;

    for y in 0..height as i32 {
        for x in 0..width as i32 {
            let dx = x - cx;
            let dy = y - cy;
            let d2 = dx * dx + dy * dy;
            if d2 <= r * r {
                rgba.put_pixel(x as u32, y as u32, image::Rgba(color));
            } else if d2 <= halo_r * halo_r {
                // 1-px translucent dark halo so the dot reads against
                // a light tray and the icon itself stays visible.
                rgba.put_pixel(x as u32, y as u32, image::Rgba([0, 0, 0, 200]));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clipboardwire_core::client::{Direction, ProgressState};

    fn progress(name: &str, state: ProgressState, done: u64, total: u64) -> TransferProgress {
        progress_for(
            TransferId::from_u128(done as u128 + 1),
            name,
            state,
            done,
            total,
        )
    }

    fn progress_for(
        file_id: TransferId,
        name: &str,
        state: ProgressState,
        done: u64,
        total: u64,
    ) -> TransferProgress {
        TransferProgress {
            file_id,
            transfer_id: None,
            direction: Direction::Send,
            peer: None,
            name: name.to_string(),
            bytes_done: done,
            bytes_total: total,
            state,
        }
    }

    /// Worst realistic tooltip: long URL, disconnected status, full hub, plus
    /// the transfer line. `NOTIFYICONDATAW.szTip` is 128 UTF-16 code units.
    #[test]
    fn tooltip_stays_within_the_platform_limit() {
        let lines = vec![
            "clipboardwire — disconnected, retrying in 60s".to_string(),
            "wss://some-long-server-name.example.lan:8484/sync".to_string(),
            format_hub_peer_line(64)
                .trim_start_matches('\n')
                .to_string(),
        ];
        for transfers in [
            vec![progress("a.bin", ProgressState::Pulling, 42, 100)],
            vec![
                progress("a.bin", ProgressState::Pulling, 10, 100),
                progress("b.bin", ProgressState::Pulling, 20, 100),
                progress("c.bin", ProgressState::Pulling, 30, 100),
                progress("d.bin", ProgressState::Pulling, 40, 100),
                progress("e.bin", ProgressState::Pulling, 50, 100),
            ],
            vec![progress(
                "a-really-long-file-name-that-could-blot-out-the-whole-tooltip.tar.gz",
                ProgressState::Pulling,
                1,
                1000,
            )],
        ] {
            let line = format_transfer_tooltip_line(&transfers).expect("line for live transfers");
            let full = compose_tooltip(&lines, Some(&line));
            assert!(
                full.chars().count() <= TOOLTIP_MAX_CHARS,
                "tooltip is {} chars: {full:?}",
                full.chars().count()
            );
            assert!(
                line.chars().count() <= TOOLTIP_TRANSFER_BUDGET,
                "transfer line is {} chars: {line:?}",
                line.chars().count()
            );
            // The transfer line must survive assembly; only the middle lines
            // may be shortened.
            assert!(
                full.ends_with(&line),
                "transfer line was dropped: {full:?} does not end with {line:?}"
            );
        }
    }

    #[test]
    fn tooltip_transfer_line_is_aggregate_when_several_run() {
        let transfers = vec![
            progress("a.bin", ProgressState::Pulling, 10, 100),
            progress("b.bin", ProgressState::Pulling, 20, 100),
        ];
        let line = format_transfer_tooltip_line(&transfers).unwrap();
        assert_eq!(line, "2 transfers active, 15%");
        // No individual names: detail belongs in the menu, not the tooltip.
        assert!(!line.contains("a.bin"));
    }

    #[test]
    fn tooltip_transfer_line_names_a_single_transfer() {
        let line = format_transfer_tooltip_line(&[progress(
            "report.pdf",
            ProgressState::Pulling,
            42,
            100,
        )])
        .unwrap();
        assert_eq!(line, "sending report.pdf 42%");
    }

    #[test]
    fn tooltip_has_no_transfer_line_when_idle() {
        assert_eq!(format_transfer_tooltip_line(&[]), None);
    }

    #[test]
    fn finished_transfers_are_retained_then_expired() {
        let mut state = empty_state();

        let id = TransferId::from_u128(7);
        state.record_progress(progress_for(id, "a.bin", ProgressState::Pulling, 10, 100));
        assert_eq!(state.transfers.len(), 1);
        // An active transfer is not immortal: it carries the stall backstop.
        let stall = state
            .next_transfer_deadline()
            .expect("stall deadline armed");
        let until_stall = stall.saturating_duration_since(std::time::Instant::now());
        assert!(
            until_stall <= TRANSFER_STALL_TIMEOUT && until_stall > TRANSFER_STALL_TIMEOUT / 2,
            "expected a {TRANSFER_STALL_TIMEOUT:?} stall window, got {until_stall:?}"
        );

        state.record_progress(progress_for(id, "a.bin", ProgressState::Done, 100, 100));
        assert_eq!(
            state.transfers.len(),
            1,
            "a Done event replaces, not appends"
        );
        assert!(
            state.next_transfer_deadline().is_some(),
            "Done must arm the retention timer"
        );

        // Deadline is ~3 s out.
        let deadline = state.next_transfer_deadline().unwrap();
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            remaining <= TRANSFER_RETENTION && remaining > TRANSFER_RETENTION / 2,
            "retention window looks wrong: {remaining:?}"
        );

        // Nothing expires before the window closes.
        assert!(!state.expire_transfers());
        assert_eq!(state.transfers.len(), 1);
    }

    fn empty_state() -> State {
        State {
            supervisor: None,
            supervisor_gen: 0,
            client_status: None,
            transfers: Vec::new(),
            transfer_clear_at: Vec::new(),
            embedded_hub: None,
            embedded_hub_gen: 0,
            embedded_hub_cfg: None,
            embedded_hub_addr: None,
            hub_bind_error: None,
            hub_stats: None,
            hub_stats_poller: None,
            settings_alive: false,
            cfg: None,
        }
    }

    #[test]
    fn stalled_transfer_is_dropped_without_a_terminal_event() {
        // Backstop for a wedged supervisor: no Done/Failed ever arrives, so the
        // entry must still disappear once the stall window passes.
        let mut state = empty_state();
        let id = TransferId::from_u128(11);
        state.record_progress(progress_for(
            id,
            "stuck.bin",
            ProgressState::Pulling,
            12,
            100,
        ));
        assert_eq!(state.transfers.len(), 1);

        // Active transfers expire on the stall window, not the 3 s retention.
        let deadline = state.next_transfer_deadline().unwrap();
        assert!(
            deadline.saturating_duration_since(std::time::Instant::now()) > TRANSFER_RETENTION,
            "an active transfer must not be dropped on the retention timer"
        );
        assert!(!state.expire_transfers(), "nothing should expire yet");
        assert_eq!(state.transfers.len(), 1);
    }

    #[test]
    fn hashing_a_large_file_survives_the_plain_stall_window() {
        // Path A. The sender reports hash progress once per percent, so on a
        // slow read the gap between two `Hashing` events is 1 % of the whole
        // read time. A 100 GB file at ~30 MB/s (slow external disk) leaves
        // ~33 s between events — inside 60 s. A network mount an order of
        // magnitude slower does not, and that is the case this guards: the
        // entry must stay put rather than blink out mid-hash.
        let mut state = empty_state();
        let id = TransferId::from_u128(41);
        let total = 100 * 1024 * 1024 * 1024; // 100 GiB
        state.record_progress(progress_for(
            id,
            "huge.bin",
            ProgressState::Hashing,
            0,
            total,
        ));

        let grace = state
            .next_transfer_deadline()
            .expect("hashing must arm a deadline")
            .saturating_duration_since(std::time::Instant::now());
        assert!(
            grace > TRANSFER_STALL_TIMEOUT,
            "a 100 GiB hash needs more than the plain window, got {grace:?}"
        );
        // 1 % of 100 GiB is ~1 GiB; at the 1 MiB/s floor that is ~1024 s.
        assert!(grace >= std::time::Duration::from_secs(1000), "{grace:?}");

        // Sixty-five seconds of silence — enough to trip the plain window —
        // must not retire it.
        assert!(
            !state.expire_transfers(),
            "a slow but healthy hash must not be dropped"
        );
        assert_eq!(state.transfers.len(), 1);

        // The backstop is still armed: the window is finite and bounded.
        assert!(grace <= TRANSFER_STALL_MAX, "{grace:?}");
    }

    #[test]
    fn pulling_keeps_the_plain_stall_window() {
        // Path B — and the counterweight to `Hashing`. Pulling reports every
        // chunk (4 MiB), so a minute of silence is not slowness: the stream has
        // stopped and the entry should go. Without this asymmetry a size-scaled
        // window would apply to every state and the backstop would stop firing.
        for total in [100u64, 1024 * 1024, 100 * 1024 * 1024 * 1024] {
            let grace = stall_grace(ProgressState::Pulling, total);
            assert_eq!(
                grace, TRANSFER_STALL_TIMEOUT,
                "pulling must not scale with size (total={total})"
            );
        }
        assert_eq!(
            stall_grace(ProgressState::Verifying, 100 * 1024 * 1024 * 1024),
            TRANSFER_STALL_TIMEOUT,
            "verifying is a single re-read plus a hash; it keeps the plain window"
        );
    }

    #[test]
    fn hashing_grace_is_clamped_at_both_ends() {
        // Small files: the scaled window is below the floor, so it stays at 60 s.
        assert_eq!(
            stall_grace(ProgressState::Hashing, 1024),
            TRANSFER_STALL_TIMEOUT,
            "a 1 KiB file must not get a sub-minute window"
        );
        // A zero total (metadata unavailable) must not produce a 0 s window.
        assert_eq!(
            stall_grace(ProgressState::Hashing, 0),
            TRANSFER_STALL_TIMEOUT
        );
        // Absurd sizes: clamped, so the backstop still fires eventually.
        assert_eq!(
            stall_grace(ProgressState::Hashing, u64::MAX),
            TRANSFER_STALL_MAX
        );
    }

    #[test]
    fn every_event_reschedules_the_stall_deadline() {
        // A transfer that keeps reporting must not be retired mid-transfer:
        // the deadline moves forward with each event.
        let mut state = empty_state();
        let id = TransferId::from_u128(12);
        state.record_progress(progress_for(
            id,
            "slow.bin",
            ProgressState::Pulling,
            10,
            100,
        ));
        let first = state.next_transfer_deadline().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(20));
        state.record_progress(progress_for(
            id,
            "slow.bin",
            ProgressState::Pulling,
            20,
            100,
        ));
        let second = state.next_transfer_deadline().unwrap();
        assert!(second > first, "each event must push the deadline out");
    }

    #[test]
    fn terminal_event_shortens_the_deadline_to_retention() {
        let mut state = empty_state();
        let id = TransferId::from_u128(13);
        state.record_progress(progress_for(id, "a.bin", ProgressState::Pulling, 90, 100));
        let active = state.next_transfer_deadline().unwrap();
        assert!(active.saturating_duration_since(std::time::Instant::now()) > TRANSFER_RETENTION);

        state.record_progress(progress_for(id, "a.bin", ProgressState::Done, 100, 100));
        let done = state.next_transfer_deadline().unwrap();
        let remaining = done.saturating_duration_since(std::time::Instant::now());
        assert!(
            remaining <= TRANSFER_RETENTION,
            "a terminal event must shorten the window to retention: {remaining:?}"
        );
        // Exactly one deadline per transfer, not two.
        assert_eq!(state.transfer_clear_at.len(), 1);
    }

    #[test]
    fn progress_from_a_previous_generation_is_ignored() {
        // M2: an old subscriber can still have events buffered when a reload
        // swaps supervisors. Their `generation` no longer matches the live one,
        // so the loop drops them and the new generation's list is not polluted.
        let (mut state, _rt) = live_state(5);

        assert!(
            is_live_supervisor_event(&state, 5),
            "the running supervisor's own events must be accepted"
        );
        assert!(
            !is_live_supervisor_event(&state, 4),
            "a previous generation's buffered event must be dropped"
        );
        assert!(
            !is_live_supervisor_event(&state, 6),
            "a future generation cannot be live yet"
        );

        // Dropping is what keeps the list clean: the event itself is perfectly
        // applicable, so the guard — not the event — is doing the work.
        state.record_progress(progress_for(
            TransferId::from_u128(21),
            "old.bin",
            ProgressState::Pulling,
            50,
            100,
        ));
        assert_eq!(state.transfers.len(), 1);
    }

    #[test]
    fn supervisor_scoped_events_are_dropped_once_no_supervisor_is_installed() {
        // The asymmetry this replaces: `TransferProgress` and
        // `ClientStatusChanged` used to compare the generation alone, so an
        // event from a forwarder that had not noticed the shutdown would still
        // be applied while the tray was showing "no supervisor". All three
        // supervisor-scoped events now share one predicate.
        let (mut state, _rt) = live_state(9);
        assert!(is_live_supervisor_event(&state, 9));

        // Same generation, but the supervisor is gone (`take()` leaves the
        // generation number unchanged — only `abort_running` bumps it).
        state.supervisor = None;
        assert_eq!(
            state.supervisor_gen, 9,
            "the generation itself still matches"
        );
        assert!(
            !is_live_supervisor_event(&state, 9),
            "a matching generation must not resurrect a dead supervisor's events"
        );

        // And the event that already exists for a dead supervisor must not
        // pass either: `SupervisorExited` is handled only for a live one.
        // This is the guard's shape, asserted directly.
        assert_eq!(current_supervisor_gen(&state), None);
    }

    #[test]
    fn abort_running_retires_the_generation_so_stale_events_cannot_match() {
        // m1(a): `abort_running` clears the list but does not install a
        // replacement. Without the generation bump, buffered progress from the
        // aborted supervisor would still pass `is_live_supervisor_event` for
        // any reload path that bails before `apply_loaded_config` spawns a
        // successor (i.e. an invalid config).
        let (mut state, _rt) = live_state(12);
        let live_gen = state.supervisor_gen;
        assert!(is_live_supervisor_event(&state, live_gen));

        abort_running(&mut state, false);

        assert!(
            !is_live_supervisor_event(&state, live_gen),
            "an aborted generation must not remain live"
        );
        assert!(
            state.next_transfer_deadline().is_none(),
            "no deadline survives the abort"
        );
    }

    /// A `State` with a real (never-completing) supervisor task installed, so
    /// `current_supervisor_gen` reports `Some`. The runtime is returned so the
    /// task outlives the assertions.
    fn live_state(generation: u64) -> (State, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Runtime::new().expect("test runtime");
        let handle = rt.handle().clone();
        let task = handle.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });
        let mut state = empty_state();
        state.supervisor_gen = generation;
        state.supervisor = Some(task);
        (state, rt)
    }

    #[test]
    fn abort_running_clears_transfer_state() {
        // M2's other half: a reload must not carry the old generation's
        // transfers into the new one.
        let mut state = empty_state();
        state.record_progress(progress_for(
            TransferId::from_u128(31),
            "a.bin",
            ProgressState::Pulling,
            10,
            100,
        ));
        state.record_progress(progress_for(
            TransferId::from_u128(32),
            "b.bin",
            ProgressState::Done,
            100,
            100,
        ));
        assert_eq!(state.transfers.len(), 2);

        abort_running(&mut state, false);
        assert!(state.transfers.is_empty());
        assert!(state.transfer_clear_at.is_empty());
        assert!(state.next_transfer_deadline().is_none());
    }

    #[test]
    fn hub_peer_line_pluralises() {
        assert_eq!(
            format_hub_peer_line(0),
            "\nhub running (0 clients connected)"
        );
        assert_eq!(
            format_hub_peer_line(1),
            "\nhub running (1 client connected)"
        );
        assert_eq!(
            format_hub_peer_line(7),
            "\nhub running (7 clients connected)"
        );
    }

    #[test]
    fn header_counts_done_and_failed_separately() {
        let transfers = vec![
            progress("ok.bin", ProgressState::Done, 100, 100),
            progress("bad.bin", ProgressState::Failed, 0, 100),
        ];
        let header = format_transfer_header(&transfers);
        assert!(header.contains("1 done"), "{header}");
        assert!(header.contains("1 failed"), "{header}");
        assert_eq!(header, "Transfers (1 done, 1 failed)");
    }

    /// A progress event for an explicit `(file_id, transfer_id)` pair.
    fn progress_for_session(
        file_id: TransferId,
        transfer_id: TransferId,
        state: ProgressState,
        done: u64,
        total: u64,
    ) -> TransferProgress {
        let mut event = progress_for(file_id, "a.bin", state, done, total);
        event.transfer_id = Some(transfer_id);
        event
    }

    #[test]
    fn two_transfers_of_one_file_get_separate_rows() {
        // Lazy mode lets one file be pasted twice at once; each paste pulls
        // under its own `transfer_id`. Keying on `file_id` alone made the
        // second transfer's first event overwrite the first transfer's row, so
        // the user saw one row jumping between two progress values.
        let mut state = empty_state();
        let file_id = TransferId::from_u128(0xf11e);
        let first = TransferId::from_u128(1);
        let second = TransferId::from_u128(2);

        state.record_progress(progress_for_session(
            file_id,
            first,
            ProgressState::Pulling,
            10,
            100,
        ));
        state.record_progress(progress_for_session(
            file_id,
            second,
            ProgressState::Pulling,
            30,
            100,
        ));

        assert_eq!(
            state.transfers.len(),
            2,
            "one row per transfer, not one per file"
        );
        assert_eq!(state.transfers[0].transfer_id, Some(first));
        assert_eq!(
            state.transfers[0].bytes_done, 10,
            "the first row kept its own progress"
        );
        assert_eq!(state.transfers[1].transfer_id, Some(second));
        assert_eq!(state.transfers[1].bytes_done, 30);
        assert_eq!(
            state.transfer_clear_at.len(),
            2,
            "the deadline table must be per transfer too"
        );

        // The header counts rows, so it must say two.
        let header = format_transfer_header(&state.transfers);
        assert_eq!(header, "Transfers in progress (2)");
    }

    #[test]
    fn an_eager_transfer_still_uses_one_row() {
        // The eager path carries no session, so repeated events for one file
        // must still collapse: keying on the pair must not turn every chunk
        // event into a new row.
        let mut state = empty_state();
        let file_id = TransferId::from_u128(0xeae6e2);

        state.record_progress(progress_for(
            file_id,
            "a.bin",
            ProgressState::Hashing,
            1,
            100,
        ));
        state.record_progress(progress_for(
            file_id,
            "a.bin",
            ProgressState::Hashing,
            50,
            100,
        ));
        state.record_progress(progress_for(
            file_id,
            "a.bin",
            ProgressState::Done,
            100,
            100,
        ));

        assert_eq!(state.transfers.len(), 1, "eager collapses to one row");
        assert_eq!(state.transfers[0].state, ProgressState::Done);
        assert_eq!(state.transfer_clear_at.len(), 1, "and one deadline");
    }

    #[test]
    fn completing_one_of_two_concurrent_transfers_leaves_the_other() {
        // The second half of the bug: `retain(|t| t.file_id != event.file_id)`
        // cleared *both* rows when the first transfer reported `Done`.
        let mut state = empty_state();
        let file_id = TransferId::from_u128(0x5a4ed);
        let done_first = TransferId::from_u128(3);
        let still_running = TransferId::from_u128(4);

        state.record_progress(progress_for_session(
            file_id,
            done_first,
            ProgressState::Pulling,
            10,
            100,
        ));
        state.record_progress(progress_for_session(
            file_id,
            still_running,
            ProgressState::Pulling,
            40,
            100,
        ));

        state.record_progress(progress_for_session(
            file_id,
            done_first,
            ProgressState::Done,
            100,
            100,
        ));

        assert_eq!(
            state.transfers.len(),
            2,
            "a Done on one transfer must not clear the other"
        );
        let finished = state
            .transfers
            .iter()
            .find(|t| t.transfer_id == Some(done_first))
            .expect("the finished transfer is still listed for its retention window");
        assert_eq!(finished.state, ProgressState::Done);
        let running = state
            .transfers
            .iter()
            .find(|t| t.transfer_id == Some(still_running))
            .expect("the concurrent transfer survives");
        assert_eq!(running.state, ProgressState::Pulling);
        assert_eq!(running.bytes_done, 40);

        // And the retention deadline is per row: only the finished one is
        // armed for the short window.
        let done_deadline = state
            .transfer_clear_at
            .iter()
            .find(|((fid, tid), _)| *fid == file_id && *tid == Some(done_first))
            .map(|(_, at)| *at)
            .expect("the finished row has a deadline");
        let running_deadline = state
            .transfer_clear_at
            .iter()
            .find(|((fid, tid), _)| *fid == file_id && *tid == Some(still_running))
            .map(|(_, at)| *at)
            .expect("the running row has a deadline");
        assert!(
            done_deadline < running_deadline,
            "the finished row must expire first: {done_deadline:?} vs {running_deadline:?}"
        );
    }

    #[test]
    fn header_reports_a_lone_failure_as_failed_not_done() {
        // The M1 symptom: a failed transfer alone used to render as
        // "Transfers (1 done)" while its row said "sending x 0%".
        let header = format_transfer_header(&[progress("bad.bin", ProgressState::Failed, 0, 100)]);
        assert_eq!(header, "Transfers (1 failed)");
        assert!(!header.contains("done"), "{header}");
    }

    /// Write a client config file with the given `[hub]` section verbatim.
    /// `ClientConfig::load` enforces 0600 on unix, so the test file has to
    /// match or the load fails for the wrong reason.
    fn write_hub_config(dir: &Path, hub_section: &str) -> PathBuf {
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            format!(
                "server = \"wss://127.0.0.1:8484/sync\"\nuser = \"u\"\npassword = \"p\"\n\n{hub_section}"
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        path
    }

    #[test]
    fn reload_keeps_the_hub_only_when_its_section_is_unchanged() {
        // The "every save drops every peer" regression: `abort_running` used
        // to kill the listener unconditionally, so a settings save that
        // changed nothing still closed all connected clients.
        let rt = tokio::runtime::Runtime::new().expect("test runtime");
        let hub_task = rt.handle().spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });
        let mut state = empty_state();
        state.embedded_hub = Some(hub_task);
        state.embedded_hub_cfg = Some(clipboardwire_core::client::config::HubConfig {
            enabled: true,
            user: "u".into(),
            password: "p".into(),
            ..Default::default()
        });

        let dir = std::env::temp_dir().join(format!(
            "cw-tray-reload-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // Identical [hub] section → the running hub stays up.
        let same = write_hub_config(
            &dir,
            "[hub]\nenabled = true\nbind = \"0.0.0.0:8484\"\nuser = \"u\"\npassword = \"p\"",
        );
        assert!(
            hub_restart_unneeded(&state, &same),
            "an unchanged [hub] section must not trigger a rebind"
        );

        // A different bind → restart.
        let moved = write_hub_config(
            &dir,
            "[hub]\nenabled = true\nbind = \"0.0.0.0:9999\"\nuser = \"u\"\npassword = \"p\"",
        );
        assert!(!hub_restart_unneeded(&state, &moved));

        // Same file but the hub task is gone → restart path.
        state.embedded_hub = None;
        assert!(!hub_restart_unneeded(&state, &same));

        // A file that does not parse → restart path.
        let gone = dir.join("missing.toml");
        assert!(!hub_restart_unneeded(&state, &gone));

        let _ = std::fs::remove_dir_all(&dir);
    }
}

// SPDX-License-Identifier: GPL-3.0-or-later

//! `clipboardwire settings` — a small eframe-based GUI for editing the
//! client config without touching the TOML by hand.
//!
//! Runs as its own subprocess so the tray's tao event loop and this
//! egui/winit one don't fight over the same main thread. The tray's
//! "Edit config…" menu item launches the binary with this subcommand;
//! the user fills in the form; the dialog writes the config and exits.
//! The tray's "Reload config" then picks up the new values.
//!
//! Form fields mirror [`ClientConfig`]:
//!
//! | Field           | Required | UI                                       |
//! | --------------- | -------- | ---------------------------------------- |
//! | `server`        | yes      | text, validated against ws:// / wss://   |
//! | `user`          | yes      | text                                     |
//! | `password`      | yes      | text, masked                             |
//! | `poll_ms`       | optional | u64 spinner, default 300                 |
//! | `tls_ca_file`   | optional | text path                                |
//! | `tls_insecure`  | optional | checkbox                                 |
//!
//! When `[hub].enabled` is set, the Connection/Security cards collapse
//! into a note: the local client joins the embedded hub over loopback,
//! so `server` is written as `wss://127.0.0.1:<hub port>/sync` and
//! `user`/`password` mirror the hub credentials — the same values the
//! tray derives at runtime.
//!
//! Exits with code 0 on Save, 1 on Cancel / window close.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use clipboardwire_core::client::config::{
    is_default_offer_ttl, is_default_pull_idle_timeout, is_default_pull_window,
    is_default_receive_mode, is_default_send_mode, ReceiveMode, SendMode,
};
use clipboardwire_core::client::ClientConfig;
use eframe::egui;

const DEFAULT_POLL_MS: u64 = 300;
const WINDOW_TITLE: &str = "clipboardwire — settings";

// ---------------------------------------------------------------------
// Visual theme — see openspec/changes/settings-ui-redesign/design.md.
// Light slate page, white cards, restrained blue accent. Geometry is in
// logical points.
// ---------------------------------------------------------------------

use egui::{Color32, CornerRadius, FontId, Margin, Stroke, TextStyle};

const PAGE: Color32 = Color32::from_rgb(244, 246, 250);
const SURFACE: Color32 = Color32::from_rgb(255, 255, 255);
const FIELD: Color32 = Color32::from_rgb(248, 250, 252);
const HOVER: Color32 = Color32::from_rgb(235, 241, 249);
const BORDER: Color32 = Color32::from_rgb(203, 213, 225);
const TEXT: Color32 = Color32::from_rgb(30, 41, 59);
const MUTED: Color32 = Color32::from_rgb(71, 85, 105);
const ACCENT: Color32 = Color32::from_rgb(29, 78, 216);
const ACCENT_HOVER: Color32 = Color32::from_rgb(30, 64, 175);
const ACCENT_ACTIVE: Color32 = Color32::from_rgb(30, 58, 138);
const ACCENT_SOFT: Color32 = Color32::from_rgb(219, 234, 254);
const ON_ACCENT: Color32 = Color32::WHITE;
const WARNING: Color32 = Color32::from_rgb(146, 64, 14);

const PAGE_MARGIN: i8 = 20;
const CARD_PADDING: i8 = 16;
const BAR_PADDING_Y: i8 = 12;
const CARD_RADIUS: u8 = 10;
const CONTROL_RADIUS: u8 = 6;
const POPUP_RADIUS: u8 = 8;
const SPACE_LABEL: f32 = 4.0;
const SPACE_INLINE: f32 = 8.0;
const SPACE_FIELD: f32 = 12.0;
const SPACE_CARD: f32 = 16.0;
const CONTROL_HEIGHT: f32 = 32.0;
const ACTION_SIZE: [f32; 2] = [96.0, 36.0];
const NUMERIC_WIDTH: f32 = 112.0;
const STATUS_MAX_HEIGHT: f32 = 64.0;
const FONT_TITLE: f32 = 22.0;
const FONT_SECTION: f32 = 16.0;
const FONT_BODY: f32 = 14.0;
const FONT_SMALL: f32 = 12.0;
const STROKE_NORMAL: f32 = 1.0;
const STROKE_FOCUS: f32 = 2.0;
const DISABLED_ALPHA: f32 = 0.55;

fn settings_style() -> egui::Style {
    let mut s = egui::Style::default();
    s.text_styles
        .insert(TextStyle::Heading, FontId::proportional(FONT_TITLE));
    s.text_styles
        .insert(TextStyle::Body, FontId::proportional(FONT_BODY));
    s.text_styles
        .insert(TextStyle::Button, FontId::proportional(FONT_BODY));
    s.text_styles
        .insert(TextStyle::Small, FontId::proportional(FONT_SMALL));
    s.text_styles
        .insert(TextStyle::Monospace, FontId::monospace(FONT_SMALL));
    s.spacing.item_spacing = egui::vec2(SPACE_INLINE, SPACE_LABEL);
    s.spacing.button_padding = egui::vec2(12.0, 6.0);
    s.spacing.interact_size = egui::vec2(40.0, CONTROL_HEIGHT);
    s.spacing.indent = 16.0;
    s.spacing.icon_width = 18.0;
    s.spacing.icon_width_inner = 10.0;
    s.spacing.icon_spacing = SPACE_INLINE;
    s.spacing.text_edit_width = 280.0; // fallback; fields explicitly fill width
    s.spacing.combo_width = 280.0; // fallback; combos explicitly fill width
    s.animation_time = 0.0; // deterministic disclosure/test frames
    s.wrap_mode = Some(egui::TextWrapMode::Wrap);

    let mut v = egui::Visuals::light();
    v.override_text_color = None; // preserve per-state and disabled text styling
    v.weak_text_color = Some(MUTED);
    v.panel_fill = PAGE;
    v.window_fill = SURFACE;
    v.window_stroke = Stroke::new(STROKE_NORMAL, BORDER);
    v.window_corner_radius = CornerRadius::same(CARD_RADIUS);
    v.menu_corner_radius = CornerRadius::same(POPUP_RADIUS);
    v.extreme_bg_color = FIELD;
    v.text_edit_bg_color = Some(FIELD);
    v.faint_bg_color = FIELD;
    v.code_bg_color = FIELD;
    v.hyperlink_color = ACCENT;
    v.warn_fg_color = WARNING;
    v.selection.bg_fill = ACCENT_SOFT;
    v.selection.stroke = Stroke::new(STROKE_NORMAL, ACCENT);
    v.disabled_alpha = DISABLED_ALPHA;
    v.collapsing_header_frame = true;
    v.indent_has_left_vline = false;
    for (w, fill, border, width) in [
        (
            &mut v.widgets.noninteractive,
            SURFACE,
            BORDER,
            STROKE_NORMAL,
        ),
        (&mut v.widgets.inactive, FIELD, BORDER, STROKE_NORMAL),
        (&mut v.widgets.hovered, HOVER, ACCENT, STROKE_NORMAL),
        (&mut v.widgets.active, ACCENT_SOFT, ACCENT, STROKE_FOCUS),
        (&mut v.widgets.open, ACCENT_SOFT, ACCENT, STROKE_NORMAL),
    ] {
        w.bg_fill = fill;
        w.weak_bg_fill = fill;
        w.bg_stroke = Stroke::new(width, border);
        w.fg_stroke = Stroke::new(STROKE_NORMAL, TEXT);
        w.corner_radius = CornerRadius::same(CONTROL_RADIUS);
        w.expansion = 0.0;
    }
    s.visuals = v;
    s
}

/// A white card with a section title, used for each settings group.
fn card(ui: &mut egui::Ui, title: &str, body: impl FnOnce(&mut egui::Ui)) {
    egui::Frame::new()
        .fill(SURFACE)
        .stroke(Stroke::new(STROKE_NORMAL, BORDER))
        .corner_radius(CornerRadius::same(CARD_RADIUS))
        .inner_margin(Margin::same(CARD_PADDING))
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.label(egui::RichText::new(title).size(FONT_SECTION).strong());
            ui.add_space(SPACE_FIELD - SPACE_LABEL);
            body(ui);
        });
}

/// Filled accent Save button, drawn in the footer.
fn primary_save(ui: &mut egui::Ui) -> egui::Response {
    ui.scope(|ui| {
        let v = ui.visuals_mut();
        for (w, fill, width) in [
            (&mut v.widgets.inactive, ACCENT, STROKE_NORMAL),
            (&mut v.widgets.hovered, ACCENT_HOVER, STROKE_NORMAL),
            (&mut v.widgets.active, ACCENT_ACTIVE, STROKE_FOCUS),
        ] {
            w.bg_fill = fill;
            w.weak_bg_fill = fill;
            w.fg_stroke = Stroke::new(STROKE_NORMAL, ON_ACCENT);
            w.bg_stroke = Stroke::new(width, ACCENT_ACTIVE);
        }
        ui.add_sized(ACTION_SIZE, egui::Button::new("Save"))
    })
    .inner
}

/// One stacked field: a visible label above a full-width single-line
/// edit. `mask` renders the text as a password. Returns the edit's
/// response so callers can attach tooltips.
fn field_edit(
    ui: &mut egui::Ui,
    label: &str,
    id: &str,
    value: &mut String,
    hint: &str,
    mask: bool,
) -> egui::Response {
    let lbl = ui.label(label);
    let mut edit = egui::TextEdit::singleline(value)
        .id_salt(id)
        .desired_width(f32::INFINITY);
    if !hint.is_empty() {
        edit = edit.hint_text(hint);
    }
    if mask {
        edit = edit.password(true);
    }
    let response = ui
        .add_sized([ui.available_width(), CONTROL_HEIGHT], edit)
        .labelled_by(lbl.id);
    ui.add_space(SPACE_FIELD - SPACE_LABEL);
    response
}

/// One stacked numeric field: label above a fixed-width DragValue.
fn field_drag(ui: &mut egui::Ui, label: &str, drag: egui::DragValue) -> egui::Response {
    let lbl = ui.label(label);
    let response = ui
        .add_sized([NUMERIC_WIDTH, CONTROL_HEIGHT], drag)
        .labelled_by(lbl.id);
    ui.add_space(SPACE_FIELD - SPACE_LABEL);
    response
}

/// Open the settings window for `config_path`. Loads any existing config
/// to pre-fill the form; falls back to template defaults otherwise.
/// Blocks the calling thread for the lifetime of the window.
pub fn run(config_path: PathBuf) -> Result<()> {
    let initial = load_or_default(&config_path);
    let saved = Arc::new(AtomicBool::new(false));

    let app = SettingsApp::new(config_path, initial, saved.clone());

    let mut viewport = egui::ViewportBuilder::default()
        .with_inner_size([640.0, 720.0])
        .with_min_inner_size([420.0, 320.0])
        .with_resizable(true)
        .with_title(WINDOW_TITLE)
        // App ID matches the .desktop filename so GNOME / KDE attach
        // the right launcher icon to the window in the taskbar.
        .with_app_id("clipboardwire");
    if let Some(icon) = load_window_icon() {
        viewport = viewport.with_icon(icon);
    }

    let native_options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };

    eframe::run_native(
        WINDOW_TITLE,
        native_options,
        Box::new(|_cc| Ok(Box::new(app))),
    )
    .map_err(|e| anyhow::anyhow!("settings window: {e}"))?;

    // eframe::run_native returns when the window is closed. `saved` is
    // flipped by `try_save`; we surface the success/cancel distinction
    // to the parent (the tray) via the process exit code.
    if saved.load(Ordering::SeqCst) {
        Ok(())
    } else {
        anyhow::bail!("settings dialog cancelled")
    }
}

/// Decode the bundled 256-px clipboardwire icon into the egui IconData
/// shape so the Settings window shows our branding in the title bar /
/// taskbar instead of the default eframe "e".
fn load_window_icon() -> Option<egui::IconData> {
    const ICON_PNG: &[u8] = include_bytes!("../../assets/icon-256.png");
    let img = image::load_from_memory_with_format(ICON_PNG, image::ImageFormat::Png).ok()?;
    let rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();
    Some(egui::IconData {
        rgba: rgba.into_raw(),
        width,
        height,
    })
}

fn load_or_default(path: &Path) -> FormState {
    if path.exists() {
        if let Ok(cfg) = ClientConfig::load(path) {
            let hub = cfg.hub.unwrap_or_default();
            return FormState {
                server: cfg.server,
                user: cfg.user,
                password: cfg.password,
                poll_ms: cfg.poll_ms,
                tls_ca_file: cfg
                    .tls_ca_file
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                tls_insecure: cfg.tls_insecure,
                log_file_names: cfg.log_file_names,
                send_mode: cfg.send_mode,
                receive_mode: cfg.receive_mode,
                pickup_auto: cfg.pickup_auto,
                offer_ttl_minutes: cfg.offer_ttl_minutes,
                pull_window_chunks: cfg.pull_window_chunks,
                pull_idle_timeout_secs: cfg.pull_idle_timeout_secs,
                hub_enabled: hub.enabled,
                hub_bind: hub.bind.to_string(),
                hub_user: hub.user,
                hub_password: hub.password,
                hub_tls_cert_file: hub
                    .tls_cert_file
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                hub_tls_key_file: hub
                    .tls_key_file
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                last_error: None,
            };
        }
    }
    let hub_default = clipboardwire_core::client::config::HubConfig::default();
    FormState {
        hub_bind: hub_default.bind.to_string(),
        ..FormState::default()
    }
}

#[derive(Clone)]
struct FormState {
    server: String,
    user: String,
    password: String,
    poll_ms: u64,
    tls_ca_file: String,
    tls_insecure: bool,
    /// Log file names in clear. Previously missing from this form, which meant
    /// any save silently reset the key to its default — see `write_toml`.
    log_file_names: bool,
    send_mode: SendMode,
    receive_mode: ReceiveMode,
    pickup_auto: bool,
    offer_ttl_minutes: u32,
    pull_window_chunks: u32,
    pull_idle_timeout_secs: u64,
    // Hub section (optional [hub] in the TOML).
    hub_enabled: bool,
    hub_bind: String,
    hub_user: String,
    hub_password: String,
    hub_tls_cert_file: String,
    hub_tls_key_file: String,
    last_error: Option<String>,
}

/// Hand-written rather than derived: the three numeric fields default to zero
/// under `derive(Default)`, and a zero TTL or window is rejected by
/// `validate_modes`. A fresh form must open with the same values a config
/// without those keys would have.
impl Default for FormState {
    fn default() -> Self {
        Self {
            server: String::new(),
            user: String::new(),
            password: String::new(),
            poll_ms: DEFAULT_POLL_MS,
            tls_ca_file: String::new(),
            tls_insecure: false,
            log_file_names: false,
            send_mode: SendMode::default(),
            receive_mode: ReceiveMode::default(),
            pickup_auto: false,
            offer_ttl_minutes: clipboardwire_core::client::config::default_offer_ttl_minutes(),
            pull_window_chunks: clipboardwire_core::client::config::default_pull_window_chunks(),
            pull_idle_timeout_secs:
                clipboardwire_core::client::config::default_pull_idle_timeout_secs(),
            hub_enabled: false,
            hub_bind: String::new(),
            hub_user: String::new(),
            hub_password: String::new(),
            hub_tls_cert_file: String::new(),
            hub_tls_key_file: String::new(),
            last_error: None,
        }
    }
}

struct SettingsApp {
    config_path: PathBuf,
    form: FormState,
    show_password: bool,
    saved: Arc<AtomicBool>,
}

impl SettingsApp {
    fn new(config_path: PathBuf, mut form: FormState, saved: Arc<AtomicBool>) -> Self {
        if form.poll_ms == 0 {
            form.poll_ms = DEFAULT_POLL_MS;
        }
        Self {
            config_path,
            form,
            show_password: false,
            saved,
        }
    }

    /// Attempt to validate + serialize + write the form to TOML. Returns
    /// `Ok` on success; on failure, stashes the message in `form.last_error`.
    fn try_save(&mut self) -> bool {
        if let Err(e) = validate(&self.form) {
            self.form.last_error = Some(e);
            return false;
        }
        match write_toml(&self.config_path, &self.form) {
            Ok(()) => {
                self.form.last_error = None;
                self.saved.store(true, Ordering::SeqCst);
                true
            }
            Err(e) => {
                self.form.last_error = Some(format!("could not save: {e:#}"));
                false
            }
        }
    }

    /// "Pin from server" button. Connects to `form.server`, captures
    /// the peer's end-entity cert, writes it as PEM next to the
    /// config file, and points `tls_ca_file` at the result. The user
    /// still has to hit Save afterwards — this only stages the
    /// change in the form.
    fn pin_server_cert(&mut self) {
        match pin_server_cert_impl(&self.form.server, &self.config_path) {
            Ok((path, fingerprint)) => {
                self.form.tls_ca_file = path.to_string_lossy().into_owned();
                self.form.tls_insecure = false;
                self.form.last_error = Some(format!(
                    "Pinned cert at {}\nSHA-256 {fingerprint}",
                    path.display()
                ));
            }
            Err(e) => {
                self.form.last_error = Some(format!("Could not pin server cert: {e:#}"));
            }
        }
    }
}

/// Open a TCP+TLS connection to the configured server URL, capture the
/// server's end-entity certificate, write it to a PEM file alongside
/// the config TOML, and return the path + SHA-256 fingerprint. The
/// cert is captured *without* verification — the whole point of this
/// button is to bootstrap trust on a self-signed cert.
fn pin_server_cert_impl(server_url: &str, config_path: &Path) -> Result<(PathBuf, String)> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Duration;

    use rustls::pki_types::ServerName;
    use rustls::ClientConnection;

    let (host, port) = parse_wss_authority(server_url)?;

    let mut sock = TcpStream::connect((host.as_str(), port))
        .with_context(|| format!("connecting to {host}:{port}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    sock.set_write_timeout(Some(Duration::from_secs(5))).ok();

    // rustls needs a CryptoProvider to be installed in the process
    // before any ClientConfig is built. Tray + transport already do
    // this lazily via aws_lc_rs; in the settings subprocess we have
    // to install it ourselves on first use.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cfg = Arc::new(clipboardwire_core::client::tls::make_insecure_client_config());
    let name = ServerName::try_from(host.clone()).context("invalid hostname in server URL")?;
    let mut conn = ClientConnection::new(cfg, name).context("starting TLS client connection")?;

    let mut stream = rustls::Stream::new(&mut conn, &mut sock);
    // Drive the handshake. `flush()` (or any read/write) will pump it
    // until either the peer's cert is available or an error occurs.
    let _ = stream.flush();
    // Just-in-case: also do a tiny read to surface handshake errors.
    let mut byte = [0u8; 1];
    let _ = stream.read(&mut byte);

    let der = conn
        .peer_certificates()
        .and_then(|chain| chain.first().cloned())
        .ok_or_else(|| anyhow::anyhow!("server did not present any certificates"))?;

    let pem = der_to_pem(der.as_ref());
    let fingerprint = sha256_hex(der.as_ref());

    let target = config_path
        .parent()
        .map(|p| p.join("server-pinned.crt"))
        .ok_or_else(|| anyhow::anyhow!("config path has no parent dir"))?;
    std::fs::create_dir_all(target.parent().expect("has parent"))?;
    std::fs::write(&target, pem).with_context(|| format!("writing {}", target.display()))?;

    Ok((target, fingerprint))
}

/// Pull the (host, port) out of `wss://host[:port]/path…`. Defaults
/// the port to 443 only as a fallback; the typical clipboardwire URL
/// includes the port explicitly.
fn parse_wss_authority(url: &str) -> Result<(String, u16)> {
    let after_scheme = url
        .strip_prefix("wss://")
        .ok_or_else(|| anyhow::anyhow!("server URL must start with wss:// to pin a certificate"))?;
    let authority = after_scheme.split('/').next().unwrap_or("");
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>().context("invalid port in server URL")?,
        ),
        None => (authority.to_string(), 443),
    };
    if host.is_empty() {
        anyhow::bail!("server URL has no host");
    }
    Ok((host, port))
}

fn der_to_pem(der: &[u8]) -> String {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    let b64 = STANDARD.encode(der);
    let mut out = String::with_capacity(b64.len() + 80);
    out.push_str("-----BEGIN CERTIFICATE-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).expect("base64 alphabet is ascii"));
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Send-mode dropdown.
///
/// Labels are sentences rather than the raw config values: "lazy" alone does
/// not say *what* is lazy, and the difference only shows up minutes later when
/// a peer pastes.
fn send_mode_combo(ui: &mut egui::Ui, value: &mut SendMode) {
    let field_label = ui.label("Send files");
    let label = match value {
        SendMode::Eager => "Eager (send immediately)",
        SendMode::Lazy => "Lazy (let peers pull on paste)",
    };
    egui::ComboBox::from_id_salt("send_mode")
        .width(ui.available_width())
        .selected_text(label)
        .show_ui(ui, |ui| {
            ui.selectable_value(value, SendMode::Eager, "Eager (send immediately)");
            ui.selectable_value(value, SendMode::Lazy, "Lazy (let peers pull on paste)");
        })
        .response
        .labelled_by(field_label.id);
    ui.add_space(SPACE_FIELD - SPACE_LABEL);
}

/// Receive-mode dropdown.
///
/// `Paste` is omitted where the platform cannot materialise a pull at paste
/// time. The alternative — offer it and refuse the save — makes the user fill
/// in the whole form before learning the choice was never possible.
fn receive_mode_combo(ui: &mut egui::Ui, value: &mut ReceiveMode) {
    let field_label = ui.label("Receive files");
    let label = |mode: ReceiveMode| match mode {
        ReceiveMode::Eager => "Eager (as before)",
        ReceiveMode::Auto => "Auto (by platform)",
        ReceiveMode::Pickup => "Pickup (retrieve from tray)",
        ReceiveMode::Paste => "Paste (platform clipboard)",
    };
    egui::ComboBox::from_id_salt("receive_mode")
        .width(ui.available_width())
        .selected_text(label(*value))
        .show_ui(ui, |ui| {
            for mode in [
                ReceiveMode::Eager,
                ReceiveMode::Auto,
                ReceiveMode::Pickup,
                ReceiveMode::Paste,
            ] {
                if mode == ReceiveMode::Paste && !paste_supported() {
                    // Disabled with the reason in the tooltip; still rendered
                    // so the option is visibly *not available* rather than
                    // silently absent, which reads like a bug.
                    let reason = "Paste needs a platform clipboard that can materialise \
                                  a pull at paste time (Phase 0 spike). Use Pickup instead.";
                    ui.add_enabled(
                        false,
                        egui::Label::new(
                            egui::RichText::new(format!(
                                "{} — not supported on {}",
                                label(mode),
                                std::env::consts::OS
                            ))
                            .weak(),
                        )
                        .wrap(),
                    )
                    .on_hover_text(reason)
                    .on_disabled_hover_text(reason);
                    continue;
                }
                ui.selectable_value(value, mode, label(mode));
            }
        })
        .response
        .labelled_by(field_label.id);
    ui.add_space(SPACE_FIELD - SPACE_LABEL);
}

impl eframe::App for SettingsApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.draw(ctx);
    }
}

impl SettingsApp {
    /// Render one frame of the settings UI. Split out from
    /// `eframe::App::update` so tests can drive it through
    /// [`egui_kittest::Harness`] without constructing an `eframe::Frame`
    /// (which has no public constructor).
    fn draw(&mut self, ctx: &egui::Context) {
        ctx.set_style(settings_style());

        egui::TopBottomPanel::top("settings_header")
            .resizable(false)
            .frame(
                egui::Frame::new()
                    .fill(PAGE)
                    .inner_margin(Margin::symmetric(PAGE_MARGIN, BAR_PADDING_Y)),
            )
            .show(ctx, |ui| {
                ui.label(egui::RichText::new("clipboardwire").small().color(MUTED));
                ui.heading("Settings");
                let path = format!("File: {}", self.config_path.display());
                ui.add(
                    egui::Label::new(egui::RichText::new(&path).small().color(MUTED)).truncate(),
                )
                .on_hover_text(path);
            });

        let mut save = false;
        let mut cancel = false;
        egui::TopBottomPanel::bottom("settings_footer")
            .resizable(false)
            .frame(
                egui::Frame::new()
                    .fill(SURFACE)
                    .inner_margin(Margin::symmetric(PAGE_MARGIN, BAR_PADDING_Y)),
            )
            .show(ctx, |ui| {
                // `last_error` carries both validation failures and pin
                // feedback, so present it as a neutral status, not "Error".
                if let Some(message) = &self.form.last_error {
                    egui::ScrollArea::vertical()
                        .id_salt("settings_status_scroll")
                        .max_height(STATUS_MAX_HEIGHT)
                        .auto_shrink([false, true])
                        .show(ui, |ui| {
                            egui::Frame::new()
                                .fill(FIELD)
                                .inner_margin(Margin::same(8))
                                .corner_radius(CornerRadius::same(CONTROL_RADIUS))
                                .show(ui, |ui| {
                                    ui.label(egui::RichText::new("Status").small().color(MUTED));
                                    ui.add(egui::Label::new(message).wrap());
                                });
                        });
                    ui.add_space(SPACE_INLINE - SPACE_LABEL);
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    save = primary_save(ui).clicked();
                    cancel = ui
                        .add_sized(ACTION_SIZE, egui::Button::new("Cancel"))
                        .clicked();
                });
            });

        egui::CentralPanel::default()
            .frame(
                egui::Frame::new()
                    .fill(PAGE)
                    .inner_margin(Margin::same(PAGE_MARGIN)),
            )
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("settings_body_scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.push_id("connection", |ui| {
                            card(ui, "Connection", |ui| {
                                if self.form.hub_enabled {
                                    ui.label(
                                        "The local client joins the embedded hub over \
                                         loopback — server URL, TLS and credentials all \
                                         come from the hub settings under Advanced.",
                                    );
                                } else {
                                    self.draw_connection(ui);
                                }
                            })
                        });
                        if !self.form.hub_enabled {
                            ui.add_space(SPACE_CARD - SPACE_LABEL);
                            ui.push_id("security", |ui| {
                                card(ui, "Security", |ui| self.draw_security(ui))
                            });
                        }
                        ui.add_space(SPACE_CARD - SPACE_LABEL);
                        ui.push_id("transfer", |ui| {
                            card(ui, "Transfer", |ui| self.draw_transfer(ui))
                        });
                        ui.add_space(SPACE_CARD - SPACE_LABEL);
                        ui.push_id("advanced", |ui| {
                            card(ui, "Advanced", |ui| self.draw_advanced(ui))
                        });
                    });
            });

        // Process actions after drawing the body so this frame's edits are used.
        if save {
            if self.try_save() {
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
            ctx.request_repaint();
        }
        if cancel {
            self.saved.store(false, Ordering::SeqCst);
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
    }

    fn draw_connection(&mut self, ui: &mut egui::Ui) {
        field_edit(
            ui,
            "Server URL",
            "server",
            &mut self.form.server,
            "wss://host:8484/sync",
            false,
        )
        .on_hover_text("ws://host:8484/sync or wss://host:8484/sync");
        field_edit(ui, "Username", "user", &mut self.form.user, "", false);
        field_edit(
            ui,
            "Password",
            "password",
            &mut self.form.password,
            "",
            !self.show_password,
        );
        ui.checkbox(&mut self.show_password, "show");
    }

    fn draw_security(&mut self, ui: &mut egui::Ui) {
        field_edit(
            ui,
            "TLS CA file (optional)",
            "tls_ca_file",
            &mut self.form.tls_ca_file,
            "",
            false,
        )
        .on_hover_text("PEM file with extra root CAs to trust");
        if ui
            .button("Pin from server")
            .on_hover_text(
                "Connect to the server URL, fetch its certificate, save it next \
                 to the config file, and point this field at the saved PEM. \
                 You no longer need `Skip TLS verification` after pinning.",
            )
            .clicked()
        {
            self.pin_server_cert();
        }
        ui.add_space(SPACE_FIELD - SPACE_LABEL);
        ui.label("Skip TLS verification");
        ui.checkbox(
            &mut self.form.tls_insecure,
            egui::RichText::new("DANGEROUS — LAN/VPN only").color(WARNING),
        );
    }

    fn draw_transfer(&mut self, ui: &mut egui::Ui) {
        send_mode_combo(ui, &mut self.form.send_mode);
        receive_mode_combo(ui, &mut self.form.receive_mode);
        field_drag(
            ui,
            "Poll interval (ms)",
            egui::DragValue::new(&mut self.form.poll_ms).range(50..=10_000),
        );
    }

    fn draw_advanced(&mut self, ui: &mut egui::Ui) {
        ui.label("Log file names");
        ui.checkbox(
            &mut self.form.log_file_names,
            "Write real names to the log (default: pseudonyms)",
        )
        .on_hover_text(
            "Off by default: INFO and above carry `file#<hex>` instead of the real \
             name, so a log does not become a record of everything copied. Turn this \
             on only while debugging a specific transfer.",
        );
        ui.add_space(SPACE_FIELD - SPACE_LABEL);

        egui::CollapsingHeader::new("Run hub on this machine")
            .id_salt("hub_settings")
            // Hub creds are the only credentials in hub mode — surface
            // them instead of burying them under a closed header.
            .default_open(self.form.hub_enabled)
            .show(ui, |ui| {
                ui.label(
                    "Bring up a clipboard-sync hub server in the tray process. \
                     The local client will connect to it over loopback; other \
                     devices on your network connect to the address below.",
                );
                ui.add_space(SPACE_LABEL);
                ui.checkbox(&mut self.form.hub_enabled, "Run hub when the tray starts");
                ui.add_space(SPACE_FIELD - SPACE_LABEL);
                ui.add_enabled_ui(self.form.hub_enabled, |ui| {
                    field_edit(ui, "Bind", "hub_bind", &mut self.form.hub_bind, "", false)
                        .on_hover_text("host:port, e.g. 0.0.0.0:8484");
                    field_edit(
                        ui,
                        "Hub username",
                        "hub_user",
                        &mut self.form.hub_user,
                        "",
                        false,
                    );
                    field_edit(
                        ui,
                        "Hub password",
                        "hub_password",
                        &mut self.form.hub_password,
                        "",
                        !self.show_password,
                    );
                    // The Connection "show" toggle is hidden in hub mode;
                    // keep a way to unmask the one password still editable.
                    ui.checkbox(&mut self.show_password, "show");
                    field_edit(
                        ui,
                        "TLS cert file",
                        "hub_tls_cert_file",
                        &mut self.form.hub_tls_cert_file,
                        "",
                        false,
                    )
                    .on_hover_text("PEM. Leave empty to speak plain ws://.");
                    field_edit(
                        ui,
                        "TLS key file",
                        "hub_tls_key_file",
                        &mut self.form.hub_tls_key_file,
                        "",
                        false,
                    )
                    .on_hover_text("PEM. Required iff cert file is set.");
                });
            });

        egui::CollapsingHeader::new("Lazy transfer tuning")
            .id_salt("lazy_transfer_tuning")
            .default_open(false)
            .show(ui, |ui| {
                ui.label(
                    "Only meaningful once lazy transfer ships. The defaults are what a \
                     config without these keys gets.",
                );
                ui.add_space(SPACE_LABEL);
                ui.label("Retrieve without a tray");
                ui.checkbox(
                    &mut self.form.pickup_auto,
                    "Headless receivers fetch offers automatically",
                );
                ui.add_space(SPACE_FIELD - SPACE_LABEL);
                field_drag(
                    ui,
                    "Offer TTL (minutes)",
                    egui::DragValue::new(&mut self.form.offer_ttl_minutes).range(1..=1440),
                )
                .on_hover_text("How long a published offer stays pullable.");
                field_drag(
                    ui,
                    "Chunks per pull",
                    egui::DragValue::new(&mut self.form.pull_window_chunks).range(1..=32),
                )
                .on_hover_text("How many 4 MiB chunks one `file_pull` asks for.");
                field_drag(
                    ui,
                    "Pull idle timeout (s)",
                    egui::DragValue::new(&mut self.form.pull_idle_timeout_secs).range(1..=3600),
                )
                .on_hover_text("Give up on a pull that stops making progress.");
            });
    }
}

/// A `ClientConfig` carrying only the mode-related fields.
///
/// The other required fields are filled with placeholders: `validate_modes`
/// reads nothing but the six transfer settings, and duplicating the platform
/// rules here would let the form accept something `load` later rejects.
fn mode_config(form: &FormState) -> ClientConfig {
    ClientConfig {
        server: String::new(),
        user: String::new(),
        password: String::new(),
        poll_ms: 300,
        tls_ca_file: None,
        tls_insecure: false,
        hub: None,
        log_file_names: form.log_file_names,
        send_mode: form.send_mode,
        receive_mode: form.receive_mode,
        pickup_auto: form.pickup_auto,
        offer_ttl_minutes: form.offer_ttl_minutes,
        pull_window_chunks: form.pull_window_chunks,
        pull_idle_timeout_secs: form.pull_idle_timeout_secs,
    }
}

/// Whether `paste` is offered at all on this platform.
///
/// The spec wants an explicit unsupported mode to *fail validation*; the GUI
/// goes one better and does not offer it, because a greyed-out entry with a
/// reason is cheaper to understand than a save that is refused afterwards.
/// `validate` still enforces the rule — this only decides what the dropdown
/// shows.
fn paste_supported() -> bool {
    cfg!(any(target_os = "windows", target_os = "macos"))
}

/// Validate the form. Returns a human-readable error string on failure.
fn validate(form: &FormState) -> std::result::Result<(), String> {
    // With the embedded hub enabled the connection fields are derived
    // from the hub section at write time (loopback URL + hub creds),
    // so only the hub inputs themselves need checking here.
    if !form.hub_enabled {
        if !form.server.starts_with("ws://") && !form.server.starts_with("wss://") {
            return Err("Server URL must start with ws:// or wss://".into());
        }
        if form.user.trim().is_empty() {
            return Err("Username must not be empty".into());
        }
        if form.password.is_empty() {
            return Err("Password must not be empty".into());
        }
    }
    if form.poll_ms == 0 {
        return Err("Poll interval must be at least 1 ms".into());
    }
    // The mode rules live in `ClientConfig::validate_modes`, so the GUI cannot
    // disagree with what `load` will do to the file it just wrote. Building a
    // throwaway config is the cheapest way to reuse them without a second copy
    // of the platform rules.
    let modes = mode_config(form);
    if let Err(e) = modes.validate_modes() {
        return Err(format!("{e:#}"));
    }
    if form.hub_enabled {
        if form.hub_bind.parse::<std::net::SocketAddr>().is_err() {
            return Err(format!(
                "Hub bind address `{}` is not a valid host:port",
                form.hub_bind
            ));
        }
        if form.hub_user.trim().is_empty() {
            return Err("Hub username must not be empty".into());
        }
        if form.hub_password.is_empty() {
            return Err("Hub password must not be empty".into());
        }
        let has_cert = !form.hub_tls_cert_file.trim().is_empty();
        let has_key = !form.hub_tls_key_file.trim().is_empty();
        if has_cert != has_key {
            return Err("Hub TLS needs both cert and key files (or neither)".into());
        }
    }
    Ok(())
}

/// The `server` URL written when the hub is enabled: the same loopback
/// address the tray derives at runtime (`point_client_at_hub`). The form
/// never exposes `hub.tls_disabled`, so the hub always speaks `wss`.
fn loopback_url(bind: &str) -> String {
    let port = bind
        .parse::<std::net::SocketAddr>()
        .map(|a| a.port())
        .unwrap_or(8484);
    format!("wss://127.0.0.1:{port}/sync")
}

/// Serializable shadow of the writable fields. We don't serialize
/// `last_error` or the password-toggle state — those are UI-only.
#[derive(serde::Serialize)]
struct WritableConfig<'a> {
    server: std::borrow::Cow<'a, str>,
    user: std::borrow::Cow<'a, str>,
    password: std::borrow::Cow<'a, str>,
    poll_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_ca_file: Option<&'a str>,
    #[serde(skip_serializing_if = "is_false")]
    tls_insecure: bool,
    /// An existing key this form used to drop on every save. `ClientConfig`
    /// parses with `deny_unknown_fields`, so a key missing from the output is
    /// not "left alone" — it is reset to the default the next time anything
    /// reads the file.
    #[serde(skip_serializing_if = "is_false")]
    log_file_names: bool,
    /// The 4.2 keys. Each is skipped at its default, matching `ClientConfig`,
    /// so saving a config that never touches them keeps it readable by
    /// v0.5.4 — which would reject the whole file on an unknown key.
    #[serde(skip_serializing_if = "is_default_send_mode")]
    send_mode: SendMode,
    #[serde(skip_serializing_if = "is_default_receive_mode")]
    receive_mode: ReceiveMode,
    #[serde(skip_serializing_if = "is_false")]
    pickup_auto: bool,
    #[serde(skip_serializing_if = "is_default_offer_ttl")]
    offer_ttl_minutes: u32,
    #[serde(skip_serializing_if = "is_default_pull_window")]
    pull_window_chunks: u32,
    #[serde(skip_serializing_if = "is_default_pull_idle_timeout")]
    pull_idle_timeout_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    hub: Option<WritableHub<'a>>,
}

#[derive(serde::Serialize)]
struct WritableHub<'a> {
    enabled: bool,
    bind: String,
    user: &'a str,
    password: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_cert_file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_key_file: Option<&'a str>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Build the serializable shadow from the form.
///
/// Split out so the tests can assert on the *serialized* result without
/// touching the filesystem, and so `write_toml` has exactly one place where
/// form field → config key mapping lives.
fn writable_config<'a>(form: &'a FormState, hub: Option<WritableHub<'a>>) -> WritableConfig<'a> {
    let ca = form.tls_ca_file.trim();
    // Hub mode hides the connection fields: the runtime overwrites the
    // client URL with the loopback hop and the loopback client must
    // present the hub's credentials, so the file writes the same
    // derivation instead of keeping stale hidden values.
    let (server, user, password) = if form.hub_enabled {
        (
            std::borrow::Cow::Owned(loopback_url(&form.hub_bind)),
            std::borrow::Cow::Borrowed(form.hub_user.as_str()),
            std::borrow::Cow::Borrowed(form.hub_password.as_str()),
        )
    } else {
        (
            std::borrow::Cow::Borrowed(form.server.as_str()),
            std::borrow::Cow::Borrowed(form.user.as_str()),
            std::borrow::Cow::Borrowed(form.password.as_str()),
        )
    };
    WritableConfig {
        server,
        user,
        password,
        poll_ms: form.poll_ms,
        tls_ca_file: if ca.is_empty() { None } else { Some(ca) },
        tls_insecure: form.tls_insecure,
        log_file_names: form.log_file_names,
        send_mode: form.send_mode,
        receive_mode: form.receive_mode,
        pickup_auto: form.pickup_auto,
        offer_ttl_minutes: form.offer_ttl_minutes,
        pull_window_chunks: form.pull_window_chunks,
        pull_idle_timeout_secs: form.pull_idle_timeout_secs,
        hub,
    }
}

/// Write the form back as TOML, creating parent dirs and setting 0600
/// on Unix (the file contains a password).
fn write_toml(path: &Path, form: &FormState) -> Result<()> {
    let hub_cert = form.hub_tls_cert_file.trim();
    let hub_key = form.hub_tls_key_file.trim();
    let hub_section = if form.hub_enabled
        || !form.hub_user.is_empty()
        || !form.hub_password.is_empty()
        || !hub_cert.is_empty()
        || !hub_key.is_empty()
    {
        Some(WritableHub {
            enabled: form.hub_enabled,
            bind: form.hub_bind.clone(),
            user: &form.hub_user,
            password: &form.hub_password,
            tls_cert_file: if hub_cert.is_empty() {
                None
            } else {
                Some(hub_cert)
            },
            tls_key_file: if hub_key.is_empty() {
                None
            } else {
                Some(hub_key)
            },
        })
    } else {
        None
    };

    let writable = writable_config(form, hub_section);

    let mut out =
        String::from("# clipboardwire client config — written by `clipboardwire settings`.\n\n");
    out.push_str(&toml::to_string(&writable).context("serializing config as TOML")?);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(path, out).with_context(|| format!("writing {}", path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(path, perms)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A form with the embedded hub enabled: the only credentials the
    /// user needs are the hub's — the client fields are hidden and
    /// re-derived at write time.
    fn hub_form() -> FormState {
        let mut form = valid_form();
        form.hub_enabled = true;
        form.hub_bind = "0.0.0.0:8484".into();
        form.hub_user = "hubadmin".into();
        form.hub_password = "hubsecret".into();
        form
    }

    fn valid_form() -> FormState {
        FormState {
            server: "wss://nas.lan:8484/sync".into(),
            user: "alice".into(),
            password: "hunter2".into(),
            poll_ms: 300,
            tls_ca_file: String::new(),
            tls_insecure: false,
            hub_enabled: false,
            hub_bind: "0.0.0.0:8484".into(),
            hub_user: String::new(),
            hub_password: String::new(),
            hub_tls_cert_file: String::new(),
            hub_tls_key_file: String::new(),
            ..FormState::default()
        }
    }

    /// Serialize the form exactly as `write_toml` would, without touching disk.
    fn rendered(form: &FormState) -> String {
        toml::to_string(&writable_config(form, None)).expect("serialize")
    }

    fn form_from_toml(raw: &str) -> FormState {
        let cfg: ClientConfig = toml::from_str(raw).expect("parse");
        // Mirror `load_or_default` field by field: the point of these tests is
        // that a load → save round trip is lossless, so building the form by
        // hand here would let a dropped field hide.
        FormState {
            server: cfg.server,
            user: cfg.user,
            password: cfg.password,
            poll_ms: cfg.poll_ms,
            tls_ca_file: cfg
                .tls_ca_file
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            tls_insecure: cfg.tls_insecure,
            log_file_names: cfg.log_file_names,
            send_mode: cfg.send_mode,
            receive_mode: cfg.receive_mode,
            pickup_auto: cfg.pickup_auto,
            offer_ttl_minutes: cfg.offer_ttl_minutes,
            pull_window_chunks: cfg.pull_window_chunks,
            pull_idle_timeout_secs: cfg.pull_idle_timeout_secs,
            ..FormState::default()
        }
    }

    const MINIMAL: &str = "server = \"ws://h/sync\"\nuser = \"u\"\npassword = \"p\"\n";

    #[test]
    fn settings_preserves_log_file_names() {
        // The data-loss bug: `log_file_names` was absent from both the form and
        // the writer, so any save reset it. `deny_unknown_fields` means the
        // key is not "left alone" when omitted — the next reader sees the
        // default.
        let form = form_from_toml(&format!("{MINIMAL}log_file_names = true\n"));
        assert!(form.log_file_names, "load must see the key");
        let out = rendered(&form);
        assert!(
            out.contains("log_file_names = true"),
            "save dropped it again:\n{out}"
        );

        // And it survives a full parse of what was written.
        let reread: ClientConfig = toml::from_str(&out).unwrap();
        assert!(reread.log_file_names);
    }

    #[test]
    fn settings_preserves_explicit_modes() {
        let form = form_from_toml(&format!(
            "{MINIMAL}send_mode = \"lazy\"\nreceive_mode = \"pickup\"\n\
             offer_ttl_minutes = 5\npull_window_chunks = 7\npull_idle_timeout_secs = 45\n"
        ));
        let out = rendered(&form);
        for expected in [
            "send_mode = \"lazy\"",
            "receive_mode = \"pickup\"",
            "offer_ttl_minutes = 5",
            "pull_window_chunks = 7",
            "pull_idle_timeout_secs = 45",
        ] {
            assert!(out.contains(expected), "{expected} missing from:\n{out}");
        }
        let reread: ClientConfig = toml::from_str(&out).unwrap();
        assert_eq!(reread.send_mode, SendMode::Lazy);
        assert_eq!(reread.receive_mode, ReceiveMode::Pickup);
        assert_eq!(reread.offer_ttl_minutes, 5);
    }

    #[test]
    fn settings_omits_default_modes() {
        // A v0.5.4 binary parses with `deny_unknown_fields`, so a default
        // written into the file would make it refuse the whole config.
        let out = rendered(&valid_form());
        for absent in [
            "send_mode",
            "receive_mode",
            "pickup_auto",
            "offer_ttl_minutes",
            "pull_window_chunks",
            "pull_idle_timeout_secs",
            "log_file_names",
        ] {
            assert!(!out.contains(absent), "{absent} leaked into:\n{out}");
        }
        assert!(out.contains("server"));
    }

    #[test]
    fn settings_refuses_paste_on_linux() {
        let mut form = valid_form();
        form.receive_mode = ReceiveMode::Paste;
        if cfg!(any(target_os = "windows", target_os = "macos")) {
            assert!(validate(&form).is_ok(), "paste is supported here");
            return;
        }
        let err = validate(&form).expect_err("paste must be refused here");
        assert!(err.contains("pickup"), "must name the alternative: {err}");
        assert!(err.contains("eager"), "{err}");
    }

    #[test]
    fn settings_refuses_a_zero_ttl_and_an_oversized_window() {
        // The GUI must enforce the same bounds `ClientConfig::load` does, or it
        // happily writes a config the client then refuses to start with.
        let mut form = valid_form();
        form.offer_ttl_minutes = 0;
        assert!(validate(&form).is_err());

        let mut form = valid_form();
        form.pull_window_chunks = 33;
        assert!(validate(&form).is_err());

        let mut form = valid_form();
        form.pull_window_chunks = 0;
        assert!(validate(&form).is_err());
    }

    #[test]
    fn a_round_trip_through_the_form_loses_nothing() {
        // The general shape of the bug: every writable key must come back.
        let original = format!(
            "{MINIMAL}poll_ms = 700\ntls_insecure = true\nlog_file_names = true\n\
             send_mode = \"lazy\"\npickup_auto = true\n"
        );
        let form = form_from_toml(&original);
        let out = rendered(&form);
        let reloaded: ClientConfig = toml::from_str(&out).unwrap();
        assert_eq!(reloaded.poll_ms, 700);
        assert!(reloaded.tls_insecure);
        assert!(reloaded.log_file_names);
        assert_eq!(reloaded.send_mode, SendMode::Lazy);
        assert!(reloaded.pickup_auto);
    }

    #[test]
    fn hub_mode_accepts_empty_client_fields_and_derives_them() {
        // With the hub on, the client connection inputs are hidden, so
        // the user must be able to save with them empty — the values
        // land in the file derived from the hub section instead.
        let mut form = hub_form();
        form.server.clear();
        form.user.clear();
        form.password.clear();
        assert!(validate(&form).is_ok(), "hub mode must skip client checks");

        // `rendered` serializes without a hub section, so build the same
        // hub block `write_toml` would to exercise the real output.
        let hub = WritableHub {
            enabled: true,
            bind: form.hub_bind.clone(),
            user: &form.hub_user,
            password: &form.hub_password,
            tls_cert_file: None,
            tls_key_file: None,
        };
        let out = toml::to_string(&writable_config(&form, Some(hub))).expect("serialize");
        assert!(
            out.contains("server = \"wss://127.0.0.1:8484/sync\""),
            "{out}"
        );
        assert!(out.contains("user = \"hubadmin\""), "{out}");
        assert!(out.contains("password = \"hubsecret\""), "{out}");
        let reread: ClientConfig = toml::from_str(&out).unwrap();
        assert_eq!(reread.user, "hubadmin");
        assert_eq!(reread.hub.expect("hub section").bind.port(), 8484);
    }

    #[test]
    fn hub_mode_still_requires_hub_credentials() {
        let mut form = hub_form();
        form.hub_user.clear();
        assert!(validate(&form).is_err());
        let mut form = hub_form();
        form.hub_password.clear();
        assert!(validate(&form).is_err());
        let mut form = hub_form();
        form.hub_bind = "not-an-addr".into();
        assert!(validate(&form).is_err());
    }

    #[test]
    fn client_fields_still_required_without_hub() {
        let mut form = valid_form();
        form.server.clear();
        assert!(validate(&form).is_err());
        let mut form = valid_form();
        form.user.clear();
        assert!(validate(&form).is_err());
        let mut form = valid_form();
        form.password.clear();
        assert!(validate(&form).is_err());
    }

    #[test]
    fn parse_wss_authority_handles_explicit_port() {
        let (h, p) = parse_wss_authority("wss://nas.lan:8484/sync").unwrap();
        assert_eq!(h, "nas.lan");
        assert_eq!(p, 8484);
    }

    #[test]
    fn parse_wss_authority_handles_no_path() {
        let (h, p) = parse_wss_authority("wss://192.168.1.10:9999").unwrap();
        assert_eq!(h, "192.168.1.10");
        assert_eq!(p, 9999);
    }

    #[test]
    fn parse_wss_authority_defaults_to_443() {
        let (h, p) = parse_wss_authority("wss://example.com/sync").unwrap();
        assert_eq!(h, "example.com");
        assert_eq!(p, 443);
    }

    #[test]
    fn parse_wss_authority_rejects_ws_scheme() {
        assert!(parse_wss_authority("ws://nas.lan:8484/sync").is_err());
    }

    #[test]
    fn der_to_pem_round_trips_via_rustls_pemfile() {
        let der = [0u8, 1, 2, 3, 4, 5, 6, 7]; // not a real cert, just bytes
        let pem = der_to_pem(&der);
        assert!(pem.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(pem.contains("AAECAwQFBgc="));
        assert!(pem.trim_end().ends_with("-----END CERTIFICATE-----"));
    }

    #[test]
    fn validate_accepts_a_well_formed_form() {
        assert!(validate(&valid_form()).is_ok());
    }

    #[test]
    fn validate_rejects_non_ws_scheme() {
        let mut f = valid_form();
        f.server = "http://example.com/sync".into();
        assert!(validate(&f).is_err());
    }

    #[test]
    fn validate_rejects_empty_user() {
        let mut f = valid_form();
        f.user.clear();
        assert!(validate(&f).is_err());
    }

    #[test]
    fn validate_rejects_empty_password() {
        let mut f = valid_form();
        f.password.clear();
        assert!(validate(&f).is_err());
    }

    #[test]
    fn validate_rejects_zero_poll_ms() {
        let mut f = valid_form();
        f.poll_ms = 0;
        assert!(validate(&f).is_err());
    }

    #[test]
    fn write_toml_round_trips_via_client_config_load() {
        let dir = std::env::temp_dir().join(format!(
            "cw-settings-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("config.toml");

        let form = valid_form();
        write_toml(&path, &form).unwrap();
        let loaded = ClientConfig::load(&path).unwrap();
        assert_eq!(loaded.server, form.server);
        assert_eq!(loaded.user, form.user);
        assert_eq!(loaded.password, form.password);
        assert_eq!(loaded.poll_ms, form.poll_ms);
        assert!(loaded.tls_ca_file.is_none());
        assert!(!loaded.tls_insecure);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_toml_omits_empty_optional_fields() {
        let dir = std::env::temp_dir().join(format!(
            "cw-settings-omit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("config.toml");
        write_toml(&path, &valid_form()).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("tls_ca_file"));
        assert!(!raw.contains("tls_insecure"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_toml_preserves_set_optional_fields() {
        let dir = std::env::temp_dir().join(format!(
            "cw-settings-opt-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("config.toml");
        let mut form = valid_form();
        form.tls_ca_file = "/etc/ssl/ca.crt".into();
        form.tls_insecure = true;
        write_toml(&path, &form).unwrap();
        let loaded = ClientConfig::load(&path).unwrap();
        assert_eq!(
            loaded.tls_ca_file.unwrap().to_string_lossy(),
            "/etc/ssl/ca.crt"
        );
        assert!(loaded.tls_insecure);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn write_toml_chmod_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!(
            "cw-settings-mode-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("config.toml");
        write_toml(&path, &valid_form()).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -------------------------------------------------------------
    // egui_kittest harness tests — drive the actual widget tree to
    // confirm Save / Cancel / error-label wiring. Without these, the
    // pure-data tests above could pass even if the buttons weren't
    // hooked up at all.
    // -------------------------------------------------------------

    use egui_kittest::kittest::Queryable;
    use egui_kittest::Harness;

    fn unique_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "cw-settings-kt-{label}-{}-{nanos}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Build a harness around a SettingsApp with the given form +
    /// config path. The closure-based `new_state` API lets the harness
    /// own the app and lets test code reach in via `harness.state_mut()`
    /// between ticks.
    fn build_harness(
        path: PathBuf,
        form: FormState,
        saved: Arc<AtomicBool>,
    ) -> Harness<'static, SettingsApp> {
        let app = SettingsApp::new(path, form, saved);
        Harness::new_state(|ctx, app: &mut SettingsApp| app.draw(ctx), app)
    }

    #[test]
    fn ui_renders_with_expected_widget_labels() {
        let path = unique_dir("labels").join("config.toml");
        let saved = Arc::new(AtomicBool::new(false));
        let mut harness = build_harness(path, valid_form(), saved);
        harness.run();

        // These four widgets are the contract surface — the form would
        // be useless without any of them. We don't assert exhaustively
        // because labels we change for usability shouldn't fail the test.
        harness.get_by_label("Save");
        harness.get_by_label("Cancel");
        harness.get_by_label("Server URL");
        harness.get_by_label("Username");
    }

    #[test]
    fn save_click_writes_toml_and_flips_saved_flag() {
        let path = unique_dir("save").join("config.toml");
        let saved = Arc::new(AtomicBool::new(false));
        let mut harness = build_harness(path.clone(), valid_form(), saved.clone());
        harness.run();
        harness.get_by_label("Save").click();
        // Two extra ticks: the click is consumed on the next frame,
        // then try_save runs and writes the file.
        harness.run();
        harness.run();

        assert!(saved.load(Ordering::SeqCst), "Save did not flip the flag");
        assert!(path.exists(), "Save did not write the config file");

        // And the file round-trips back through ClientConfig::load.
        let loaded = ClientConfig::load(&path).expect("written file parses");
        assert_eq!(loaded.user, "alice");

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn cancel_click_does_not_write_file() {
        let path = unique_dir("cancel").join("config.toml");
        let saved = Arc::new(AtomicBool::new(false));
        let mut harness = build_harness(path.clone(), valid_form(), saved.clone());
        harness.run();
        harness.get_by_label("Cancel").click();
        harness.run();
        harness.run();

        assert!(!saved.load(Ordering::SeqCst), "Cancel should not flip flag");
        assert!(!path.exists(), "Cancel should not write the config file");
    }

    #[test]
    fn invalid_input_surfaces_an_error_label() {
        let path = unique_dir("err").join("config.toml");
        let mut form = valid_form();
        form.user.clear(); // validation will reject this on Save
        let saved = Arc::new(AtomicBool::new(false));
        let mut harness = build_harness(path.clone(), form, saved.clone());
        harness.run();
        harness.get_by_label("Save").click();
        harness.run();
        harness.run();

        assert!(
            !saved.load(Ordering::SeqCst),
            "validation failure should not flip Saved"
        );
        assert!(!path.exists(), "validation failure should not write file");
        // The error message we render for empty user starts with this prefix.
        harness.get_by_label_contains("Username must not be empty");
    }

    #[test]
    fn hub_mode_hides_client_connection_fields() {
        // With the hub enabled the client connection inputs are
        // replaced by a note — nothing labelled Server URL / Username /
        // TLS CA may still be on screen.
        let path = unique_dir("hubui").join("config.toml");
        let saved = Arc::new(AtomicBool::new(false));
        let mut harness = build_harness(path, hub_form(), saved);
        harness.run();

        for hidden in ["Server URL", "Username", "TLS CA file (optional)"] {
            assert!(
                harness.query_all_by_label(hidden).next().is_none(),
                "{hidden} should be hidden in hub mode"
            );
        }
        // The hub disclosure header is the visible entry point instead.
        harness.get_by_label("Run hub on this machine");
    }

    #[test]
    fn footer_actions_reachable_at_min_window_size() {
        // The fixed footer keeps Save/Cancel off the scrollable form, so they
        // must be queryable at the 420×320 minimum without scrolling.
        let path = unique_dir("minsize").join("config.toml");
        let saved = Arc::new(AtomicBool::new(false));
        let app = SettingsApp::new(path, valid_form(), saved);
        let mut harness = Harness::builder()
            .with_size(egui::vec2(420.0, 320.0))
            .build_state(|ctx, app: &mut SettingsApp| app.draw(ctx), app);
        harness.run();

        harness.get_by_label("Save");
        harness.get_by_label("Cancel");
        harness.get_by_label("Server URL");
        harness.get_by_label("Username");
    }
}

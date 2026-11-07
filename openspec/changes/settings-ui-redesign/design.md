# Settings window redesign

## Scope and decision

Design only; no source changes accompany this document. Implementation targets `cli/src/settings.rs`, especially `SettingsApp::draw`, `send_mode_combo`, and `receive_mode_combo`. Inspection baseline: 2026-11-04. `cli/Cargo.toml` requests eframe and egui_kittest `0.32`; `Cargo.lock` resolves eframe, egui, and egui_kittest to **0.32.3**.

Use a single vertically scrollable settings page, with three always-open cards (Connection, Security, Transfer), followed by an Advanced card containing the existing two disclosures. Keep the title and action footer outside the body scroll area. Use a deliberately light theme: slate page, white cards, restrained blue accents, clear borders, and a filled primary Save button. The settings subprocess owns its egui context, so this theme affects only this window. No theme preference, sidebar navigation, icons, custom fonts, new config fields, or dependencies are needed. `egui_extras` offers no useful advantage for this layout.

Assumption: increasing the initial window size is acceptable. Recommend **640 × 720 logical points**, retain **420 × 320** as the minimum, and keep resizing enabled. The existing 520 × 360 size must still work. The form intentionally scrolls; fitting every control into the initial viewport is not a goal. Single-column fields avoid a narrow label column and adapt without breakpoint-dependent widget identities.

## Wireframe and layout

This is the full scrollable page, not a claim that all cards fit simultaneously. At the initial size the lower cards continue below the fold.

```text
+----------------------------------------------------------------+
| clipboardwire                                                  | fixed header
| Settings                                                       |
| File: /Users/dawn/.../client.toml                               |
+----------------------------------------------------------------+
|                                                                |
| +-- Connection ----------------------------------------------+ | ^
| | Server URL                                                 | | |
| | [wss://host:8484/sync                                      ] | | |
| | Username                                                   | | |
| | [alice                                                   ] | | |
| | Password                                                   | | | body
| | [************************                                ] | | | scrolls
| | [ ] show                                                   | | |
| +------------------------------------------------------------+ | |
|                                                                | |
| +-- Security ------------------------------------------------+ | |
| | TLS CA file (optional)                                     | | |
| | [/path/to/ca.pem                                          ] | | |
| | [ Pin from server ]                                        | | |
| | Skip TLS verification                                      | | |
| | [ ] DANGEROUS — LAN/VPN only                                | | |
| +------------------------------------------------------------+ | |
|                                                                | |
| +-- Transfer ------------------------------------------------+ | |
| | Send files                                                 | | |
| | [Eager (send immediately)                               v ] | | |
| | Receive files                                              | | |
| | [Auto (by platform)                                     v ] | | |
| | Poll interval (ms)                                         | | |
| | [ 300       ]                                              | | |
| +------------------------------------------------------------+ | |
|                                                                | |
| +-- Advanced ------------------------------------------------+ | |
| | Log file names                                             | | |
| | [ ] Write real names to the log (default: pseudonyms)        | | |
| | > Run hub on this machine                                  | | |
| | > Lazy transfer tuning                                     | | v
| +------------------------------------------------------------+ |
+----------------------------------------------------------------+
| [Status message, only when present; wraps, bounded scroll]      | fixed footer
|                                         [ Cancel ] [  Save  ]  |
+----------------------------------------------------------------+
```

- Header: 20-point horizontal padding, 12-point vertical padding. A small muted `clipboardwire` label, 22-point `Settings` heading, then a 12-point `File: …` line. Truncate the path to one line, keep the entire path in hover text. Never truncate editable paths or status-message contents.
- Body: 20-point outer margin; full-width cards with 16-point inner padding, 10-point corners, and a 1-point border. No card shadows or decorative gradients. Card title: 16-point bold. Separate cards by 16 points total, title from contents by 12, field groups by 12, and label from control by 4. Explicitly account for egui's automatic item spacing rather than adding these values twice.
- Fields: stack label over control at every width. Text edits and closed combo boxes use available card width; numeric controls use 112 points. Target 32-point controls; action buttons are 96 × 36. Show-password and Pin buttons sit on the next line at all widths, avoiding horizontal overflow or layout-dependent IDs.
- Advanced: Log file names is always rendered. Keep `Run hub on this machine` and `Lazy transfer tuning` as separately collapsed, initially closed headers. Opening them grows the scrollable body. Do not collapse Connection/Security/Transfer or hide controls based on the current transfer mode.
- Footer: white surface, 1-point top divider, 20-point horizontal and 12-point vertical padding. Save is rightmost; Cancel is immediately to its left with 8 points between. Never scroll these buttons with the form. No new keyboard shortcuts, automatic saving, reset button, unsaved-change prompt, or connection-status claims.
- Status: reserve no empty banner when `last_error` is `None`. Otherwise show its exact complete text in a wrapped label inside a separate vertical scroll area, capped at 64 points, above the actions. Request a repaint after an action creates a message. The bounded area keeps actions visible at minimum height, including long file-system errors and certificate fingerprints. The existing `last_error` also carries successful pin information: use a neutral `Status` presentation with normal text and a subtle background for **all** messages, rather than incorrectly labelling every message “Error.” Failure messages retain their exact wording; do not parse strings to invent status types or change the underlying state model.

## Theme tokens and egui style

All geometry is in logical points. Use egui's bundled proportional font; retain its font fallback and keyboard focus handling. These Rust constants and helpers specify the complete set of theme overrides. Properties not assigned here deliberately retain `egui::Style::default()` / `Visuals::light()` behavior, including popup shadows and scrollbar mechanics.

```rust
use eframe::egui::{self, Color32, CornerRadius, FontId, Margin, Stroke, TextStyle};

const PAGE: Color32 = Color32::from_rgb(244, 246, 250);       // #F4F6FA
const SURFACE: Color32 = Color32::from_rgb(255, 255, 255);    // #FFFFFF
const FIELD: Color32 = Color32::from_rgb(248, 250, 252);      // #F8FAFC
const HOVER: Color32 = Color32::from_rgb(235, 241, 249);      // #EBF1F9
const BORDER: Color32 = Color32::from_rgb(203, 213, 225);     // #CBD5E1
const TEXT: Color32 = Color32::from_rgb(30, 41, 59);          // #1E293B
const MUTED: Color32 = Color32::from_rgb(71, 85, 105);        // #475569
const ACCENT: Color32 = Color32::from_rgb(29, 78, 216);       // #1D4ED8
const ACCENT_HOVER: Color32 = Color32::from_rgb(30, 64, 175); // #1E40AF
const ACCENT_ACTIVE: Color32 = Color32::from_rgb(30, 58, 138);// #1E3A8A
const ACCENT_SOFT: Color32 = Color32::from_rgb(219, 234, 254);// #DBEAFE
const ON_ACCENT: Color32 = Color32::WHITE;
const WARNING: Color32 = Color32::from_rgb(146, 64, 14);     // #92400E
const ERROR: Color32 = Color32::from_rgb(185, 28, 28);        // #B91C1C

const INITIAL_SIZE: [f32; 2] = [640.0, 720.0];
const MIN_SIZE: [f32; 2] = [420.0, 320.0];
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
    s.text_styles.insert(TextStyle::Heading, FontId::proportional(FONT_TITLE));
    s.text_styles.insert(TextStyle::Body, FontId::proportional(FONT_BODY));
    s.text_styles.insert(TextStyle::Button, FontId::proportional(FONT_BODY));
    s.text_styles.insert(TextStyle::Small, FontId::proportional(FONT_SMALL));
    s.text_styles.insert(TextStyle::Monospace, FontId::monospace(FONT_SMALL));
    s.spacing.item_spacing = egui::vec2(SPACE_INLINE, SPACE_LABEL);
    s.spacing.button_padding = egui::vec2(12.0, 6.0);
    s.spacing.interact_size = egui::vec2(40.0, CONTROL_HEIGHT);
    s.spacing.indent = 16.0;
    s.spacing.icon_width = 18.0;
    s.spacing.icon_width_inner = 10.0;
    s.spacing.icon_spacing = SPACE_INLINE;
    s.spacing.text_edit_width = 280.0; // fallback; fields explicitly fill width
    s.spacing.combo_width = 280.0;     // fallback; combos explicitly fill width
    s.animation_time = 0.0;            // deterministic disclosure/test frames
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
    v.error_fg_color = ERROR;
    v.selection.bg_fill = ACCENT_SOFT;
    v.selection.stroke = Stroke::new(STROKE_NORMAL, ACCENT);
    v.disabled_alpha = DISABLED_ALPHA;
    v.collapsing_header_frame = true;
    v.indent_has_left_vline = false;
    for (w, fill, border, width) in [
        (&mut v.widgets.noninteractive, SURFACE, BORDER, STROKE_NORMAL),
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
    }).inner
}
```

Use `WARNING` for the existing `DANGEROUS — LAN/VPN only` checkbox caption without removing its wording. Do not tint an entire Security card amber or require another confirmation. Blue is reserved for primary action, focused/hovered controls, and selections. Disabled controls retain actual disabled interaction semantics, not just gray paint. No decorative use of red.

## Mapping every existing control

Labels and hover strings below are contracts. Preserve the full current hover text verbatim when moving widgets; hints and section headings do not replace it. The 19 config fields remain bound directly to `self.form`, and the shared `self.show_password` remains UI-only.

| Existing location / field | New placement and widget | Preservation details |
| --- | --- | --- |
| Heading, config path | Fixed header described above | Keep full `config_path` on hover; no browse action. |
| Grid / `server` | Connection / `Server URL`, full-width `TextEdit::singleline` | Keep `ws://host:8484/sync or wss://host:8484/sync` tooltip. |
| Grid / `user` | Connection / `Username`, full-width single-line edit | No trimming or normalization on editing. |
| Grid / `password` | Connection / `Password`, full-width single-line edit | `.password(!self.show_password)`; retain checkbox label `show` underneath. |
| Grid / `tls_ca_file` | Security / `TLS CA file (optional)`, full-width edit | Preserve `PEM file with extra root CAs to trust` tooltip. |
| Pin button | Security / secondary `Pin from server` below CA edit | Keep full existing multi-sentence tooltip and call `self.pin_server_cert()` directly. Remain clickable for any URL; existing handler rejects non-wss URLs. |
| Grid / `tls_insecure` | Security / `Skip TLS verification` label and checkbox beneath | Keep `DANGEROUS — LAN/VPN only` caption. Do not disable when URL is ws or a CA path is present. |
| Grid / `send_mode` | Transfer / `Send files`, full-width existing combo | Preserve ID salt `send_mode`, option order, labels, and enum bindings. |
| Grid / `receive_mode` | Transfer / `Receive files`, full-width existing combo | Preserve ID salt `receive_mode`, option order, platform gating, and disabled Paste row. |
| Grid / `poll_ms` | Transfer / `Poll interval (ms)`, 112-point `DragValue` | Keep `50..=10_000`; do not confuse UI range with validation's nonzero rule. |
| Grid / `log_file_names` | Advanced / `Log file names`, checkbox beneath | Keep `Write real names to the log (default: pseudonyms)` and the full privacy/debugging tooltip. Wrap caption. |
| Hub / `hub_enabled` | Advanced → `Run hub on this machine` / checkbox | Keep original explanatory paragraph and `Run hub when the tray starts`. Header open state is independent of enabled state. |
| Hub / `hub_bind` | Same disclosure / `Bind`, full-width edit | Keep `host:port, e.g. 0.0.0.0:8484` tooltip; retain actual `SocketAddr` validation, not a new hostname parser. |
| Hub / `hub_user` | Same disclosure / `Hub username`, full-width edit | Remains disabled with hub off. |
| Hub / `hub_password` | Same disclosure / `Hub password`, full-width edit | Keep `.password(!self.show_password)`; no second visibility state. |
| Hub / `hub_tls_cert_file` | Same disclosure / `TLS cert file`, full-width edit | Keep `PEM. Leave empty to speak plain ws://.` tooltip. |
| Hub / `hub_tls_key_file` | Same disclosure / `TLS key file`, full-width edit | Keep `PEM. Required iff cert file is set.` tooltip. |
| Tuning / `pickup_auto` | Advanced → `Lazy transfer tuning` / `Retrieve without a tray` label and checkbox | Keep `Headless receivers fetch offers automatically`. Keep existing explanatory paragraph. |
| Tuning / `offer_ttl_minutes` | Same disclosure / `Offer TTL (minutes)`, 112-point `DragValue` | Keep `1..=1440` and `How long a published offer stays pullable.` tooltip. |
| Tuning / `pull_window_chunks` | Same disclosure / `Chunks per pull`, 112-point `DragValue` | Keep `1..=32` and “How many 4 MiB chunks one `file_pull` asks for.” tooltip, including the literal backticks around `file_pull`. |
| Tuning / `pull_idle_timeout_secs` | Same disclosure / `Pull idle timeout (s)`, 112-point `DragValue` | Keep `1..=3600` and `Give up on a pull that stops making progress.` tooltip. |
| `last_error` label | Fixed footer / bounded, wrapped status label | Keep complete string and persistence until existing handlers change it. No clearing on edit or delayed validation. |
| Save / Cancel | Fixed footer / primary Save, secondary Cancel | Keep exact labels and click semantics; Save remains enabled even for invalid input. |

All five hub detail edits stay inside `ui.add_enabled_ui(self.form.hub_enabled, …)`. Do not clear their values when disabled or when their disclosure closes. Keep the four tuning controls enabled regardless of send/receive modes, as today. The two explanations include existing implementation-era wording; rewriting that copy is outside this visual change.

## Implementation shape keyed to existing functions

The following is a layout skeleton: `draw_connection`, `draw_security`, `draw_transfer`, and `draw_advanced` are proposed private `SettingsApp` methods populated using the mapping above. They only render controls; validation, serialization, certificate fetching, and process control stay in their existing functions. Small local helpers are sufficient; no component framework or generalized settings schema is needed.

```rust
// Inside SettingsApp::draw(ctx), before constructing any panels:
ctx.set_theme(egui::Theme::Light);
ctx.set_style(settings_style());

egui::TopBottomPanel::top("settings_header")
    .resizable(false)
    .frame(egui::Frame::new().fill(PAGE)
        .inner_margin(Margin::symmetric(PAGE_MARGIN, BAR_PADDING_Y)))
    .show(ctx, |ui| {
        ui.label(egui::RichText::new("clipboardwire").small().color(MUTED));
        ui.heading("Settings");
        let path = format!("File: {}", self.config_path.display());
        ui.add(egui::Label::new(egui::RichText::new(&path).small().color(MUTED))
            .truncate()).on_hover_text(path);
    });

let mut save = false;
let mut cancel = false;
egui::TopBottomPanel::bottom("settings_footer")
    .resizable(false)
    .frame(egui::Frame::new().fill(SURFACE)
        .inner_margin(Margin::symmetric(PAGE_MARGIN, BAR_PADDING_Y)))
    .show(ctx, |ui| {
        // Panel top divider; decoration only, never paint interactive controls.
        let r = ui.max_rect();
        let y = r.top() - f32::from(BAR_PADDING_Y);
        ui.painter().line_segment(
            [egui::pos2(r.left(), y), egui::pos2(r.right(), y)],
            Stroke::new(STROKE_NORMAL, BORDER),
        );
        if let Some(message) = &self.form.last_error {
            egui::ScrollArea::vertical()
                .id_salt("settings_status_scroll")
                .max_height(STATUS_MAX_HEIGHT)
                .auto_shrink([false, true])
                .show(ui, |ui| {
                    egui::Frame::new().fill(FIELD)
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
            cancel = ui.add_sized(ACTION_SIZE, egui::Button::new("Cancel")).clicked();
        });
    });

// CentralPanel must be last: it receives the space left between the bars.
egui::CentralPanel::default()
    .frame(egui::Frame::new().fill(PAGE).inner_margin(Margin::same(PAGE_MARGIN)))
    .show(ctx, |ui| {
        egui::ScrollArea::vertical()
            .id_salt("settings_body_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.push_id("connection", |ui| card(ui, "Connection", |ui| self.draw_connection(ui)));
                ui.add_space(SPACE_CARD - SPACE_LABEL);
                ui.push_id("security", |ui| card(ui, "Security", |ui| self.draw_security(ui)));
                ui.add_space(SPACE_CARD - SPACE_LABEL);
                ui.push_id("transfer", |ui| card(ui, "Transfer", |ui| self.draw_transfer(ui)));
                ui.add_space(SPACE_CARD - SPACE_LABEL);
                ui.push_id("advanced", |ui| card(ui, "Advanced", |ui| self.draw_advanced(ui)));
            });
    });

// Process actions after drawing the body so any edits from this frame are used.
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
```

Calling the small style helper at the start of `draw` makes styling identical in native and harness execution, with no app state or initialization flag. Do not install it only in `run_native`'s creation closure: current tests bypass that closure. If later profiling warrants caching, cache the style without changing this entry-point contract. `update` continues to call `draw` directly.

Representative field code inside `draw_connection` (repeat this binding pattern for other edits):

```rust
let label = ui.label("Server URL");
ui.add_sized([ui.available_width(), CONTROL_HEIGHT],
    egui::TextEdit::singleline(&mut self.form.server)
        .id_salt("server")
        .hint_text("wss://host:8484/sync"))
    .labelled_by(label.id)
    .on_hover_text("ws://host:8484/sync or wss://host:8484/sync");
ui.add_space(SPACE_FIELD - SPACE_LABEL);
// Next label/control pair follows. Never use an empty hint as its only label.
```

Use literal field-name ID salts for text edits (`server`, `user`, `password`, etc.). Attach each visible label to its control using `Response::labelled_by`; do the same for numeric edits. Checkboxes already have their full visible captions; keep those. Maintain stable section `push_id` scopes as shown. Do not derive IDs from current text, labels that can change, paths, available width, or disclosure contents.

For `send_mode_combo`, preserve its signature and current match/options. Only add width and label association. To preserve the signature, move the existing `Send files` label from the caller into the helper, so it appears exactly once:

```rust
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
        }).response.labelled_by(field_label.id);
}
```

Apply the same outer change to `receive_mode_combo`, with label `Receive files` and ID salt `receive_mode`. Keep its existing label closure and full loop in place. Its unsupported branch remains a **disabled Label**, not a selectable option or a hidden row:

```rust
// Inside the unchanged receive-mode loop:
if mode == ReceiveMode::Paste && !paste_supported() {
    let reason = "Paste needs a platform clipboard that can materialise a pull at \
                  paste time (Phase 0 spike). Use Pickup instead.";
    ui.add_enabled(false, egui::Label::new(
        egui::RichText::new(format!(
            "{} — not supported on {}", label(mode), std::env::consts::OS
        )).weak()).wrap())
        .on_hover_text(reason)
        .on_disabled_hover_text(reason);
    continue;
}
ui.selectable_value(value, mode, label(mode));
```

The additional `on_disabled_hover_text` ensures the existing reason can actually be shown for a disabled response in 0.32.3. Preserve the exact four labels: `Eager (as before)`, `Auto (by platform)`, `Pickup (retrieve from tray)`, `Paste (platform clipboard)`. On narrow windows the unsupported caption wraps in the popup; verify the popup remains within the viewport. Do not repair an already-loaded unsupported Paste value silently: it remains visible as the selected value and validation must still reject Save.

For Advanced, use `CollapsingHeader::new("Run hub on this machine").id_salt("hub_settings").default_open(false)` and an equivalent `lazy_transfer_tuning` ID for the existing tuning header. Replace each inner Grid with the stacked field pattern. Use ordinary egui widgets throughout; only the divider is painted manually.

## Behavior invariants and implementation cautions

1. **Validation stays in `try_save` / `validate`.** Retain order, exact messages, required server/user/password checks, nonzero poll rule, `ClientConfig::validate_modes`, and hub-only bind/credential/cert-key checks. Do not add live validation, URL restrictions, whitespace normalization, clamping beyond existing widget ranges, or automatic opening of a disclosure on failure. The fixed status area makes validation results visible regardless of body scroll position.
2. **Persistence remains unchanged.** `load_or_default`, `FormState::default`, `mode_config`, `WritableConfig`, and `write_toml` retain current defaults, omission rules, and round-trip behavior. In particular, preserve `log_file_names`, all six transfer-related settings, and optional hub serialization. Closing a disclosure does not reset anything.
3. **Save and exit semantics stay unchanged.** Successful `try_save()` writes TOML, clears `last_error`, sets `saved` with `Ordering::SeqCst`, then closes. Failure leaves the window open and flag false. Cancel explicitly stores false and closes; OS window close does not save. `run` returns success only after Save; the subprocess continues to exit 0 for saved and 1 for cancelled/window-close. Preserve native title, app ID, icon, tray subprocess launch, and existing reload behavior. This redesign does not promise an immediate tray reload.
4. **Pin is not transactional.** The handler performs synchronous network/certificate work, writes `server-pinned.crt` immediately, stages the CA path, clears `tls_insecure`, and stores path/fingerprint feedback. Save is still required to write TOML; Cancel does not remove that certificate file. Do not describe Cancel as undoing all disk activity. Keep the handler and error behavior; do not add a background-task lifecycle or simulated progress indicator in a visual redesign. After calling it, request repaint so the already-drawn footer updates.
5. **Version-sensitive APIs.** Use `Frame::new()`, `.corner_radius(CornerRadius::same(u8))`, `Margin::same(i8)` / `symmetric(i8, i8)`, `ComboBox::from_id_salt`, `ScrollArea::id_salt`, and `DragValue::range`. Older examples with float `Rounding`, float `Margin` fields, or `id_source` are inappropriate here. Panel ordering matters: header/footer before CentralPanel. `window_corner_radius` styles egui windows; card corners require their own Frame setting and native OS window decorations are unaffected.
6. **Size, focus, and overflow.** Apply the recommended viewport sizes in `run` only during implementation. At 420 × 320, retain a usable body viewport between bars even with the 64-point status area. Use available widths after frame padding and scrollbar allocation. Wrap long checkbox captions/explanations and status strings, including unbroken paths/fingerprints. Keep single-line edits horizontally scrollable internally. Check Tab/Shift-Tab order after moving footer construction before the body; no widget should trap focus. Do not claim visual order equals keyboard order automatically. Preserve native keyboard operation, selection strokes, popup navigation, and disabled semantics.
7. **No broad refactor.** Small rendering helpers and style constants are enough. Do not alter `FormState`'s serialized meaning, transport code, tray lifecycle, or tests unrelated to settings. This document itself authorizes no source edits.

API details were checked against the installed 0.32.3 sources: `egui/src/style.rs`, `containers/frame.rs`, `response.rs`, `widgets/text_edit/builder.rs`, `epaint/src/margin.rs`, and `egui_kittest/src/builder.rs`. Online docs retrieval was unavailable; local sources matching the lockfile are the API authority for this design. The snippets are implementation examples, not a compiled replacement file.

## Implementation acceptance and kittest impact

Keep `SettingsApp::draw(&egui::Context)` independent of `eframe::Frame`, native windows, and network initialization. Preserve existing tests `ui_renders_with_expected_widget_labels`, `save_click_writes_toml_and_flips_saved_flag`, `cancel_click_does_not_write_file`, and `invalid_input_surfaces_an_error_label`, as well as existing serialization/validation tests. Keep the exact query strings `Save`, `Cancel`, `Server URL`, `Username`, and `Username must not be empty`. The first two fields remain above the fold at the harness's default 800 × 600 size.

Attaching `labelled_by` improves editable-control accessibility, but a label-only query may now match a label and its associated control. If a query becomes ambiguous, scope it by the intended AccessKit role instead of deleting label associations or reverting to coordinates. Use semantic widget interactions for new checks. Scrolled-off controls may require scrolling into view before querying/clicking; expanded panels need a harness frame before querying their children. Give disclosures stable IDs and allow no animation-dependent sleeps.

Explicit-size harness construction for implementation checks:

```rust
let mut harness = Harness::builder()
    .with_size(egui::vec2(420.0, 320.0))
    .build_state(|ctx, app: &mut SettingsApp| app.draw(ctx), app);
harness.run();
// Save and Cancel remain queryable/clickable without scrolling the form.
```

Verify the implementation at 640 × 720, the old 520 × 360, and 420 × 320, plus 2× pixels-per-point. Use the following focused acceptance checks; screenshots complement behavior tests rather than replacing them:

- Valid Save still round-trips the full config and flips the saved flag; failed Save retains all entered data, presents the exact message, and leaves actions reachable. Cancel writes no TOML. Keep the existing subprocess contract intact.
- Expand both Advanced disclosures, scroll to every field, and confirm hub detail controls are disabled when hub is off but retain values. Toggle the existing `show` checkbox and verify both passwords follow it.
- Exercise both combos through real widgets. On unsupported platforms the Paste row is rendered, disabled, and explains why; on supported platforms it is selectable. Use the current platform gating without changing production conditions for tests.
- Inspect long config/CA paths, a long save error, and pin success text containing a fingerprint. Status and combo popups must wrap without moving Save/Cancel offscreen. Test pin feedback by arranging form state where possible; do not require a live server for layout tests. Separately verify the existing Pin action manually against a controlled endpoint during implementation.
- Check visible focus, keyboard-only navigation, primary hover/press states, disabled text legibility, and complete scroll access at minimum size. Confirm source/manifest/lockfile remain unchanged for this design-only delivery; run relevant Rust checks only when implementing the design.

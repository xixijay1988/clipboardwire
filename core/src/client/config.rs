// SPDX-License-Identifier: GPL-3.0-or-later

//! Client configuration loaded from a TOML file at
//! `~/.config/clipboardwire/config.toml` (or platform equivalent).
//!
//! Format:
//! ```toml
//! server   = "ws://nas.lan:8484/sync"
//! user     = "alice"
//! password = "hunter2"
//! poll_ms  = 300  # optional
//! ```

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::protocol::MAX_FRAME_BYTES;
use crate::server::ServerConfig;

fn default_poll_ms() -> u64 {
    300
}

fn default_hub_bind() -> SocketAddr {
    "0.0.0.0:8484".parse().expect("static address")
}

fn default_hub_max_conns() -> usize {
    64
}

fn default_hub_max_frame() -> usize {
    MAX_FRAME_BYTES
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// WebSocket endpoint, e.g. `ws://nas.lan:8484/sync` or `wss://…`.
    pub server: String,
    /// HTTP Basic auth username.
    pub user: String,
    /// HTTP Basic auth password.
    pub password: String,
    /// Clipboard polling interval in milliseconds. Default 300.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    /// Optional PEM-encoded CA bundle. Certificates here are trusted *in
    /// addition to* the built-in Mozilla root list. Use this to trust a
    /// self-signed server certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_file: Option<std::path::PathBuf>,
    /// **DANGEROUS.** When `true`, the client skips TLS certificate
    /// validation entirely. Only safe on a fully trusted network (e.g.
    /// loopback in `host` mode). Default `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub tls_insecure: bool,
    /// Optional `[hub]` section. When `hub.enabled = true`, launching
    /// `clipboardwire connect --tray` first binds a hub server in-process,
    /// then connects the local client to it over loopback. Useful when
    /// you want one machine to be both the relay and a clipboard
    /// participant without running two `clipboardwire` invocations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub: Option<HubConfig>,
    /// Log file names in clear instead of a pseudonym.
    ///
    /// Default `false`: INFO and above carry `file#<hex>` rather than the real
    /// name, so a log does not become a record of everything copied. Set this
    /// only while debugging a specific transfer. The hub has the equivalent
    /// `CLIPBOARDWIRE_LOG_FILE_NAMES` env var.
    #[serde(default, skip_serializing_if = "is_false")]
    pub log_file_names: bool,

    /// How files are sent.
    ///
    /// Defaults to `eager` — the v0.5.4 behaviour — until the lazy path ships;
    /// Phase 8.1 flips the default so no intermediate build ships half a
    /// feature. The default is skipped when serialising: writing
    /// `send_mode = "eager"` into every config would make a v0.5.4 binary
    /// refuse the file outright, because it parses with
    /// `deny_unknown_fields`.
    #[serde(default, skip_serializing_if = "is_default_send_mode")]
    pub send_mode: SendMode,

    /// How files are received. Skipped when serialising for the same reason as
    /// [`Self::send_mode`].
    #[serde(default, skip_serializing_if = "is_default_receive_mode")]
    pub receive_mode: ReceiveMode,

    /// Headless receivers fetch lazily-arrived files without a tray.
    #[serde(default, skip_serializing_if = "is_false")]
    pub pickup_auto: bool,

    /// How long a published offer stays pullable.
    #[serde(
        default = "default_offer_ttl_minutes",
        skip_serializing_if = "is_default_offer_ttl"
    )]
    pub offer_ttl_minutes: u32,

    /// Chunks asked for per `file_pull`.
    #[serde(
        default = "default_pull_window_chunks",
        skip_serializing_if = "is_default_pull_window"
    )]
    pub pull_window_chunks: u32,

    /// Give up on a pull that stops making progress for this long.
    #[serde(
        default = "default_pull_idle_timeout_secs",
        skip_serializing_if = "is_default_pull_idle_timeout"
    )]
    pub pull_idle_timeout_secs: u64,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// How this client sends files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SendMode {
    /// Publish an offer and let the receiver pull. Needs a hub that
    /// advertises `lazy-file`.
    Lazy,
    /// Stream chunks immediately, as v0.5.4 did.
    #[default]
    Eager,
}

/// How this client receives files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReceiveMode {
    /// Resolve by platform and spike outcome (see [`ReceiveMode::resolved`]).
    Auto,
    /// Lazy pull, materialised through the platform clipboard at paste time.
    Paste,
    /// Lazy pull, retrieved from the tray. Works everywhere.
    Pickup,
    /// Stream chunks, as v0.5.4 did.
    #[default]
    Eager,
}

impl ReceiveMode {
    /// Resolve `Auto` for this platform.
    ///
    /// Phase 0's spikes decide whether `paste` is viable: Windows needs 0.2,
    /// macOS needs 0.1. Until those have run, `Auto` resolves to `Pickup`,
    /// which works everywhere and is the documented fallback for a failed
    /// spike. **This function must be revisited when the spikes report** — it
    /// is the single place the outcome is recorded.
    ///
    /// Linux stays on `Eager`: that is what v0.5.4 did, and the scenario in the
    /// spec ("Linux with `auto`") says a default config there must not change
    /// behaviour yet.
    pub fn resolved(self) -> Self {
        match self {
            Self::Auto => {
                if cfg!(any(target_os = "windows", target_os = "macos")) {
                    // Spike-dependent; Pickup until 0.1/0.2 pass.
                    Self::Pickup
                } else {
                    // Linux and everything else: eager, as in v0.5.4.
                    Self::Eager
                }
            }
            other => other,
        }
    }
}

// The `is_default_*` predicates and the defaults they compare against are
// `pub` because the settings GUI builds its own serialization shadow
// (`WritableConfig`) and must skip exactly the same values. Two copies of "what
// counts as default" would drift, and the drift would be invisible: a saved
// config would either pin a default into the file (making it unreadable by
// older builds) or drop an explicit setting.

pub fn is_default_send_mode(mode: &SendMode) -> bool {
    *mode == SendMode::default()
}

pub fn is_default_receive_mode(mode: &ReceiveMode) -> bool {
    *mode == ReceiveMode::default()
}

pub fn default_offer_ttl_minutes() -> u32 {
    30
}

pub fn default_pull_window_chunks() -> u32 {
    3
}

pub fn default_pull_idle_timeout_secs() -> u64 {
    30
}

pub fn is_default_offer_ttl(minutes: &u32) -> bool {
    *minutes == default_offer_ttl_minutes()
}

pub fn is_default_pull_window(chunks: &u32) -> bool {
    *chunks == default_pull_window_chunks()
}

pub fn is_default_pull_idle_timeout(secs: &u64) -> bool {
    *secs == default_pull_idle_timeout_secs()
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HubConfig {
    /// Whether to bring up a hub on this machine alongside the client.
    #[serde(default)]
    pub enabled: bool,
    /// `host:port` to bind to. Default `0.0.0.0:8484`.
    #[serde(default = "default_hub_bind")]
    pub bind: SocketAddr,
    /// HTTP Basic auth username for the hub. Other clients connect with
    /// this user; the local in-process client always passes auth so
    /// it doesn't need to match `ClientConfig::user`, but using the same
    /// credentials keeps the config simple.
    pub user: String,
    /// HTTP Basic auth password for the hub.
    pub password: String,
    /// Maximum concurrent connected clients.
    #[serde(default = "default_hub_max_conns")]
    pub max_conns: usize,
    /// Maximum WebSocket frame size, in bytes.
    #[serde(default = "default_hub_max_frame")]
    pub max_frame_bytes: usize,
    /// Optional PEM cert file. When set together with `tls_key_file`,
    /// the hub speaks `wss://` using these files. If both are unset and
    /// `tls_disabled` is false, the hub auto-generates a self-signed
    /// cert under `state_dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cert_file: Option<PathBuf>,
    /// Optional PEM key file. Required iff `tls_cert_file` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_key_file: Option<PathBuf>,
    /// Disable TLS entirely. The hub will serve plain `ws://` and skip
    /// self-signed cert generation. LAN/VPN-only deployments only.
    #[serde(default, skip_serializing_if = "is_false")]
    pub tls_disabled: bool,
    /// Where the hub persists state across restarts (currently just the
    /// auto-generated self-signed cert). Defaults to the parent dir of
    /// the client config file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<PathBuf>,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_hub_bind(),
            user: String::new(),
            password: String::new(),
            max_conns: default_hub_max_conns(),
            max_frame_bytes: default_hub_max_frame(),
            tls_cert_file: None,
            tls_key_file: None,
            tls_disabled: false,
            state_dir: None,
        }
    }
}

impl HubConfig {
    /// Convert this hub config into the [`ServerConfig`] the existing
    /// `server::bind` / `server::serve` helpers expect.
    pub fn to_server_config(&self) -> ServerConfig {
        ServerConfig {
            bind: self.bind,
            user: self.user.clone(),
            password: self.password.clone(),
            max_conns: self.max_conns,
            max_frame_bytes: self.max_frame_bytes,
            tls_cert_file: self.tls_cert_file.clone(),
            tls_key_file: self.tls_key_file.clone(),
            tls_disabled: self.tls_disabled,
            state_dir: self.state_dir.clone(),
            stats: None,
        }
    }
}

impl ClientConfig {
    /// Where the client persists state that must survive a restart but must
    /// not live in `config.toml` — today, the durable `device_id`.
    ///
    /// Note: [`HubConfig`] has its own optional `state_dir` (it holds the
    /// auto-generated TLS cert); the client config has no equivalent field and
    /// there is deliberately no new one. The identity has to survive the
    /// settings dialog rewriting the whole config file from its form fields,
    /// so the only safe place for it is outside that file.
    ///
    /// Platform default: `BaseDirs::data_local_dir()/clipboardwire`
    /// (`~/.local/share/clipboardwire` on Linux,
    /// `~/Library/Application Support/clipboardwire` on macOS,
    /// `%LOCALAPPDATA%\clipboardwire` on Windows).
    pub fn resolved_state_dir(&self) -> Result<PathBuf> {
        let base = directories::BaseDirs::new()
            .ok_or_else(|| anyhow!("could not locate the platform's data directory"))?;
        Ok(base.data_local_dir().join("clipboardwire"))
    }

    /// Load and validate a config from disk. Refuses to start if the file is
    /// readable by group or world on Unix (since the file holds a password).
    pub fn load(path: &Path) -> Result<Self> {
        check_perms(path)?;
        let raw =
            fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: ClientConfig =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Platform-default path for the client config file:
    ///
    /// - Linux: `$XDG_CONFIG_HOME/clipboardwire/config.toml`
    ///   (falls back to `~/.config/clipboardwire/config.toml`)
    /// - macOS: `~/Library/Application Support/clipboardwire/config.toml`
    /// - Windows: `%APPDATA%\clipboardwire\config.toml`
    ///
    /// The Windows layout intentionally avoids `directories::ProjectDirs`'
    /// default nested `\config\` subdirectory — `%APPDATA%\clipboardwire\config\config.toml`
    /// reads as redundant and surprises Windows users. Linux/macOS keep
    /// their XDG-compliant locations via `BaseDirs::config_dir()`.
    pub fn default_path() -> Result<PathBuf> {
        #[cfg(windows)]
        {
            let appdata = std::env::var_os("APPDATA")
                .ok_or_else(|| anyhow!("APPDATA environment variable is not set"))?;
            Ok(PathBuf::from(appdata)
                .join("clipboardwire")
                .join("config.toml"))
        }
        #[cfg(not(windows))]
        {
            let base = directories::BaseDirs::new()
                .ok_or_else(|| anyhow!("could not locate the user's config directory"))?;
            Ok(base.config_dir().join("clipboardwire").join("config.toml"))
        }
    }

    /// Write a placeholder config to `path`, creating parent directories as
    /// needed. The file is `chmod 0600` on Unix and contains comments
    /// pointing at each setting. Used on first run to bootstrap a new
    /// install — see `cli/src/main.rs`.
    pub fn write_template(path: &Path) -> Result<()> {
        const TEMPLATE: &str = "# clipboardwire client config\n\
            # Edit this file with your hub's URL and credentials, then re-run.\n\
            \n\
            # WebSocket endpoint. Use wss:// when the hub has TLS configured.\n\
            server   = \"wss://CHANGE-ME-host.lan:8484/sync\"\n\
            \n\
            # HTTP Basic credentials, shared with the hub's\n\
            # CLIPBOARDWIRE_USER / CLIPBOARDWIRE_PASSWORD env vars.\n\
            user     = \"CHANGE-ME\"\n\
            password = \"CHANGE-ME\"\n\
            \n\
            # Optional: clipboard polling interval in milliseconds (default 300).\n\
            # poll_ms = 300\n\
            \n\
            # Optional: PEM bundle of extra CAs to trust (e.g. your private CA).\n\
            # tls_ca_file = \"C:\\\\path\\\\to\\\\ca.crt\"\n\
            \n\
            # Set to true only on a fully trusted LAN/VPN with a self-signed\n\
            # server cert. Skips all TLS certificate verification.\n\
            # tls_insecure = true\n";

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::write(path, TEMPLATE)
            .with_context(|| format!("writing template to {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path)
                .with_context(|| format!("stat {}", path.display()))?
                .permissions();
            perms.set_mode(0o600);
            fs::set_permissions(path, perms)
                .with_context(|| format!("chmod 600 {}", path.display()))?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if !self.server.starts_with("ws://") && !self.server.starts_with("wss://") {
            bail!(
                "`server` must start with ws:// or wss:// (got `{}`)",
                self.server
            );
        }
        if self.user.is_empty() {
            bail!("`user` must not be empty");
        }
        if self.password.is_empty() {
            bail!("`password` must not be empty");
        }
        if self.poll_ms == 0 {
            bail!("`poll_ms` must be greater than 0");
        }
        self.validate_modes()
    }

    /// Reject an explicit mode this platform cannot support, and bounds that
    /// would make the lazy path misbehave.
    ///
    /// Only **explicit** choices fail. `auto` always resolves to something
    /// workable, so a default config never refuses to start on a platform
    /// where one paste method is missing — that is the whole reason `auto`
    /// exists.
    ///
    /// Called from [`Self::validate`], so every path that parses a config from
    /// disk or from the environment goes through it.
    pub fn validate_modes(&self) -> Result<()> {
        // `paste` needs a platform clipboard that can materialise a pull at
        // paste time (Phase 0 spike 0.1/0.2). Linux has no such hook, and
        // silently degrading an explicit choice to something else would leave
        // the user believing files were arriving when they were not.
        if self.receive_mode == ReceiveMode::Paste
            && !cfg!(any(target_os = "windows", target_os = "macos"))
        {
            bail!(
                "receive_mode = \"paste\" is not supported on {}. \
                 Use \"pickup\" (retrieve from the tray) or \"eager\".",
                std::env::consts::OS
            );
        }
        if self.pull_window_chunks == 0 || self.pull_window_chunks > 32 {
            bail!(
                "pull_window_chunks must be 1..=32, got {}",
                self.pull_window_chunks
            );
        }
        if self.offer_ttl_minutes == 0 {
            bail!("offer_ttl_minutes must be at least 1");
        }
        Ok(())
    }
}

#[cfg(unix)]
fn check_perms(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let md = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let mode = md.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        bail!(
            "config file {} is readable by group/other (mode {:o}); run `chmod 600 {}`",
            path.display(),
            mode,
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_perms(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_config(content: &str) -> PathBuf {
        // A monotonic counter, not a timestamp: `SystemTime::now()` can return
        // the same value twice within one clock tick, and two tests running in
        // parallel would then share a path — the second test reads the first
        // test's config and asserts against the wrong error. That made
        // `rejects_empty_password` fail roughly one run in ten.
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "clipboardwire-test-cfg-{}-{}.toml",
            std::process::id(),
            id
        ));
        fs::write(&path, content).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        path
    }

    #[test]
    fn parses_minimal_config() {
        let path = write_config(
            r#"
            server = "ws://nas.lan:8484/sync"
            user = "alice"
            password = "hunter2"
        "#,
        );
        let cfg = ClientConfig::load(&path).unwrap();
        assert_eq!(cfg.server, "ws://nas.lan:8484/sync");
        assert_eq!(cfg.user, "alice");
        assert_eq!(cfg.password, "hunter2");
        assert_eq!(cfg.poll_ms, 300);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn log_file_names_defaults_to_false() {
        let path = write_config(
            r#"
            server = "ws://nas.lan:8484/sync"
            user = "alice"
            password = "hunter2"
        "#,
        );
        let cfg = ClientConfig::load(&path).unwrap();
        assert!(!cfg.log_file_names, "redaction must be the default");
        fs::remove_file(&path).ok();
    }

    #[test]
    fn log_file_names_can_be_enabled() {
        let path = write_config(
            r#"
            server = "ws://nas.lan:8484/sync"
            user = "alice"
            password = "hunter2"
            log_file_names = true
        "#,
        );
        let cfg = ClientConfig::load(&path).unwrap();
        assert!(cfg.log_file_names);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn parses_poll_ms_override() {
        let path = write_config(
            r#"
            server = "wss://example.com/sync"
            user = "u"
            password = "p"
            poll_ms = 100
        "#,
        );
        let cfg = ClientConfig::load(&path).unwrap();
        assert_eq!(cfg.poll_ms, 100);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn rejects_unknown_fields() {
        let path = write_config(
            r#"
            server = "ws://localhost/sync"
            user = "u"
            password = "p"
            mystery = "nope"
        "#,
        );
        let err = ClientConfig::load(&path).unwrap_err();
        assert!(format!("{err:#}").to_lowercase().contains("mystery"));
        fs::remove_file(&path).ok();
    }

    #[test]
    fn rejects_non_ws_scheme() {
        let path = write_config(
            r#"
            server = "http://localhost/sync"
            user = "u"
            password = "p"
        "#,
        );
        let err = ClientConfig::load(&path).unwrap_err();
        assert!(format!("{err:#}").contains("ws://"));
        fs::remove_file(&path).ok();
    }

    #[test]
    fn rejects_empty_password() {
        let path = write_config(
            r#"
            server = "ws://localhost/sync"
            user = "u"
            password = ""
        "#,
        );
        let err = ClientConfig::load(&path).unwrap_err();
        assert!(format!("{err:#}").contains("password"));
        fs::remove_file(&path).ok();
    }

    #[test]
    fn write_template_creates_parent_and_file() {
        let dir = std::env::temp_dir().join(format!(
            "clipboardwire-template-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("nested").join("config.toml");
        let _ = fs::remove_dir_all(&dir);

        ClientConfig::write_template(&path).expect("template write");
        assert!(path.exists());
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("CHANGE-ME"));
        assert!(content.contains("server"));

        // The template itself should parse minus the CHANGE-ME placeholders —
        // i.e. it's syntactically valid TOML that would just fail validation.
        let parsed: Result<ClientConfig, _> = toml::from_str(&content);
        assert!(parsed.is_ok(), "template should be valid TOML");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[cfg(unix)]
    fn write_template_sets_0600_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!(
            "clipboardwire-template-mode-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("config.toml");
        let _ = fs::remove_dir_all(&dir);

        ClientConfig::write_template(&path).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "template should be chmod 600");

        fs::remove_dir_all(&dir).ok();
    }

    // ---- Transfer modes (4.2 / 4.3) ----

    /// A minimal valid config, for tests that only care about one field.
    fn base_toml(extra: &str) -> String {
        format!("server = \"ws://localhost/sync\"\nuser = \"u\"\npassword = \"p\"\n{extra}")
    }

    #[test]
    fn send_mode_parses_lazy_and_eager() {
        let lazy: ClientConfig = toml::from_str(&base_toml("send_mode = \"lazy\"\n")).unwrap();
        assert_eq!(lazy.send_mode, SendMode::Lazy);
        let eager: ClientConfig = toml::from_str(&base_toml("send_mode = \"eager\"\n")).unwrap();
        assert_eq!(eager.send_mode, SendMode::Eager);
    }

    #[test]
    fn receive_mode_parses_all_four() {
        for (raw, expected) in [
            ("auto", ReceiveMode::Auto),
            ("paste", ReceiveMode::Paste),
            ("pickup", ReceiveMode::Pickup),
            ("eager", ReceiveMode::Eager),
        ] {
            let cfg: ClientConfig =
                toml::from_str(&base_toml(&format!("receive_mode = \"{raw}\"\n")))
                    .unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!(cfg.receive_mode, expected, "{raw}");
        }
    }

    #[test]
    fn an_unknown_mode_is_rejected() {
        // Not `deny_unknown_fields`: the *value* is outside the enum's set,
        // which serde rejects on its own. Tested here because the failure mode
        // differs — a typo in a mode name must not silently fall back to the
        // default.
        for bad in ["send_mode = \"banana\"\n", "receive_mode = \"banana\"\n"] {
            let parsed: Result<ClientConfig, _> = toml::from_str(&base_toml(bad));
            let err = parsed.expect_err(bad).to_string();
            assert!(err.contains("banana"), "{bad}: {err}");
        }
    }

    #[test]
    fn defaults_are_eager_for_both_modes() {
        // The Phase 8.1 flip changes this test, deliberately: until the lazy
        // path ships, a config with no opinion must behave exactly like
        // v0.5.4. If this ever fails without a matching 8.1 change, a build
        // has shipped half a feature.
        let cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
        assert_eq!(cfg.send_mode, SendMode::Eager);
        assert_eq!(cfg.receive_mode, ReceiveMode::Eager);
        assert!(!cfg.pickup_auto);
        assert_eq!(cfg.offer_ttl_minutes, 30);
        assert_eq!(cfg.pull_window_chunks, 3);
        assert_eq!(cfg.pull_idle_timeout_secs, 30);
    }

    #[test]
    fn default_values_are_not_serialised() {
        // A v0.5.4 binary parses with `deny_unknown_fields`, so writing a
        // default-valued `send_mode = "eager"` into every config would make
        // that binary refuse the file. Defaults therefore stay out of the file
        // and only an explicit choice appears.
        let cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
        let out = toml::to_string(&cfg).unwrap();
        for absent in [
            "send_mode",
            "receive_mode",
            "pickup_auto",
            "offer_ttl_minutes",
            "pull_window_chunks",
            "pull_idle_timeout_secs",
        ] {
            assert!(!out.contains(absent), "{absent} leaked into:\n{out}");
        }
        // The pre-existing keys are still written; this is not "serialise
        // nothing".
        assert!(out.contains("server"));
        assert!(out.contains("poll_ms"));
    }

    #[test]
    fn an_explicit_value_is_serialised() {
        let cfg: ClientConfig = toml::from_str(&base_toml(
            "send_mode = \"lazy\"\nreceive_mode = \"pickup\"\n",
        ))
        .unwrap();
        let out = toml::to_string(&cfg).unwrap();
        assert!(out.contains("send_mode = \"lazy\""), "{out}");
        assert!(out.contains("receive_mode = \"pickup\""), "{out}");
        // Still absent: the default-valued TTLs.
        assert!(!out.contains("offer_ttl_minutes"), "{out}");
    }

    #[test]
    fn auto_resolves_to_eager_on_linux() {
        if cfg!(any(target_os = "windows", target_os = "macos")) {
            return;
        }
        assert_eq!(ReceiveMode::Auto.resolved(), ReceiveMode::Eager);
    }

    #[test]
    fn auto_resolves_to_pickup_on_desktop() {
        if !cfg!(any(target_os = "windows", target_os = "macos")) {
            return;
        }
        // Spike-dependent: `pickup` until 0.1/0.2 report, then possibly
        // `paste`. The assertion lives here so the decision has one home.
        assert_eq!(ReceiveMode::Auto.resolved(), ReceiveMode::Pickup);
    }

    #[test]
    fn explicit_paste_is_refused_on_linux() {
        if cfg!(any(target_os = "windows", target_os = "macos")) {
            return;
        }
        let mut cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
        cfg.receive_mode = ReceiveMode::Paste;
        let err = cfg
            .validate_modes()
            .expect_err("paste must be refused here");
        let msg = format!("{err:#}");
        // Actionable: it must name the alternatives, not just say "no".
        assert!(msg.contains("pickup"), "{msg}");
        assert!(msg.contains("eager"), "{msg}");
        assert!(msg.contains(std::env::consts::OS), "{msg}");
    }

    #[test]
    fn explicit_paste_is_accepted_on_desktop() {
        if !cfg!(any(target_os = "windows", target_os = "macos")) {
            return;
        }
        let mut cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
        cfg.receive_mode = ReceiveMode::Paste;
        cfg.validate_modes().expect("paste is supported here");
    }

    #[test]
    fn auto_is_never_refused_on_any_platform() {
        // The point of `auto`: a config that does not name a concrete strategy
        // must not fail to start, on any platform.
        for mode in [ReceiveMode::Auto, ReceiveMode::Eager, ReceiveMode::Pickup] {
            let mut cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
            cfg.receive_mode = mode;
            cfg.validate_modes()
                .unwrap_or_else(|e| panic!("{mode:?} must validate: {e:#}"));
        }
    }

    #[test]
    fn a_zero_pull_window_is_rejected() {
        let mut cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
        cfg.pull_window_chunks = 0;
        let err = cfg.validate_modes().expect_err("0 is not a window");
        assert!(format!("{err:#}").contains("1..=32"), "{err:#}");
    }

    #[test]
    fn a_huge_pull_window_is_rejected() {
        let mut cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
        cfg.pull_window_chunks = 33;
        let err = cfg.validate_modes().expect_err("33 exceeds the bound");
        assert!(format!("{err:#}").contains("1..=32"), "{err:#}");
    }

    #[test]
    fn a_zero_ttl_is_rejected() {
        let mut cfg: ClientConfig = toml::from_str(&base_toml("")).unwrap();
        cfg.offer_ttl_minutes = 0;
        let err = cfg
            .validate_modes()
            .expect_err("a zero TTL kills every offer");
        assert!(format!("{err:#}").contains("at least 1"), "{err:#}");
    }

    #[test]
    fn load_runs_the_mode_validation() {
        // `load` is the only entry point production uses, so a rule enforced
        // only by `validate_modes` is useless unless `load` calls it.
        let path = write_config(&base_toml("pull_window_chunks = 0\n"));
        let err = ClientConfig::load(&path).unwrap_err();
        assert!(format!("{err:#}").contains("pull_window_chunks"), "{err:#}");
        fs::remove_file(&path).ok();
    }

    #[test]
    #[cfg(unix)]
    fn rejects_world_readable_config() {
        use std::os::unix::fs::PermissionsExt;
        let path = write_config(
            r#"
            server = "ws://localhost/sync"
            user = "u"
            password = "p"
        "#,
        );
        // Loosen permissions to 644.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let err = ClientConfig::load(&path).unwrap_err();
        assert!(format!("{err:#}").contains("chmod 600"));
        fs::remove_file(&path).ok();
    }
}

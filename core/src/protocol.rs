// SPDX-License-Identifier: GPL-3.0-or-later

//! Wire protocol types. See `PROTOCOL.md` for the full spec.

use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

/// Value advertised by the server in the `welcome` frame.
///
/// Bumped to 0.4 for the lazy file-transfer surface: `file_offer`,
/// `file_pull`, `file_digest`, `file_abort`, the `caps`/`peers`/`offers`
/// fields of `welcome`, and `peer_changed`. Every one of those is additive and
/// every new field carries `#[serde(default)]`, so a 0.3 client still parses a
/// 0.4 `welcome` and ignores what it does not know.
///
/// This is the wire version only; it is unrelated to the crate version in
/// `Cargo.toml`.
pub const PROTOCOL_VERSION: &str = "clipboardwire/0.4.0";

/// Maximum WebSocket frame size accepted by the server (50 MiB) — large
/// enough for typical screenshot-sized PNGs without operator tuning,
/// and for the per-chunk size used by file transfers (4 MiB raw →
/// ~5.5 MiB base64 → plenty of headroom).
pub const MAX_FRAME_BYTES: usize = 50 * 1024 * 1024;

/// Transport-level ceiling for a `caps` query value, in bytes.
///
/// Exceeding it is treated as abuse rather than as a long capability list: the
/// hub ignores the whole value (see [`Caps::parse_guarded`]). This is *not* a
/// parsing rule — parsing has no length bound, so every implementation derives
/// the same set from the same list regardless of the token spellings it does
/// not recognise.
pub const MAX_CAPS_BYTES: usize = 512;

/// Longest `file_offer.name` accepted, in bytes.
///
/// Offers are cached by the hub and replayed in every `welcome`, so an
/// unbounded name lets one peer make every later handshake fail on frame size.
pub const MAX_OFFER_NAME_BYTES: usize = 1024;

/// Longest `file_offer.content_type` accepted, in bytes.
pub const MAX_OFFER_CONTENT_TYPE_BYTES: usize = 255;

/// Bytes per `file_chunk` frame (raw, pre-base64). Chunks are independent
/// JSON frames; the receiver assembles them by `file_id` + `chunk_index`.
/// 4 MiB strikes a balance between per-frame memory peaks, total chunk
/// count for big files, and reconnect-recovery granularity. With base64
/// inflation a chunk's wire size is roughly 5.5 MiB, well inside
/// [`MAX_FRAME_BYTES`].
pub const FILE_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Largest pull window a `file_pull` may request, in chunks. Bounds the
/// source's per-request read burst; a larger window is rejected with
/// `file_abort{window_too_large}` rather than clamped.
pub const MAX_PULL_WINDOW: u32 = 32;

/// `content_type` for UTF-8 text payloads.
pub const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
/// `content_type` for PNG image payloads.
pub const IMAGE_PNG_CONTENT_TYPE: &str = "image/png";

/// Back-compat alias kept while we migrate callers. Prefer
/// [`TEXT_CONTENT_TYPE`] in new code.
pub const SUPPORTED_CONTENT_TYPE: &str = TEXT_CONTENT_TYPE;

/// Returns whether `content_type` is one of the textual MIME types whose
/// payload travels in the `content` field rather than `content_b64`.
pub fn is_text_content_type(content_type: &str) -> bool {
    content_type.starts_with("text/")
}

/// `clip` frame body. Either `content` (text) or `content_b64` (binary) is
/// set; never both, never neither — see [`ClipFrame::validate`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClipFrame {
    pub id: Uuid,
    pub ts: i64,
    pub content_type: String,
    /// Text payload (UTF-8 string). Present iff `content_type` is textual.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Base64-encoded binary payload. Present iff `content_type` is binary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_b64: Option<String>,
    /// Filled by the server on relay; absent on the client's outbound send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<Uuid>,
}

impl ClipFrame {
    /// Returns `Err` if neither or both payload fields are set, or if the
    /// payload kind disagrees with `content_type`.
    pub fn validate(&self) -> Result<(), &'static str> {
        match (self.content.is_some(), self.content_b64.is_some()) {
            (true, true) => Err("clip frame has both `content` and `content_b64`"),
            (false, false) => Err("clip frame has neither `content` nor `content_b64`"),
            (true, false) if !is_text_content_type(&self.content_type) => {
                Err("`content` requires a text/* content_type")
            }
            (false, true) if is_text_content_type(&self.content_type) => {
                Err("`content_b64` requires a non-text content_type")
            }
            _ => Ok(()),
        }
    }
}

/// A connected peer as reported in [`WelcomeFrame::peers`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WelcomePeer {
    pub device: Uuid,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caps: Vec<String>,
}

/// `welcome` frame body.
///
/// The three fields after `client_id` are all optional on the wire: a v0.4 hub
/// adds them, and a v0.3 client must still be able to parse the frame (it will
/// simply ignore them). `last_clip` keeps its pre-existing shape — always
/// serialised, `null` when there is nothing cached.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WelcomeFrame {
    pub server: String,
    pub client_id: Uuid,
    /// Hub capabilities. Empty when the hub advertises nothing, which is also
    /// what a pre-0.4 hub looks like on the wire.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caps: Vec<String>,
    /// Snapshot at connect time; `peer_changed` keeps it current afterwards.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<WelcomePeer>,
    /// Live offers, same shape as `file_offer`. Empty on a quiet hub.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub offers: Vec<FileOfferFrame>,
    pub last_clip: Option<ClipFrame>,
}

/// `error` frame body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorFrame {
    pub code: ErrorCode,
    pub message: String,
}

/// Protocol-level error codes — also the `name` column of close codes 4001–4005.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unauthorized,
    BadFrame,
    FrameTooLarge,
    UnsupportedType,
    DuplicateSession,
}

impl ErrorCode {
    /// WebSocket close code that pairs with this error (per `PROTOCOL.md` §6).
    pub fn close_code(self) -> u16 {
        match self {
            Self::Unauthorized => 4001,
            Self::BadFrame => 4002,
            Self::FrameTooLarge => 4003,
            Self::UnsupportedType => 4004,
            Self::DuplicateSession => 4005,
        }
    }
}

/// A single chunk of a file transfer.
///
/// One file is sent as a sequence of `file_chunk` frames sharing the
/// same `file_id`. The receiver assembles them into a temp file under
/// the receiver's downloads directory (see
/// `client::file::FileReceiver`), and on the final chunk verifies the
/// SHA-256 against the value carried in every chunk before moving the
/// finished file into place.
///
/// Two shapes share this frame (protocol v0.4):
///
/// - **eager**: no `transfer_id`; `file_sha256` is required and `to` is
///   forbidden. This is the v0.3 shape and MUST serialise byte-identically
///   to it, so `clipboardwire send` never changes.
/// - **lazy**: `to` and `transfer_id` are required and `file_sha256` is
///   forbidden; the digest travels in [`FileDigestFrame`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChunkFrame {
    /// Offer id. All chunks of the same file share this.
    pub file_id: Uuid,
    /// Original filename (no directory component). The receiver
    /// sanitises this before using it as a path.
    pub name: String,
    /// Best-effort MIME type. May be `application/octet-stream`.
    pub content_type: String,
    /// 0-based chunk index.
    pub chunk_index: u32,
    /// Total number of chunks for this file. Receivers know they're
    /// done assembling when they've received every index in
    /// `0..total_chunks`.
    pub total_chunks: u32,
    /// SHA-256 of the *entire* file as a colon-separated uppercase
    /// hex string. Eager chunks repeat it in every chunk so a receiver
    /// that joins mid-transfer can still verify on completion; lazy
    /// chunks omit it and use [`FileDigestFrame`] instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_sha256: Option<String>,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Base64-encoded bytes for this chunk. Decoded length is at most
    /// [`FILE_CHUNK_BYTES`] (smaller for the last chunk).
    pub payload_b64: String,
    /// Filled by the hub on relay; absent on the client's outbound send.
    /// Kept for v0.3 compatibility — new code uses `from_device`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<Uuid>,
    /// Destination `device_id` for a directed (lazy) chunk. Forbidden on
    /// eager chunks, which are broadcast.
    ///
    /// Declared after `from` so an eager chunk keeps the v0.3 field order
    /// and therefore serialises byte-identically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<Uuid>,
    /// Session key for a lazy transfer. Forbidden on eager chunks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<Uuid>,
    /// Source `device_id`, stamped by the hub on relay. Never sent by a
    /// client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_device: Option<Uuid>,
}

impl FileChunkFrame {
    /// Sanity-check fields a malicious sender could exploit. The hub
    /// calls this before relaying so a single bad client can't make us
    /// fan-out garbage.
    ///
    /// Validation splits on the presence of `transfer_id`: eager chunks
    /// follow the v0.3 rules, lazy chunks require a destination and a
    /// session key, and neither may look like the other.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.name.is_empty() {
            return Err("file_chunk name must not be empty");
        }
        if self.name.contains('\0') {
            return Err("file_chunk name must not contain NUL bytes");
        }
        if self.total_chunks == 0 {
            return Err("file_chunk total_chunks must be >= 1");
        }
        if self.chunk_index >= self.total_chunks {
            return Err("file_chunk chunk_index >= total_chunks");
        }
        if self.payload_b64.is_empty() {
            return Err("file_chunk payload_b64 must not be empty");
        }

        match self.transfer_id {
            // Eager: the v0.3 shape. The whole-file digest rides along and
            // the frame is broadcast, so a destination makes no sense.
            None => {
                match self.file_sha256.as_deref() {
                    Some(sha) if !sha.is_empty() => {}
                    Some(_) => return Err("file_chunk file_sha256 must not be empty"),
                    None => return Err("eager file_chunk requires file_sha256"),
                }
                if self.to.is_some() {
                    return Err("eager file_chunk must not carry `to`");
                }
            }
            // Lazy: directed delivery under a session key; the digest is
            // delivered once, in `file_digest`.
            Some(_) => {
                if self.to.is_none() {
                    return Err("lazy file_chunk requires `to`");
                }
                if self.file_sha256.is_some() {
                    return Err("lazy file_chunk must not carry file_sha256");
                }
            }
        }
        Ok(())
    }
}

/// Typed rejection reason for the v0.4 control frames.
///
/// The hub maps these onto the `file_abort` reason it sends back, so a bad
/// control frame ends **one transfer** rather than the connection (design
/// §4.3: "Control-frame rejections are always transfer-scoped"). Matching a
/// string would make that mapping fragile the moment a message is reworded.
///
/// This is deliberately separate from the `&'static str` returned by
/// [`ClipFrame::validate`] and [`FileChunkFrame::validate`]: those are v0.3
/// frames whose errors close the connection, and their message text is part of
/// the existing `bad_frame` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFrameError {
    /// `file_offer` name was empty.
    EmptyName,
    /// `file_offer` name contained a NUL byte.
    NameContainsNul,
    /// `file_offer` name exceeded [`MAX_OFFER_NAME_BYTES`].
    NameTooLong,
    /// `file_offer` content_type exceeded [`MAX_OFFER_CONTENT_TYPE_BYTES`].
    ContentTypeTooLong,
    /// `file_offer` described a zero-byte file.
    EmptyFile,
    /// `file_offer` carried a non-positive `expires_at_ms`.
    InvalidExpiry,
    /// `file_pull` asked for `window == 0`.
    EmptyWindow,
    /// `file_pull` asked for more than [`MAX_PULL_WINDOW`] chunks.
    WindowTooLarge,
    /// `file_digest` carried an empty hash.
    EmptyDigest,
    /// `file_abort` carried an empty reason.
    EmptyReason,
    /// A session-scoped `file_abort` omitted `to`.
    MissingDestination,
    /// `peer_changed` carried an event this build does not know.
    UnknownPeerEvent,
}

impl ControlFrameError {
    /// The `file_abort` reason the hub should send back, when one applies.
    ///
    /// `None` means there is no honest wire reason: a malformed offer has no
    /// session to abort, and the known reason set contains nothing meaning
    /// "unparseable". The hub should drop the frame (and, for an offer, not
    /// register it) rather than invent a reason — [`AbortReason::Failed`] exists
    /// only as a *receiver-side* mapping target and has no wire value by design.
    pub fn abort_reason(self) -> Option<AbortReason> {
        match self {
            // The one bound the protocol names explicitly: a bad pull window is
            // answered with `file_abort{window_too_large}` and the connection
            // stays open. `window == 0` is the same offence.
            Self::WindowTooLarge | Self::EmptyWindow => Some(AbortReason::WindowTooLarge),
            Self::EmptyName
            | Self::NameContainsNul
            | Self::NameTooLong
            | Self::ContentTypeTooLong
            | Self::EmptyFile
            | Self::InvalidExpiry
            | Self::EmptyDigest
            | Self::EmptyReason
            | Self::MissingDestination
            | Self::UnknownPeerEvent => None,
        }
    }
}

impl std::fmt::Display for ControlFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Self::EmptyName => "file_offer name must not be empty",
            Self::NameContainsNul => "file_offer name must not contain NUL bytes",
            Self::NameTooLong => "file_offer name exceeds MAX_OFFER_NAME_BYTES",
            Self::ContentTypeTooLong => {
                "file_offer content_type exceeds MAX_OFFER_CONTENT_TYPE_BYTES"
            }
            Self::EmptyFile => "file_offer file_size must be >= 1",
            Self::InvalidExpiry => "file_offer expires_at_ms must be a positive timestamp",
            Self::EmptyWindow => "file_pull window must be >= 1",
            Self::WindowTooLarge => "file_pull window exceeds MAX_PULL_WINDOW",
            Self::EmptyDigest => "file_digest file_sha256 must not be empty",
            Self::EmptyReason => "file_abort reason must not be empty",
            Self::MissingDestination => "session-scoped file_abort requires `to`",
            Self::UnknownPeerEvent => "peer_changed event must be \"joined\" or \"left\"",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for ControlFrameError {}

/// `file_offer` frame body — a copy announcement that moves no bytes.
///
/// `expires_at_ms` is an **absolute** deadline, not a TTL: the source owns
/// it, so a hub restart or a reconnect does not restart the clock.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileOfferFrame {
    pub file_id: Uuid,
    /// Groups the files of one copy/paste action.
    pub batch_id: Uuid,
    /// When the source copied the file, in Unix milliseconds.
    pub ts: i64,
    pub name: String,
    pub content_type: String,
    pub file_size: u64,
    /// Absolute expiry deadline, Unix milliseconds.
    pub expires_at_ms: i64,
    /// Filled by the hub on relay; absent on the client's outbound send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_device: Option<Uuid>,
}

impl FileOfferFrame {
    /// Field-shape checks only.
    ///
    /// Whether `expires_at_ms` is in the future and inside the 24 h cap is the
    /// **hub's** call (design §4.3): the protocol layer must not depend on the
    /// system clock, and a peer comparing against its own skewed clock would
    /// reject offers a correct hub accepts.
    ///
    /// The `name` / `content_type` limits are structural rather than
    /// clock-dependent, so they live here: the hub caches offers and replays
    /// them in every `welcome`, which makes an oversized one a denial of
    /// service against future connections.
    pub fn validate(&self) -> Result<(), ControlFrameError> {
        if self.name.is_empty() {
            return Err(ControlFrameError::EmptyName);
        }
        if self.name.contains('\0') {
            return Err(ControlFrameError::NameContainsNul);
        }
        if self.name.len() > MAX_OFFER_NAME_BYTES {
            return Err(ControlFrameError::NameTooLong);
        }
        if self.content_type.len() > MAX_OFFER_CONTENT_TYPE_BYTES {
            return Err(ControlFrameError::ContentTypeTooLong);
        }
        if self.file_size == 0 {
            return Err(ControlFrameError::EmptyFile);
        }
        if self.expires_at_ms <= 0 {
            return Err(ControlFrameError::InvalidExpiry);
        }
        Ok(())
    }
}

/// `file_pull` frame body — the receiver asks the source for a window.
///
/// `to` names the **source** device. The first pull of a session creates it
/// at `chunk_index = 0`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FilePullFrame {
    pub file_id: Uuid,
    pub transfer_id: Uuid,
    /// Source `device_id`.
    pub to: Uuid,
    pub chunk_index: u32,
    pub window: u32,
    /// Filled by the hub on relay, never by the client.
    ///
    /// **Load-bearing, not decorative.** The source answers with chunks
    /// carrying `to: <puller>`; without knowing the puller it cannot address
    /// its own reply and would have to guess, which would deliver file
    /// contents to an arbitrary device.
    ///
    /// Declared last so a new field is always appended, matching
    /// [`FileChunkFrame`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_device: Option<Uuid>,
}

impl FilePullFrame {
    /// `window` must be `1..=MAX_PULL_WINDOW`. A larger window is rejected
    /// with `file_abort{window_too_large}` by the source rather than clamped;
    /// see [`ControlFrameError::abort_reason`].
    ///
    /// The starting-index rule belongs to the sender (§5.5): the protocol type
    /// carries no `total_chunks`, so it cannot bound `chunk_index`.
    pub fn validate(&self) -> Result<(), ControlFrameError> {
        if self.window == 0 {
            return Err(ControlFrameError::EmptyWindow);
        }
        if self.window > MAX_PULL_WINDOW {
            return Err(ControlFrameError::WindowTooLarge);
        }
        Ok(())
    }
}

/// `file_digest` frame body — the whole-file SHA-256 for one lazy session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileDigestFrame {
    pub file_id: Uuid,
    pub transfer_id: Uuid,
    /// Destination `device_id`.
    pub to: Uuid,
    /// Source `device_id`, stamped by the hub on relay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_device: Option<Uuid>,
    pub file_sha256: String,
}

impl FileDigestFrame {
    /// A digest frame exists only to carry the hash; an empty one can never
    /// verify anything.
    pub fn validate(&self) -> Result<(), ControlFrameError> {
        if self.file_sha256.is_empty() {
            return Err(ControlFrameError::EmptyDigest);
        }
        Ok(())
    }
}

/// `file_abort` frame body.
///
/// `reason` is an **open** string set: [`AbortReason::parse`] maps unknown
/// values to [`AbortReason::Failed`] and logs the raw value at DEBUG, so adding
/// a reason later is not a breaking change.
///
/// **Scope is decided by `transfer_id`, never by the reason** (design §3.6):
///
/// - with `transfer_id` → session-scoped; `to` names the one receiver and is
///   required. Replies to a specific `file_pull` (`busy`, `window_too_large`,
///   `invalid_start`, `unknown_offer`, `expired`) always look like this.
/// - without `transfer_id` → offer-scoped; `to` may be **absent**, meaning
///   broadcast. The hub expands it into one directed copy per recipient, so the
///   source can abort an offer (`superseded`, `source_gone`) without knowing
///   who is holding it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileAbortFrame {
    pub file_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<Uuid>,
    /// Destination `device_id`. Absent means "broadcast to every holder of the
    /// offer"; only legal when `transfer_id` is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<Uuid>,
    /// Source `device_id`, stamped by the hub on relay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_device: Option<Uuid>,
    /// Raw reason string; see [`AbortReason`].
    pub reason: String,
}

impl FileAbortFrame {
    /// `reason` is an **open** set, so an unknown but non-empty reason is
    /// legal. An empty one is not.
    ///
    /// `to` is required exactly when the abort is session-scoped: a specific
    /// session belongs to one receiver, while an offer-scoped abort may be
    /// broadcast and have the hub fill in the recipients.
    pub fn validate(&self) -> Result<(), ControlFrameError> {
        if self.reason.is_empty() {
            return Err(ControlFrameError::EmptyReason);
        }
        if self.transfer_id.is_some() && self.to.is_none() {
            return Err(ControlFrameError::MissingDestination);
        }
        Ok(())
    }
}

/// Known values of [`FileAbortFrame::reason`], plus the catch-all [`Self::Failed`].
///
/// The wire value stays a free-form string — this enum is only for the code
/// that reacts to an abort, so an unknown reason degrades to "failed" instead
/// of being rejected.
///
/// [`Self::Failed`] is deliberately **not** a wire value: it is the mapping
/// target for reasons this build does not know, and re-emitting it would
/// rewrite a peer's unknown reason into a fabricated known one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    SourceGone,
    SourceChanged,
    UserCancel,
    UnknownOffer,
    Expired,
    Busy,
    WindowTooLarge,
    InvalidStart,
    Superseded,
    /// Not a wire value: the mapping target for any reason this build does
    /// not know.
    Failed,
}

impl AbortReason {
    /// Canonical wire string, or `None` for a reason that has no wire value
    /// ([`Self::Failed`]).
    ///
    /// Use this — not [`Self::parse`] — when constructing a frame: a parsed
    /// unknown reason must be forwarded verbatim, never re-encoded.
    pub fn to_wire(self) -> Option<&'static str> {
        match self {
            Self::SourceGone => Some("source_gone"),
            Self::SourceChanged => Some("source_changed"),
            Self::UserCancel => Some("user_cancel"),
            Self::UnknownOffer => Some("unknown_offer"),
            Self::Expired => Some("expired"),
            Self::Busy => Some("busy"),
            Self::WindowTooLarge => Some("window_too_large"),
            Self::InvalidStart => Some("invalid_start"),
            Self::Superseded => Some("superseded"),
            Self::Failed => None,
        }
    }

    /// Map a wire reason onto the known set. Unknown values are tolerated and
    /// collapse to [`Self::Failed`]; the raw value is logged at DEBUG so a
    /// future reason is still diagnosable.
    pub fn parse(raw: &str) -> Self {
        match raw {
            "source_gone" => Self::SourceGone,
            "source_changed" => Self::SourceChanged,
            "user_cancel" => Self::UserCancel,
            "unknown_offer" => Self::UnknownOffer,
            "expired" => Self::Expired,
            "busy" => Self::Busy,
            "window_too_large" => Self::WindowTooLarge,
            "invalid_start" => Self::InvalidStart,
            "superseded" => Self::Superseded,
            other => {
                debug!(
                    reason = other,
                    "unknown file_abort reason; treating as failed"
                );
                Self::Failed
            }
        }
    }
}

/// `peer_changed` frame body — a device joined or left the hub.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerChangedFrame {
    /// The peer's `device_id`.
    pub device: Uuid,
    /// Capability tokens the peer advertised.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caps: Vec<String>,
    pub event: PeerEvent,
}

impl PeerChangedFrame {
    /// `event` must name a known transition. It is typed as [`PeerEvent`], so
    /// an unknown wire string already fails deserialisation — this check makes
    /// the rule explicit at the frame level, where a future non-enum
    /// representation would otherwise silently lose it.
    ///
    /// `caps` is **not** rejected for containing a malformed entry. Unknown and
    /// malformed tokens carry no meaning, so the frame is still a valid
    /// join/leave notice; rejecting it would leave this client unable to track
    /// membership just because the hub grew a token grammar this build does not
    /// know. Use [`Self::known_caps`] to read the meaningful subset, which
    /// matches how `caps=` itself treats garbage.
    pub fn validate(&self) -> Result<(), ControlFrameError> {
        if !self.event.is_known() {
            return Err(ControlFrameError::UnknownPeerEvent);
        }
        Ok(())
    }

    /// The advertised capabilities, with malformed and unknown tokens dropped.
    ///
    /// This is the same filter [`Caps::parse`] applies, so a peer's live view
    /// and its `welcome` entry agree.
    pub fn known_caps(&self) -> Caps {
        let mut caps = Caps::default();
        for token in &self.caps {
            if let Some(flag) = Caps::token_for(token) {
                caps.bits |= flag.bits;
            }
        }
        caps
    }
}

/// Direction of a [`PeerChangedFrame`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerEvent {
    Joined,
    Left,
}

impl PeerEvent {
    /// Canonical wire string for this event.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Joined => "joined",
            Self::Left => "left",
        }
    }

    /// Whether this is one of the two transitions the protocol defines.
    pub fn is_known(self) -> bool {
        matches!(self, Self::Joined | Self::Left)
    }
}

/// Capability tokens advertised on the `/sync` upgrade request.
///
/// Wire grammar (design §3.2): comma-separated **lowercase** tokens matching
/// `1*( a-z / 0-9 / "-" )`.
///
/// The parsed set is a pure function of which *known* tokens appear in the
/// input:
///
/// ```text
/// caps ≙ { t ∈ known : t ∈ input }
/// ```
///
/// Empty, malformed and unknown tokens are ignored and cost nothing, so a
/// future token this build has never heard of cannot push a known token out of
/// the result. That is what makes the value forward-compatible: an older parser
/// reading `some-future-token,lazy-file` still yields `lazy-file`, and every
/// implementation agrees on the same input.
///
/// There is no token-count bound. The only real limit is that the known table
/// must fit the backing `u8` — see [`Caps::KNOWN`], where that is a compile
/// time assertion. A count limit would contradict the invariant above as soon
/// as the known set outgrew it: two builds would disagree about the same input.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Caps {
    bits: u8,
}

impl Caps {
    /// Can consume `file_offer` and pull chunks.
    pub const LAZY_FILE: Self = Self { bits: 1 << 0 };
    /// Can receive file bytes at all.
    pub const FILE_RECV: Self = Self { bits: 1 << 1 };
    /// Understands `transfer_id` as the session key.
    pub const TRANSFER_ID: Self = Self { bits: 1 << 2 };

    /// The single source of truth for the known token set.
    ///
    /// Both directions derive from this table: [`Self::parse`] and
    /// [`Self::known_caps`] look tokens up here, and [`Self::tokens`] renders
    /// from it. Keeping one list is what stops parsing and broadcasting from
    /// drifting apart — a capability the hub can read but never advertises is
    /// invisible to every peer.
    pub const KNOWN: [(Self, &'static str); 3] = [
        (Self::LAZY_FILE, "lazy-file"),
        (Self::FILE_RECV, "file-recv"),
        (Self::TRANSFER_ID, "transfer-id"),
    ];

    fn token_for(token: &str) -> Option<Self> {
        Self::KNOWN
            .iter()
            .find(|(_, known)| *known == token)
            .map(|(flag, _)| *flag)
    }

    /// Parse an upgrade-query `caps=` value.
    ///
    /// Splits on `,` and keeps every known token. Matching is exact: design
    /// §3.2 defines the grammar without optional whitespace, so `" lazy-file"`
    /// is not the same token as `"lazy-file"`. Trimming here would make a
    /// strict implementation and this one disagree on the same input, which is
    /// exactly what the invariant above forbids.
    ///
    /// There is no length bound here: this is a pure parse of whatever string
    /// it is given, so callers that need the transport-level size guard must
    /// use [`Self::parse_guarded`].
    pub fn parse(raw: &str) -> Self {
        let mut caps = Self::default();

        for token in raw.split(',') {
            if !is_caps_token(token) {
                continue;
            }
            let Some(flag) = Self::token_for(token) else {
                // Unknown but well-formed: ignored, and it costs nothing.
                continue;
            };
            // A repeat adds no information; short-circuit so it costs no work.
            if caps.contains(flag) {
                continue;
            }
            caps.bits |= flag.bits;
        }
        caps
    }

    /// [`Self::parse`], but reject an oversized `caps` value outright.
    ///
    /// A value longer than [`MAX_CAPS_BYTES`] is a transport-level abuse rather
    /// than a capability list, so the whole value is ignored (equivalent to
    /// advertising nothing) instead of being partially parsed. Keeping this
    /// separate from [`Self::parse`] means the parse rule itself has no size
    /// branch: two implementations can agree on the token semantics without
    /// agreeing on a truncation point.
    pub fn parse_guarded(raw: &str) -> Self {
        if raw.len() > MAX_CAPS_BYTES {
            debug!(
                bytes = raw.len(),
                limit = MAX_CAPS_BYTES,
                "ignoring oversized caps value"
            );
            return Self::default();
        }
        Self::parse(raw)
    }

    /// Whether every capability in `other` is present.
    pub fn contains(self, other: Self) -> bool {
        self.bits & other.bits == other.bits
    }

    /// Whether no known capability is present.
    pub fn is_empty(self) -> bool {
        self.bits == 0
    }

    /// Canonical token list, suitable for `welcome`/`peer_changed`.
    ///
    /// Rendered from [`Self::KNOWN`], so a capability can never be parseable
    /// but unadvertisable.
    pub fn tokens(self) -> Vec<String> {
        Self::KNOWN
            .iter()
            .filter(|(flag, _)| self.contains(*flag))
            .map(|(_, token)| token.to_string())
            .collect()
    }
}

/// The backing store is one byte, so the known table may hold at most eight
/// capabilities. Adding a ninth would shift past bit 7; this fails the build
/// rather than silently wrapping.
const _: () = assert!(Caps::KNOWN.len() <= u8::BITS as usize);

impl std::ops::BitOr for Caps {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self {
            bits: self.bits | rhs.bits,
        }
    }
}

impl std::ops::BitOrAssign for Caps {
    fn bitor_assign(&mut self, rhs: Self) {
        self.bits |= rhs.bits;
    }
}

/// `token = 1*( a-z / 0-9 / "-" )` — lowercase only, per design §3.2.
fn is_caps_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Any frame on the wire. `#[serde(tag = "type")]` flattens the variant fields
/// alongside a `"type": "<variant>"` discriminator, matching `PROTOCOL.md` §3.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Clip(ClipFrame),
    Welcome(WelcomeFrame),
    Error(ErrorFrame),
    FileChunk(FileChunkFrame),
    FileOffer(FileOfferFrame),
    FilePull(FilePullFrame),
    FileDigest(FileDigestFrame),
    FileAbort(FileAbortFrame),
    PeerChanged(PeerChangedFrame),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clip_frame_text_roundtrip() {
        let id = Uuid::new_v4();
        let from = Uuid::new_v4();
        let f = Frame::Clip(ClipFrame {
            id,
            ts: 1_716_163_200_000,
            content_type: TEXT_CONTENT_TYPE.to_string(),
            content: Some("hello world".to_string()),
            content_b64: None,
            from: Some(from),
        });
        let s = serde_json::to_string(&f).unwrap();
        assert!(s.contains(r#""type":"clip""#));
        assert!(s.contains(r#""content":"hello world""#));
        assert!(!s.contains("content_b64"));
        let back: Frame = serde_json::from_str(&s).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn clip_frame_binary_roundtrip() {
        let f = Frame::Clip(ClipFrame {
            id: Uuid::new_v4(),
            ts: 0,
            content_type: IMAGE_PNG_CONTENT_TYPE.to_string(),
            content: None,
            content_b64: Some("iVBORw0KGgo=".to_string()),
            from: None,
        });
        let s = serde_json::to_string(&f).unwrap();
        assert!(s.contains(r#""content_type":"image/png""#));
        assert!(s.contains(r#""content_b64":"iVBORw0KGgo=""#));
        assert!(!s.contains(r#""content":"#));
        let back: Frame = serde_json::from_str(&s).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn clip_frame_validate_rejects_missing_payload() {
        let f = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: TEXT_CONTENT_TYPE.to_string(),
            content: None,
            content_b64: None,
            from: None,
        };
        assert!(f.validate().is_err());
    }

    #[test]
    fn clip_frame_validate_rejects_double_payload() {
        let f = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: TEXT_CONTENT_TYPE.to_string(),
            content: Some("x".into()),
            content_b64: Some("eA==".into()),
            from: None,
        };
        assert!(f.validate().is_err());
    }

    #[test]
    fn clip_frame_validate_rejects_mismatched_kind() {
        let text_with_b64 = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: TEXT_CONTENT_TYPE.to_string(),
            content: None,
            content_b64: Some("eA==".into()),
            from: None,
        };
        assert!(text_with_b64.validate().is_err());

        let bin_with_text = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: IMAGE_PNG_CONTENT_TYPE.to_string(),
            content: Some("x".into()),
            content_b64: None,
            from: None,
        };
        assert!(bin_with_text.validate().is_err());
    }

    #[test]
    fn welcome_frame_serializes_with_type_tag() {
        let f = Frame::Welcome(WelcomeFrame {
            server: PROTOCOL_VERSION.to_string(),
            client_id: Uuid::nil(),
            caps: Vec::new(),
            peers: Vec::new(),
            offers: Vec::new(),
            last_clip: None,
        });
        let s = serde_json::to_string(&f).unwrap();
        assert!(s.starts_with(r#"{"type":"welcome""#));
        assert!(s.contains(r#""last_clip":null"#));
    }

    #[test]
    fn error_codes_have_documented_close_codes() {
        assert_eq!(ErrorCode::Unauthorized.close_code(), 4001);
        assert_eq!(ErrorCode::BadFrame.close_code(), 4002);
        assert_eq!(ErrorCode::FrameTooLarge.close_code(), 4003);
        assert_eq!(ErrorCode::UnsupportedType.close_code(), 4004);
        assert_eq!(ErrorCode::DuplicateSession.close_code(), 4005);
    }

    #[test]
    fn unknown_type_fails_to_deserialize() {
        // §3 wants unknown frame types ignored, but the wire type has to reject
        // them first: serde cannot produce a `Frame` for an unrecognised tag.
        //
        // Current behaviour, as of Phase 2: `ws.rs` treats the parse failure as
        // `bad_frame` and closes the connection. That is the prerelease bug
        // described in design §1; task 3.1 (Phase 3) changes the reader to log
        // at DEBUG and keep the connection open. This test only pins the
        // protocol-layer fact that a future tag does not deserialise — it must
        // stay true after 3.1 too.
        let s = r#"{"type":"future_frame","foo":1}"#;
        let r: Result<Frame, _> = serde_json::from_str(s);
        assert!(r.is_err());
    }

    #[test]
    fn file_chunk_frame_roundtrip() {
        let f = Frame::FileChunk(FileChunkFrame {
            file_id: Uuid::nil(),
            name: "doc.pdf".to_string(),
            content_type: "application/pdf".to_string(),
            chunk_index: 0,
            total_chunks: 3,
            file_sha256: Some(
                "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:\
                 AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
                    .replace(['\n', ' '], ""),
            ),
            file_size: 12_345_678,
            payload_b64: "iVBORw0KGgo=".to_string(),
            from: None,
            to: None,
            transfer_id: None,
            from_device: None,
        });
        let s = serde_json::to_string(&f).unwrap();
        assert!(s.contains(r#""type":"file_chunk""#));
        assert!(s.contains(r#""name":"doc.pdf""#));
        assert!(s.contains(r#""chunk_index":0"#));
        let back: Frame = serde_json::from_str(&s).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn file_chunk_validate_rejects_traversal_in_name() {
        let mut chunk = base_chunk();
        chunk.name = "".into();
        assert!(chunk.validate().is_err());

        chunk.name = "ok\0sneaky".into();
        assert!(chunk.validate().is_err());
    }

    #[test]
    fn file_chunk_validate_rejects_out_of_range_index() {
        let mut chunk = base_chunk();
        chunk.chunk_index = 4;
        chunk.total_chunks = 4;
        assert!(chunk.validate().is_err());
    }

    #[test]
    fn file_chunk_validate_rejects_zero_total() {
        let mut chunk = base_chunk();
        chunk.total_chunks = 0;
        assert!(chunk.validate().is_err());
    }

    fn base_chunk() -> FileChunkFrame {
        FileChunkFrame {
            file_id: Uuid::nil(),
            name: "a.bin".into(),
            content_type: "application/octet-stream".into(),
            chunk_index: 0,
            total_chunks: 1,
            file_sha256: Some("00".into()),
            file_size: 1,
            payload_b64: "AA==".into(),
            from: None,
            to: None,
            transfer_id: None,
            from_device: None,
        }
    }

    fn base_lazy_chunk() -> FileChunkFrame {
        FileChunkFrame {
            file_sha256: None,
            to: Some(Uuid::nil()),
            transfer_id: Some(Uuid::nil()),
            ..base_chunk()
        }
    }

    #[test]
    fn clip_frame_from_field_is_omitted_when_none() {
        let f = ClipFrame {
            id: Uuid::nil(),
            ts: 0,
            content_type: TEXT_CONTENT_TYPE.to_string(),
            content: Some(String::new()),
            content_b64: None,
            from: None,
        };
        let s = serde_json::to_string(&f).unwrap();
        assert!(!s.contains("\"from\""));
    }

    // -- Phase 2: eager compatibility fixture -----------------------------

    /// Serialised by the shipped v0.3 code before any of these fields existed.
    /// Do not regenerate from current code: this is the byte-for-byte contract
    /// that keeps `clipboardwire send` compatible with v0.3 hubs and peers.
    const V03_EAGER_CHUNK_JSON: &str = concat!(
        r#"{"type":"file_chunk","#,
        r#""file_id":"00000000-0000-0000-0000-000000000000","#,
        r#""name":"fixture.txt","#,
        r#""content_type":"text/plain; charset=utf-8","#,
        r#""chunk_index":0,"#,
        r#""total_chunks":1,"#,
        r#""file_sha256":"AA:BB","#,
        r#""file_size":3,"#,
        r#""payload_b64":"aGk="}"#,
    );

    fn eager_fixture_chunk() -> FileChunkFrame {
        FileChunkFrame {
            file_id: Uuid::nil(),
            name: "fixture.txt".to_string(),
            content_type: "text/plain; charset=utf-8".to_string(),
            chunk_index: 0,
            total_chunks: 1,
            file_sha256: Some("AA:BB".to_string()),
            file_size: 3,
            payload_b64: "aGk=".to_string(),
            from: None,
            to: None,
            transfer_id: None,
            from_device: None,
        }
    }

    #[test]
    fn eager_chunk_serialises_byte_identically_to_v03() {
        let json = serde_json::to_string(&Frame::FileChunk(eager_fixture_chunk())).unwrap();
        assert_eq!(json, V03_EAGER_CHUNK_JSON);
        // And the fixture still parses back into the same value.
        let back: Frame = serde_json::from_str(V03_EAGER_CHUNK_JSON).unwrap();
        assert_eq!(back, Frame::FileChunk(eager_fixture_chunk()));
    }

    #[test]
    fn eager_chunk_with_hub_stamp_keeps_v03_field_order() {
        // The hub stamps `from` on relay; that field existed in v0.3 and must
        // still come before the new optional fields (all of which are absent
        // on an eager chunk).
        let mut chunk = eager_fixture_chunk();
        chunk.from = Some(Uuid::nil());
        let json = serde_json::to_string(&Frame::FileChunk(chunk)).unwrap();
        let expected = V03_EAGER_CHUNK_JSON.replace(
            r#""payload_b64":"aGk="}"#,
            r#""payload_b64":"aGk=","from":"00000000-0000-0000-0000-000000000000"}"#,
        );
        assert_eq!(json, expected);
    }

    // -- Phase 2: lazy chunk shape ----------------------------------------

    #[test]
    fn lazy_chunk_omits_file_sha256_and_round_trips() {
        let f = Frame::FileChunk(base_lazy_chunk());
        let json = serde_json::to_string(&f).unwrap();
        assert!(!json.contains("file_sha256"), "{json}");
        assert!(json.contains(r#""transfer_id":"00000000-0000-0000-0000-000000000000""#));
        assert!(json.contains(r#""to":"00000000-0000-0000-0000-000000000000""#));
        let back: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn lazy_chunk_from_device_round_trips() {
        let mut chunk = base_lazy_chunk();
        chunk.from_device = Some(Uuid::from_u128(7));
        let json = serde_json::to_string(&Frame::FileChunk(chunk.clone())).unwrap();
        assert!(json.contains(r#""from_device":"00000000-0000-0000-0000-000000000007""#));
        let Frame::FileChunk(back) = serde_json::from_str(&json).unwrap() else {
            panic!("expected file_chunk");
        };
        assert_eq!(back, chunk);
    }

    #[test]
    fn chunk_without_optional_fields_deserialises_as_eager() {
        // A v0.3 peer omits every new field; that must still parse.
        let Frame::FileChunk(chunk) = serde_json::from_str(V03_EAGER_CHUNK_JSON).unwrap() else {
            panic!("expected file_chunk");
        };
        assert!(chunk.transfer_id.is_none());
        assert!(chunk.to.is_none());
        assert!(chunk.from_device.is_none());
        assert_eq!(chunk.file_sha256.as_deref(), Some("AA:BB"));
    }

    // -- Phase 2: split validation ----------------------------------------

    #[test]
    fn eager_chunk_requires_file_sha256() {
        let mut chunk = base_chunk();
        chunk.file_sha256 = None;
        assert!(chunk.validate().is_err());
    }

    #[test]
    fn eager_chunk_rejects_empty_file_sha256() {
        let mut chunk = base_chunk();
        chunk.file_sha256 = Some(String::new());
        assert!(chunk.validate().is_err());
    }

    #[test]
    fn eager_chunk_rejects_to() {
        let mut chunk = base_chunk();
        chunk.to = Some(Uuid::nil());
        assert!(chunk.validate().is_err());
    }

    #[test]
    fn lazy_chunk_requires_to() {
        let mut chunk = base_lazy_chunk();
        chunk.to = None;
        assert!(chunk.validate().is_err());
    }

    #[test]
    fn lazy_chunk_rejects_file_sha256() {
        let mut chunk = base_lazy_chunk();
        chunk.file_sha256 = Some("00".into());
        assert!(chunk.validate().is_err());
    }

    #[test]
    fn both_valid_shapes_pass_validation() {
        let mut lazy = base_lazy_chunk();
        lazy.from_device = Some(Uuid::nil());
        assert!(lazy.validate().is_ok());
        assert!(base_chunk().validate().is_ok());
    }

    #[test]
    fn lazy_chunk_keeps_the_common_field_checks() {
        let mut chunk = base_lazy_chunk();
        chunk.name = String::new();
        assert!(chunk.validate().is_err());

        let mut chunk = base_lazy_chunk();
        chunk.payload_b64 = String::new();
        assert!(chunk.validate().is_err());

        let mut chunk = base_lazy_chunk();
        chunk.total_chunks = 0;
        assert!(chunk.validate().is_err());
    }

    // -- Phase 2: new frame round-trips -----------------------------------

    fn sample_offer() -> FileOfferFrame {
        FileOfferFrame {
            file_id: Uuid::from_u128(1),
            batch_id: Uuid::from_u128(2),
            ts: 1_716_163_200_000,
            name: "report.pdf".to_string(),
            content_type: "application/pdf".to_string(),
            file_size: 1_073_741_824,
            expires_at_ms: 1_716_165_000_000,
            from_device: Some(Uuid::from_u128(3)),
        }
    }

    #[test]
    fn file_offer_round_trips_with_absolute_expiry() {
        let f = Frame::FileOffer(sample_offer());
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains(r#""type":"file_offer""#));
        assert!(json.contains(r#""expires_at_ms":1716165000000"#));
        assert!(!json.contains("ttl"));
        let back: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn file_offer_omits_from_device_before_the_hub_stamps_it() {
        let mut offer = sample_offer();
        offer.from_device = None;
        let json = serde_json::to_string(&Frame::FileOffer(offer)).unwrap();
        assert!(!json.contains("from_device"), "{json}");
    }

    #[test]
    fn file_pull_round_trips() {
        let f = Frame::FilePull(FilePullFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: Uuid::from_u128(2),
            to: Uuid::from_u128(3),
            chunk_index: 3,
            window: 3,
            from_device: Some(Uuid::from_u128(4)),
        });
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains(r#""type":"file_pull""#));
        let back: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn file_pull_round_trips_with_from_device() {
        // The field the source needs to address its own reply. Losing it in
        // serialisation would leave every chunk unaddressable.
        let puller = Uuid::from_u128(0xabcd);
        let f = Frame::FilePull(FilePullFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: Uuid::from_u128(2),
            to: Uuid::from_u128(3),
            chunk_index: 0,
            window: 3,
            from_device: Some(puller),
        });
        let json = serde_json::to_string(&f).unwrap();
        assert!(
            json.contains(r#""from_device":"00000000-0000-0000-0000-00000000abcd""#),
            "{json}"
        );
        let back: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
        match back {
            Frame::FilePull(p) => assert_eq!(p.from_device, Some(puller)),
            other => panic!("expected file_pull, got {other:?}"),
        }
    }

    #[test]
    fn file_pull_omits_from_device_when_absent() {
        // What a client sends: only the hub may write this field. Omitting it
        // keeps a client's frame free of an unauthenticated claim, and the
        // absent field deserialises back to `None`, which is what makes the
        // hub's `is_none` guard a real check rather than a formality.
        let pull = FilePullFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: Uuid::from_u128(2),
            to: Uuid::from_u128(3),
            chunk_index: 0,
            window: 3,
            from_device: None,
        };
        let json = serde_json::to_string(&Frame::FilePull(pull.clone())).unwrap();
        assert!(!json.contains("from_device"), "{json}");
        let back: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Frame::FilePull(pull));
    }

    #[test]
    fn file_digest_round_trips() {
        let f = Frame::FileDigest(FileDigestFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: Uuid::from_u128(2),
            to: Uuid::from_u128(3),
            from_device: Some(Uuid::from_u128(4)),
            file_sha256: "AA:BB".to_string(),
        });
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains(r#""type":"file_digest""#));
        let back: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn file_abort_round_trips_with_and_without_transfer_id() {
        let with_session = Frame::FileAbort(FileAbortFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: Some(Uuid::from_u128(2)),
            to: Some(Uuid::from_u128(3)),
            from_device: Some(Uuid::from_u128(4)),
            reason: AbortReason::Busy.to_wire().unwrap().to_string(),
        });
        let json = serde_json::to_string(&with_session).unwrap();
        assert!(json.contains(r#""type":"file_abort""#));
        assert!(json.contains(r#""reason":"busy""#));
        assert_eq!(serde_json::from_str::<Frame>(&json).unwrap(), with_session);

        let no_session = Frame::FileAbort(FileAbortFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: None,
            to: Some(Uuid::from_u128(3)),
            from_device: None,
            reason: AbortReason::SourceGone.to_wire().unwrap().to_string(),
        });
        let json = serde_json::to_string(&no_session).unwrap();
        assert!(!json.contains("transfer_id"), "{json}");
        assert_eq!(serde_json::from_str::<Frame>(&json).unwrap(), no_session);
    }

    #[test]
    fn peer_changed_round_trips_for_both_events() {
        let joined = Frame::PeerChanged(PeerChangedFrame {
            device: Uuid::from_u128(1),
            caps: vec!["lazy-file".to_string(), "file-recv".to_string()],
            event: PeerEvent::Joined,
        });
        let json = serde_json::to_string(&joined).unwrap();
        assert!(json.contains(r#""type":"peer_changed""#));
        assert!(json.contains(r#""event":"joined""#));
        assert_eq!(serde_json::from_str::<Frame>(&json).unwrap(), joined);

        let left = Frame::PeerChanged(PeerChangedFrame {
            device: Uuid::from_u128(1),
            caps: Vec::new(),
            event: PeerEvent::Left,
        });
        let json = serde_json::to_string(&left).unwrap();
        assert!(json.contains(r#""event":"left""#));
        assert!(!json.contains("caps"), "{json}");
        assert_eq!(serde_json::from_str::<Frame>(&json).unwrap(), left);
    }

    // -- Phase 2: open abort reason set -----------------------------------

    #[test]
    fn known_abort_reasons_map_to_themselves() {
        for reason in [
            AbortReason::SourceGone,
            AbortReason::SourceChanged,
            AbortReason::UserCancel,
            AbortReason::UnknownOffer,
            AbortReason::Expired,
            AbortReason::Busy,
            AbortReason::WindowTooLarge,
            AbortReason::InvalidStart,
            AbortReason::Superseded,
        ] {
            assert_eq!(AbortReason::parse(reason.to_wire().unwrap()), reason);
        }
    }

    #[test]
    fn unknown_abort_reason_is_tolerated_as_failed() {
        let f = Frame::FileAbort(FileAbortFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: Some(Uuid::from_u128(2)),
            to: Some(Uuid::from_u128(3)),
            from_device: None,
            reason: "some_future_reason".to_string(),
        });
        let json = serde_json::to_string(&f).unwrap();
        // The frame itself is neither rejected nor rewritten on the wire.
        let back: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
        let Frame::FileAbort(abort) = back else {
            panic!("expected file_abort");
        };
        assert_eq!(AbortReason::parse(&abort.reason), AbortReason::Failed);
    }

    #[test]
    fn failed_is_not_a_wire_value() {
        assert_eq!(AbortReason::Failed.to_wire(), None);
        // Every other variant must have one, or a sender cannot emit it.
        for reason in [
            AbortReason::SourceGone,
            AbortReason::SourceChanged,
            AbortReason::UserCancel,
            AbortReason::UnknownOffer,
            AbortReason::Expired,
            AbortReason::Busy,
            AbortReason::WindowTooLarge,
            AbortReason::InvalidStart,
            AbortReason::Superseded,
        ] {
            assert!(reason.to_wire().is_some(), "{reason:?} needs a wire value");
        }
    }

    #[test]
    fn unknown_reason_is_never_rewritten_into_a_known_one() {
        // The trap this guards: `parse(raw).to_wire()` would replace a peer's
        // unknown reason with a fabricated known one. It must yield None so the
        // caller forwards the original string.
        let parsed = AbortReason::parse("some_future_reason");
        assert_eq!(parsed, AbortReason::Failed);
        assert_eq!(parsed.to_wire(), None);

        // And "failed" itself is not accepted back as a known reason.
        assert_eq!(AbortReason::parse("failed"), AbortReason::Failed);
        assert_eq!(AbortReason::parse("failed").to_wire(), None);
    }

    // -- Phase 2 review: per-frame validation ------------------------------

    #[test]
    fn file_offer_validate_rejects_empty_name() {
        let mut offer = sample_offer();
        offer.name = String::new();
        assert!(offer.validate().is_err());
        assert!(sample_offer().validate().is_ok());
    }

    #[test]
    fn file_offer_validate_rejects_nul_in_name() {
        let mut offer = sample_offer();
        offer.name = "ok\0sneaky.pdf".to_string();
        assert!(offer.validate().is_err());
    }

    #[test]
    fn file_offer_validate_rejects_zero_byte_file() {
        let mut offer = sample_offer();
        offer.file_size = 0;
        assert!(offer.validate().is_err());
    }

    #[test]
    fn file_offer_validate_rejects_non_positive_expiry() {
        let mut offer = sample_offer();
        offer.expires_at_ms = 0;
        assert!(offer.validate().is_err());

        offer.expires_at_ms = -1;
        assert!(offer.validate().is_err());
    }

    #[test]
    fn file_offer_validate_allows_an_expiry_in_the_past() {
        // Whether a deadline is stale is the hub's call (design §4.3); the
        // protocol layer must not read the clock.
        let mut offer = sample_offer();
        offer.expires_at_ms = 1;
        assert!(offer.validate().is_ok());
    }

    #[test]
    fn file_offer_validate_bounds_the_name_and_content_type() {
        // Offers are cached and replayed in every `welcome`, so one oversized
        // entry would break every later handshake.
        let mut offer = sample_offer();
        offer.name = "a".repeat(MAX_OFFER_NAME_BYTES);
        assert!(offer.validate().is_ok());

        offer.name = "a".repeat(MAX_OFFER_NAME_BYTES + 1);
        assert_eq!(offer.validate(), Err(ControlFrameError::NameTooLong));

        let mut offer = sample_offer();
        offer.content_type = "a".repeat(MAX_OFFER_CONTENT_TYPE_BYTES);
        assert!(offer.validate().is_ok());

        offer.content_type = "a".repeat(MAX_OFFER_CONTENT_TYPE_BYTES + 1);
        assert_eq!(offer.validate(), Err(ControlFrameError::ContentTypeTooLong));
    }

    #[test]
    fn only_a_bad_window_maps_to_a_wire_abort_reason() {
        // `window` is the one bound the protocol spells out. Everything else
        // has no honest wire reason: `AbortReason::Failed` is a receiver-side
        // mapping target with no wire value, so the hub must drop the frame
        // instead of fabricating one.
        for err in [
            ControlFrameError::WindowTooLarge,
            ControlFrameError::EmptyWindow,
        ] {
            assert_eq!(err.abort_reason(), Some(AbortReason::WindowTooLarge));
        }

        for err in [
            ControlFrameError::EmptyName,
            ControlFrameError::NameContainsNul,
            ControlFrameError::NameTooLong,
            ControlFrameError::ContentTypeTooLong,
            ControlFrameError::EmptyFile,
            ControlFrameError::InvalidExpiry,
            ControlFrameError::EmptyDigest,
            ControlFrameError::EmptyReason,
            ControlFrameError::MissingDestination,
            ControlFrameError::UnknownPeerEvent,
        ] {
            assert_eq!(err.abort_reason(), None, "{err:?}");
            // Whatever maps to a reason must itself be serialisable.
            assert!(!err.to_string().is_empty(), "{err:?}");
        }

        assert!(AbortReason::WindowTooLarge.to_wire().is_some());
    }

    #[test]
    fn control_frame_validation_is_typed_not_textual() {
        // Matching on the variant, rather than on a message string, is what
        // lets the hub choose `file_abort{window_too_large}` reliably.
        let pull = FilePullFrame {
            file_id: Uuid::nil(),
            transfer_id: Uuid::nil(),
            to: Uuid::nil(),
            chunk_index: 0,
            window: MAX_PULL_WINDOW + 1,
            from_device: None,
        };
        assert_eq!(pull.validate(), Err(ControlFrameError::WindowTooLarge));
    }

    #[test]
    fn file_pull_validate_rejects_zero_window() {
        let pull = FilePullFrame {
            file_id: Uuid::nil(),
            transfer_id: Uuid::nil(),
            to: Uuid::nil(),
            chunk_index: 0,
            window: 0,
            from_device: None,
        };
        assert!(pull.validate().is_err());
    }

    #[test]
    fn file_pull_validate_rejects_oversized_window() {
        let mut pull = FilePullFrame {
            file_id: Uuid::nil(),
            transfer_id: Uuid::nil(),
            to: Uuid::nil(),
            chunk_index: 0,
            window: MAX_PULL_WINDOW,
            from_device: None,
        };
        assert!(pull.validate().is_ok());

        pull.window = MAX_PULL_WINDOW + 1;
        assert!(pull.validate().is_err());
    }

    #[test]
    fn file_digest_validate_rejects_empty_hash() {
        let mut digest = FileDigestFrame {
            file_id: Uuid::nil(),
            transfer_id: Uuid::nil(),
            to: Uuid::nil(),
            from_device: None,
            file_sha256: String::new(),
        };
        assert!(digest.validate().is_err());

        digest.file_sha256 = "AA:BB".to_string();
        assert!(digest.validate().is_ok());
    }

    #[test]
    fn file_abort_validate_rejects_empty_reason() {
        let mut abort = FileAbortFrame {
            file_id: Uuid::nil(),
            transfer_id: None,
            to: None,
            from_device: None,
            reason: String::new(),
        };
        assert_eq!(abort.validate(), Err(ControlFrameError::EmptyReason));

        abort.reason = "source_gone".to_string();
        assert!(abort.validate().is_ok());

        // An unknown but non-empty reason is legal: the set is open.
        abort.reason = "some_future_reason".to_string();
        assert!(abort.validate().is_ok());
    }

    #[test]
    fn session_scoped_abort_requires_a_destination() {
        let mut abort = FileAbortFrame {
            file_id: Uuid::nil(),
            transfer_id: Some(Uuid::from_u128(2)),
            to: Some(Uuid::from_u128(3)),
            from_device: None,
            reason: "busy".to_string(),
        };
        assert!(abort.validate().is_ok());

        abort.to = None;
        assert_eq!(abort.validate(), Err(ControlFrameError::MissingDestination));
    }

    #[test]
    fn offer_scoped_abort_may_be_broadcast() {
        // `to` absent with no `transfer_id` is the broadcast form the hub
        // expands per recipient; it must be accepted and serialise without `to`.
        let frame = Frame::FileAbort(FileAbortFrame {
            file_id: Uuid::from_u128(1),
            transfer_id: None,
            to: None,
            from_device: None,
            reason: AbortReason::Superseded.to_wire().unwrap().to_string(),
        });
        let Frame::FileAbort(abort) = &frame else {
            panic!("expected file_abort");
        };
        assert!(abort.validate().is_ok());

        let json = serde_json::to_string(&frame).unwrap();
        assert!(!json.contains(r#""to":"#), "{json}");
        assert!(!json.contains("transfer_id"), "{json}");
        assert!(json.contains(r#""reason":"superseded""#));
        assert_eq!(serde_json::from_str::<Frame>(&json).unwrap(), frame);
    }

    #[test]
    fn peer_changed_accepts_known_events_and_tokens() {
        for event in [PeerEvent::Joined, PeerEvent::Left] {
            let frame = PeerChangedFrame {
                device: Uuid::nil(),
                caps: vec!["lazy-file".to_string(), "file-recv".to_string()],
                event,
            };
            assert!(frame.validate().is_ok(), "{event:?}");
            assert_eq!(frame.known_caps(), Caps::LAZY_FILE | Caps::FILE_RECV);
        }
        assert_eq!(PeerEvent::Joined.as_str(), "joined");
        assert_eq!(PeerEvent::Left.as_str(), "left");
        assert!(PeerEvent::Joined.is_known());
        assert!(PeerEvent::Left.is_known());
    }

    #[test]
    fn peer_changed_rejects_unknown_event() {
        // `event` is a typed enum, so an unknown wire value fails earlier than
        // `validate` — at deserialisation. Pin both halves of that contract.
        let json = r#"{"type":"peer_changed","device":"00000000-0000-0000-0000-000000000000","event":"vanished"}"#;
        assert!(serde_json::from_str::<Frame>(json).is_err());
        // And every value `PeerEvent` can hold is one `validate` accepts, so
        // the explicit frame-level check can never reject a well-formed enum.
        for event in [PeerEvent::Joined, PeerEvent::Left] {
            assert!(event.is_known());
        }
    }

    #[test]
    fn peer_changed_filters_malformed_caps_instead_of_rejecting_the_frame() {
        // Membership notices must survive a caps vocabulary this build does not
        // share: drop the bad entries, keep the frame.
        let mut frame = PeerChangedFrame {
            device: Uuid::nil(),
            caps: vec![
                "lazy-file".to_string(),
                "LAZY-FILE".to_string(),
                "has space".to_string(),
                String::new(),
                "some-future-token".to_string(),
                "transfer-id".to_string(),
            ],
            event: PeerEvent::Joined,
        };
        assert!(frame.validate().is_ok());
        assert_eq!(
            frame.known_caps(),
            Caps::LAZY_FILE | Caps::TRANSFER_ID,
            "only the known, well-formed tokens count"
        );

        // Even with nothing recognisable the frame is still a valid notice.
        frame.caps = vec!["nonsense".to_string()];
        assert!(frame.validate().is_ok());
        assert!(frame.known_caps().is_empty());
    }

    // -- Phase 2: caps grammar --------------------------------------------

    #[test]
    fn caps_parses_known_tokens() {
        let caps = Caps::parse("lazy-file,file-recv,transfer-id");
        assert!(caps.contains(Caps::LAZY_FILE));
        assert!(caps.contains(Caps::FILE_RECV));
        assert!(caps.contains(Caps::TRANSFER_ID));
        assert_eq!(caps.tokens(), vec!["lazy-file", "file-recv", "transfer-id"]);
    }

    #[test]
    fn caps_ignores_unknown_and_malformed_tokens() {
        let caps = Caps::parse("lazy-file,bogus,has space,lazy-file");
        assert_eq!(caps, Caps::LAZY_FILE);
        assert_eq!(Caps::parse(""), Caps::default());
        assert!(Caps::parse("bogus").is_empty());
        // Empty tokens from consecutive or trailing commas are ignored too.
        assert_eq!(Caps::parse(",lazy-file,,transfer-id,"), {
            Caps::LAZY_FILE | Caps::TRANSFER_ID
        });
    }

    #[test]
    fn long_unknown_token_does_not_crowd_out_known_tokens() {
        // The invariant: caps depend only on which known tokens are present.
        // A long unknown token neither truncates the input nor consumes budget,
        // so both known tokens survive regardless of its length.
        for filler in [40usize, 50, 65, 500] {
            let input = format!("{},lazy-file,transfer-id", "x".repeat(filler));
            assert_eq!(
                Caps::parse(&input),
                Caps::LAZY_FILE | Caps::TRANSFER_ID,
                "filler length {filler}"
            );
        }
    }

    #[test]
    fn caps_is_forward_compatible_with_unknown_tokens() {
        // An older parser must agree with a newer sender: the token it has
        // never heard of is simply absent from the result.
        assert_eq!(Caps::parse("some-future-token,lazy-file"), Caps::LAZY_FILE);
        assert_eq!(
            Caps::parse("lazy-file,some-future-token,transfer-id"),
            Caps::LAZY_FILE | Caps::TRANSFER_ID
        );
    }

    #[test]
    fn unknown_tokens_do_not_consume_any_budget() {
        // 20 unknown tokens precede the known ones; all of them cost nothing,
        // so both known tokens are still accumulated. There is no token-count
        // limit any more, only the `u8` width of the known table.
        let mut tokens = vec!["unknown"; 20];
        tokens.push("lazy-file");
        tokens.push("transfer-id");
        assert_eq!(
            Caps::parse(&tokens.join(",")),
            Caps::LAZY_FILE | Caps::TRANSFER_ID
        );
    }

    #[test]
    fn repeated_tokens_add_nothing_and_cost_nothing() {
        // `lazy-file` × 20 followed by `transfer-id`: a repeat adds no
        // information, so it must not push a later capability out.
        let mut tokens = vec!["lazy-file"; 20];
        tokens.push("transfer-id");
        assert_eq!(
            Caps::parse(&tokens.join(",")),
            Caps::LAZY_FILE | Caps::TRANSFER_ID
        );
    }

    #[test]
    fn parse_guarded_ignores_an_oversized_value() {
        // Padding uses a legal unknown token, so this test exercises the size
        // guard rather than relying on whitespace tolerance (the grammar has
        // none; see `caps_rejects_whitespace_padded_tokens`).
        let pad_to = |len: usize| {
            let mut value = String::from("lazy-file,");
            value.push_str(&"x".repeat(len - value.len()));
            assert_eq!(value.len(), len);
            value
        };

        // Exactly at the limit is still parsed.
        let at_limit = pad_to(MAX_CAPS_BYTES);
        assert_eq!(Caps::parse_guarded(&at_limit), Caps::LAZY_FILE);

        // One byte over: the whole value is ignored, not partially parsed, even
        // though a known token sits at the front.
        let over = pad_to(MAX_CAPS_BYTES + 1);
        assert_eq!(Caps::parse_guarded(&over), Caps::default());
    }

    #[test]
    fn caps_rejects_whitespace_padded_tokens() {
        // design §3.2 has no optional whitespace, so padding is a different
        // token. A strict implementation returns nothing here; accepting it
        // would make the two disagree on the same input.
        assert_eq!(Caps::parse(" lazy-file"), Caps::default());
        assert_eq!(Caps::parse("lazy-file "), Caps::default());
        assert_eq!(Caps::parse("lazy-file, transfer-id"), Caps::LAZY_FILE);
        assert_eq!(Caps::parse("lazy-file,\ttransfer-id"), Caps::LAZY_FILE);
    }

    #[test]
    fn known_table_round_trips_through_tokens() {
        // `parse` and `tokens` must derive from the same table, or the hub can
        // read a capability it will never advertise.
        for bits in 0u8..=0b111 {
            let caps = Caps { bits };
            let rendered = caps.tokens().join(",");
            assert_eq!(Caps::parse(&rendered), caps, "bits {bits:#05b}");
        }
        assert_eq!(Caps::KNOWN.len(), 3);
        // Every table entry parses back to itself and renders its own token.
        for (flag, token) in Caps::KNOWN {
            assert_eq!(Caps::parse(token), flag);
            assert_eq!(flag.tokens(), vec![token]);
        }
    }

    #[test]
    fn plain_parse_has_no_length_bound() {
        // `parse` is the pure token rule; the size guard is a separate entry
        // point so the two cannot disagree about truncation points.
        let huge = format!("{},lazy-file", "x".repeat(MAX_CAPS_BYTES * 2));
        assert_eq!(Caps::parse(&huge), Caps::LAZY_FILE);
        assert_eq!(Caps::parse_guarded(&huge), Caps::default());
    }

    #[test]
    fn caps_rejects_uppercase_tokens() {
        // design §3.2 grammar is lowercase only.
        assert_eq!(Caps::parse("LAZY-FILE"), Caps::default());
        assert_eq!(Caps::parse("Lazy-File"), Caps::default());
        assert_eq!(Caps::parse("lazy-file,LAZY-FILE"), Caps::LAZY_FILE);
    }

    #[test]
    fn caps_union_and_contains() {
        let caps = Caps::LAZY_FILE | Caps::FILE_RECV;
        assert!(caps.contains(Caps::LAZY_FILE));
        assert!(!caps.contains(Caps::TRANSFER_ID));
        assert_eq!(caps.tokens(), vec!["lazy-file", "file-recv"]);
    }

    #[test]
    fn protocol_version_tracks_the_lazy_file_transfer_release() {
        // The wire version, not the crate version. `tests/end_to_end.rs`
        // asserts `welcome.server == PROTOCOL_VERSION`, so it stays in sync on
        // its own; this literal is the only place a bump has to be mirrored.
        assert_eq!(PROTOCOL_VERSION, "clipboardwire/0.4.0");
    }
}

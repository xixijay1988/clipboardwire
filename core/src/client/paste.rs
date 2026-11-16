//! Delayed-render clipboard promise behind `receive_mode = "paste"`.
//!
//! Instead of pulling an offer the moment it arrives and dropping the file in
//! the user's Downloads, the offer arms the clipboard with a stand-in for the
//! file. The transfer only starts when a paste target asks for the bytes — at
//! that point [`PasteEvent::Pull`] goes back to the supervisor, which runs the
//! normal lazy pull into a temp spool, verifies the digest, and hands the
//! finished path back. A corrupt pull can never reach the paste destination.
//!
//! Two backends implement the same observable contract:
//!
//! - **Windows** owns the OLE clipboard with an `IDataObject` advertising
//!   `FileGroupDescriptorW` + `FileContents`; a paste target asking for
//!   `FileContents` is what starts the pull. `GetData` returns an `IStream`
//!   over the spool immediately, so the copy's progress tracks the network.
//!   `OleSetClipboard` callbacks are marshalled onto the owning thread, so a
//!   dedicated STA thread with a message pump hosts the promise — `GetData`
//!   may block for the duration of a whole pull, and keeping it off the tao
//!   tray loop is the point of that thread.
//! - **macOS** creates an empty placeholder per file under
//!   `<spool>/armed/<file_id>/<name>`, registers an `NSFilePresenter` for it,
//!   and puts the placeholder `file://` URLs on the general pasteboard. A
//!   Cmd+V in Finder coordinates a read of the placeholder, which fires
//!   `relinquishPresentedItemToReader:` on the presenter's own
//!   `NSOperationQueue`; the callback runs the pull, copies the verified
//!   bytes into the placeholder (or unlinks it, surfacing error -43), and
//!   only then replies. No byte progress is possible — Finder shows
//!   indeterminate "Preparing to copy" for the whole pull.
//!
//! Other platforms keep the same API as a no-op so the supervisor stays
//! platform-free: `receive_mode="paste"` is rejected on Linux at config load.

use std::path::PathBuf;
use tokio::sync::mpsc::UnboundedSender;

/// One advertised file: what the descriptor needs plus the pull key.
#[derive(Debug, Clone)]
pub struct PromiseFile {
    pub file_id: uuid::Uuid,
    pub name: String,
    pub size: u64,
}

/// Message from the clipboard thread to the supervisor.
pub enum PasteEvent {
    /// A paste target asked for `file_id`'s bytes. The supervisor replies
    /// immediately — a [`SpoolStream`] whose bytes fill in as the pull lands,
    /// or an error when the offer is gone. `GetData` returns the `IStream`
    /// right away; the pull itself streams into it.
    Pull {
        file_id: uuid::Uuid,
        reply: std::sync::mpsc::SyncSender<Result<SpoolStream, String>>,
    },
}

/// Write-frontier shared between the supervisor (chunk writer) and a
/// rendering `IStream` (reader).
///
/// `committed` is bytes safe to read — the assembler's contiguous prefix.
/// `outcome` is set exactly once at the end: `Ok` after SHA-256 verification,
/// `Err` on any failure — which is how a failed pull becomes a failed paste:
/// the stream's `Read` returns the error and the target deletes its partial
/// destination file, the same semantics as a network drive dropping mid-copy.
pub struct SpoolFrontier {
    inner: std::sync::Mutex<SpoolInner>,
    cond: std::sync::Condvar,
}

struct SpoolInner {
    committed: u64,
    outcome: Option<Result<(), String>>,
}

impl SpoolFrontier {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner: std::sync::Mutex::new(SpoolInner {
                committed: 0,
                outcome: None,
            }),
            cond: std::sync::Condvar::new(),
        })
    }

    /// Already-verified file: the whole length is readable, EOF at `len`.
    pub fn completed() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner: std::sync::Mutex::new(SpoolInner {
                committed: u64::MAX,
                outcome: Some(Ok(())),
            }),
            cond: std::sync::Condvar::new(),
        })
    }

    /// More verified-contiguous bytes are readable.
    pub fn update(&self, committed: u64) {
        let mut g = self.inner.lock().unwrap();
        g.committed = g.committed.max(committed);
        drop(g);
        self.cond.notify_all();
    }

    /// End of story: verified (`Ok`) or failed (`Err` with a reason).
    pub fn finish(&self, outcome: Result<(), String>) {
        let mut g = self.inner.lock().unwrap();
        if g.outcome.is_none() {
            g.outcome = Some(outcome);
        }
        drop(g);
        self.cond.notify_all();
    }

    /// Block until the verdict is in: `Ok` once the whole file is spooled
    /// and verified, `Err` with the failure reason.
    ///
    /// The macOS presenter waits on this rather than reading through the
    /// frontier — its reader cannot see the placeholder until the reply,
    /// so there is nothing to stream: only the final verdict matters.
    pub fn wait(&self) -> Result<(), String> {
        let mut g = self.inner.lock().unwrap();
        while g.outcome.is_none() {
            g = self.cond.wait(g).unwrap();
        }
        g.outcome.clone().unwrap()
    }

    /// Like [`Self::wait`], but gives up after `timeout` with an `Err`
    /// describing the deadline rather than a pull failure.
    ///
    /// The macOS render uses this because a pull can stall indefinitely:
    /// while the transport is disconnected the supervisor skips its
    /// stall/restart checks entirely, so no verdict ever lands — and an
    /// unbounded wait would leave the reader (and the presenter-queue
    /// thread that owns it) parked on the condvar forever.
    pub fn wait_timeout(&self, timeout: std::time::Duration) -> Result<(), String> {
        let deadline = std::time::Instant::now() + timeout;
        let mut g = self.inner.lock().unwrap();
        loop {
            if let Some(outcome) = g.outcome.clone() {
                return outcome;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!("pull verdict did not arrive within {timeout:?}"));
            }
            let (guard, _) = self.cond.wait_timeout(g, remaining).unwrap();
            g = guard;
        }
    }
}

/// A renderable file: the partial path plus the frontier that says how much
/// of it exists. `Clone` is cheap — the frontier is shared.
#[derive(Clone)]
pub struct SpoolStream {
    pub path: PathBuf,
    pub len: u64,
    pub frontier: std::sync::Arc<SpoolFrontier>,
}

/// Handle the supervisor uses to (re)arm the clipboard promise.
pub struct PasteSource {
    /// Windows and the no-op stub take commands on a channel — the STA
    /// thread owns the promise object.
    #[cfg(not(target_os = "macos"))]
    cmd: std::sync::mpsc::Sender<PasteCommand>,
    /// macOS needs no command thread: presenter callbacks arrive on each
    /// presenter's own `NSOperationQueue` (GCD-backed, headless-safe), so
    /// `arm()` can update the registry and write the pasteboard inline.
    #[cfg(target_os = "macos")]
    imp: std::sync::Mutex<imp::MacPaste>,
}

#[cfg(not(target_os = "macos"))]
enum PasteCommand {
    // `Vec` is unused on the no-promise stubs; kept so `arm()` has one signature.
    #[allow(dead_code)]
    Arm(Vec<PromiseFile>),
}

impl PasteSource {
    /// Replace the clipboard promise with one describing `files`. An empty
    /// vec just releases the clipboard.
    #[cfg(not(target_os = "macos"))]
    pub fn arm(&self, files: Vec<PromiseFile>) {
        let _ = self.cmd.send(PasteCommand::Arm(files));
    }

    /// Replace the armed placeholders with ones describing `files`. An
    /// empty vec disarms: presenters removed, `armed/` deleted.
    #[cfg(target_os = "macos")]
    pub fn arm(&self, files: Vec<PromiseFile>) {
        match self.imp.lock() {
            Ok(mut imp) => imp.arm(files),
            Err(_) => tracing::warn!("paste presenter registry poisoned; skipping arm"),
        }
    }
}

/// Spawn the promise host. `events` receives paste-time pull requests.
#[cfg(any(windows, target_os = "macos"))]
pub fn spawn(events: UnboundedSender<PasteEvent>) -> PasteSource {
    imp::spawn(events)
}

/// Stub for platforms with no paste-time hook: the promise can never
/// fire, so the channel ends.
#[cfg(not(any(windows, target_os = "macos")))]
pub fn spawn(events: UnboundedSender<PasteEvent>) -> PasteSource {
    drop(events);
    let (cmd, _rx) = std::sync::mpsc::channel();
    PasteSource { cmd }
}

/// Where pulled bytes wait to be rendered. Not the Downloads folder: this is
/// a cache the user never sees, cleaned by the OS like any temp file.
pub fn spool_dir() -> PathBuf {
    std::env::temp_dir().join("clipboardwire-paste")
}

/// `<spool>/armed` — the root the macOS backend fills with one
/// `<file_id>/<name>` placeholder per armed offer.
///
/// Platform-neutral even though only macOS creates it: the clipboard
/// poller consults it on every OS, and "paths under our own armed dir are
/// never publishable as offers" should have exactly one home.
pub fn armed_dir() -> PathBuf {
    spool_dir().join("armed")
}

/// Whether `path` is one of our own armed placeholders. Such a path on
/// the clipboard is the residue of an arm, not a user copy — publishing
/// it as an offer would echo our own spool back to the hub (and after a
/// paste fills the placeholder, would re-echo on every identity change).
///
/// Compared under both spellings of the spool root: TMPDIR on macOS is
/// `/var/folders/...`, which the pasteboard can hand back as
/// `/private/var/folders/...`. `temp_dir` itself always exists, so
/// canonicalizing it works even when `armed/` or the placeholder is
/// already gone (canonicalizing the placeholder could fail on a dead
/// path and leak it through the filter).
pub fn is_armed_placeholder(path: &std::path::Path) -> bool {
    path.starts_with(armed_dir()) || path.starts_with(canonical_armed_dir())
}

/// The armed root under the canonical spelling of TMPDIR — the form a
/// reader may report. Cached: TMPDIR is fixed for the process, and the
/// clipboard poller consults this on every files read.
fn canonical_armed_dir() -> &'static std::path::Path {
    static CANON: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    CANON.get_or_init(|| {
        std::env::temp_dir()
            .canonicalize()
            .map(|t| t.join("clipboardwire-paste").join("armed"))
            .unwrap_or_else(|_| armed_dir())
    })
}

/// Where one armed offer's placeholder lives:
/// `armed/<file_id>/<sanitized-name>`.
///
/// The per-offer directory does two jobs at once: same-named offers can
/// never collide with each other, and a placeholder can never coincide
/// with the `<file_id>.partial` the pull writes at the spool root. The
/// name is sanitized with the same rule the landing path uses.
#[cfg(any(target_os = "macos", test))]
fn placeholder_path(file_id: uuid::Uuid, name: &str) -> PathBuf {
    armed_dir()
        .join(file_id.to_string())
        .join(crate::client::file::sanitize_filename(name))
}

/// Longest a paste render waits for the pull verdict before giving up
/// and unlinking the placeholder (Finder surfaces -43).
///
/// Must be bounded: while the transport is disconnected the supervisor
/// skips its stall/restart checks, so a pull can sit un-unfinished
/// forever — and an unbounded render would leave Finder in "Preparing to
/// copy" permanently, plus leak the presenter-queue thread if the user
/// cancels the copy. Sized well above the machinery's own failure bound
/// while connected — `TAIL_STALL_TIMEOUT_MS` (15 s) across
/// `MAX_RESTARTS + 1` attempts is ~1 min — with margin for a large but
/// healthy pull to digest.
#[cfg(any(target_os = "macos", test))]
const PASTE_RENDER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// Fill `placeholder` from a paste pull's outcome, or delete it so the
/// waiting reader surfaces a copy error instead of an empty file.
///
/// `spool` is the supervisor's reply to [`PasteEvent::Pull`]: `Ok`
/// carries the partial path + frontier, `Err` means the offer is gone.
/// Waits on the frontier for the digest verdict — no longer than
/// [`PASTE_RENDER_TIMEOUT`] — then copies the verified bytes into the
/// placeholder: copy, not rename, because the verified partial stays in
/// the spool for repeat pastes (`paste_sink.ready`) and `std::fs::copy`
/// on APFS is a clone. A failed pull, a timed-out pull *or* a failed
/// copy ends with the placeholder unlinked — Finder then reports error
/// -43, the analogue of Windows' `E_ABORT`.
///
/// Platform-neutral so the fill-or-unlink decision is testable without
/// AppKit; the macOS presenter is the only caller.
#[cfg(any(target_os = "macos", test))]
fn render_placeholder(spool: Result<SpoolStream, String>, placeholder: &std::path::Path) {
    render_placeholder_within(spool, placeholder, PASTE_RENDER_TIMEOUT)
}

/// [`render_placeholder`] with the deadline as a parameter so tests can
/// exercise the timeout path without waiting minutes.
#[cfg(any(target_os = "macos", test))]
fn render_placeholder_within(
    spool: Result<SpoolStream, String>,
    placeholder: &std::path::Path,
    timeout: std::time::Duration,
) {
    let filled = spool
        .and_then(|s| s.frontier.wait_timeout(timeout).map(|()| s))
        .and_then(|s| {
            std::fs::copy(&s.path, placeholder)
                .map(|_| ())
                .map_err(|e| e.to_string())
        });
    if let Err(reason) = filled {
        tracing::debug!(
            path = %placeholder.display(),
            error = %reason,
            "paste render failed; removing the placeholder"
        );
        let _ = std::fs::remove_file(placeholder);
    }
}

#[cfg(windows)]
mod imp {
    use super::{PasteCommand, PasteEvent, PromiseFile, SpoolFrontier};
    use std::cell::{Cell, RefCell};
    use std::path::PathBuf;
    use tokio::sync::mpsc::UnboundedSender;
    use windows::core::{implement, HRESULT, PCWSTR};
    use windows::Win32::Foundation::{FILETIME, POINTL, SIZE};
    use windows::Win32::System::Com::IBindCtx;
    use windows::Win32::System::Com::*;
    use windows::Win32::System::DataExchange::RegisterClipboardFormatW;
    use windows::Win32::System::Memory::{GlobalAlloc, GlobalLock, GlobalUnlock, GMEM_MOVEABLE};
    use windows::Win32::System::Ole::{OleInitialize, OleSetClipboard, DROPEFFECT_COPY};
    use windows::Win32::UI::Shell::{
        IDataObjectAsyncCapability, IDataObjectAsyncCapability_Impl, FILEDESCRIPTORW,
        FILEGROUPDESCRIPTORW,
    };
    use windows::Win32::UI::WindowsAndMessaging::{
        DispatchMessageW, PeekMessageW, TranslateMessage, MSG, PM_REMOVE,
    };

    const S_OK: HRESULT = HRESULT(0);
    const S_FALSE: HRESULT = HRESULT(1);
    const E_NOTIMPL: HRESULT = HRESULT(0x80004001u32 as i32);
    const E_FAIL: HRESULT = HRESULT(0x80004005u32 as i32);
    const DV_E_FORMATETC: HRESULT = HRESULT(0x80040064u32 as i32);
    const DV_E_TYMED: HRESULT = HRESULT(0x80040069u32 as i32);
    const DV_E_LINDEX: HRESULT = HRESULT(0x80040068u32 as i32);

    const FD_ATTRIBUTES: u32 = 0x4;
    const FD_FILESIZE: u32 = 0x40;
    const FD_PROGRESSUI: u32 = 0x4000;

    struct Formats {
        fgd_w: u32,
        fc: u32,
        pde: u32,
    }

    fn register_formats() -> Formats {
        fn reg(s: &str) -> u32 {
            let w: Vec<u16> = s.encode_utf16().chain(std::iter::once(0)).collect();
            unsafe { RegisterClipboardFormatW(PCWSTR::from_raw(w.as_ptr())) }
        }
        Formats {
            fgd_w: reg("FileGroupDescriptorW"),
            fc: reg("FileContents"),
            pde: reg("Preferred DropEffect"),
        }
    }

    fn hglobal_with(bytes: &[u8]) -> windows::core::Result<STGMEDIUM> {
        unsafe {
            let hg = GlobalAlloc(GMEM_MOVEABLE, bytes.len().max(1))?;
            let ptr = GlobalLock(hg);
            if ptr.is_null() {
                return Err(E_FAIL.into());
            }
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
            let _ = GlobalUnlock(hg);
            Ok(STGMEDIUM {
                tymed: TYMED_HGLOBAL.0 as u32,
                u: STGMEDIUM_0 { hGlobal: hg },
                pUnkForRelease: std::mem::ManuallyDrop::new(None),
            })
        }
    }

    fn group_descriptor(files: &[PromiseFile]) -> Vec<u8> {
        let count = files.len();
        let size = std::mem::size_of::<u32>() + count * std::mem::size_of::<FILEDESCRIPTORW>();
        let mut buf = vec![0u8; size];
        unsafe {
            let fgd = buf.as_mut_ptr() as *mut FILEGROUPDESCRIPTORW;
            (*fgd).cItems = count as u32;
            let base = buf.as_mut_ptr().add(4) as *mut FILEDESCRIPTORW;
            for (i, f) in files.iter().enumerate() {
                let d = base.add(i);
                (*d).dwFlags = FD_ATTRIBUTES | FD_FILESIZE | FD_PROGRESSUI;
                (*d).dwFileAttributes = 0x20;
                (*d).nFileSizeHigh = (f.size >> 32) as u32;
                (*d).nFileSizeLow = (f.size & 0xFFFF_FFFF) as u32;
                (*d).clsid = windows::core::GUID::zeroed();
                (*d).sizel = SIZE { cx: 0, cy: 0 };
                (*d).pointl = POINTL { x: 0, y: 0 };
                let ft = FILETIME {
                    dwLowDateTime: 0,
                    dwHighDateTime: 0,
                };
                (*d).ftCreationTime = ft;
                (*d).ftLastAccessTime = ft;
                (*d).ftLastWriteTime = ft;
                let wide: Vec<u16> = f.name.encode_utf16().chain(std::iter::once(0)).collect();
                let n = wide.len().min(260);
                let dst = std::ptr::addr_of_mut!((*d).cFileName) as *mut u16;
                std::ptr::copy_nonoverlapping(wide.as_ptr(), dst, n);
            }
        }
        buf
    }

    /// IStream over the pull's partial file, live. `Read` blocks at the
    /// verified frontier, so Explorer's progress dialog tracks the actual
    /// network transfer. The final `Read` past `len` waits for the digest
    /// verdict: verified → clean EOF, failed → error, and Explorer removes
    /// its half-copied destination itself.
    #[implement(IStream)]
    struct SpoolIStream {
        /// The `.partial` path — opened lazily since it may not exist yet
        /// when `GetData` arms the stream.
        path: PathBuf,
        file: RefCell<Option<std::fs::File>>,
        len: u64,
        pos: Cell<u64>,
        frontier: std::sync::Arc<SpoolFrontier>,
    }

    impl ISequentialStream_Impl for SpoolIStream_Impl {
        fn Read(&self, pv: *mut core::ffi::c_void, cb: u32, pcbread: *mut u32) -> HRESULT {
            if pv.is_null() {
                return E_FAIL;
            }
            let mut delivered = 0u32;
            loop {
                // Frontier state: bytes readable now + final verdict if set.
                let (committed, outcome) = {
                    let g = self.frontier.inner.lock().unwrap();
                    (g.committed, g.outcome.clone())
                };
                let avail = committed.saturating_sub(self.pos.get());
                if avail > 0 && self.pos.get() < self.len {
                    let want = ((cb - delivered) as u64)
                        .min(avail)
                        .min(self.len - self.pos.get()) as usize;
                    if want > 0 {
                        // Open the partial on first use — the pull writes it.
                        {
                            let mut file = self.file.borrow_mut();
                            if file.is_none() {
                                *file = std::fs::File::open(&self.path).ok();
                            }
                        }
                        let file = self.file.borrow();
                        let Some(f) = file.as_ref() else {
                            return E_FAIL;
                        };
                        let buf = unsafe {
                            std::slice::from_raw_parts_mut(
                                (pv as *mut u8).add(delivered as usize),
                                want,
                            )
                        };
                        match std::os::windows::fs::FileExt::seek_read(f, buf, self.pos.get()) {
                            Ok(0) => {
                                // Writer hasn't reached this offset yet.
                            }
                            Ok(n) => {
                                self.pos.set(self.pos.get() + n as u64);
                                delivered += n as u32;
                                if delivered >= cb {
                                    break;
                                }
                                continue;
                            }
                            Err(_) => return E_FAIL,
                        }
                    }
                }
                if delivered > 0 {
                    break;
                }
                if let Some(outcome) = outcome {
                    if !pcbread.is_null() {
                        unsafe { *pcbread = delivered };
                    }
                    return match outcome {
                        // EOF is legal only once every byte left us.
                        Ok(()) if self.pos.get() >= self.len => S_FALSE,
                        Ok(()) => E_FAIL, // "verified" before all bytes served
                        Err(_) => E_FAIL,
                    };
                }
                // Nothing readable — block until the writer advances or
                // finishes. Hold the mutex across the wait.
                let mut g = self.frontier.inner.lock().unwrap();
                if g.committed <= self.pos.get() && g.outcome.is_none() {
                    g = self.frontier.cond.wait(g).unwrap();
                }
                drop(g);
            }
            if !pcbread.is_null() {
                unsafe { *pcbread = delivered };
            }
            if delivered < cb {
                S_FALSE
            } else {
                S_OK
            }
        }
        fn Write(&self, _pv: *const core::ffi::c_void, _cb: u32, _w: *mut u32) -> HRESULT {
            E_FAIL
        }
    }

    impl IStream_Impl for SpoolIStream_Impl {
        fn Seek(
            &self,
            mv: i64,
            origin: STREAM_SEEK,
            newpos: *mut u64,
        ) -> windows::core::Result<()> {
            let base = match origin.0 {
                0 => 0i64,
                1 => self.pos.get() as i64,
                _ => self.len as i64,
            };
            let np = base + mv;
            if np < 0 {
                return Err(E_FAIL.into());
            }
            self.pos.set(np as u64);
            if !newpos.is_null() {
                unsafe { *newpos = np as u64 };
            }
            Ok(())
        }
        fn SetSize(&self, _s: u64) -> windows::core::Result<()> {
            Err(E_FAIL.into())
        }
        fn CopyTo(
            &self,
            _s: windows::core::Ref<'_, IStream>,
            _c: u64,
            _r: *mut u64,
            _w: *mut u64,
        ) -> windows::core::Result<()> {
            Err(E_NOTIMPL.into())
        }
        fn Commit(&self, _f: &STGC) -> windows::core::Result<()> {
            Ok(())
        }
        fn Revert(&self) -> windows::core::Result<()> {
            Ok(())
        }
        fn LockRegion(&self, _o: u64, _c: u64, _l: &LOCKTYPE) -> windows::core::Result<()> {
            Err(E_FAIL.into())
        }
        fn UnlockRegion(&self, _o: u64, _c: u64, _l: u32) -> windows::core::Result<()> {
            Err(E_FAIL.into())
        }
        fn Stat(&self, stg: *mut STATSTG, _f: &STATFLAG) -> windows::core::Result<()> {
            if stg.is_null() {
                return Err(E_FAIL.into());
            }
            unsafe {
                *stg = std::mem::zeroed();
                (*stg).r#type = 2;
                (*stg).cbSize = self.len;
            }
            Ok(())
        }
        fn Clone(&self) -> windows::core::Result<IStream> {
            Err(E_NOTIMPL.into())
        }
    }

    impl IDataObjectAsyncCapability_Impl for PromiseDataObject_Impl {
        /// The flags only tell the consumer whether async operations are
        /// permitted; the actual bytes still flow through `GetData`, which
        /// already blocks for the pull. Advertising this is what makes
        /// Explorer wrap the paste in a progress UI.
        fn SetAsyncMode(&self, fdoopasync: windows::core::BOOL) -> windows::core::Result<()> {
            self.async_mode.set(fdoopasync.as_bool());
            Ok(())
        }
        fn GetAsyncMode(&self) -> windows::core::Result<windows::core::BOOL> {
            Ok(self.async_mode.get().into())
        }
        fn StartOperation(
            &self,
            _pbcreserved: windows::core::Ref<'_, IBindCtx>,
        ) -> windows::core::Result<()> {
            self.in_operation.set(true);
            Ok(())
        }
        fn InOperation(&self) -> windows::core::Result<windows_core::BOOL> {
            Ok(self.in_operation.get().into())
        }
        fn EndOperation(
            &self,
            _hresult: HRESULT,
            _pbcreserved: windows::core::Ref<'_, IBindCtx>,
            _dweffects: u32,
        ) -> windows::core::Result<()> {
            self.in_operation.set(false);
            Ok(())
        }
    }

    #[implement(IEnumFORMATETC)]
    struct FormatEnum {
        items: Vec<FORMATETC>,
        idx: Cell<usize>,
    }

    impl IEnumFORMATETC_Impl for FormatEnum_Impl {
        fn Next(&self, celt: u32, rgelt: *mut FORMATETC, pcelt: *mut u32) -> HRESULT {
            if rgelt.is_null() {
                return E_FAIL;
            }
            let mut done = 0u32;
            while done < celt {
                let i = self.idx.get();
                if i >= self.items.len() {
                    break;
                }
                unsafe { *rgelt.add(done as usize) = self.items[i] };
                self.idx.set(i + 1);
                done += 1;
            }
            if !pcelt.is_null() {
                unsafe { *pcelt = done };
            }
            if done < celt {
                S_FALSE
            } else {
                S_OK
            }
        }
        fn Skip(&self, c: u32) -> windows::core::Result<()> {
            self.idx.set(self.idx.get() + c as usize);
            Ok(())
        }
        fn Reset(&self) -> windows::core::Result<()> {
            self.idx.set(0);
            Ok(())
        }
        fn Clone(&self) -> windows::core::Result<IEnumFORMATETC> {
            Ok(IEnumFORMATETC::from(FormatEnum {
                items: self.items.clone(),
                idx: Cell::new(self.idx.get()),
            }))
        }
    }

    #[implement(IDataObject, IDataObjectAsyncCapability)]
    struct PromiseDataObject {
        formats: Formats,
        files: Vec<PromiseFile>,
        events: UnboundedSender<PasteEvent>,
        /// Reported via `IDataObjectAsyncCapability`. With it set, Explorer
        /// runs `GetData` inside its own async operation — a real progress
        /// dialog and a cancellable paste instead of a frozen busy cursor
        /// while a large file pulls.
        async_mode: Cell<bool>,
        in_operation: Cell<bool>,
    }

    impl PromiseDataObject {
        fn fmt(&self, cf: u32, lindex: i32, tymed: u32) -> FORMATETC {
            FORMATETC {
                cfFormat: cf as u16,
                ptd: std::ptr::null_mut(),
                dwAspect: DVASPECT_CONTENT.0,
                lindex,
                tymed,
            }
        }
    }

    impl IDataObject_Impl for PromiseDataObject_Impl {
        fn GetData(&self, f: *const FORMATETC) -> windows::core::Result<STGMEDIUM> {
            let f = unsafe { &*f };
            if f.cfFormat == self.formats.fgd_w as u16 {
                if f.tymed & TYMED_HGLOBAL.0 as u32 == 0 {
                    return Err(DV_E_TYMED.into());
                }
                return hglobal_with(&group_descriptor(&self.files));
            }
            if f.cfFormat == self.formats.pde as u16 {
                return hglobal_with(&(DROPEFFECT_COPY.0).to_le_bytes());
            }
            if f.cfFormat == self.formats.fc as u16 {
                let i = if f.lindex < 0 { 0 } else { f.lindex as usize };
                let Some(file) = self.files.get(i) else {
                    return Err(DV_E_LINDEX.into());
                };
                if f.tymed & TYMED_ISTREAM.0 as u32 == 0 {
                    return Err(DV_E_TYMED.into());
                }
                tracing::info!(
                    file_id = %file.file_id,
                    "paste requested; starting pull"
                );
                // The supervisor replies with a live SpoolStream immediately;
                // the pull streams into it while the target reads.
                let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
                if self
                    .events
                    .send(PasteEvent::Pull {
                        file_id: file.file_id,
                        reply: reply_tx,
                    })
                    .is_err()
                {
                    return Err(E_FAIL.into());
                }
                let spool = match reply_rx.recv_timeout(std::time::Duration::from_secs(10)) {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "paste pull could not start");
                        return Err(E_FAIL.into());
                    }
                    Err(_) => return Err(E_FAIL.into()),
                };
                let stream = IStream::from(SpoolIStream {
                    path: spool.path,
                    file: RefCell::new(None),
                    len: spool.len,
                    pos: Cell::new(0),
                    frontier: spool.frontier,
                });
                return Ok(STGMEDIUM {
                    tymed: TYMED_ISTREAM.0 as u32,
                    u: STGMEDIUM_0 {
                        pstm: std::mem::ManuallyDrop::new(Some(stream)),
                    },
                    pUnkForRelease: std::mem::ManuallyDrop::new(None),
                });
            }
            Err(DV_E_FORMATETC.into())
        }

        fn GetDataHere(
            &self,
            _f: *const FORMATETC,
            _m: *mut STGMEDIUM,
        ) -> windows::core::Result<()> {
            Err(E_NOTIMPL.into())
        }

        fn QueryGetData(&self, f: *const FORMATETC) -> HRESULT {
            let f = unsafe { &*f };
            if f.dwAspect & DVASPECT_CONTENT.0 == 0 {
                return DV_E_FORMATETC;
            }
            if f.cfFormat == self.formats.fgd_w as u16 && f.tymed & TYMED_HGLOBAL.0 as u32 != 0 {
                return S_OK;
            }
            if f.cfFormat == self.formats.fc as u16 && f.tymed & TYMED_ISTREAM.0 as u32 != 0 {
                let i = if f.lindex < 0 { 0 } else { f.lindex as usize };
                return if i < self.files.len() {
                    S_OK
                } else {
                    DV_E_LINDEX
                };
            }
            if f.cfFormat == self.formats.pde as u16 && f.tymed & TYMED_HGLOBAL.0 as u32 != 0 {
                return S_OK;
            }
            S_FALSE
        }

        fn GetCanonicalFormatEtc(&self, _f: *const FORMATETC, _o: *mut FORMATETC) -> HRESULT {
            E_NOTIMPL
        }

        fn SetData(
            &self,
            _f: *const FORMATETC,
            _m: *const STGMEDIUM,
            _r: windows::core::BOOL,
        ) -> windows::core::Result<()> {
            Err(E_NOTIMPL.into())
        }

        fn EnumFormatEtc(&self, dir: u32) -> windows::core::Result<IEnumFORMATETC> {
            if dir != DATADIR_GET.0 as u32 {
                return Err(E_NOTIMPL.into());
            }
            let items = vec![
                self.fmt(self.formats.fgd_w, -1, TYMED_HGLOBAL.0 as u32),
                self.fmt(self.formats.fc, -1, TYMED_ISTREAM.0 as u32),
                self.fmt(self.formats.pde, -1, TYMED_HGLOBAL.0 as u32),
            ];
            Ok(IEnumFORMATETC::from(FormatEnum {
                items,
                idx: Cell::new(0),
            }))
        }

        fn DAdvise(
            &self,
            _f: *const FORMATETC,
            _g: u32,
            _a: windows::core::Ref<'_, IAdviseSink>,
        ) -> windows::core::Result<u32> {
            Err(E_NOTIMPL.into())
        }
        fn DUnadvise(&self, _c: u32) -> windows::core::Result<()> {
            Ok(())
        }
        fn EnumDAdvise(&self) -> windows::core::Result<IEnumSTATDATA> {
            Err(E_NOTIMPL.into())
        }
    }

    pub fn spawn(events: UnboundedSender<PasteEvent>) -> super::PasteSource {
        let (cmd, rx) = std::sync::mpsc::channel::<PasteCommand>();
        std::thread::Builder::new()
            .name("clipboardwire-paste".into())
            .spawn(move || sta_loop(rx, events))
            .expect("spawn paste STA thread");
        super::PasteSource { cmd }
    }

    fn sta_loop(rx: std::sync::mpsc::Receiver<PasteCommand>, events: UnboundedSender<PasteEvent>) {
        unsafe {
            if OleInitialize(None).is_err() {
                tracing::error!("OleInitialize failed; paste mode disabled");
                return;
            }
        }
        let formats = register_formats();
        unsafe {
            let mut msg = MSG::default();
            loop {
                // OLE callbacks ride the thread's message queue.
                while PeekMessageW(&mut msg, None, 0, 0, PM_REMOVE).as_bool() {
                    let _ = TranslateMessage(&msg);
                    DispatchMessageW(&msg);
                }
                match rx.try_recv() {
                    Ok(PasteCommand::Arm(files)) => {
                        // OleSetClipboard AddRefs the object, so the handle may
                        // be dropped here — the promise lives until replaced.
                        if files.is_empty() {
                            let _ = OleSetClipboard(None);
                        } else {
                            let obj = IDataObject::from(PromiseDataObject {
                                formats: Formats {
                                    fgd_w: formats.fgd_w,
                                    fc: formats.fc,
                                    pde: formats.pde,
                                },
                                files,
                                events: events.clone(),
                                async_mode: Cell::new(true),
                                in_operation: Cell::new(false),
                            });
                            if OleSetClipboard(&obj).is_ok() {
                                tracing::debug!("clipboard promise armed");
                            }
                        }
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        std::thread::sleep(std::time::Duration::from_millis(4));
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        let _ = OleSetClipboard(None);
                        return;
                    }
                }
            }
        }
    }
}

/// macOS backend: armed offers become 0-byte placeholders under
/// `<spool>/armed/<file_id>/<name>`, each presented to the file-
/// coordination daemon. A Cmd+V in Finder coordinates a read of the
/// placeholder, which fires `relinquishPresentedItemToReader:` on the
/// presenter's own `NSOperationQueue`; that callback runs the normal lazy
/// pull, copies the verified bytes into the placeholder (or unlinks it,
/// surfacing a -43 copy error), and only then answers the relinquish.
///
/// No dedicated thread exists here: the presenter queues are the
/// callbacks' threads, and `arm` mutates the registry inline from the
/// supervisor. Blocking a presenter queue for the whole pull is the
/// relinquish contract — the reader waits for our reply either way.
/// Shape and the nested `reader(reacquirer)` reply proven by
/// `tools/spike-macos-paste` (see `openspec/changes/macos-paste-presenter/`).
#[cfg(target_os = "macos")]
mod imp {
    use super::{PasteEvent, PromiseFile};
    use crate::client::files_clipboard;
    use std::path::PathBuf;
    use tokio::sync::mpsc::UnboundedSender;
    use tracing::{debug, info, warn};
    use uuid::Uuid;

    use block2::RcBlock;
    use objc2::rc::{Allocated, Retained};
    use objc2::{class, define_class, msg_send, ClassType, DefinedClass};
    use objc2_app_kit::NSPasteboard;
    use objc2_foundation::{NSError, NSObject, NSOperationQueue, NSString, NSURL};

    /// The nested reply signature `relinquish…To(Reader|Writer):` takes:
    /// a block we must call *with a reacquirer block of our own*. Calling
    /// it argument-less corrupts Foundation's bookkeeping — the spike hit
    /// `didRelinquish called, but there are no replies` after a copy that
    /// had already succeeded.
    type ReplyBlock = block2::DynBlock<dyn Fn(*mut block2::DynBlock<dyn Fn()>)>;

    struct Ivars {
        url: Retained<NSURL>,
        queue: Retained<NSOperationQueue>,
        /// The placeholder on disk — what `reader` must fill (or unlink).
        path: PathBuf,
        file_id: Uuid,
        events: UnboundedSender<PasteEvent>,
        /// Set for the duration of a render, so `disarm` can warn when a
        /// re-arm kills a paste still waiting on its pull.
        pending: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    define_class!(
        #[unsafe(super(NSObject))]
        #[name = "CWPastePresenter"]
        #[ivars = Ivars]
        struct Presenter;

        impl Presenter {
            #[unsafe(method(presentedItemURL))]
            fn presented_item_url(&self) -> *mut NSURL {
                Retained::autorelease_return(self.ivars().url.clone())
            }

            #[unsafe(method(presentedItemOperationQueue))]
            fn presented_item_operation_queue(&self) -> *mut NSOperationQueue {
                Retained::autorelease_return(self.ivars().queue.clone())
            }

            /// A coordinated reader (Finder's Cmd+V copy) wants the
            /// placeholder's bytes. This callback IS the relinquish: it
            /// stays on the presenter queue for the whole pull and replies
            /// only once the placeholder holds verified bytes — or is
            /// gone, which is how a failed pull becomes error -43.
            #[unsafe(method(relinquishPresentedItemToReader:))]
            unsafe fn relinquish_to_reader(&self, reader: &ReplyBlock) {
                let file_id = self.ivars().file_id;
                info!(file_id = %file_id, "paste requested; starting pull");
                self.ivars()
                    .pending
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
                let result = match self
                    .ivars()
                    .events
                    .send(PasteEvent::Pull {
                        file_id,
                        reply: reply_tx,
                    }) {
                    Ok(()) => match reply_rx.recv() {
                        Ok(r) => r,
                        Err(_) => Err("supervisor dropped the pull reply".to_string()),
                    },
                    Err(_) => Err("paste event channel closed".to_string()),
                };
                super::render_placeholder(result, &self.ivars().path);
                // The reply takes OUR reacquirer block; Foundation invokes
                // it when the read completes. Copying the incoming block to
                // the heap keeps the spike's proven shape even though we
                // answer inside the callback.
                let reader = reader.copy();
                let reacquirer = RcBlock::new(|| {
                    debug!("reacquired after read");
                });
                let reacquirer_ptr = &*reacquirer
                    as *const block2::DynBlock<dyn Fn()>
                    as *mut block2::DynBlock<dyn Fn()>;
                reader.call((reacquirer_ptr,));
                self.ivars()
                    .pending
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                // `reader.call` hands the reacquirer to Foundation, which
                // retains it for the rest of the coordinated read — the
                // documented caller passes a stack-block literal here,
                // which could not outlive this callback otherwise — so
                // dropping our RcBlock on return is safe.
            }

            /// Nothing but us writes the placeholder — allow immediately.
            #[unsafe(method(relinquishPresentedItemToWriter:))]
            unsafe fn relinquish_to_writer(&self, writer: &ReplyBlock) {
                debug!(path = %self.ivars().path.display(), "relinquish to writer");
                let reacquirer = RcBlock::new(|| {
                    debug!("reacquired after write");
                });
                let reacquirer_ptr = &*reacquirer
                    as *const block2::DynBlock<dyn Fn()>
                    as *mut block2::DynBlock<dyn Fn()>;
                writer.call((reacquirer_ptr,));
                // Same retention contract as the reader reply above:
                // Foundation owns the reacquirer once `call` returns.
            }

            #[unsafe(method(accommodatePresentedItemDeletionWithCompletionHandler:))]
            unsafe fn accommodate_deletion(
                &self,
                completion: &block2::DynBlock<dyn Fn(*mut NSError)>,
            ) {
                debug!(path = %self.ivars().path.display(), "presented item deletion");
                let _ = std::fs::remove_file(&self.ivars().path);
                completion.call((std::ptr::null_mut(),));
            }

            #[unsafe(method(presentedItemDidMoveToURL:))]
            unsafe fn did_move(&self, new_url: &NSURL) {
                debug!(
                    path = %self.ivars().path.display(),
                    ?new_url,
                    "presented item moved"
                );
            }

            #[unsafe(method(presentedItemDidChange))]
            fn did_change(&self) {
                // Fires after Finder's copy touches the file; not a signal
                // to act on.
                debug!(path = %self.ivars().path.display(), "presented item changed");
            }
        }
    );

    impl Presenter {
        /// The presenter plus the flag it sets while a render is in
        /// flight — the registry keeps the flag so `disarm` can warn
        /// about kills of in-progress pastes.
        fn new(
            path: PathBuf,
            file_id: Uuid,
            events: UnboundedSender<PasteEvent>,
        ) -> (
            Retained<Self>,
            std::sync::Arc<std::sync::atomic::AtomicBool>,
        ) {
            let url = NSURL::fileURLWithPath(&NSString::from_str(&path.to_string_lossy()));
            let queue = NSOperationQueue::new();
            queue.setName(Some(&NSString::from_str("clipboardwire.paste.presenter")));
            // One callback at a time per file: the relinquish blocks for
            // the whole pull, and nothing else may interleave on it.
            queue.setMaxConcurrentOperationCount(1);
            let pending = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let this: Allocated<Self> = unsafe { msg_send![Self::class(), alloc] };
            let this = this.set_ivars(Ivars {
                url,
                queue,
                path,
                file_id,
                events,
                pending: pending.clone(),
            });
            (unsafe { msg_send![super(this), init] }, pending)
        }
    }

    // Callbacks run on the presenter's own NSOperationQueue — GCD worker
    // threads — and the `Retained<Presenter>` lives in the supervisor's
    // `Mutex`, so the class must be Send+Sync. Sound because the ivars
    // are written once at init and only ever read afterwards.
    unsafe impl Send for Presenter {}
    unsafe impl Sync for Presenter {}

    /// The current arm: one registered presenter per armed file, replaced
    /// wholesale on the next `arm` — a new arm means a new clipboard
    /// anyway.
    pub struct MacPaste {
        events: UnboundedSender<PasteEvent>,
        presenters: Vec<Retained<Presenter>>,
        /// Each presenter's in-flight flag, parallel to `presenters`, so
        /// `disarm` can log when a re-arm kills a still-running render.
        pending: Vec<std::sync::Arc<std::sync::atomic::AtomicBool>>,
        /// Placeholder paths of the current arm, kept so `disarm` can tell
        /// whether the pasteboard still holds *our* URLs.
        armed_paths: Vec<PathBuf>,
    }

    impl MacPaste {
        /// Replace the armed set: drop the old presenters and stale
        /// `armed/` tree, create one 0-byte placeholder per file, register
        /// a presenter for each, and put their `file://` URLs on the
        /// general pasteboard.
        pub fn arm(&mut self, files: Vec<PromiseFile>) {
            self.disarm();
            let mut armed = Vec::with_capacity(files.len());
            for f in &files {
                let path = super::placeholder_path(f.file_id, &f.name);
                let Some(dir) = path.parent() else {
                    continue;
                };
                let made = std::fs::create_dir_all(dir)
                    .and_then(|()| std::fs::File::create(&path).map(|_| ()));
                if let Err(e) = made {
                    warn!(
                        file_id = %f.file_id,
                        error = %e,
                        "could not create a paste placeholder"
                    );
                    continue;
                }
                let (presenter, pending) =
                    Presenter::new(path.clone(), f.file_id, self.events.clone());
                // msg_send! — avoids needing ProtocolObject conformance;
                // file coordination only checks respondsToSelector:.
                unsafe {
                    let _: () = msg_send![class!(NSFileCoordinator), addFilePresenter: &*presenter];
                }
                self.presenters.push(presenter);
                self.pending.push(pending);
                armed.push(path);
            }
            self.armed_paths = armed;
            if self.armed_paths.is_empty() {
                return;
            }
            // The same write the inbound-apply path performs
            // (`files_clipboard::write_files` = clearContents +
            // writeObjects of the file:// URLs). Our own poller never
            // re-publishes these paths — `is_armed_placeholder` filters
            // them out of every files read.
            match files_clipboard::write_files(&self.armed_paths) {
                Ok(()) => debug!(count = self.armed_paths.len(), "clipboard promise armed"),
                Err(e) => warn!(
                    error = %format!("{e:#}"),
                    "could not put placeholder URLs on the pasteboard"
                ),
            }
        }

        /// Unregister every presenter and delete `armed/`. The pasteboard
        /// is cleared only while it still holds exactly our armed URLs —
        /// a user copy that landed after the arm keeps the clipboard.
        fn disarm(&mut self) {
            // A re-arm while a render is still pulling kills that paste
            // (its placeholder is deleted below → -43). Correct — the new
            // copy replaces the clipboard — but worth a trace.
            let in_flight = self
                .pending
                .iter()
                .filter(|p| p.load(std::sync::atomic::Ordering::Relaxed))
                .count();
            if in_flight > 0 {
                warn!(
                    count = in_flight,
                    "re-arm killed an in-flight paste render; that paste will fail"
                );
            }
            self.pending.clear();
            for presenter in self.presenters.drain(..) {
                unsafe {
                    let _: () =
                        msg_send![class!(NSFileCoordinator), removeFilePresenter: &*presenter];
                }
            }
            let root = super::armed_dir();
            match std::fs::remove_dir_all(&root) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => warn!(
                    error = %e,
                    dir = %root.display(),
                    "could not remove the armed placeholder dir"
                ),
            }
            let armed = std::mem::take(&mut self.armed_paths);
            if armed.is_empty() {
                return;
            }
            if let Ok(Some(current)) = files_clipboard::read_files() {
                if current == armed {
                    NSPasteboard::generalPasteboard().clearContents();
                }
            }
        }
    }

    impl Drop for MacPaste {
        fn drop(&mut self) {
            self.disarm();
        }
    }

    pub fn spawn(events: UnboundedSender<PasteEvent>) -> super::PasteSource {
        super::PasteSource {
            imp: std::sync::Mutex::new(MacPaste {
                events,
                presenters: Vec::new(),
                pending: Vec::new(),
                armed_paths: Vec::new(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique scratch dir under the OS temp dir — the suite cannot use a
    /// shared tempdir crate, so uniqueness is pid + counter.
    fn scratch(label: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("cw-paste-test-{label}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn frontier_wait_returns_ok_for_a_completed_spool() {
        assert_eq!(SpoolFrontier::completed().wait(), Ok(()));
    }

    #[test]
    fn frontier_wait_blocks_until_the_verdict_lands() {
        let frontier = SpoolFrontier::new();
        let writer = frontier.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            writer.finish(Ok(()));
        });
        assert_eq!(frontier.wait(), Ok(()));
        t.join().unwrap();
    }

    #[test]
    fn frontier_wait_propagates_the_failure_reason() {
        let frontier = SpoolFrontier::new();
        frontier.finish(Err("offer was withdrawn".to_string()));
        assert_eq!(frontier.wait(), Err("offer was withdrawn".to_string()));
    }

    #[test]
    fn placeholder_path_lives_under_armed_and_sanitizes_the_name() {
        let id = uuid::Uuid::new_v4();
        let path = placeholder_path(id, "../evil name.txt");
        assert!(is_armed_placeholder(&path));
        assert_eq!(path.parent().unwrap(), armed_dir().join(id.to_string()));
        // Same rule the landing path uses: separators and controls become
        // underscores, never a traversal.
        assert_eq!(path.file_name().unwrap(), ".._evil name.txt");
    }

    #[test]
    fn same_named_offers_never_collide() {
        let a = placeholder_path(uuid::Uuid::new_v4(), "report.pdf");
        let b = placeholder_path(uuid::Uuid::new_v4(), "report.pdf");
        assert_ne!(a, b);
    }

    #[test]
    fn the_pull_partial_is_not_an_armed_placeholder() {
        // `<file_id>.partial` sits at the spool root — outside `armed/` —
        // so the echo filter must not swallow it either.
        let partial = spool_dir().join(format!("{}.partial", uuid::Uuid::new_v4()));
        assert!(!is_armed_placeholder(&partial));
        assert!(is_armed_placeholder(&placeholder_path(
            uuid::Uuid::new_v4(),
            "x"
        )));
    }

    #[test]
    fn render_placeholder_copies_verified_bytes_in() {
        let dir = scratch("fill");
        let partial = dir.join("partial");
        std::fs::write(&partial, b"verified bytes").unwrap();
        let placeholder = dir.join("armed").join("file.txt");
        std::fs::create_dir_all(placeholder.parent().unwrap()).unwrap();
        std::fs::write(&placeholder, b"").unwrap();

        render_placeholder(
            Ok(SpoolStream {
                path: partial.clone(),
                len: 14,
                frontier: SpoolFrontier::completed(),
            }),
            &placeholder,
        );

        // Copy, not rename: the verified partial stays for repeat pastes.
        assert_eq!(std::fs::read(&placeholder).unwrap(), b"verified bytes");
        assert_eq!(std::fs::read(&partial).unwrap(), b"verified bytes");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn render_placeholder_waits_for_the_frontier() {
        let dir = scratch("wait");
        let partial = dir.join("partial");
        std::fs::write(&partial, b"late bytes").unwrap();
        let placeholder = dir.join("armed").join("file.txt");
        std::fs::create_dir_all(placeholder.parent().unwrap()).unwrap();
        std::fs::write(&placeholder, b"").unwrap();
        let frontier = SpoolFrontier::new();
        let writer = frontier.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            writer.finish(Ok(()));
        });

        render_placeholder(
            Ok(SpoolStream {
                path: partial,
                len: 10,
                frontier,
            }),
            &placeholder,
        );

        assert_eq!(std::fs::read(&placeholder).unwrap(), b"late bytes");
        t.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn render_placeholder_unlinks_when_the_pull_fails() {
        let dir = scratch("fail");
        let placeholder = dir.join("armed").join("file.txt");
        std::fs::create_dir_all(placeholder.parent().unwrap()).unwrap();
        std::fs::write(&placeholder, b"").unwrap();

        render_placeholder(Err("offer was withdrawn".to_string()), &placeholder);

        // The reader's coordinated read now hits ENOENT — the -43 surface.
        assert!(!placeholder.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn render_placeholder_unlinks_when_the_frontier_fails() {
        let dir = scratch("frontierfail");
        let placeholder = dir.join("armed").join("file.txt");
        std::fs::create_dir_all(placeholder.parent().unwrap()).unwrap();
        std::fs::write(&placeholder, b"").unwrap();
        let frontier = SpoolFrontier::new();
        frontier.finish(Err("digest mismatch".to_string()));

        render_placeholder(
            Ok(SpoolStream {
                path: dir.join("partial"),
                len: 0,
                frontier,
            }),
            &placeholder,
        );

        assert!(!placeholder.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn render_placeholder_unlinks_when_the_copy_fails() {
        // A missing spool path must not leave a 0-byte placeholder for the
        // reader to copy — unlink it so the paste reports an error.
        let dir = scratch("copyfail");
        let placeholder = dir.join("armed").join("file.txt");
        std::fs::create_dir_all(placeholder.parent().unwrap()).unwrap();
        std::fs::write(&placeholder, b"").unwrap();

        render_placeholder(
            Ok(SpoolStream {
                path: dir.join("does-not-exist"),
                len: 5,
                frontier: SpoolFrontier::completed(),
            }),
            &placeholder,
        );

        assert!(!placeholder.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn render_placeholder_gives_up_and_unlinks_on_timeout() {
        // A pull that never resolves — e.g. the transport went down and
        // the stall checks are paused — must still release the reader:
        // give up after the deadline and unlink so Finder surfaces -43.
        let dir = scratch("timeout");
        let placeholder = dir.join("armed").join("file.txt");
        std::fs::create_dir_all(placeholder.parent().unwrap()).unwrap();
        std::fs::write(&placeholder, b"").unwrap();
        let started = std::time::Instant::now();

        render_placeholder_within(
            Ok(SpoolStream {
                path: dir.join("partial"),
                len: 0,
                frontier: SpoolFrontier::new(), // verdict never lands
            }),
            &placeholder,
            std::time::Duration::from_millis(50),
        );

        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
        assert!(!placeholder.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn placeholder_filter_recognizes_the_canonical_tmp_spelling() {
        // Whatever spelling `temp_dir()` uses, the same armed root under
        // TMPDIR's canonical spelling must also be recognized — on macOS
        // that is `/private/var/folders/...` while `temp_dir()` reports
        // `/var/folders/...`, the exact pair a pasteboard reader can
        // hand back.
        let canon = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join("clipboardwire-paste")
            .join("armed")
            .join(uuid::Uuid::new_v4().to_string())
            .join("x.txt");
        assert!(is_armed_placeholder(&canon));
    }
}

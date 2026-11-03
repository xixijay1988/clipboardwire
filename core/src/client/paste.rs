//! Delayed-render clipboard promise behind `receive_mode = "paste"` (Windows).
//!
//! Instead of pulling an offer the moment it arrives and dropping the file in
//! the user's Downloads, this module owns the OLE clipboard with an
//! `IDataObject` that advertises `FileGroupDescriptorW` + `FileContents`. The
//! transfer only starts when a paste target (Explorer, an Office app, a file
//! dialog) asks for `FileContents` — at that point [`PasteEvent::Pull`] goes
//! back to the supervisor, which runs the normal lazy pull into a temp spool,
//! verifies the digest, and hands the finished path back. Only then does an
//! `IStream` over the verified file get returned, so a corrupt pull can never
//! reach the paste destination.
//!
//! Threading: `OleSetClipboard` callbacks are marshalled onto the thread that
//! owns the object, so a dedicated STA thread with a message pump hosts the
//! promise. `GetData` may block for the duration of a whole pull — keeping it
//! off the tao tray loop is the point of this thread. Commands arrive on a
//! channel; the pump polls it between messages.
//!
//! The non-Windows build keeps the same API as a no-op so the supervisor stays
//! platform-free: `Paste` never resolves there (`receive_mode="paste"` falls
//! back to `pickup` on macOS and is rejected on Linux).

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
    /// A paste target asked for `file_id`'s bytes. The supervisor must reply
    /// with the spooled-and-verified path, or an error to fail the paste.
    Pull {
        file_id: uuid::Uuid,
        reply: std::sync::mpsc::SyncSender<Result<PathBuf, String>>,
    },
}

/// Handle the supervisor uses to (re)arm the clipboard promise.
pub struct PasteSource {
    cmd: std::sync::mpsc::Sender<PasteCommand>,
}

enum PasteCommand {
    // `Vec` is unused on non-Windows stubs; kept so `arm()` has one signature.
    #[allow(dead_code)]
    Arm(Vec<PromiseFile>),
}

impl PasteSource {
    /// Replace the clipboard promise with one describing `files`. An empty
    /// vec just releases the clipboard.
    pub fn arm(&self, files: Vec<PromiseFile>) {
        let _ = self.cmd.send(PasteCommand::Arm(files));
    }
}

/// Spawn the promise host. `events` receives paste-time pull requests.
#[cfg(windows)]
pub fn spawn(events: UnboundedSender<PasteEvent>) -> PasteSource {
    imp::spawn(events)
}

/// Non-Windows stub: the promise can never fire, so the channel ends.
#[cfg(not(windows))]
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

#[cfg(windows)]
mod imp {
    use super::{PasteCommand, PasteEvent, PromiseFile};
    use std::cell::Cell;
    use tokio::sync::mpsc::UnboundedSender;
    use windows::core::{implement, HRESULT, PCWSTR};
    use windows::Win32::Foundation::{FILETIME, POINTL, SIZE};
    use windows::Win32::System::Com::*;
    use windows::Win32::System::Com::IBindCtx;
    use windows::Win32::System::DataExchange::RegisterClipboardFormatW;
    use windows::Win32::System::Memory::{GlobalAlloc, GlobalLock, GlobalUnlock, GMEM_MOVEABLE};
    use windows::Win32::System::Ole::{OleInitialize, OleSetClipboard, DROPEFFECT_COPY};
    use windows::Win32::UI::Shell::{
        FILEDESCRIPTORW, FILEGROUPDESCRIPTORW, IDataObjectAsyncCapability,
        IDataObjectAsyncCapability_Impl,
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

    /// IStream over an on-disk file — the verified spool, seek-read per call.
    /// `seek_read` keeps `&self` so no interior mutability is needed for the
    /// file handle; only the position is a Cell.
    #[implement(IStream)]
    struct FileStream {
        file: std::fs::File,
        len: u64,
        pos: Cell<u64>,
    }

    impl ISequentialStream_Impl for FileStream_Impl {
        fn Read(&self, pv: *mut core::ffi::c_void, cb: u32, pcbread: *mut u32) -> HRESULT {
            if pv.is_null() {
                return E_FAIL;
            }
            let want = (cb as usize).min((self.len - self.pos.get()) as usize);
            if want == 0 {
                if !pcbread.is_null() {
                    unsafe { *pcbread = 0 };
                }
                return S_FALSE;
            }
            let buf = unsafe { std::slice::from_raw_parts_mut(pv as *mut u8, want) };
            match std::os::windows::fs::FileExt::seek_read(&self.file, buf, self.pos.get()) {
                Ok(n) => {
                    self.pos.set(self.pos.get() + n as u64);
                    if !pcbread.is_null() {
                        unsafe { *pcbread = n as u32 };
                    }
                    if n < want {
                        S_FALSE
                    } else {
                        S_OK
                    }
                }
                Err(_) => E_FAIL,
            }
        }
        fn Write(&self, _pv: *const core::ffi::c_void, _cb: u32, _w: *mut u32) -> HRESULT {
            E_FAIL
        }
    }

    impl IStream_Impl for FileStream_Impl {
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
                // Block this STA thread until the supervisor finishes the
                // pull: the spool path arrives verified or not at all.
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
                let path = match reply_rx.recv() {
                    Ok(Ok(p)) => p,
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "paste pull failed");
                        return Err(E_FAIL.into());
                    }
                    Err(_) => return Err(E_FAIL.into()),
                };
                let file = std::fs::File::open(&path).map_err(|_| E_FAIL)?;
                let len = file.metadata().map_err(|_| E_FAIL)?.len();
                let stream = IStream::from(FileStream {
                    file,
                    len,
                    pos: Cell::new(0),
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

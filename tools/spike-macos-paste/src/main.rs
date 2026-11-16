// SPDX-License-Identifier: GPL-3.0-or-later
//
// Spike: can an NSFilePresenter on an empty placeholder file make Finder's
// Cmd+V wait while "the pull" happens?
//
// Modes:
//   spike-macos-paste arm --delay <secs> [--files N] [--fail fill|empty|unlink] [--dir <path>]
//     Create N zero-byte placeholders, register one NSFilePresenter per file,
//     put their file:// URLs on the general pasteboard, then sit and log every
//     presenter callback.
//
//   spike-macos-paste reader <path>
//     Perform an NSFileCoordinator coordinated READ on the path. This is the
//     deterministic, GUI-free way to prove the presenter machinery works:
//     run `arm` in one terminal and `reader` in another.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use block2::{Block, RcBlock};
use objc2::rc::{Allocated, Retained};
use objc2::{class, define_class, msg_send, ClassType, DefinedClass};
use objc2_app_kit::NSPasteboard;
use objc2_foundation::{NSArray, NSError, NSOperationQueue, NSObject, NSString, NSURL};

static T0: OnceLock<Instant> = OnceLock::new();

macro_rules! log {
    ($($arg:tt)*) => {
        eprintln!("[{:>8.3}s] {}", T0.get().unwrap().elapsed().as_secs_f64(), format!($($arg)*))
    };
}

unsafe extern "C" {
    /// Run the main run loop forever. Kept as a raw FFI decl so the spike
    /// does not need objc2-core-foundation.
    fn CFRunLoopRun();
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FailMode {
    /// Write the payload, then release the reader. Normal success path.
    Fill,
    /// Release the reader with the file still at 0 bytes — what a failed
    /// pull looks like today.
    Empty,
    /// Delete the placeholder BEFORE releasing the reader, so Finder's
    /// coordinated read hits ENOENT. If this surfaces a Finder error it is
    /// a strictly better failure path than copying an empty file.
    Unlink,
}

#[derive(Clone, Copy)]
struct Cfg {
    delay: Duration,
    files: usize,
    fail: FailMode,
}

struct Ivars {
    url: Retained<NSURL>,
    queue: Retained<NSOperationQueue>,
    path: PathBuf,
    cfg: Cfg,
}

/// Wrap the heap-copied reader block so it can cross to a std::thread.
/// NSFileCoordinator hands us a block that is only valid for the duration
/// of the call; copying moves it to the heap.
/// The relinquish reply block: called with OUR reacquirer block as its
/// argument (a raw `DynBlock` pointer, per the generated protocol
/// signature). Wrapping so it can cross to the pull thread — field access
/// would capture the non-Send field, so go through `invoke_with`.
type ReplyBlock = RcBlock<dyn Fn(*mut block2::DynBlock<dyn Fn()>)>;
struct SendBlock(ReplyBlock);
unsafe impl Send for SendBlock {}

impl SendBlock {
    fn invoke_with(&self, reacquirer: *mut block2::DynBlock<dyn Fn()>) {
        self.0.call((reacquirer,));
    }
}

define_class!(
    #[unsafe(super(NSObject))]
    #[name = "CWSpikePresenter"]
    #[ivars = Ivars]
    struct SpikePresenter;

    impl SpikePresenter {
        #[unsafe(method(presentedItemURL))]
        fn presented_item_url(&self) -> *mut NSURL {
            Retained::autorelease_return(self.ivars().url.clone())
        }

        #[unsafe(method(presentedItemOperationQueue))]
        fn presented_item_operation_queue(&self) -> *mut NSOperationQueue {
            Retained::autorelease_return(self.ivars().queue.clone())
        }

        // ---- reader coordination: the whole point of the spike ----

        #[unsafe(method(relinquishPresentedItemToReader:))]
        unsafe fn relinquish_to_reader(
            &self,
            reader: &block2::DynBlock<dyn Fn(*mut block2::DynBlock<dyn Fn()>)>,
        ) {
            log!(
                "relinquishPresentedItemToReader <- {}",
                self.ivars().path.display()
            );
            let path = self.ivars().path.clone();
            let cfg = self.ivars().cfg;
            let reader = SendBlock(reader.copy());
            std::thread::spawn(move || {
                // Call through the wrapper so the closure captures the whole
                // SendBlock — field access would capture the non-Send field.
                log!("  simulated pull started, sleeping {:?}", cfg.delay);
                std::thread::sleep(cfg.delay);
                match cfg.fail {
                    FailMode::Fill => {
                        let body = format!(
                            "SPIKE-OK path={} delay={:?}\n",
                            path.display(),
                            cfg.delay
                        );
                        std::fs::write(&path, body).expect("fill placeholder");
                        log!("  wrote payload into {}", path.display());
                    }
                    FailMode::Empty => {
                        log!("  leaving {} at 0 bytes", path.display());
                    }
                    FailMode::Unlink => {
                        std::fs::remove_file(&path).expect("unlink placeholder");
                        log!("  unlinked {}", path.display());
                    }
                }
                log!("  releasing reader");
                // `reader` takes a reacquirer block argument — Foundation
                // invokes it when the reader finishes. This nested signature
                // is what makes an asynchronous reply legal; calling the
                // block with no argument corrupts the bookkeeping.
                let reacquirer = RcBlock::new(|| {
                    log!("  reacquired after read");
                });
                let reacquirer_ptr = &*reacquirer
                    as *const block2::DynBlock<dyn Fn()>
                    as *mut block2::DynBlock<dyn Fn()>;
                reader.invoke_with(reacquirer_ptr);
                // Foundation retains the reacquirer until the read ends, but
                // the spike doesn't depend on that: forget it so it cannot
                // dangle even if nothing copies it.
                std::mem::forget(reacquirer);
            });
        }

        // ---- writer coordination: allow immediately, we are the writer ----

        #[unsafe(method(relinquishPresentedItemToWriter:))]
        unsafe fn relinquish_to_writer(
            &self,
            writer: &block2::DynBlock<dyn Fn(*mut block2::DynBlock<dyn Fn()>)>,
        ) {
            log!(
                "relinquishPresentedItemToWriter <- {}",
                self.ivars().path.display()
            );
            let reacquirer = RcBlock::new(|| {
                log!("  reacquired after write");
            });
            let reacquirer_ptr = &*reacquirer
                as *const block2::DynBlock<dyn Fn()>
                as *mut block2::DynBlock<dyn Fn()>;
            writer.call((reacquirer_ptr,));
            std::mem::forget(reacquirer);
        }

        // ---- bookkeeping callbacks, implemented so none are a surprise ----

        #[unsafe(method(accommodatePresentedItemDeletionWithCompletionHandler:))]
        unsafe fn accommodate_deletion(
            &self,
            completion: &Block<dyn Fn(*mut NSError)>,
        ) {
            log!(
                "accommodatePresentedItemDeletion <- {}",
                self.ivars().path.display()
            );
            let _ = std::fs::remove_file(&self.ivars().path);
            completion.call((std::ptr::null_mut(),));
        }

        #[unsafe(method(presentedItemDidMoveToURL:))]
        unsafe fn did_move(&self, new_url: &NSURL) {
            log!(
                "presentedItemDidMoveToURL {} -> {:?}",
                self.ivars().path.display(),
                new_url
            );
        }

        #[unsafe(method(presentedItemDidChange))]
        fn did_change(&self) {
            log!("presentedItemDidChange <- {}", self.ivars().path.display());
        }
    }
);

impl SpikePresenter {
    fn new(path: PathBuf, cfg: Cfg) -> Retained<Self> {
        let url = NSURL::fileURLWithPath(&NSString::from_str(&path.to_string_lossy()));
        let queue = NSOperationQueue::new();
        queue.setName(Some(&NSString::from_str("clipboardwire.spike.presenter")));
        let this: Allocated<Self> = unsafe { msg_send![Self::class(), alloc] };
        let this = this.set_ivars(Ivars {
            url,
            queue,
            path,
            cfg,
        });
        unsafe { msg_send![super(this), init] }
    }
}

fn arm(cfg: Cfg) {
    let spool = std::env::temp_dir().join("cw-paste-spike");
    std::fs::create_dir_all(&spool).expect("create spool dir");

    let mut presenters = Vec::new();
    let mut urls: Vec<Retained<NSURL>> = Vec::new();
    for i in 0..cfg.files {
        let path = spool.join(format!("spike-file-{}.txt", i + 1));
        std::fs::write(&path, b"").expect("create placeholder");
        let presenter = SpikePresenter::new(path.clone(), cfg);
        // Runtime registration. msg_send avoids needing ProtocolObject
        // conformance — file coordination only checks respondsToSelector.
        unsafe {
            let _: () = msg_send![class!(NSFileCoordinator), addFilePresenter: &*presenter];
        }
        log!("presenter registered for {}", path.display());
        urls.push(NSURL::fileURLWithPath(&NSString::from_str(
            &path.to_string_lossy(),
        )));
        presenters.push(presenter);
    }

    // Put the file URLs on the general pasteboard — same write the client's
    // files_clipboard backend does.
    unsafe {
        let pasteboard = NSPasteboard::generalPasteboard();
        pasteboard.clearContents();
        let url_refs: Vec<&NSURL> = urls.iter().map(|u| &**u).collect();
        let array: Retained<NSArray<NSURL>> = NSArray::from_slice(&url_refs);
        let writable: &NSArray<
            objc2::runtime::ProtocolObject<dyn objc2_app_kit::NSPasteboardWriting>,
        > = std::mem::transmute(&*array);
        if !pasteboard.writeObjects(writable) {
            panic!("NSPasteboard.writeObjects returned NO");
        }
    }
    log!(
        "armed: {} placeholder(s) on the general pasteboard — Cmd+V in Finder now",
        cfg.files
    );

    // Keep the presenters alive and the process resident. Presenter
    // callbacks arrive on each presenter's NSOperationQueue, but running
    // the main run loop is cheap insurance for delivery.
    let _ = presenters;
    unsafe { CFRunLoopRun() };
}

/// GUI-free proof: be the coordinated reader ourselves, in a second process.
fn reader(path: &Path) {
    let url = NSURL::fileURLWithPath(&NSString::from_str(&path.to_string_lossy()));
    // NSFileCoordinator with no presenter of its own (`new` == init with a
    // nil file presenter).
    let coord: Retained<NSObject> = unsafe { msg_send![class!(NSFileCoordinator), new] };
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done2 = done.clone();
    let accessor = RcBlock::new(move |new_url: std::ptr::NonNull<NSURL>| {
        let ns_url = unsafe { new_url.as_ref() };
        let p = ns_url.path().expect("accessor URL has no path").to_string();
        match std::fs::read(&p) {
            Ok(bytes) => log!("accessor read {} bytes from {}", bytes.len(), p),
            Err(e) => log!("accessor read FAILED: {e}"),
        }
        done2.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    let mut err: *mut NSError = std::ptr::null_mut();
    log!("coordinating read on {}", path.display());
    unsafe {
        let _: () = msg_send![
            &*coord,
            coordinateReadingItemAtURL: &*url,
            options: 0usize,
            error: &mut err,
            byAccessor: &*accessor
        ];
    }
    if err.is_null() {
        log!("coordinated read returned without error");
    } else {
        let desc = unsafe { (*err).localizedDescription() };
        log!("coordinated read returned NSError: {desc}");
    }
}

fn main() {
    T0.set(Instant::now()).unwrap();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("");
    match mode {
        "arm" => {
            let mut delay = Duration::from_secs(5);
            let mut files = 1;
            let mut fail = FailMode::Fill;
            let mut it = args.iter().skip(1);
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--delay" => {
                        delay = Duration::from_secs(
                            it.next().expect("--delay needs a value").parse().unwrap(),
                        )
                    }
                    "--files" => {
                        files = it.next().expect("--files needs a value").parse().unwrap()
                    }
                    "--fail" => {
                        fail = match it.next().expect("--fail needs a value").as_str() {
                            "fill" => FailMode::Fill,
                            "empty" => FailMode::Empty,
                            "unlink" => FailMode::Unlink,
                            other => panic!("unknown --fail {other}"),
                        }
                    }
                    other => panic!("unknown arg {other}"),
                }
            }
            arm(Cfg { delay, files, fail });
        }
        "reader" => {
            let path = args.get(1).expect("reader needs a path");
            reader(Path::new(path));
        }
        _ => {
            eprintln!(
                "usage:\n  spike arm [--delay S] [--files N] [--fail fill|empty|unlink]\n  spike reader <path>"
            );
            std::process::exit(2);
        }
    }
}

//! Process-shared blocking event used by production process transport.
//!
//! The shared sequence is the protocol; the OS primitive is only a parking
//! mechanism. A publisher stores the new monotonic cursor, atomically claims a
//! registered waiter, and only then enters the kernel. A waiter registers with
//! an exchange, rechecks the sequence, then parks. The shared modification
//! order of those exchanges proves that either the waiter observes the cursor
//! or the publisher observes the waiter; there is no lost-wakeup gap.

use std::cell::UnsafeCell;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

const EVENT_MAGIC: u32 = 0x5053_4556; // "PSEV"
const EVENT_NAME_LEN: usize = 96;

/// Bytes embedded in a process-shared mapping.
///
/// The fixed 128-byte representation is identical on Linux, macOS, and
/// Windows. `name` is empty on Linux, a private named FIFO on macOS, and a
/// named Event Object on Windows.
#[repr(C, align(64))]
pub struct SharedEventState {
    magic: AtomicU32,
    sequence: AtomicU32,
    waiters: AtomicU32,
    name_len: AtomicU32,
    name: UnsafeCell<[u8; EVENT_NAME_LEN]>,
}

const _: () = assert!(std::mem::size_of::<SharedEventState>() == 128);

// SAFETY: `name` is written before `magic` is published and is immutable after
// initialization. All subsequently mutable fields are atomics.
unsafe impl Sync for SharedEventState {}

/// Result of one bounded park attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitOutcome {
    Changed,
    TimedOut,
}

/// A process-local handle to one event embedded in shared memory.
pub struct ProcessEvent {
    state: NonNull<SharedEventState>,
    backend: platform::Backend,
}

// SAFETY: the state is process-shared and internally atomic; platform handles
// support concurrent notification and one SPSC waiter.
unsafe impl Send for ProcessEvent {}
unsafe impl Sync for ProcessEvent {}

impl ProcessEvent {
    /// Initialize a new event in a live process-shared mapping.
    ///
    /// # Safety
    ///
    /// `state` must be aligned, writable for `SharedEventState`, and remain
    /// mapped until this handle is dropped. No other process may access it until
    /// this function returns.
    pub unsafe fn create(state: *mut SharedEventState) -> io::Result<Self> {
        let state = NonNull::new(state)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "null event state"))?;
        // SAFETY: guaranteed by the caller; the mapping is not published yet.
        unsafe {
            state.as_ptr().write(SharedEventState {
                magic: AtomicU32::new(0),
                sequence: AtomicU32::new(0),
                waiters: AtomicU32::new(0),
                name_len: AtomicU32::new(0),
                name: UnsafeCell::new([0; EVENT_NAME_LEN]),
            });
        }
        // SAFETY: the state was initialized immediately above and remains live.
        let backend = unsafe { platform::Backend::create(state.as_ref())? };
        // Release-publish the immutable name and initialized OS object.
        unsafe { state.as_ref() }
            .magic
            .store(EVENT_MAGIC, Ordering::Release);
        Ok(Self { state, backend })
    }

    /// Open an event initialized by another process.
    ///
    /// # Safety
    ///
    /// `state` must point to the same live process-shared mapping for the
    /// lifetime of the returned handle.
    pub unsafe fn open(state: *mut SharedEventState) -> io::Result<Self> {
        let state = NonNull::new(state)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "null event state"))?;
        // SAFETY: guaranteed by the caller.
        let shared = unsafe { state.as_ref() };
        if shared.magic.load(Ordering::Acquire) != EVENT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "process event is not initialized",
            ));
        }
        // SAFETY: the acquire above observed completed initialization.
        let backend = unsafe { platform::Backend::open(shared)? };
        Ok(Self { state, backend })
    }

    #[inline]
    pub fn observe(&self) -> u32 {
        self.shared().sequence.load(Ordering::Acquire)
    }

    /// Publish a new cursor and wake a registered peer.
    ///
    /// Passing the ring cursor rather than incrementing a separate counter
    /// avoids another shared read-modify-write. The waiter claim is still an
    /// exchange: its modification order is the lost-wakeup proof.
    #[inline]
    pub fn notify(&self, cursor: u64) -> io::Result<()> {
        let shared = self.shared();
        shared.sequence.store(cursor as u32, Ordering::Release);
        if shared.waiters.swap(0, Ordering::AcqRel) != 0 {
            self.backend.wake(&shared.sequence)?;
        }
        Ok(())
    }

    /// Park until the observed sequence changes or `timeout` elapses.
    ///
    /// Callers must check their actual ring predicate before and after this
    /// method; notifications are deliberately allowed to be stale or spurious.
    pub fn wait(&self, observed: u32, timeout: Duration) -> io::Result<WaitOutcome> {
        let shared = self.shared();
        if shared.sequence.load(Ordering::Acquire) != observed {
            return Ok(WaitOutcome::Changed);
        }

        if shared.waiters.swap(1, Ordering::AcqRel) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process event supports one waiter",
            ));
        }
        let result = if shared.sequence.load(Ordering::Acquire) != observed {
            Ok(WaitOutcome::Changed)
        } else {
            self.backend.wait(&shared.sequence, observed, timeout)
        };
        shared.waiters.store(0, Ordering::Release);

        match result? {
            WaitOutcome::TimedOut if shared.sequence.load(Ordering::Acquire) != observed => {
                Ok(WaitOutcome::Changed)
            }
            outcome => Ok(outcome),
        }
    }

    #[inline]
    fn shared(&self) -> &SharedEventState {
        // SAFETY: construction requires the mapping to outlive the handle.
        unsafe { self.state.as_ref() }
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use loom::thread;

    #[test]
    fn waiter_claim_exchange_closes_the_lost_wakeup_gap() {
        loom::model(|| {
            let sequence = Arc::new(AtomicU32::new(0));
            let waiter = Arc::new(AtomicU32::new(0));
            let decided_to_park = Arc::new(AtomicBool::new(false));
            let woke = Arc::new(AtomicBool::new(false));

            let waiter_thread = {
                let sequence = sequence.clone();
                let waiter = waiter.clone();
                let decided_to_park = decided_to_park.clone();
                thread::spawn(move || {
                    assert_eq!(waiter.swap(1, Ordering::AcqRel), 0);
                    if sequence.load(Ordering::Acquire) == 0 {
                        decided_to_park.store(true, Ordering::Release);
                    } else {
                        waiter.store(0, Ordering::Release);
                    }
                })
            };

            let publisher_thread = {
                let sequence = sequence.clone();
                let waiter = waiter.clone();
                let woke = woke.clone();
                thread::spawn(move || {
                    sequence.store(1, Ordering::Release);
                    if waiter.swap(0, Ordering::AcqRel) != 0 {
                        woke.store(true, Ordering::Release);
                    }
                })
            };

            waiter_thread.join().unwrap();
            publisher_thread.join().unwrap();
            assert!(
                !decided_to_park.load(Ordering::Acquire) || woke.load(Ordering::Acquire),
                "waiter decided to park but publisher missed it"
            );
        });
    }
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn write_name(state: &SharedEventState, name: &str) -> io::Result<()> {
    if name.is_empty() || name.len() >= EVENT_NAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process event name is empty or too long",
        ));
    }
    // SAFETY: create() is the sole accessor before the release magic store.
    unsafe {
        let destination = &mut *state.name.get();
        destination[..name.len()].copy_from_slice(name.as_bytes());
        destination[name.len()] = 0;
    }
    state.name_len.store(name.len() as u32, Ordering::Relaxed);
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn read_name(state: &SharedEventState) -> io::Result<&str> {
    let len = state.name_len.load(Ordering::Relaxed) as usize;
    if len == 0 || len >= EVENT_NAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid process event name length",
        ));
    }
    // SAFETY: name is immutable after the release magic store observed by open.
    let bytes = unsafe { &*state.name.get() };
    std::str::from_utf8(&bytes[..len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "event name is not UTF-8"))
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn random_suffix() -> io::Result<String> {
    let mut token = [0u8; 16];
    getrandom::getrandom(&mut token)
        .map_err(|error| io::Error::other(format!("event name randomness failed: {error}")))?;
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(token.len() * 2);
    for byte in token {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    Ok(output)
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{Duration, WaitOutcome, io};
    use std::sync::atomic::AtomicU32;

    pub(super) struct Backend;

    impl Backend {
        pub(super) unsafe fn create(_state: &super::SharedEventState) -> io::Result<Self> {
            Ok(Self)
        }

        pub(super) unsafe fn open(_state: &super::SharedEventState) -> io::Result<Self> {
            Ok(Self)
        }

        pub(super) fn wait(
            &self,
            sequence: &AtomicU32,
            observed: u32,
            timeout: Duration,
        ) -> io::Result<WaitOutcome> {
            let seconds = timeout.as_secs().min(libc::time_t::MAX as u64) as libc::time_t;
            let timespec = libc::timespec {
                tv_sec: seconds,
                tv_nsec: timeout.subsec_nanos() as libc::c_long,
            };
            // FUTEX_WAIT (without FUTEX_PRIVATE_FLAG) is required: this atomic
            // lives in a MAP_SHARED mapping and the peer is another process.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    sequence as *const AtomicU32,
                    libc::FUTEX_WAIT,
                    observed,
                    &timespec as *const libc::timespec,
                )
            };
            if result == 0 {
                return Ok(WaitOutcome::Changed);
            }
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EAGAIN | libc::EINTR) => Ok(WaitOutcome::Changed),
                Some(libc::ETIMEDOUT) => Ok(WaitOutcome::TimedOut),
                _ => Err(error),
            }
        }

        pub(super) fn wake(&self, sequence: &AtomicU32) -> io::Result<()> {
            let result = unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    sequence as *const AtomicU32,
                    libc::FUTEX_WAKE,
                    i32::MAX,
                )
            };
            if result >= 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{Duration, WaitOutcome, io, random_suffix, read_name, write_name};
    use std::ffi::CString;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

    static NEXT_NAME: AtomicU64 = AtomicU64::new(1);

    pub(super) struct Backend {
        fd: libc::c_int,
        path: CString,
        owner: bool,
    }

    unsafe impl Send for Backend {}
    unsafe impl Sync for Backend {}

    impl Backend {
        pub(super) unsafe fn create(state: &super::SharedEventState) -> io::Result<Self> {
            let id = NEXT_NAME.fetch_add(1, Ordering::Relaxed);
            let path = format!("/tmp/psi-{}-{id:x}", random_suffix()?);
            write_name(state, &path)?;
            let path = CString::new(path).expect("generated FIFO path has no NUL");
            if unsafe { libc::mkfifo(path.as_ptr(), 0o600) } != 0 {
                return Err(io::Error::last_os_error());
            }
            match open_fifo(&path) {
                Ok(fd) => Ok(Self {
                    fd,
                    path,
                    owner: true,
                }),
                Err(error) => {
                    unsafe { libc::unlink(path.as_ptr()) };
                    Err(error)
                }
            }
        }

        pub(super) unsafe fn open(state: &super::SharedEventState) -> io::Result<Self> {
            let path = CString::new(read_name(state)?)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "event name has NUL"))?;
            Ok(Self {
                fd: open_fifo(&path)?,
                path,
                owner: false,
            })
        }

        pub(super) fn wait(
            &self,
            _sequence: &AtomicU32,
            _observed: u32,
            timeout: Duration,
        ) -> io::Result<WaitOutcome> {
            let mut descriptor = libc::pollfd {
                fd: self.fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let millis = if timeout.is_zero() {
                0
            } else {
                timeout.as_millis().clamp(1, libc::c_int::MAX as u128) as libc::c_int
            };
            let result = unsafe { libc::poll(&mut descriptor, 1, millis) };
            if result == 0 {
                return Ok(WaitOutcome::TimedOut);
            }
            if result < 0 {
                let error = io::Error::last_os_error();
                return match error.raw_os_error() {
                    Some(libc::EINTR) => Ok(WaitOutcome::Changed),
                    _ => Err(error),
                };
            }
            if descriptor.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                return Err(io::Error::other("process event FIFO failed"));
            }
            let mut tokens = [0u8; 64];
            let read = unsafe {
                libc::read(
                    self.fd,
                    tokens.as_mut_ptr().cast::<libc::c_void>(),
                    tokens.len(),
                )
            };
            if read >= 0 {
                Ok(WaitOutcome::Changed)
            } else {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::EAGAIN | libc::EINTR) => Ok(WaitOutcome::Changed),
                    _ => Err(error),
                }
            }
        }

        pub(super) fn wake(&self, _sequence: &AtomicU32) -> io::Result<()> {
            let token = 1u8;
            let written =
                unsafe { libc::write(self.fd, (&token as *const u8).cast::<libc::c_void>(), 1) };
            if written == 1 {
                Ok(())
            } else {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    // A full FIFO is already a pending wake.
                    Some(libc::EAGAIN) => Ok(()),
                    _ => Err(error),
                }
            }
        }
    }

    impl Drop for Backend {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
                if self.owner {
                    libc::unlink(self.path.as_ptr());
                }
            }
        }
    }

    fn open_fifo(path: &CString) -> io::Result<libc::c_int> {
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd >= 0 {
            Ok(fd)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use super::super::windows_security::OwnerOnlySecurity;
    use super::{Duration, WaitOutcome, io, random_suffix, read_name, write_name};
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::System::Threading::{
        CreateEventW, EVENT_MODIFY_STATE, OpenEventW, SYNCHRONIZATION_SYNCHRONIZE, SetEvent,
        WaitForSingleObject,
    };

    static NEXT_NAME: AtomicU64 = AtomicU64::new(1);

    pub(super) struct Backend {
        handle: HANDLE,
    }

    unsafe impl Send for Backend {}
    unsafe impl Sync for Backend {}

    impl Backend {
        pub(super) unsafe fn create(state: &super::SharedEventState) -> io::Result<Self> {
            let id = NEXT_NAME.fetch_add(1, Ordering::Relaxed);
            let name = format!("Local\\prescient-event-{}-{id:x}", random_suffix()?);
            write_name(state, &name)?;
            let wide = wide_name(&name);
            let mut security = OwnerOnlySecurity::new()?;
            let attributes = security.attributes();
            let handle = unsafe { CreateEventW(&attributes, 0, 0, wide.as_ptr()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
                unsafe { CloseHandle(handle) };
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "process-event name collision",
                ));
            }
            Ok(Self { handle })
        }

        pub(super) unsafe fn open(state: &super::SharedEventState) -> io::Result<Self> {
            let wide = wide_name(read_name(state)?);
            let handle = unsafe {
                OpenEventW(
                    EVENT_MODIFY_STATE | SYNCHRONIZATION_SYNCHRONIZE,
                    0,
                    wide.as_ptr(),
                )
            };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            Ok(Self { handle })
        }

        pub(super) fn wait(
            &self,
            _sequence: &AtomicU32,
            _observed: u32,
            timeout: Duration,
        ) -> io::Result<WaitOutcome> {
            let millis = timeout.as_millis().min((u32::MAX - 1) as u128) as u32;
            match unsafe { WaitForSingleObject(self.handle, millis) } {
                WAIT_OBJECT_0 => Ok(WaitOutcome::Changed),
                WAIT_TIMEOUT => Ok(WaitOutcome::TimedOut),
                _ => Err(io::Error::last_os_error()),
            }
        }

        pub(super) fn wake(&self, _sequence: &AtomicU32) -> io::Result<()> {
            if unsafe { SetEvent(self.handle) } != 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }

    impl Drop for Backend {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.handle);
            }
        }
    }

    fn wide_name(name: &str) -> Vec<u16> {
        name.encode_utf16().chain(std::iter::once(0)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::MaybeUninit;
    use std::sync::Arc;
    use std::time::Instant;

    struct LocalEvent {
        state: Box<MaybeUninit<SharedEventState>>,
        event: Arc<ProcessEvent>,
    }

    impl LocalEvent {
        fn new() -> Self {
            let mut state = Box::new(MaybeUninit::uninit());
            let event = unsafe { ProcessEvent::create(state.as_mut_ptr()) }.unwrap();
            Self {
                state,
                event: Arc::new(event),
            }
        }
    }

    #[test]
    fn notification_wakes_waiter() {
        let fixture = LocalEvent::new();
        let waiter = fixture.event.clone();
        let thread = std::thread::spawn(move || waiter.wait(0, Duration::from_secs(1)).unwrap());
        while fixture.event.shared().waiters.load(Ordering::Acquire) == 0 {
            std::thread::park_timeout(Duration::from_micros(50));
        }
        fixture.event.notify(1).unwrap();
        assert_eq!(thread.join().unwrap(), WaitOutcome::Changed);
        assert_eq!(fixture.event.observe(), 1);
        let _keep_mapping_live = fixture.state;
    }

    #[test]
    fn notification_before_registration_is_not_lost() {
        let fixture = LocalEvent::new();
        fixture.event.notify(7).unwrap();
        assert_eq!(
            fixture.event.wait(0, Duration::from_secs(1)).unwrap(),
            WaitOutcome::Changed
        );
        let _keep_mapping_live = fixture.state;
    }

    #[test]
    fn separately_opened_handle_wakes_the_same_event() {
        let fixture = LocalEvent::new();
        // SAFETY: Box keeps the initialized state at a stable address through
        // the complete lifetime of both handles.
        let opened = unsafe { ProcessEvent::open(fixture.state.as_ptr().cast_mut()) }.unwrap();
        let opened = Arc::new(opened);
        let waiter = opened.clone();
        let thread = std::thread::spawn(move || waiter.wait(0, Duration::from_secs(1)).unwrap());
        while fixture.event.shared().waiters.load(Ordering::Acquire) == 0 {
            std::thread::park_timeout(Duration::from_micros(50));
        }
        fixture.event.notify(11).unwrap();
        assert_eq!(thread.join().unwrap(), WaitOutcome::Changed);
        assert_eq!(opened.observe(), 11);
        let _keep_mapping_live = fixture.state;
    }

    #[test]
    fn timeout_blocks_instead_of_spinning() {
        let fixture = LocalEvent::new();
        let wall_started = Instant::now();
        #[cfg(target_os = "linux")]
        let cpu_started = thread_cpu_time();
        assert_eq!(
            fixture.event.wait(0, Duration::from_millis(50)).unwrap(),
            WaitOutcome::TimedOut
        );
        assert!(wall_started.elapsed() >= Duration::from_millis(40));
        #[cfg(target_os = "linux")]
        assert!(
            thread_cpu_time() - cpu_started < Duration::from_millis(10),
            "a blocking timeout consumed too much thread CPU"
        );
        let _keep_mapping_live = fixture.state;
    }

    #[cfg(target_os = "linux")]
    fn thread_cpu_time() -> Duration {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) },
            0
        );
        Duration::new(value.tv_sec as u64, value.tv_nsec as u32)
    }
}

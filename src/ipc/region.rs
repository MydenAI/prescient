//! Portable named process-shared memory for supervised workers.
//!
//! The descriptor is a capability locator, not an authorization decision. On
//! POSIX the object requests mode 0600 with `O_EXCL`; on Windows the mapping
//! receives an explicit one-ACE DACL for the current user. The caller still
//! authenticates the private control handshake before entering the data path.

use std::io;
use std::ptr::NonNull;

/// Serializable locator for reopening one shared region in a spawned process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionHandle {
    name: String,
    len: usize,
}

impl RegionHandle {
    /// Validate and reconstruct a shared-region locator from serialized parts.
    pub(crate) fn from_parts(name: String, len: usize) -> io::Result<Self> {
        if name.is_empty() || name.len() > 240 {
            return Err(invalid_input("shared-region name length is invalid"));
        }
        if len == 0 || len > isize::MAX as usize {
            return Err(invalid_input("shared-region length is invalid"));
        }
        Ok(Self { name, len })
    }

    /// Return the platform mapping name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Return the mapped byte length.
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

/// One writable mapping of a named process-shared region.
pub struct SharedRegion {
    ptr: NonNull<u8>,
    len: usize,
    _backend: platform::Backend,
    locked: bool,
}

// SAFETY: the mapping itself has no Rust-owned interior state. Callers must use
// process-safe atomics and explicit slot ownership for concurrent access.
unsafe impl Send for SharedRegion {}
unsafe impl Sync for SharedRegion {}

impl SharedRegion {
    /// Create a new zero-filled region with a cryptographically random name.
    pub fn create(len: usize) -> io::Result<(Self, RegionHandle)> {
        if len == 0 || len > isize::MAX as usize {
            return Err(invalid_input("shared-region length is invalid"));
        }
        let mut token = [0u8; 12];
        getrandom::getrandom(&mut token)
            .map_err(|error| io::Error::other(format!("region name randomness failed: {error}")))?;
        let suffix = hex(&token);
        let name = platform::region_name(&suffix);
        let backend = platform::Backend::create(&name, len)?;
        let ptr = NonNull::new(backend.ptr().cast::<u8>())
            .ok_or_else(|| io::Error::other("shared-region mapping returned null"))?;
        let handle = RegionHandle::from_parts(name, len)?;
        Ok((
            Self {
                ptr,
                len,
                _backend: backend,
                locked: false,
            },
            handle,
        ))
    }

    /// Reopen an existing region by name in a separately spawned process.
    pub fn open(handle: &RegionHandle) -> io::Result<Self> {
        let backend = platform::Backend::open(handle.name(), handle.len())?;
        let ptr = NonNull::new(backend.ptr().cast::<u8>())
            .ok_or_else(|| io::Error::other("shared-region mapping returned null"))?;
        Ok(Self {
            ptr,
            len: handle.len(),
            _backend: backend,
            locked: false,
        })
    }

    /// Require the complete mapping to remain resident for this handle's life.
    pub fn lock(&mut self) -> io::Result<()> {
        if !self.locked {
            platform::lock(self.ptr.as_ptr(), self.len)?;
            self.locked = true;
        }
        Ok(())
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    /// Base pointer for typed layouts owned by the transport protocol.
    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for SharedRegion {
    fn drop(&mut self) {
        if self.locked {
            platform::unlock(self.ptr.as_ptr(), self.len);
        }
        // `backend` unmaps and closes after this method returns.
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod platform {
    use std::ffi::{CString, c_void};
    use std::io;

    pub(super) struct Backend {
        ptr: *mut c_void,
        len: usize,
        fd: libc::c_int,
        owner_name: Option<CString>,
    }

    unsafe impl Send for Backend {}
    unsafe impl Sync for Backend {}

    // POSIX `shm_open` sets `FD_CLOEXEC`; Darwin rejects `O_CLOEXEC` in `oflag`.
    impl Backend {
        pub(super) fn create(name: &str, len: usize) -> io::Result<Self> {
            let name = CString::new(name)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "region name has NUL"))?;
            let fd = unsafe {
                libc::shm_open(
                    name.as_ptr(),
                    libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                    0o600,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            if unsafe { libc::ftruncate(fd, len as libc::off_t) } != 0 {
                let error = io::Error::last_os_error();
                unsafe {
                    libc::close(fd);
                    libc::shm_unlink(name.as_ptr());
                }
                return Err(error);
            }
            match map(fd, len) {
                Ok(ptr) => Ok(Self {
                    ptr,
                    len,
                    fd,
                    owner_name: Some(name),
                }),
                Err(error) => {
                    unsafe {
                        libc::close(fd);
                        libc::shm_unlink(name.as_ptr());
                    }
                    Err(error)
                }
            }
        }

        pub(super) fn open(name: &str, len: usize) -> io::Result<Self> {
            let name = CString::new(name)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "region name has NUL"))?;
            let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDWR, 0) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            if let Err(error) = validate_backing_len(fd, len) {
                unsafe { libc::close(fd) };
                return Err(error);
            }
            match map(fd, len) {
                Ok(ptr) => Ok(Self {
                    ptr,
                    len,
                    fd,
                    owner_name: None,
                }),
                Err(error) => {
                    unsafe { libc::close(fd) };
                    Err(error)
                }
            }
        }

        pub(super) fn ptr(&self) -> *mut c_void {
            self.ptr
        }
    }

    impl Drop for Backend {
        fn drop(&mut self) {
            unsafe {
                libc::munmap(self.ptr, self.len);
                libc::close(self.fd);
                if let Some(name) = &self.owner_name {
                    libc::shm_unlink(name.as_ptr());
                }
            }
        }
    }

    fn validate_backing_len(fd: libc::c_int, len: usize) -> io::Result<()> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let actual_len = unsafe { stat.assume_init() }.st_size;
        let expected_len = expected_backing_len(len)?;
        if actual_len < 0 || actual_len as u64 != expected_len {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared-region length does not match descriptor",
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) fn expected_backing_len(len: usize) -> io::Result<u64> {
        Ok(len as u64)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn expected_backing_len(len: usize) -> io::Result<u64> {
        // XNU rounds POSIX shared-memory objects to the current VM page size.
        let page_len = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_len <= 0 {
            return Err(io::Error::other("unable to query VM page size"));
        }
        let page_len = page_len as usize;
        let remainder = len % page_len;
        let rounded = if remainder == 0 {
            len
        } else {
            len.checked_add(page_len - remainder).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "region size overflow")
            })?
        };
        Ok(rounded as u64)
    }
    fn map(fd: libc::c_int, len: usize) -> io::Result<*mut c_void> {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            Err(io::Error::last_os_error())
        } else {
            Ok(ptr)
        }
    }

    pub(super) fn region_name(suffix: &str) -> String {
        format!("/p-{suffix}")
    }

    pub(super) fn lock(ptr: *mut u8, len: usize) -> io::Result<()> {
        if unsafe { libc::mlock(ptr.cast::<c_void>(), len) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn unlock(ptr: *mut u8, len: usize) {
        unsafe {
            libc::munlock(ptr.cast::<c_void>(), len);
        }
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use super::super::windows_security::OwnerOnlySecurity;
    use std::ffi::c_void;
    use std::io;
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, HANDLE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::Memory::{
        CreateFileMappingW, FILE_MAP_ALL_ACCESS, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile,
        OpenFileMappingW, PAGE_READWRITE, UnmapViewOfFile, VirtualLock, VirtualUnlock,
    };

    pub(super) struct Backend {
        ptr: *mut c_void,
        handle: HANDLE,
    }

    unsafe impl Send for Backend {}
    unsafe impl Sync for Backend {}

    impl Backend {
        pub(super) fn create(name: &str, len: usize) -> io::Result<Self> {
            let name = wide(name);
            let size = len as u64;
            let mut security = OwnerOnlySecurity::new()?;
            let attributes = security.attributes();
            let handle = unsafe {
                CreateFileMappingW(
                    INVALID_HANDLE_VALUE,
                    &attributes,
                    PAGE_READWRITE,
                    (size >> 32) as u32,
                    size as u32,
                    name.as_ptr(),
                )
            };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
                unsafe { CloseHandle(handle) };
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "shared-region name collision",
                ));
            }
            map(handle, len)
        }

        pub(super) fn open(name: &str, len: usize) -> io::Result<Self> {
            let name = wide(name);
            let handle = unsafe { OpenFileMappingW(FILE_MAP_ALL_ACCESS, 0, name.as_ptr()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            map(handle, len)
        }

        pub(super) fn ptr(&self) -> *mut c_void {
            self.ptr
        }
    }

    impl Drop for Backend {
        fn drop(&mut self) {
            unsafe {
                UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS { Value: self.ptr });
                CloseHandle(self.handle);
            }
        }
    }

    fn map(handle: HANDLE, len: usize) -> io::Result<Backend> {
        let view = unsafe { MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, len) };
        if view.Value.is_null() {
            let error = io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            Err(error)
        } else {
            Ok(Backend {
                ptr: view.Value,
                handle,
            })
        }
    }

    pub(super) fn region_name(suffix: &str) -> String {
        format!("Local\\prescient-region-{}-{suffix}", std::process::id())
    }

    pub(super) fn lock(ptr: *mut u8, len: usize) -> io::Result<()> {
        if unsafe { VirtualLock(ptr.cast::<c_void>(), len) } != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn unlock(ptr: *mut u8, len: usize) {
        unsafe {
            VirtualUnlock(ptr.cast::<c_void>(), len);
        }
    }

    fn wide(value: &str) -> Vec<u16> {
        value.encode_utf16().chain(std::iter::once(0)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn generated_posix_region_name_fits_darwin_limit() {
        let (_owner, handle) = SharedRegion::create(1).unwrap();
        let name = handle.name();
        assert_eq!(name.len(), 27);
        assert!(name.len() <= 31);
        assert!(name.starts_with("/p-"));
        assert!(
            name[3..]
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        );
    }
    #[test]
    fn separately_opened_region_observes_shared_bytes() {
        let (owner, handle) = SharedRegion::create(4_097).unwrap();
        let peer = SharedRegion::open(&handle).unwrap();
        unsafe {
            owner.as_ptr().add(17).write(0xA5);
            assert_eq!(peer.as_ptr().add(17).read(), 0xA5);
            peer.as_ptr().add(4_096).write(0x3C);
            assert_eq!(owner.as_ptr().add(4_096).read(), 0x3C);
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn reopen_rejects_a_descriptor_beyond_the_backing_extent() {
        let (_owner, handle) = SharedRegion::create(4_097).unwrap();
        let beyond_backing = platform::expected_backing_len(handle.len()).unwrap() as usize + 1;
        let forged = RegionHandle::from_parts(handle.name().to_owned(), beyond_backing).unwrap();
        let error = SharedRegion::open(&forged).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn live_mapping_survives_creator_handle_drop() {
        let (owner, handle) = SharedRegion::create(128).unwrap();
        let peer = SharedRegion::open(&handle).unwrap();
        unsafe { owner.as_ptr().write(91) };
        drop(owner);
        assert_eq!(unsafe { peer.as_ptr().read() }, 91);
    }

    #[test]
    fn malformed_handles_are_rejected() {
        assert!(RegionHandle::from_parts(String::new(), 1).is_err());
        assert!(RegionHandle::from_parts("valid".into(), 0).is_err());
    }
}

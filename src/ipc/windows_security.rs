//! Owner-only security attributes for named Windows kernel objects.
//!
//! A null `SECURITY_ATTRIBUTES` pointer inherits the token's default DACL,
//! which can include principals beyond the current user. Process transport
//! instead creates a single-ACE ACL before either the mapping or Event object
//! exists, so there is no permissive creation window.

use std::io;
use std::mem::size_of;

use windows_sys::Win32::Foundation::{CloseHandle, GENERIC_ALL, HANDLE};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, AddAccessAllowedAceEx, GetLengthSid,
    GetTokenInformation, InitializeAcl, InitializeSecurityDescriptor, PSID, SECURITY_ATTRIBUTES,
    SECURITY_DESCRIPTOR, SetSecurityDescriptorDacl, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

pub(super) struct OwnerOnlySecurity {
    _sid: OwnedSid,
    _acl: Vec<u32>,
    descriptor: SECURITY_DESCRIPTOR,
}

impl OwnerOnlySecurity {
    pub(super) fn new() -> io::Result<Self> {
        let sid = OwnedSid::for_current_user()?;
        let unrounded = size_of::<ACL>()
            .checked_add(size_of::<ACCESS_ALLOWED_ACE>())
            .and_then(|bytes| bytes.checked_sub(size_of::<u32>()))
            .and_then(|bytes| bytes.checked_add(sid.len()))
            .ok_or_else(|| io::Error::other("ACL size overflow"))?;
        let acl_bytes = unrounded.next_multiple_of(size_of::<u32>());
        let acl_len = u32::try_from(acl_bytes).map_err(|_| io::Error::other("ACL too large"))?;
        let mut acl = vec![0u32; acl_bytes / size_of::<u32>()];
        let acl_ptr = acl.as_mut_ptr().cast::<ACL>();
        let mut descriptor = SECURITY_DESCRIPTOR::default();

        // SAFETY: the DWORD-aligned ACL and copied SID outlive the descriptor
        // and every object-creation call that receives `attributes()`.
        unsafe {
            if InitializeAcl(acl_ptr, acl_len, ACL_REVISION) == 0 {
                return Err(io::Error::last_os_error());
            }
            if AddAccessAllowedAceEx(acl_ptr, ACL_REVISION, 0, GENERIC_ALL, sid.as_psid()) == 0 {
                return Err(io::Error::last_os_error());
            }
            if InitializeSecurityDescriptor(
                (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                SECURITY_DESCRIPTOR_REVISION,
            ) == 0
            {
                return Err(io::Error::last_os_error());
            }
            if SetSecurityDescriptorDacl(
                (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                1,
                acl_ptr,
                0,
            ) == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(Self {
            _sid: sid,
            _acl: acl,
            descriptor,
        })
    }

    pub(super) fn attributes(&mut self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: (&mut self.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
            bInheritHandle: 0,
        }
    }
}

struct OwnedSid {
    words: Vec<u32>,
    len: usize,
}

impl OwnedSid {
    fn for_current_user() -> io::Result<Self> {
        // SAFETY: all out-pointers reference live storage, and TokenHandle
        // closes the process token on every return path.
        unsafe {
            let mut raw_token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw_token) == 0 {
                return Err(io::Error::last_os_error());
            }
            let token = TokenHandle(raw_token);
            let mut needed = 0u32;
            GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &mut needed);
            if needed == 0 {
                return Err(io::Error::last_os_error());
            }
            let words = (needed as usize).div_ceil(size_of::<u64>()).max(1);
            let mut token_buffer = vec![0u64; words];
            let capacity = u32::try_from(words * size_of::<u64>())
                .map_err(|_| io::Error::other("token buffer too large"))?;
            let mut written = capacity;
            if GetTokenInformation(
                token.0,
                TokenUser,
                token_buffer.as_mut_ptr().cast(),
                capacity,
                &mut written,
            ) == 0
            {
                return Err(io::Error::last_os_error());
            }
            let sid = (*token_buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid;
            if sid.is_null() {
                return Err(io::Error::other("process token has no user SID"));
            }
            let len = GetLengthSid(sid) as usize;
            if len == 0 {
                return Err(io::Error::last_os_error());
            }
            let mut owned = vec![0u32; len.div_ceil(size_of::<u32>())];
            std::ptr::copy_nonoverlapping(sid.cast::<u8>(), owned.as_mut_ptr().cast::<u8>(), len);
            Ok(Self { words: owned, len })
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn as_psid(&self) -> PSID {
        self.words.as_ptr() as PSID
    }
}

struct TokenHandle(HANDLE);

impl Drop for TokenHandle {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}

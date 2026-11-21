//! Linux exec handoff for a single Gateway or smoke-test credential.

use crate::{placement, redact::Secret};
use std::{
    ffi::{OsStr, OsString},
    io::{self, Read, Write},
    net::Shutdown,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::{
            ffi::{OsStrExt, OsStringExt},
            net::UnixStream,
            process::CommandExt,
        },
    },
    process::Command,
    time::Duration,
};

/// Non-secret metadata. Only the numeric descriptor and validated source name
/// cross exec in the environment.
pub use crate::placement::GATEWAY_TOKEN_FD_ENV as FD_ENV;
const MAX_TOKEN: usize = 4096;

fn valid_source(name: &str) -> bool {
    name == placement::GATEWAY_TOKEN_ENV || placement::valid_token_env(name)
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid inherited Gateway credential",
    )
}

/// Keep the returned endpoint alive through spawn or exec. Only the forked
/// child clears CLOEXEC. The anonymous socket is prefilled and write-closed,
/// so launch never waits for child code or leaves a writer open.
pub fn attach(command: &mut Command, name: &str, token: &OsStr) -> io::Result<UnixStream> {
    let bytes = token.as_bytes();
    if !valid_source(name) || bytes.len() > MAX_TOKEN {
        return Err(invalid());
    }
    let (child, mut parent) = UnixStream::pair()?;
    parent.set_nonblocking(true)?;
    parent.write_all(bytes)?;
    parent.shutdown(Shutdown::Write)?;
    let fd = child.as_raw_fd();
    if fd < 3 {
        return Err(invalid());
    }
    command.env(FD_ENV, format!("{fd}:{name}"));
    // SAFETY: the caller retains the endpoint through spawn, and fcntl does
    // not allocate or take locks in the forked child.
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(child)
}

pub struct InheritedToken {
    name: String,
    token: Secret<OsString>,
}

impl InheritedToken {
    pub fn for_name(self, name: &OsStr) -> io::Result<Secret<OsString>> {
        if name == OsStr::new(&self.name) {
            Ok(self.token)
        } else {
            Err(invalid())
        }
    }
}

/// Read once during startup, after disabling process dumps and before opening
/// application FDs or spawning any children.
///
/// # Safety
/// If FD_ENV is set, its descriptor must be independently owned and inherited
/// from the launcher. No other code may own it.
pub unsafe fn take_inherited() -> io::Result<Option<InheritedToken>> {
    let Some(spec) = std::env::var_os(FD_ENV) else {
        return Ok(None);
    };
    let (fd, name) = spec
        .to_str()
        .and_then(|spec| spec.split_once(':'))
        .ok_or_else(invalid)?;
    let fd = fd
        .parse::<RawFd>()
        .ok()
        .filter(|&fd| fd >= 3)
        .ok_or_else(invalid)?;
    // SAFETY: prctl queries the current process and requires no pointer arguments.
    if !valid_source(name) || unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) } != 0 {
        return Err(invalid());
    }
    // SAFETY: ownership of this descriptor transfers from the launcher.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || flags & libc::FD_CLOEXEC != 0 {
        return Err(invalid());
    }
    // SAFETY: the caller guarantees exclusive ownership and a valid descriptor.
    let socket = unsafe { UnixStream::from_raw_fd(fd) };
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
        || !socket.peer_addr()?.is_unnamed()
    {
        return Err(invalid());
    }
    socket.set_read_timeout(Some(Duration::from_secs(1)))?;
    let mut bytes = Vec::with_capacity(MAX_TOKEN + 1);
    let result = socket.take((MAX_TOKEN + 1) as u64).read_to_end(&mut bytes);
    if result.is_err() || bytes.len() > MAX_TOKEN {
        crate::secret_file::wipe(&mut bytes);
        return Err(invalid());
    }
    Ok(Some(InheritedToken {
        name: name.into(),
        token: Secret::new(OsString::from_vec(bytes)),
    }))
}

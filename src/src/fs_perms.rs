//! Permission-bit checks for already-open secret files and directories.
//!
//! On Unix these functions operate on the supplied handle, not a path, and
//! require exactly 0600 for files or 0700 for directories (including special
//! bits). They do not check ownership, POSIX ACLs, parent directories, hard
//! links, or filesystem enforcement. Opening/creating the handle securely is
//! the caller's responsibility. Setting permissions after writing a secret can
//! expose it before the change; create it privately before writing.
//!
//! Windows ACL support is deliberately unsupported: a mode-bit approximation or
//! unchecked command success cannot establish a current-user-only DACL.

use std::{fs::File, io};

/// The required kind of object and its Unix permission bits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretKind {
    File,
    Directory,
}

/// Check an open object's type and platform-specific permissions.
///
/// An insecure mode returns `PermissionDenied`; unsupported platforms return
/// `Unsupported`. A successful Unix check is a mode-bit check only.
pub fn check(handle: &File, kind: SecretKind) -> io::Result<()> {
    platform::check(handle, kind)
}

/// Set and re-check permissions on an existing open object.
///
/// This is not an atomic private-file creation operation. A concurrent actor
/// with permission to change the object can alter its permissions afterward.
pub fn restrict(handle: &File, kind: SecretKind) -> io::Result<()> {
    platform::restrict(handle, kind)
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn expected(kind: SecretKind) -> u32 {
        match kind {
            SecretKind::File => 0o600,
            SecretKind::Directory => 0o700,
        }
    }

    fn validate_type(metadata: &std::fs::Metadata, kind: SecretKind) -> io::Result<()> {
        let valid = match kind {
            SecretKind::File => metadata.is_file(),
            SecretKind::Directory => metadata.is_dir(),
        };
        if valid {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected secret object type",
            ))
        }
    }

    pub fn check(handle: &File, kind: SecretKind) -> io::Result<()> {
        let metadata = handle.metadata()?;
        validate_type(&metadata, kind)?;
        if metadata.mode() & 0o7777 != expected(kind) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "insecure secret mode",
            ));
        }
        Ok(())
    }

    pub fn restrict(handle: &File, kind: SecretKind) -> io::Result<()> {
        validate_type(&handle.metadata()?, kind)?;
        handle.set_permissions(std::fs::Permissions::from_mode(expected(kind)))?;
        check(handle, kind)
    }
}

#[cfg(not(unix))]
mod platform {
    use super::*;

    pub fn check(_: &File, _: SecretKind) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secret ACL verification is unsupported",
        ))
    }

    pub fn restrict(_: &File, _: SecretKind) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secret ACL configuration is unsupported",
        ))
    }
}

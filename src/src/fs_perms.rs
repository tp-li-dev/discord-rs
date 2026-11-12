//! Permission checks and path-based opening/creation of secret files.
//!
//! On Unix [`check`] and [`restrict`] operate on the supplied handle and
//! require exactly 0600 for files or 0700 for directories (including special
//! bits). They do not check ownership, POSIX ACLs, parent directories, hard
//! links, or filesystem enforcement. Opening/creating the handle securely is
//! the caller's responsibility. Setting permissions after writing a secret can
//! expose it before the change; create it privately before writing.
//!
//! # Windows contract and TOCTOU limits
//!
//! Use the path-based APIs on Windows. The old [`check`] and [`restrict`]
//! remain `Unsupported`: an already-open handle cannot reveal links followed
//! before it was supplied. Path APIs reject reparse points (including ancestor
//! junctions), alternate streams and UNC/device paths. They use
//! `FILE_FLAG_OPEN_REPARSE_POINT` and inspect `File::metadata()` on each open
//! handle (Rust's Windows implementation uses `GetFileInformationByHandle`).
//! Ancestor handles deny delete sharing during permission preparation. The
//! returned file denies read/write/delete sharing until closed. This prevents
//! a reader from opening the initially empty file before its DACL is restricted.
//!
//! Windows PowerShell and icacls from the trusted `SystemRoot` installation are
//! required. Child processes receive no inherited application environment or
//! secret bytes. icacls disables inheritance, removes old ACEs and grants only
//! the current process user SID full control. Both its displayed ACL and the
//! security descriptor are checked; the owner must also be that SID. Unknown
//! tool/output formats fail closed with `Unsupported`.
//!
//! **This is not an atomic handle-based DACL transaction.** icacls/Get-Acl use
//! paths. Holding handles reduces replacement races, but does not prove path
//! identity against drive remapping, filesystem/filter behavior, hard links,
//! privileged actors or concurrent DACL changes. All ancestors, drive mappings,
//! the process environment and Windows tools must be trusted. Rechecking cannot
//! eliminate TOCTOU or revoke existing mappings/access; administrators and the
//! current user are not isolated. Hostile/shared directories are unsupported by
//! this contract. No filesystem-wide confidentiality guarantee is made.
//! Creation failure before writing can leave an empty file with a partially
//! changed ACL; failure during writing can leave partial content. There is no
//! automatic path-based deletion/rollback, atomic publication or durability
//! guarantee. Existing application secret paths are not migrated by this module.

use std::{fs::File, io, path::Path};

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

/// Open an existing secret file read-only, rejecting insecure permissions.
///
/// Does not change its ACL/mode or read its contents. See the module's Windows
/// TOCTOU/trusted-directory contract. Unix follows the normal `File::open`
/// path semantics and performs the same mode/type check as [`check`].
pub fn open_secret_file(path: impl AsRef<Path>) -> io::Result<File> {
    platform::open_secret_file(path.as_ref())
}

/// Restrict an existing file and return a read-only handle after verification.
///
/// This cannot undo earlier disclosure. Windows also sets the owner to the
/// current user; any error can leave a partially changed ACL. Unix uses the
/// existing handle-based [`restrict`] behavior. See the module's TOCTOU limits.
pub fn restrict_secret_file(path: impl AsRef<Path>) -> io::Result<File> {
    platform::restrict_secret_file(path.as_ref())
}

/// Create a new file, restrict and verify it, then write `contents`.
///
/// Never truncates an existing object. The returned handle is readable/writable
/// and positioned at EOF. No content is written before permission verification
/// succeeds. Unix creates with mode 0600 and rechecks the exact mode, preserving
/// the old mode-bit-only contract. Windows initially creates an empty file with
/// exclusive sharing, then configures its DACL. See the module's TOCTOU and
/// failure/partial-file limitations. This operation does not call `sync_all`.
pub fn create_secret_file(path: impl AsRef<Path>, contents: &[u8]) -> io::Result<File> {
    use io::Write;
    let mut handle = platform::create_secret_file(path.as_ref())?;
    handle.write_all(contents)?;
    Ok(handle)
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

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

    pub fn open_secret_file(path: &Path) -> io::Result<File> {
        let handle = File::open(path)?;
        check(&handle, SecretKind::File)?;
        Ok(handle)
    }

    pub fn restrict_secret_file(path: &Path) -> io::Result<File> {
        let handle = File::open(path)?;
        restrict(&handle, SecretKind::File)?;
        Ok(handle)
    }

    pub fn create_secret_file(path: &Path) -> io::Result<File> {
        let handle = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        restrict(&handle, SecretKind::File)?;
        Ok(handle)
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::{
        fs::OpenOptions,
        os::windows::{
            fs::{MetadataExt, OpenOptionsExt},
            process::CommandExt,
        },
        path::{Component, PathBuf, Prefix},
        process::{Command, Stdio},
    };

    const OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const REPARSE_POINT: u32 = 0x400;
    const READ_ATTRIBUTES: u32 = 0x80;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    fn unsupported() -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "secret ACL operation is unsupported",
        )
    }

    pub fn check(_: &File, _: SecretKind) -> io::Result<()> {
        Err(unsupported())
    }

    pub fn restrict(_: &File, _: SecretKind) -> io::Result<()> {
        Err(unsupported())
    }

    fn validate_handle(handle: &File, kind: SecretKind) -> io::Result<()> {
        // Query the OPEN_REPARSE_POINT handle, never fs::metadata(path).
        let metadata = handle.metadata()?;
        if metadata.file_attributes() & REPARSE_POINT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "secret reparse point rejected",
            ));
        }
        if match kind {
            SecretKind::File => !metadata.is_file(),
            SecretKind::Directory => !metadata.is_dir(),
        } {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected secret object type",
            ));
        }
        Ok(())
    }

    fn local_path(path: &Path) -> io::Result<PathBuf> {
        // Avoid GetFullPathName normalizing away a link/.. traversal, and avoid
        // alternate namespaces/streams or icacls wildcard/line ambiguities.
        if path.as_os_str().is_empty()
            || path.components().any(|c| matches!(c, Component::ParentDir))
        {
            return Err(unsupported());
        }
        let absolute = std::path::absolute(path)?;
        if !matches!(absolute.components().next(), Some(Component::Prefix(p)) if matches!(p.kind(), Prefix::Disk(_)))
        {
            return Err(unsupported());
        }
        // Inspect the original spelling too: Win32 can normalize a trailing
        // dot/space away before we get the absolute path.
        for component in path.components().chain(absolute.components()) {
            if let Component::Normal(name) = component {
                let name = name.to_str().ok_or_else(unsupported)?;
                let stem = name
                    .split('.')
                    .next()
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                if name.ends_with(['.', ' '])
                    || name
                        .chars()
                        .any(|c| c.is_control() || ":*?\"<>|".contains(c))
                    || matches!(
                        stem.as_str(),
                        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
                    )
                    || (stem.starts_with("COM") || stem.starts_with("LPT"))
                        && stem.chars().count() == 4
                {
                    return Err(unsupported());
                }
            }
        }
        if absolute.file_name().is_none() {
            return Err(unsupported());
        }
        Ok(absolute)
    }

    fn hold_ancestors(path: &Path) -> io::Result<Vec<File>> {
        let ancestors: Vec<_> = path.parent().ok_or_else(unsupported)?.ancestors().collect();
        let mut handles = Vec::with_capacity(ancestors.len());
        for parent in ancestors.into_iter().rev() {
            let handle = OpenOptions::new()
                .access_mode(READ_ATTRIBUTES)
                .share_mode(0x1 | 0x2) // FILE_SHARE_READ | FILE_SHARE_WRITE, no DELETE
                .custom_flags(OPEN_REPARSE_POINT | BACKUP_SEMANTICS)
                .open(parent)?;
            validate_handle(&handle, SecretKind::Directory)?;
            handles.push(handle);
        }
        Ok(handles)
    }

    // Static script: paths are environment data, never PowerShell source. Native
    // output is captured, not logged; errors never include a path, SID or bytes.
    // Get-Acl verifies protection/owner/raw ACEs independently of localized
    // icacls display text. The display is also parsed, not merely exit-checked.
    const ACL_SCRIPT: &str = r#"
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
try {
    $path = $env:FS_PERMS_PATH
    $icacls = [IO.Path]::Combine($env:SystemRoot, 'System32\icacls.exe')
    if (-not [IO.File]::Exists($icacls)) { exit 77 }
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    $account = $sid.Translate([Security.Principal.NTAccount]).Value
    function Read-Acl {
        Microsoft.PowerShell.Security\Get-Acl -LiteralPath $path
    }
    function Run-Icacls([string[]] $options) {
        $captured = @(& $icacls $path @options /L 2>$null)
        if ($LASTEXITCODE -ne 0) { exit 13 }
        return $captured
    }
    if ($env:FS_PERMS_RESTRICT -eq '1') {
        $null = Run-Icacls @('/setowner', ('*' + $sid.Value))
        $null = Run-Icacls @('/inheritance:r', '/grant:r', ('*' + $sid.Value + ':(F)'))
        $acl = Read-Acl
        $identities = @($acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]) |
            ForEach-Object { $_.IdentityReference.Value } | Sort-Object -Unique)
        foreach ($identity in $identities) {
            if ($identity -ne $sid.Value) {
                $null = Run-Icacls @('/remove', ('*' + $identity))
            }
        }
        $null = Run-Icacls @('/remove:d', ('*' + $sid.Value))
        $null = Run-Icacls @('/grant:r', ('*' + $sid.Value + ':(F)'))
    }
    function Assert-PrivateAcl {
        $acl = Read-Acl
        $raw = [Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(), 0)
        if (-not $acl.AreAccessRulesProtected -or $raw.Owner.Value -ne $sid.Value -or
            $null -eq $raw.DiscretionaryAcl -or $raw.DiscretionaryAcl.Count -ne 1) { exit 13 }
        $ace = $raw.DiscretionaryAcl[0]
        if ($ace.AceType -ne [Security.AccessControl.AceType]::AccessAllowed -or
            $ace.AceFlags -ne [Security.AccessControl.AceFlags]::None -or
            $ace.AccessMask -ne 2032127 -or $ace.SecurityIdentifier.Value -ne $sid.Value) { exit 13 }
    }
    Assert-PrivateAcl
    $lines = @(Run-Icacls @())
    # A single unflagged Full Control entry; do not depend on localized summary text.
    if ($lines.Count -lt 1 -or $lines[0].TrimEnd() -cne ($path + ' ' + $account + ':(F)')) { exit 77 }
    foreach ($line in $lines | Select-Object -Skip 1) {
        if ($line -match ':\(') { exit 77 }
    }
    Assert-PrivateAcl
    [Console]::Out.Write('verified')
    exit 0
} catch { exit 77 }
"#;

    fn acl(path: &Path, restrict: bool) -> io::Result<()> {
        let root = std::env::var_os("SystemRoot").ok_or_else(unsupported)?;
        let drive = match Path::new(&root).components().next() {
            Some(Component::Prefix(prefix)) if matches!(prefix.kind(), Prefix::Disk(_)) => {
                prefix.as_os_str()
            }
            _ => return Err(unsupported()),
        };
        let system = Path::new(&root).join("System32");
        let output = Command::new(system.join("WindowsPowerShell/v1.0/powershell.exe"))
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                ACL_SCRIPT,
            ])
            .env_clear()
            .env("SystemRoot", &root)
            .env("WINDIR", &root)
            // Windows shell APIs expand this in system cache paths. Omitting it
            // can create a literal "%SystemDrive%" directory in the working directory.
            .env("SystemDrive", drive)
            // Windows PowerShell uses PATHEXT even for an absolute .exe path.
            // Keep only the native executable extension, not the caller's value.
            .env("PATHEXT", ".EXE")
            .env(
                "PSModulePath",
                system.join("WindowsPowerShell/v1.0/Modules"),
            )
            .env("FS_PERMS_PATH", path)
            .env("FS_PERMS_RESTRICT", if restrict { "1" } else { "0" })
            .creation_flags(CREATE_NO_WINDOW)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .map_err(|_| unsupported())?;
        match output.status.code() {
            Some(0) if output.stdout == b"verified" => Ok(()),
            Some(13) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "secret ACL rejected",
            )),
            _ => Err(unsupported()),
        }
    }

    fn prepare(path: &Path, create: bool, restrict: bool) -> io::Result<File> {
        let path = local_path(path)?;
        let _ancestors = hold_ancestors(&path)?;
        let handle = OpenOptions::new()
            .read(true)
            .write(create)
            .create_new(create)
            .share_mode(0)
            .custom_flags(OPEN_REPARSE_POINT | BACKUP_SEMANTICS)
            .open(&path)?;
        validate_handle(&handle, SecretKind::File)?;
        acl(&path, restrict)?;
        validate_handle(&handle, SecretKind::File)?;
        Ok(handle)
    }

    pub fn open_secret_file(path: &Path) -> io::Result<File> {
        prepare(path, false, false)
    }

    pub fn restrict_secret_file(path: &Path) -> io::Result<File> {
        prepare(path, false, true)
    }

    pub fn create_secret_file(path: &Path) -> io::Result<File> {
        prepare(path, true, true)
    }
}

#[cfg(not(any(unix, windows)))]
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

    pub fn open_secret_file(_: &Path) -> io::Result<File> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secret file opening is unsupported",
        ))
    }

    pub fn restrict_secret_file(_: &Path) -> io::Result<File> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secret ACL configuration is unsupported",
        ))
    }

    pub fn create_secret_file(_: &Path) -> io::Result<File> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secret file creation is unsupported",
        ))
    }
}

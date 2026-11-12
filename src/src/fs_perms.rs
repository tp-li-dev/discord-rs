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
//! the current process user SID full control. Both its saved SDDL ACL and the
//! security descriptor are checked; the owner must also be that SID. Unknown
//! tool/output formats fail closed with `Unsupported`.
//!
//! **This is not an atomic handle-based DACL transaction.** icacls/Get-Acl use
//! paths. Holding handles reduces replacement races, but does not prove path
//! identity against drive remapping, filesystem/filter behavior, hard links,
//! privileged actors or concurrent DACL changes. All ancestors, drive mappings,
//! the process environment, temporary directory and Windows tools must be trusted.
//! ACL verification briefly writes an ACL-only UTF-16 snapshot to a random,
//! create-new temporary file and removes it before returning. Rechecking cannot
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

    // Pure parsers shared by production and sample tests. /save writes UTF-16
    // SDDL, including numeric SIDs (and well-known SDDL SID aliases). Unlike the
    // console display, neither account-name translation nor an OEM code page is
    // needed. RawSecurityDescriptor expands aliases to SecurityIdentifier.Value.
    const ACL_PARSER: &str = r#"
function Test-PrivateDacl($raw, [string] $sid) {
    if (($raw.ControlFlags -band [Security.AccessControl.ControlFlags]::DiscretionaryAclPresent) -eq 0 -or
        ($raw.ControlFlags -band [Security.AccessControl.ControlFlags]::DiscretionaryAclProtected) -eq 0 -or
        $null -eq $raw.DiscretionaryAcl -or $raw.DiscretionaryAcl.Count -ne 1) { return $false }
    $ace = $raw.DiscretionaryAcl[0]
    return ($ace.AceType -eq [Security.AccessControl.AceType]::AccessAllowed -and
        $ace.AceFlags -eq [Security.AccessControl.AceFlags]::None -and
        $ace.AccessMask -eq 2032127 -and $ace.SecurityIdentifier.Value -ceq $sid)
}
function Test-IcaclsSave([string[]] $lines, [string] $name, [string] $sid) {
    # One non-recursive /save record: literal basename followed by its DACL.
    if ($lines.Count -ne 2 -or
        -not [string]::Equals($lines[0], $name, [StringComparison]::OrdinalIgnoreCase) -or
        -not $lines[1].StartsWith('D:', [StringComparison]::Ordinal)) { return 'saved-acl-format' }
    try {
        $raw = [Security.AccessControl.RawSecurityDescriptor]::new($lines[1])
        if (-not (Test-PrivateDacl $raw $sid)) { return 'saved-acl-rejected' }
    } catch { return 'saved-acl-format' }
    return 'verified'
}
"#;

    // Static script: paths are environment data, never PowerShell source. No
    // native output or exception messages leave the child. Get-Acl checks the
    // actual owner/DACL before and after independently parsing icacls /save.
    // The temporary snapshot contains ACL metadata only, never secret contents;
    // its parent is held just like the target's ancestors. Only a successfully
    // created file is deleted. The caller must also trust this temporary directory.
    const ACL_SCRIPT: &str = r#"
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$stage = 'tool-unavailable'
$exitCode = 77
$result = 'tool-unavailable'
$snapshotCreated = $false
try {
    $path = $env:FS_PERMS_PATH
    $icacls = [IO.Path]::Combine($env:SystemRoot, 'System32\icacls.exe')
    if (-not [IO.File]::Exists($icacls)) { throw 'unavailable' }
    $stage = 'token-sid-unavailable'
    # WindowsIdentity reads the process access token through Windows APIs. Do
    # not translate to NTAccount or consult whoami/USERNAME/USERPROFILE.
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    if ($null -eq $sid -or -not $sid.Value.StartsWith('S-1-')) { throw 'unavailable' }
    function Read-Acl {
        Microsoft.PowerShell.Security\Get-Acl -LiteralPath $path
    }
    function Run-Icacls([string[]] $options) {
        # Windows PowerShell otherwise turns native stderr into a terminating
        # NativeCommandError before LASTEXITCODE can classify an ACL rejection.
        $ErrorActionPreference = 'Continue'
        $null = & $icacls $path @options /L 2>$null
        if ($LASTEXITCODE -ne 0) {
            $script:exitCode = 13
            throw 'icacls failed'
        }
    }
    if ($env:FS_PERMS_RESTRICT -eq '1') {
        $stage = 'set-owner'
        $null = Run-Icacls @('/setowner', ('*' + $sid.Value))
        $stage = 'disable-inheritance'
        $null = Run-Icacls @('/inheritance:r', '/grant:r', ('*' + $sid.Value + ':(F)'))
        $stage = 'read-acl'
        $acl = Read-Acl
        $identities = @($acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]) |
            ForEach-Object { $_.IdentityReference.Value } | Sort-Object -Unique)
        $stage = 'remove-other-aces'
        foreach ($identity in $identities) {
            if ($identity -ne $sid.Value) {
                $null = Run-Icacls @('/remove', ('*' + $identity))
            }
        }
        $stage = 'remove-deny'
        $null = Run-Icacls @('/remove:d', ('*' + $sid.Value))
        $stage = 'grant-current-user'
        $null = Run-Icacls @('/grant:r', ('*' + $sid.Value + ':(F)'))
    }
    function Assert-PrivateAcl {
        $acl = Read-Acl
        $raw = [Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(), 0)
        if (-not $acl.AreAccessRulesProtected -or $raw.Owner.Value -cne $sid.Value -or
            -not (Test-PrivateDacl $raw $sid.Value)) {
            $script:exitCode = 13
            throw 'ACL rejected'
        }
    }
    $stage = 'verify-acl'
    Assert-PrivateAcl
    $stage = 'create-acl-snapshot'
    $snapshotPath = $env:FS_PERMS_SNAPSHOT_PATH
    $snapshot = [IO.File]::Open($snapshotPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
    $snapshotCreated = $true
    $snapshot.Dispose()
    $stage = 'save-acl'
    $null = Run-Icacls @('/save', $snapshotPath)
    $stage = 'read-saved-acl'
    $lines = [IO.File]::ReadAllLines($snapshotPath, [Text.Encoding]::Unicode)
    $stage = Test-IcaclsSave $lines ([IO.Path]::GetFileName($path)) $sid.Value
    if ($stage -ne 'verified') {
        if ($stage -eq 'saved-acl-rejected') { $exitCode = 13 }
        throw 'saved ACL rejected'
    }
    $stage = 'verify-acl'
    Assert-PrivateAcl
    $result = 'verified'
    $exitCode = 0
} catch {
    $result = $stage
} finally {
    if ($snapshotCreated) {
        try { [IO.File]::Delete($snapshotPath) }
        catch { $result = 'cleanup-acl-snapshot'; $exitCode = 77 }
    }
}
[Console]::Out.Write($result)
exit $exitCode
"#;

    fn acl_failure(output: &std::process::Output) -> io::Error {
        // Whitelist child responses. Never forward arbitrary stdout/stderr,
        // exception messages, paths, account names, SIDs or native tool output.
        let reason = match output.stdout.as_slice() {
            b"tool-unavailable" => "trusted icacls executable is unavailable",
            b"token-sid-unavailable" => "current process token SID is unavailable",
            b"set-owner" => "icacls owner assignment failed",
            b"disable-inheritance" => "icacls inheritance removal failed",
            b"read-acl" => "security descriptor read failed",
            b"remove-other-aces" => "icacls removal of other principals failed",
            b"remove-deny" => "icacls deny ACE removal failed",
            b"grant-current-user" => "icacls current user grant failed",
            b"verify-acl" => "owner or protected single-user DACL verification failed",
            b"create-acl-snapshot" => "ACL snapshot creation failed in trusted temporary directory",
            b"save-acl" => "icacls SDDL export failed",
            b"read-saved-acl" => "icacls UTF-16 SDDL snapshot read failed",
            b"saved-acl-format" => "unrecognized icacls UTF-16 SDDL record",
            b"saved-acl-rejected" => "icacls SDDL is not a protected single-user DACL",
            b"cleanup-acl-snapshot" => "ACL snapshot cleanup failed",
            _ => "unrecognized ACL helper response",
        };
        let kind = if output.status.code() == Some(13) {
            io::ErrorKind::PermissionDenied
        } else {
            io::ErrorKind::Unsupported
        };
        io::Error::new(
            kind,
            format!(
                "secret ACL operation failed: {reason} (helper exit {:?})",
                output.status.code()
            ),
        )
    }

    fn powershell_command() -> io::Result<Command> {
        let root = std::env::var_os("SystemRoot").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "secret ACL: SystemRoot is unavailable",
            )
        })?;
        let drive = match Path::new(&root).components().next() {
            Some(Component::Prefix(prefix)) if matches!(prefix.kind(), Prefix::Disk(_)) => {
                prefix.as_os_str()
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "secret ACL: SystemRoot is not a local drive path",
                ));
            }
        };
        let system = Path::new(&root).join("System32");
        let mut command = Command::new(system.join("WindowsPowerShell/v1.0/powershell.exe"));
        command
            .args(["-NoLogo", "-NoProfile", "-NonInteractive", "-Command"])
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
            .creation_flags(CREATE_NO_WINDOW)
            .stdin(Stdio::null())
            .stderr(Stdio::null());
        Ok(command)
    }

    fn acl(path: &Path, restrict: bool) -> io::Result<()> {
        // Do not require write access to an existing secret's parent just to
        // check it. Use a random, create-new metadata file in the caller's trusted
        // temporary directory, with the same reparse/ancestor checks as the target.
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "secret ACL: snapshot name randomness is unavailable",
            )
        })?;
        let name: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let snapshot = local_path(&std::env::temp_dir().join(format!("fs-perms-acl-{name}")))
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    "secret ACL: temporary snapshot path is unsupported",
                )
            })?;
        let _snapshot_ancestors = hold_ancestors(&snapshot).map_err(|error| {
            io::Error::new(
                error.kind(),
                "secret ACL: temporary snapshot ancestor verification failed",
            )
        })?;
        let output = powershell_command()?
            .arg(format!("{ACL_PARSER}\n{ACL_SCRIPT}"))
            .env("FS_PERMS_PATH", path)
            .env("FS_PERMS_SNAPSHOT_PATH", &snapshot)
            .env("FS_PERMS_RESTRICT", if restrict { "1" } else { "0" })
            .output()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "secret ACL: trusted PowerShell launch failed ({:?})",
                        error.kind()
                    ),
                )
            })?;
        match output.status.code() {
            Some(0) if output.stdout == b"verified" => Ok(()),
            _ => Err(acl_failure(&output)),
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

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn icacls_saved_samples_are_sid_based_and_fail_closed() {
            // These are /save records, not the lossy/localized console display.
            // Parsing is pure: no account lookup, token query or filesystem I/O.
            let samples = r#"
function Assert-Sample($label, $lines, $name, $sid, $expected) {
    $actual = Test-IcaclsSave $lines $name $sid
    if ($actual -cne $expected) { [Console]::Out.Write($label); exit 1 }
}
$admin = 'S-1-5-21-111-222-333-500'
$runner = 'S-1-5-21-444-555-666-1001'
$name = "runneradmin 日本語 ' `$() ; [secret].txt"
# Administrator and runneradmin need no reverse lookup to a display account name.
Assert-Sample 'Administrator' @('secret.txt', "D:PAI(A;;FA;;;$admin)") 'secret.txt' $admin 'verified'
Assert-Sample 'runneradmin' @($name, "D:PAI(A;;FA;;;$runner)") $name $runner 'verified'
Assert-Sample 'numeric-mask' @($name, "D:P(A;;0x1f01ff;;;$runner)") $name $runner 'verified'
Assert-Sample 'different-user' @($name, "D:P(A;;FA;;;$admin)") $name $runner 'saved-acl-rejected'
# Display equivalents: NT AUTHORITY\SYSTEM:(I)(F), BUILTIN\Administrators:(I)(OI)(CI)(F).
Assert-Sample 'inherited-SYSTEM' @($name, "D:PAI(A;;FA;;;$runner)(A;ID;FA;;;SY)") $name $runner 'saved-acl-rejected'
Assert-Sample 'inherited-Administrators' @($name, "D:PAI(A;;FA;;;$runner)(A;OICIID;FA;;;BA)") $name $runner 'saved-acl-rejected'
Assert-Sample 'explicit-SYSTEM' @($name, "D:P(A;;FA;;;$runner)(A;;FA;;;S-1-5-18)") $name $runner 'saved-acl-rejected'
Assert-Sample 'explicit-Administrators' @($name, "D:P(A;;FA;;;$runner)(A;;FA;;;S-1-5-32-544)") $name $runner 'saved-acl-rejected'
Assert-Sample 'inherited-current-user' @($name, "D:P(A;ID;FA;;;$runner)") $name $runner 'saved-acl-rejected'
Assert-Sample 'inherit-only-current-user' @($name, "D:P(A;OIIO;FA;;;$runner)") $name $runner 'saved-acl-rejected'
Assert-Sample 'unprotected' @($name, "D:AI(A;;FA;;;$runner)") $name $runner 'saved-acl-rejected'
Assert-Sample 'deny' @($name, "D:P(D;;FA;;;$runner)") $name $runner 'saved-acl-rejected'
Assert-Sample 'read-only' @($name, "D:P(A;;FR;;;$runner)") $name $runner 'saved-acl-rejected'
Assert-Sample 'duplicate' @($name, "D:P(A;;FA;;;$runner)(A;;FA;;;$runner)") $name $runner 'saved-acl-rejected'
Assert-Sample 'empty-DACL' @($name, 'D:P') $name $runner 'saved-acl-rejected'
Assert-Sample 'null-DACL' @($name, 'D:NO_ACCESS_CONTROL') $name $runner 'saved-acl-rejected'
Assert-Sample 'malformed' @($name, 'D:invalid') $name $runner 'saved-acl-format'
Assert-Sample 'missing' @() $name $runner 'saved-acl-format'
Assert-Sample 'extra-record' @($name, "D:P(A;;FA;;;$runner)", 'other.txt', 'D:P') $name $runner 'saved-acl-format'
Assert-Sample 'wrong-path' @('other.txt', "D:P(A;;FA;;;$runner)") $name $runner 'saved-acl-format'
# A UTF-16 roundtrip preserves names that an English OEM console renders as '?'.
$record = $name + "`r`nD:P(A;;FA;;;$runner)`r`n"
$roundtrip = [Text.Encoding]::Unicode.GetString([Text.Encoding]::Unicode.GetBytes($record))
Assert-Sample 'utf16-roundtrip' ($roundtrip.TrimEnd("`r", "`n") -split "`r`n") $name $runner 'verified'
[Console]::Out.Write('verified')
"#;
            let output = powershell_command()
                .unwrap()
                .arg(format!("{ACL_PARSER}\n{samples}"))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "sample failed: {:?}",
                output.stdout
            );
            assert_eq!(output.stdout, b"verified");
        }

        #[test]
        fn acl_helper_errors_explain_stage_without_forwarding_output() {
            use std::os::windows::process::ExitStatusExt;
            let mut output = std::process::Output {
                status: std::process::ExitStatus::from_raw(77),
                stdout: b"token-sid-unavailable".to_vec(),
                stderr: b"private test fixture data".to_vec(),
            };
            let error = acl_failure(&output);
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(error.to_string().contains("current process token SID"));
            output.stdout = b"private test fixture data".to_vec();
            assert!(!acl_failure(&output).to_string().contains("fixture"));
            output.status = std::process::ExitStatus::from_raw(13);
            output.stdout = b"saved-acl-rejected".to_vec();
            assert_eq!(acl_failure(&output).kind(), io::ErrorKind::PermissionDenied);
        }
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

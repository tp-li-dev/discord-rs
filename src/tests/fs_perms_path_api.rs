use src::fs_perms::{create_secret_file, open_secret_file, restrict_secret_file};
use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).unwrap();
        let name: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = std::env::temp_dir().join(format!("fs-perms-path-{name}"));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn contents(file: &mut File) -> Vec<u8> {
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    bytes
}

#[cfg(any(unix, windows))]
#[test]
fn create_verifies_before_writing_and_open_preserves_content() {
    let fixture = Fixture::new();
    let path = fixture.path("secret");
    let mut file = create_secret_file(&path, b"public test fixture").unwrap();
    assert_eq!(file.stream_position().unwrap(), 19);
    assert_eq!(contents(&mut file), b"public test fixture");
    assert_private(&path, &file);
    drop(file);
    let mut opened = open_secret_file(&path).unwrap();
    assert_eq!(contents(&mut opened), b"public test fixture");
}

#[cfg(any(unix, windows))]
#[test]
fn create_does_not_truncate_an_existing_file() {
    let fixture = Fixture::new();
    let path = fixture.path("existing");
    fs::write(&path, b"original fixture").unwrap();
    assert_eq!(
        create_secret_file(&path, b"replacement fixture")
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(&path).unwrap(), b"original fixture");
}

#[cfg(any(unix, windows))]
#[test]
fn directory_and_missing_file_are_rejected() {
    let fixture = Fixture::new();
    for result in [
        open_secret_file(&fixture.0),
        restrict_secret_file(&fixture.0),
    ] {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }
    assert_eq!(
        open_secret_file(fixture.path("missing"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}

#[cfg(unix)]
fn assert_private(_: &Path, file: &File) {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        file.metadata().unwrap().permissions().mode() & 0o7777,
        0o600
    );
}

#[cfg(unix)]
#[test]
fn unix_existing_mode_checks_and_symlink_semantics_are_preserved() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let fixture = Fixture::new();
    let path = fixture.path("existing");
    fs::write(&path, b"original fixture").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        open_secret_file(&path).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    let link = fixture.path("link");
    symlink(&path, &link).unwrap();
    let mut file = restrict_secret_file(&link).unwrap();
    assert_private(&path, &file);
    assert_eq!(contents(&mut file), b"original fixture");
    open_secret_file(&link).unwrap();
}

#[cfg(windows)]
fn powershell(path: &Path, script: &str) -> std::process::Output {
    use std::{
        os::windows::process::CommandExt,
        process::{Command, Stdio},
    };
    let root = std::env::var_os("SystemRoot").unwrap();
    let system = Path::new(&root).join("System32");
    let output = Command::new(system.join("WindowsPowerShell/v1.0/powershell.exe"))
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            script,
        ])
        .env_clear()
        .env("SystemRoot", &root)
        .env("WINDIR", &root)
        .env(
            "SystemDrive",
            Path::new(&root).components().next().unwrap().as_os_str(),
        )
        .env("PATHEXT", ".EXE")
        .env(
            "PSModulePath",
            system.join("WindowsPowerShell/v1.0/Modules"),
        )
        .env("FS_PERMS_TEST_PATH", path)
        .creation_flags(0x0800_0000)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(output.status.success(), "test ACL helper failed");
    output
}

#[cfg(windows)]
fn assert_private(path: &Path, _: &File) {
    // Independent check of the actual DACL, including inheritance and owner.
    powershell(
        path,
        r#"
$ErrorActionPreference = 'Stop'
try {
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $acl = Get-Acl -LiteralPath $env:FS_PERMS_TEST_PATH
    $descriptor = [Security.AccessControl.RawSecurityDescriptor]::new($acl.Sddl)
    if (-not $acl.AreAccessRulesProtected -or $descriptor.Owner.Value -ne $sid -or
        $descriptor.DiscretionaryAcl.Count -ne 1) { exit 1 }
    $ace = $descriptor.DiscretionaryAcl[0]
    if ($ace.SecurityIdentifier.Value -ne $sid -or $ace.AccessMask -ne 2032127 -or
        $ace.AceType.ToString() -ne 'AccessAllowed' -or $ace.AceFlags.ToString() -ne 'None') { exit 2 }
    exit 0
} catch { exit 3 }
"#,
    );
}

#[cfg(windows)]
#[test]
fn windows_removes_explicit_allow_deny_and_inherited_aces() {
    let fixture = Fixture::new();
    let path = fixture.path("broad-acl");
    fs::write(&path, b"original fixture").unwrap();
    powershell(
        &path,
        r#"
$ErrorActionPreference = 'Stop'
$tool = Join-Path $env:SystemRoot 'System32\icacls.exe'
$null = & $tool $env:FS_PERMS_TEST_PATH /grant '*S-1-1-0:(R)' '*S-1-5-32-545:(R)' /L
if ($LASTEXITCODE -ne 0) { exit 1 }
$null = & $tool $env:FS_PERMS_TEST_PATH /deny '*S-1-5-7:(R)' /L
exit $LASTEXITCODE
"#,
    );
    assert_eq!(
        open_secret_file(&path).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    let mut file = restrict_secret_file(&path).unwrap();
    assert_private(&path, &file);
    assert_eq!(contents(&mut file), b"original fixture");
    drop(file);
    open_secret_file(&path).unwrap();
}

#[cfg(windows)]
#[test]
fn windows_rechecks_permissions_on_every_open() {
    let fixture = Fixture::new();
    let path = fixture.path("changed-acl");
    drop(create_secret_file(&path, b"public fixture").unwrap());
    powershell(
        &path,
        r#"
$tool = Join-Path $env:SystemRoot 'System32\icacls.exe'
$null = & $tool $env:FS_PERMS_TEST_PATH /grant '*S-1-1-0:(R)' /L
exit $LASTEXITCODE
"#,
    );
    assert_eq!(
        open_secret_file(&path).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    let file = restrict_secret_file(&path).unwrap();
    assert_private(&path, &file);
}

#[cfg(windows)]
#[test]
fn windows_acl_failure_leaves_no_content() {
    let fixture = Fixture::new();
    // OWNER RIGHTS suppresses the owner's implicit WRITE_DAC. Descendants can
    // still be created/read/written/deleted, but changing their DACL must fail.
    powershell(
        &fixture.0,
        r#"
$tool = Join-Path $env:SystemRoot 'System32\icacls.exe'
$null = & $tool $env:FS_PERMS_TEST_PATH /deny '*S-1-3-4:(OI)(CI)(WDAC,WO)' /L
exit $LASTEXITCODE
"#,
    );
    let path = fixture.path("no-write-before-acl");
    assert!(create_secret_file(&path, b"must never be written").is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
}

#[cfg(windows)]
#[test]
fn windows_file_handle_blocks_readers_writers_and_replacement() {
    let fixture = Fixture::new();
    let path = fixture.path("held");
    let file = create_secret_file(&path, b"public fixture").unwrap();
    assert!(File::open(&path).is_err());
    assert!(fs::OpenOptions::new().write(true).open(&path).is_err());
    assert!(fs::rename(&path, fixture.path("renamed")).is_err());
    drop(file);
    open_secret_file(&path).unwrap();
}

#[cfg(windows)]
#[test]
fn windows_unicode_spaces_and_shell_metacharacters_are_literal() {
    let fixture = Fixture::new();
    let path = fixture.path("日本語 ' $() ; [secret].txt");
    let file = create_secret_file(&path, b"public fixture").unwrap();
    assert_private(&path, &file);
    drop(file);
    open_secret_file(&path).unwrap();
}

#[cfg(windows)]
#[test]
fn windows_unicode_profile_style_parent_and_filename_are_literal() {
    let fixture = Fixture::new();
    let parent = fixture.path("runneradmin 日本語 ' $() ; [profile]");
    fs::create_dir(&parent).unwrap();
    let path = parent.join("秘密 🔒 ' $() ; [secret].txt");
    let file = create_secret_file(&path, b"public fixture")
        .unwrap_or_else(|error| panic!("Unicode parent ACL preparation failed: {error}"));
    assert_private(&path, &file);
    drop(file);
    let mut file = open_secret_file(&path)
        .unwrap_or_else(|error| panic!("Unicode parent ACL verification failed: {error}"));
    assert_eq!(contents(&mut file), b"public fixture");
    // Verification must not leave ACL metadata files next to the secret.
    assert_eq!(fs::read_dir(&parent).unwrap().count(), 1);
}

#[cfg(windows)]
#[test]
fn windows_unsupported_names_do_not_create_files() {
    let fixture = Fixture::new();
    for path in [
        fixture.path("base:stream"),
        fixture.path("wild*card"),
        fixture.path("trailing."),
        fixture.path("CON"),
        fixture.path("..\\outside"),
        PathBuf::from(r"\\server\share\secret"),
        PathBuf::from(r"\\.\NUL"),
        PathBuf::new(),
    ] {
        for result in [
            create_secret_file(&path, b"public fixture"),
            open_secret_file(&path),
            restrict_secret_file(&path),
        ] {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Unsupported);
        }
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
}

#[cfg(windows)]
#[test]
fn windows_symlinks_including_dangling_links_are_rejected() {
    let fixture = Fixture::new();
    let target = fixture.path("target");
    fs::write(&target, b"original fixture").unwrap();
    let link = fixture.path("link");
    if let Err(error) = std::os::windows::fs::symlink_file(&target, &link) {
        if matches!(error.raw_os_error(), Some(5 | 1314)) {
            eprintln!("SKIP: Windows denied symlink creation");
            return;
        }
        panic!("test symlink creation failed");
    }
    for result in [open_secret_file(&link), restrict_secret_file(&link)] {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }
    assert!(create_secret_file(&link, b"replacement fixture").is_err());
    assert_eq!(fs::read(&target).unwrap(), b"original fixture");
    fs::remove_file(&target).unwrap();
    assert!(open_secret_file(&link).is_err());
    assert!(create_secret_file(&link, b"replacement fixture").is_err());
    assert!(!target.exists());
}

#[cfg(windows)]
#[test]
fn windows_junction_ancestor_is_rejected_without_changing_target() {
    let fixture = Fixture::new();
    let target = fixture.path("target-dir");
    fs::create_dir(&target).unwrap();
    fs::write(target.join("existing"), b"original fixture").unwrap();
    powershell(
        &fixture.0,
        r#"
$ErrorActionPreference = 'Stop'
try {
    $null = New-Item -ItemType Junction -Path (Join-Path $env:FS_PERMS_TEST_PATH 'junction') `
        -Target (Join-Path $env:FS_PERMS_TEST_PATH 'target-dir')
    exit 0
} catch { exit 1 }
"#,
    );
    let link = fixture.path("junction");
    for result in [
        open_secret_file(&link),
        restrict_secret_file(&link),
        open_secret_file(link.join("existing")),
        restrict_secret_file(link.join("existing")),
        create_secret_file(link.join("new"), b"replacement fixture"),
    ] {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }
    assert!(!target.join("new").exists());
    assert_eq!(
        fs::read(target.join("existing")).unwrap(),
        b"original fixture"
    );
}

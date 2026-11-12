use src::fs_perms::{SecretKind, check, restrict};
use std::{
    fs::{self, File},
    io,
    path::PathBuf,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).unwrap();
        let name: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let path = std::env::temp_dir().join(format!("fs-perms-{name}"));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(unix)]
#[test]
fn unix_modes_types_and_handle_identity() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Fixture::new();
    let path = dir.0.join("secret");
    let file = File::create(&path).unwrap();
    file.set_permissions(fs::Permissions::from_mode(0o644))
        .unwrap();
    assert_eq!(
        check(&file, SecretKind::File).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    restrict(&file, SecretKind::File).unwrap();
    assert_eq!(
        file.metadata().unwrap().permissions().mode() & 0o7777,
        0o600
    );
    check(&file, SecretKind::File).unwrap();
    assert_eq!(
        restrict(&file, SecretKind::Directory).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    file.set_permissions(fs::Permissions::from_mode(0o4600))
        .unwrap();
    assert_eq!(
        check(&file, SecretKind::File).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    let moved = dir.0.join("moved");
    fs::rename(&path, &moved).unwrap();
    let replacement = File::create(&path).unwrap();
    replacement
        .set_permissions(fs::Permissions::from_mode(0o644))
        .unwrap();
    restrict(&file, SecretKind::File).unwrap();
    assert_eq!(
        replacement.metadata().unwrap().permissions().mode() & 0o7777,
        0o644
    );
    let directory = File::open(&dir.0).unwrap();
    restrict(&directory, SecretKind::Directory).unwrap();
    check(&directory, SecretKind::Directory).unwrap();
    assert_eq!(
        directory.metadata().unwrap().permissions().mode() & 0o7777,
        0o700
    );
}

#[cfg(not(unix))]
#[test]
fn unsupported_platform_fails_closed() {
    let dir = Fixture::new();
    let file = File::create(dir.0.join("secret")).unwrap();
    for kind in [SecretKind::File, SecretKind::Directory] {
        assert_eq!(
            check(&file, kind).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            restrict(&file, kind).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
}

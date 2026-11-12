#![cfg(windows)]

use src::fs_perms::{SecretKind, check, restrict};
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::windows::{fs::OpenOptionsExt, io::AsRawHandle},
    path::PathBuf,
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).unwrap();
        let name: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = std::env::temp_dir().join(format!("fs-perms-links-{name}"));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetFinalPathNameByHandleW(
        handle: std::os::windows::io::RawHandle,
        path: *mut u16,
        len: u32,
        flags: u32,
    ) -> u32;
}

fn final_path(handle: &File) -> Vec<u16> {
    let mut path = vec![0_u16; 32768];
    // The buffer remains valid for the duration of the call and the handle is open.
    let len =
        unsafe { GetFinalPathNameByHandleW(handle.as_raw_handle(), path.as_mut_ptr(), 32768, 0) }
            as usize;
    assert!(len > 0 && len < path.len(), "final path query failed");
    path.truncate(len);
    path
}

fn assert_unsupported(handle: &File, kind: SecretKind) {
    assert_eq!(
        check(handle, kind).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        restrict(handle, kind).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}

#[test]
fn windows_handles_remain_fail_closed() {
    let fixture = Fixture::new();
    let file = File::create(fixture.0.join("secret")).unwrap();
    assert_unsupported(&file, SecretKind::File);
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(0x02000000) // FILE_FLAG_BACKUP_SEMANTICS
        .open(&fixture.0)
        .unwrap();
    assert_unsupported(&directory, SecretKind::Directory);
}

#[test]
fn followed_symlink_does_not_reveal_its_open_path() {
    let fixture = Fixture::new();
    let target = fixture.0.join("target");
    let link = fixture.0.join("link");
    File::create(&target).unwrap();
    if let Err(error) = std::os::windows::fs::symlink_file(&target, &link) {
        // Creating symlinks may require Developer Mode or a privilege.
        if matches!(error.raw_os_error(), Some(5 | 1314)) {
            eprintln!("symlink check skipped: Windows denied link creation");
            return;
        }
        panic!("could not create test symlink: {error}");
    }
    let direct = File::open(&target).unwrap();
    let followed = File::open(&link).unwrap();
    assert_eq!(final_path(&direct), final_path(&followed));
    assert_unsupported(&followed, SecretKind::File);
}

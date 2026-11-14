//! `--secret-file` の権限検査・形式検査・生成の統合試験（実ファイルシステム使用）。
//! 所有者が別ユーザー・root の経路は非 root では作れないため、純関数の単体試験
//! （`secret_file::tests`）で扱う。Linux/Unix のみ。
#![cfg(unix)]

use src::secret_file::{SecretFileError, generate_secret_file, load_secret_file};
use std::{
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const GOOD: &str = "a3f19c0be4d7625810cf3a9e7b2d4468905e1fa7c3b8d2e60491a5f7e8c0b3d9";

fn directory(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("secret-file-tests");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

/// umask に依存せず、指定したモードで内容を書く。
fn write_with_mode(path: &Path, contents: &[u8], mode: u32) {
    std::fs::write(path, contents).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn good_file(dir: &Path, name: &str, mode: u32) -> PathBuf {
    let path = dir.join(name);
    write_with_mode(&path, format!("{GOOD}\n").as_bytes(), mode);
    path
}

#[test]
fn accepts_owner_only_modes_and_returns_the_value() {
    let dir = directory("accept");
    for (name, mode) in [("a", 0o600), ("b", 0o400)] {
        let path = good_file(&dir, name, mode);
        let secret = load_secret_file(&path).unwrap();
        assert_eq!(secret.expose_secret()[0], 0xa3);
        assert_eq!(secret.expose_secret()[31], 0xd9);
    }
    // 前後の空白・CRLF も許容する。
    let path = dir.join("padded");
    write_with_mode(&path, format!("\n  {GOOD}\r\n\r\n").as_bytes(), 0o600);
    assert!(load_secret_file(&path).is_ok());
}

#[test]
fn rejects_group_or_other_access_and_special_bits() {
    let dir = directory("modes");
    // 所有者が読めないモード（0200/0000）は open で先に失敗するためここでは扱わない。
    for mode in [
        0o640, 0o604, 0o660, 0o606, 0o644, 0o666, 0o700, 0o500, 0o1600, 0o4600,
    ] {
        let path = good_file(&dir, &format!("m{mode:o}"), mode);
        assert_eq!(
            load_secret_file(&path).err(),
            Some(SecretFileError::InsecureMode),
            "{mode:o}"
        );
    }
}

#[test]
fn rejects_symlinks_directories_hard_links_and_bad_paths() {
    let dir = directory("types");
    let real = good_file(&dir, "real", 0o600);

    let link = dir.join("link");
    symlink(&real, &link).unwrap();
    assert_eq!(
        load_secret_file(&link).err(),
        Some(SecretFileError::Symlink)
    );

    let hard = dir.join("hard");
    std::fs::hard_link(&real, &hard).unwrap();
    assert_eq!(
        load_secret_file(&real).err(),
        Some(SecretFileError::HardLinked)
    );
    assert_eq!(
        load_secret_file(&hard).err(),
        Some(SecretFileError::HardLinked)
    );

    let subdir = dir.join("sub");
    std::fs::create_dir(&subdir).unwrap();
    std::fs::set_permissions(&subdir, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        load_secret_file(&subdir).err(),
        Some(SecretFileError::NotRegularFile)
    );

    assert_eq!(
        load_secret_file(Path::new("relative.secret")).err(),
        Some(SecretFileError::PathInvalid)
    );
    assert_eq!(
        load_secret_file(&dir.join("sub").join("..").join("real")).err(),
        Some(SecretFileError::PathInvalid)
    );
    assert_eq!(
        load_secret_file(&dir.join("missing")).err(),
        Some(SecretFileError::Unreadable)
    );
}

#[test]
fn rejects_bad_content_after_the_permission_check() {
    let dir = directory("content");
    let cases: Vec<(&str, Vec<u8>, SecretFileError)> = vec![
        ("empty", Vec::new(), SecretFileError::InvalidFormat),
        ("short", b"abcd\n".to_vec(), SecretFileError::InvalidFormat),
        (
            "nonhex",
            format!("{}zz\n", &GOOD[..62]).into_bytes(),
            SecretFileError::InvalidFormat,
        ),
        ("binary", vec![0x9fu8; 32], SecretFileError::InvalidFormat),
        (
            "zero",
            format!("{}\n", "0".repeat(64)).into_bytes(),
            SecretFileError::WeakSecret,
        ),
        (
            "repeat",
            format!("{}\n", "ab".repeat(32)).into_bytes(),
            SecretFileError::WeakSecret,
        ),
        (
            "big",
            format!("{GOOD}{}\n", " ".repeat(200)).into_bytes(),
            SecretFileError::TooLarge,
        ),
    ];
    for (name, contents, expected) in cases {
        let path = dir.join(name);
        write_with_mode(&path, &contents, 0o600);
        assert_eq!(load_secret_file(&path).err(), Some(expected), "{name}");
    }
}

#[test]
fn error_text_contains_neither_path_nor_secret() {
    let dir = directory("redaction");
    let path = good_file(&dir, "secret-name-marker", 0o644);
    let error = load_secret_file(&path).err().unwrap();
    for text in [error.to_string(), format!("{error:?}")] {
        assert!(!text.contains("secret-name-marker"));
        assert!(!text.contains(dir.to_str().unwrap()));
        assert!(!text.contains(&GOOD[..16]));
    }
}

#[test]
fn generate_creates_a_private_loadable_file_and_never_overwrites() {
    let dir = directory("generate");
    let path = dir.join("botd.secret");
    generate_secret_file(&path).unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    assert!(metadata.is_file());
    assert_eq!(metadata.mode() & 0o7777, 0o600);
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(text.len(), 65);
    assert!(text.ends_with('\n'));
    assert!(
        text[..64]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    let first = *load_secret_file(&path).unwrap().expose_secret();

    // 既存ファイルは上書きしない（内容も変わらない）。
    assert_eq!(
        generate_secret_file(&path).err(),
        Some(SecretFileError::AlreadyExists)
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);

    // 既存のシンボリックリンクにも書かない。
    let target = dir.join("target");
    let link = dir.join("link");
    symlink(&target, &link).unwrap();
    assert!(generate_secret_file(&link).is_err());
    assert!(!target.exists());

    let other = dir.join("other.secret");
    generate_secret_file(&other).unwrap();
    assert_ne!(first, *load_secret_file(&other).unwrap().expose_secret());

    assert_eq!(
        generate_secret_file(Path::new("relative.secret")).err(),
        Some(SecretFileError::PathInvalid)
    );
}

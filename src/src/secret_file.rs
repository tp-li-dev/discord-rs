//! 管理用 secret（32 バイト）のファイル渡し（`botd --secret-file` / `botctl --secret-file`）。
//!
//! ファイル形式は、32 バイトの secret を 64 文字の 16 進（大文字小文字可）で書いた 1 行です。前後の空白・
//! 改行は許容し、内部に空白があるもの・生バイナリは拒否します。
//!
//! 読み込み前に Unix でのみ次を検査します（Linux 以外・非 Unix は常に [`SecretFileError::Unsupported`]）。
//! - 絶対パスで `..` を含まないこと。最終要素がシンボリックリンクでない通常ファイルであること
//!   （`lstat` と `open` 後の `fstat` の dev/ino が一致すること。途中ディレクトリのリンクは検査しない）。
//! - ハードリンク数が 1 であること。
//! - モードがちょうど 0600 または 0400 であること（グループ/その他の権限なし。特殊ビットなし）。
//! - 所有者が現在の実効ユーザーであること。例外として、所有者が root で 0400 のとき、かつパスが環境変数
//!   `CREDENTIALS_DIRECTORY`（systemd の `LoadCredential=` が作るディレクトリ）配下のときだけ許容する。
//!
//! `fs_perms` の `open_secret_file` は 0600 固定・所有者検査なし・リンク検査なしのため、この要件には足りず
//! ここで独自に検査します（新規作成は `fs_perms::create_secret_file` を使用）。
//!
//! エラーは理由の種別だけを表し、secret・パス・OS のエラー文字列を含みません。読み込み用バッファは使用後に
//! 消去しますが（コンパイラに最適化されにくい書込みを行う最善努力）、メモリ全体からの完全な消去は保証しません。

use crate::redact::Secret;
use std::{ffi::OsStr, fmt, path::Path};

pub const SECRET_BYTES: usize = 32;
const HEX_LEN: usize = SECRET_BYTES * 2;
/// 64 文字の 16 進 + 余白の改行・空白に十分な上限。これを超えるファイルは拒否する。
const MAX_FILE_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretFileError {
    Unsupported,
    PathInvalid,
    Unreadable,
    Symlink,
    NotRegularFile,
    Raced,
    HardLinked,
    InsecureMode,
    WrongOwner,
    TooLarge,
    InvalidFormat,
    WeakSecret,
    EntropyUnavailable,
    AlreadyExists,
    WriteFailed,
}

impl SecretFileError {
    /// 理由の種別だけを表す固定文字列。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "secret file is supported only on Linux/Unix",
            Self::PathInvalid => "secret file path must be absolute without parent traversal",
            Self::Unreadable => "secret file is unreadable",
            Self::Symlink => "secret file must not be a symbolic link",
            Self::NotRegularFile => "secret file must be a regular file",
            Self::Raced => "secret file changed while being opened",
            Self::HardLinked => "secret file must not have additional hard links",
            Self::InsecureMode => "secret file mode must be 0600 or 0400",
            Self::WrongOwner => "secret file owner is not permitted",
            Self::TooLarge => "secret file is too large",
            Self::InvalidFormat => "secret file must contain 64 hexadecimal characters",
            Self::WeakSecret => "secret value is too weak",
            Self::EntropyUnavailable => "cannot generate a secret",
            Self::AlreadyExists => "secret output file already exists",
            Self::WriteFailed => "cannot write the secret file",
        }
    }
}

impl fmt::Display for SecretFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for SecretFileError {}

/// バッファを 0 で上書きする。最適化で消されないよう volatile 書込みと fence を使う。
pub fn wipe(buffer: &mut [u8]) {
    for byte in buffer.iter_mut() {
        // SAFETY: `byte` is a valid, aligned, exclusive reference.
        unsafe { std::ptr::write_volatile(byte, 0) };
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

/// 全ゼロ・同一バイトの繰返し・異なるバイト値が 10 種類未満・連続する昇順/降順の列を弱い値として扱う。
pub fn is_weak_secret(secret: &[u8; SECRET_BYTES]) -> bool {
    let mut seen = [false; 256];
    let mut distinct = 0usize;
    for byte in secret {
        if !seen[usize::from(*byte)] {
            seen[usize::from(*byte)] = true;
            distinct += 1;
        }
    }
    distinct < 10
        || secret.windows(2).all(|w| w[1] == w[0].wrapping_add(1))
        || secret.windows(2).all(|w| w[1] == w[0].wrapping_sub(1))
}

fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// 64 文字の 16 進（前後の ASCII 空白・改行は許容）を 32 バイトへ変換する。弱い値は拒否する。
pub fn parse_hex_secret(input: &[u8]) -> Result<Secret<[u8; SECRET_BYTES]>, SecretFileError> {
    let trimmed = input.trim_ascii();
    if trimmed.len() != HEX_LEN {
        return Err(SecretFileError::InvalidFormat);
    }
    let mut out = [0u8; SECRET_BYTES];
    for (slot, pair) in out.iter_mut().zip(trimmed.chunks_exact(2)) {
        match (nibble(pair[0]), nibble(pair[1])) {
            (Some(high), Some(low)) => *slot = high << 4 | low,
            _ => {
                wipe(&mut out);
                return Err(SecretFileError::InvalidFormat);
            }
        }
    }
    if is_weak_secret(&out) {
        wipe(&mut out);
        return Err(SecretFileError::WeakSecret);
    }
    let secret = Secret::new(out);
    wipe(&mut out);
    Ok(secret)
}

/// `path` が systemd の credentials ディレクトリ（`CREDENTIALS_DIRECTORY`）の厳密な配下か。
pub fn under_credentials_directory(path: &Path, credentials: Option<&OsStr>) -> bool {
    let Some(directory) = credentials.filter(|d| !d.is_empty()).map(Path::new) else {
        return false;
    };
    directory.is_absolute()
        && !directory
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        && !path
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        && path != directory
        && path.starts_with(directory)
}

/// モードと所有者の判定（純関数）。`mode` は `st_mode`、`uid` はファイルの所有者。
pub fn check_permissions(
    mode: u32,
    uid: u32,
    effective_uid: u32,
    under_credentials: bool,
) -> Result<(), SecretFileError> {
    let bits = mode & 0o7777;
    if bits != 0o600 && bits != 0o400 {
        return Err(SecretFileError::InsecureMode);
    }
    if uid == effective_uid || (uid == 0 && bits == 0o400 && under_credentials) {
        Ok(())
    } else {
        Err(SecretFileError::WrongOwner)
    }
}

/// 権限を検査してから secret ファイルを読む。非 Unix では常に `Unsupported`。
pub fn load_secret_file(path: &Path) -> Result<Secret<[u8; SECRET_BYTES]>, SecretFileError> {
    #[cfg(unix)]
    {
        load_with(
            path,
            effective_uid(),
            std::env::var_os("CREDENTIALS_DIRECTORY").as_deref(),
        )
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(SecretFileError::Unsupported)
    }
}

#[cfg(unix)]
fn effective_uid() -> u32 {
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    // SAFETY: POSIX geteuid has no preconditions and cannot fail.
    unsafe { geteuid() }
}

#[cfg(unix)]
fn load_with(
    path: &Path,
    effective_uid: u32,
    credentials: Option<&OsStr>,
) -> Result<Secret<[u8; SECRET_BYTES]>, SecretFileError> {
    use std::{io::Read, os::unix::fs::MetadataExt};
    if !path.is_absolute()
        || path
            .components()
            .any(|c| c == std::path::Component::ParentDir)
    {
        return Err(SecretFileError::PathInvalid);
    }
    let link = std::fs::symlink_metadata(path).map_err(|_| SecretFileError::Unreadable)?;
    if link.file_type().is_symlink() {
        return Err(SecretFileError::Symlink);
    }
    if !link.is_file() {
        return Err(SecretFileError::NotRegularFile);
    }
    let mut file = std::fs::File::open(path).map_err(|_| SecretFileError::Unreadable)?;
    let metadata = file.metadata().map_err(|_| SecretFileError::Unreadable)?;
    if !metadata.is_file() {
        return Err(SecretFileError::NotRegularFile);
    }
    if metadata.dev() != link.dev() || metadata.ino() != link.ino() {
        return Err(SecretFileError::Raced);
    }
    if metadata.nlink() != 1 {
        return Err(SecretFileError::HardLinked);
    }
    check_permissions(
        metadata.mode(),
        metadata.uid(),
        effective_uid,
        under_credentials_directory(path, credentials),
    )?;
    let mut buffer = [0u8; MAX_FILE_BYTES + 1];
    let mut length = 0usize;
    let read = loop {
        if length == buffer.len() {
            break Ok(length);
        }
        match file.read(&mut buffer[length..]) {
            Ok(0) => break Ok(length),
            Ok(count) => length += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break Err(SecretFileError::Unreadable),
        }
    };
    let result = match read {
        Ok(length) if length > MAX_FILE_BYTES => Err(SecretFileError::TooLarge),
        Ok(length) => parse_hex_secret(&buffer[..length]),
        Err(error) => Err(error),
    };
    wipe(&mut buffer);
    result
}

/// `/dev/urandom` 由来の 32 バイトを 16 進 1 行で、モード 0600 の新規ファイルとして書く。
/// 既存のファイルは上書きしない（`create_new`）。生成した値は返さず、ログにも出さない。
pub fn generate_secret_file(path: &Path) -> Result<(), SecretFileError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| c == std::path::Component::ParentDir)
    {
        return Err(SecretFileError::PathInvalid);
    }
    let mut bytes = [0u8; SECRET_BYTES];
    let mut attempts = 0;
    loop {
        if getrandom::fill(&mut bytes).is_err() {
            wipe(&mut bytes);
            return Err(SecretFileError::EntropyUnavailable);
        }
        if !is_weak_secret(&bytes) {
            break;
        }
        attempts += 1;
        if attempts >= 8 {
            wipe(&mut bytes);
            return Err(SecretFileError::EntropyUnavailable);
        }
    }
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut line = [0u8; HEX_LEN + 1];
    for (index, byte) in bytes.iter().enumerate() {
        line[index * 2] = DIGITS[usize::from(byte >> 4)];
        line[index * 2 + 1] = DIGITS[usize::from(byte & 0x0f)];
    }
    line[HEX_LEN] = b'\n';
    wipe(&mut bytes);
    let result = match crate::fs_perms::create_secret_file(path, &line) {
        Ok(file) => file.sync_all().map_err(|_| SecretFileError::WriteFailed),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(SecretFileError::AlreadyExists)
        }
        Err(_) => Err(SecretFileError::WriteFailed),
    };
    wipe(&mut line);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "a3f19c0be4d7625810cf3a9e7b2d4468905e1fa7c3b8d2e60491a5f7e8c0b3d9";

    #[test]
    fn parses_hex_with_surrounding_whitespace() {
        let a = parse_hex_secret(GOOD.as_bytes()).unwrap();
        assert_eq!(a.expose_secret()[0], 0xa3);
        assert_eq!(a.expose_secret()[31], 0xd9);
        let b = parse_hex_secret(format!("  \t{GOOD}\r\n\n").as_bytes()).unwrap();
        assert_eq!(a.expose_secret(), b.expose_secret());
        let upper = parse_hex_secret(GOOD.to_uppercase().as_bytes()).unwrap();
        assert_eq!(a.expose_secret(), upper.expose_secret());
    }

    #[test]
    fn rejects_bad_formats() {
        let bad: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"\n".to_vec(),
            GOOD.as_bytes()[..62].to_vec(),
            format!("{GOOD}00").into_bytes(),
            format!("{}g", &GOOD[..63]).into_bytes(),
            format!("{} {}", &GOOD[..32], &GOOD[32..]).into_bytes(),
            format!("{GOOD}\n{GOOD}\n").into_bytes(),
            format!("0x{}", &GOOD[..62]).into_bytes(),
            vec![0xa3; 32],
            "{\"secret\":[1,2,3]}".as_bytes().to_vec(),
        ];
        for input in bad {
            assert_eq!(
                parse_hex_secret(&input).err(),
                Some(SecretFileError::InvalidFormat)
            );
        }
    }

    #[test]
    fn rejects_weak_values() {
        let weak = [
            "0".repeat(64),
            "ff".repeat(32),
            "ab".repeat(32),
            "0123456789abcdef".repeat(4),
            (0u8..32).map(|b| format!("{b:02x}")).collect::<String>(),
            (0u8..32)
                .map(|b| format!("{:02x}", 200u8.wrapping_sub(b)))
                .collect::<String>(),
        ];
        for value in weak {
            assert_eq!(
                parse_hex_secret(value.as_bytes()).err(),
                Some(SecretFileError::WeakSecret),
                "{value}"
            );
        }
    }

    #[test]
    fn permission_rules() {
        assert!(check_permissions(0o100600, 1000, 1000, false).is_ok());
        assert!(check_permissions(0o100400, 1000, 1000, false).is_ok());
        for mode in [
            0o640, 0o604, 0o660, 0o644, 0o700, 0o200, 0o1600, 0o4600, 0o000,
        ] {
            assert_eq!(
                check_permissions(0o100000 | mode, 1000, 1000, false),
                Err(SecretFileError::InsecureMode),
                "{mode:o}"
            );
        }
        assert_eq!(
            check_permissions(0o100600, 1001, 1000, false),
            Err(SecretFileError::WrongOwner)
        );
        // root 所有は credentials ディレクトリ配下かつ 0400 のときだけ。
        assert!(check_permissions(0o100400, 0, 1000, true).is_ok());
        assert_eq!(
            check_permissions(0o100400, 0, 1000, false),
            Err(SecretFileError::WrongOwner)
        );
        assert_eq!(
            check_permissions(0o100600, 0, 1000, true),
            Err(SecretFileError::WrongOwner)
        );
        assert_eq!(
            check_permissions(0o100400, 1001, 1000, true),
            Err(SecretFileError::WrongOwner)
        );
    }

    #[test]
    fn credentials_directory_matching_is_strict() {
        let dir = Some(OsStr::new("/run/credentials/botd.service"));
        let inside = Path::new("/run/credentials/botd.service/botd.secret");
        assert!(under_credentials_directory(inside, dir));
        assert!(!under_credentials_directory(inside, None));
        assert!(!under_credentials_directory(inside, Some(OsStr::new(""))));
        assert!(!under_credentials_directory(
            Path::new("/run/credentials/botd.service"),
            dir
        ));
        // 文字列としての前方一致ではなく要素単位で比較する。
        assert!(!under_credentials_directory(
            Path::new("/run/credentials/botd.service-evil/botd.secret"),
            dir
        ));
        assert!(!under_credentials_directory(
            Path::new("/run/credentials/botd.service/../x"),
            dir
        ));
        assert!(!under_credentials_directory(
            inside,
            Some(OsStr::new("relative/dir"))
        ));
    }

    #[test]
    fn wipe_zeroes_the_buffer() {
        let mut buffer = [0x5au8; 48];
        wipe(&mut buffer);
        assert_eq!(buffer, [0u8; 48]);
    }

    #[test]
    fn errors_never_format_secrets() {
        let text = format!("{}", SecretFileError::InvalidFormat);
        assert!(!text.contains(GOOD));
        let secret = parse_hex_secret(GOOD.as_bytes()).unwrap();
        assert!(!format!("{secret:?}").contains("a3f1"));
    }
}

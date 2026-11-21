//! Read the Linux boot ID, including under systemd's `ProcSubset=pid`.

use std::{io, path::Path};

const PROC_PATH: &str = "/proc/sys/kernel/random/boot_id";
pub(crate) const CREDENTIAL: &str = "identify-boot-id";

pub(crate) fn read() -> io::Result<String> {
    let credentials = std::env::var_os("CREDENTIALS_DIRECTORY").map(std::path::PathBuf::from);
    read_from(Path::new(PROC_PATH), credentials.as_deref())
}

pub(crate) fn read_from(proc_path: &Path, credentials: Option<&Path>) -> io::Result<String> {
    let contents = match std::fs::read_to_string(proc_path) {
        Ok(contents) => contents,
        Err(error) => match credentials {
            Some(dir) => std::fs::read_to_string(dir.join(CREDENTIAL))?,
            None => return Err(error),
        },
    };
    let boot = contents.trim();
    if !valid(boot) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid kernel boot ID",
        ));
    }
    Ok(boot.to_owned())
}

pub(crate) fn valid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

// Read the managed test credential without printing it or spawning children.
fn fixture_credential(name: &str) -> Option<std::ffi::OsString> {
    #[cfg(target_os = "linux")]
    {
        use std::{
            io::Read,
            os::{
                fd::FromRawFd,
                unix::{ffi::OsStringExt, net::UnixStream},
            },
        };
        assert!(std::env::var_os(name).is_none());
        let metadata = std::env::var("DISCORD_RS_GATEWAY_TOKEN_FD").ok()?;
        let (fd, source) = metadata.split_once(':').unwrap();
        assert_eq!(source, name);
        let fd = fd.parse().unwrap();
        // SAFETY: this process owns the launch-scoped inherited socket.
        let socket = unsafe { UnixStream::from_raw_fd(fd) };
        let mut bytes = Vec::new();
        socket.take(4097).read_to_end(&mut bytes).unwrap();
        assert!(bytes.len() <= 4096);
        Some(std::ffi::OsString::from_vec(bytes))
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::env::var_os(name)
    }
}

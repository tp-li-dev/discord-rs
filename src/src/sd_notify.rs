//! Minimal systemd `sd_notify` client (`Type=notify` / `WatchdogSec=`).
//!
//! Only unix datagram sockets are used; on other platforms every send is a
//! no-op. The target comes solely from the `NOTIFY_SOCKET` environment variable
//! set by the service manager. Nothing is sent when it is absent, and send
//! failures never abort the daemon (callers may ignore them). The unit tests do
//! not verify behavior against a real systemd instance.
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

/// Upper bound for the `STATUS=` text, in bytes.
pub const STATUS_LIMIT: usize = 200;

/// Where datagrams are sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// Filesystem socket (absolute path).
    Path(PathBuf),
    /// Linux abstract namespace socket (name without the leading `@`).
    Abstract(Vec<u8>),
}

/// Parses a `NOTIFY_SOCKET` value. Only absolute paths and `@name` are accepted;
/// empty values, relative paths, and values containing NUL are ignored.
pub fn parse_notify_socket(value: Option<&OsStr>) -> Option<Target> {
    let text = value?.to_str()?;
    if text.is_empty() || text.contains('\0') {
        return None;
    }
    if let Some(name) = text.strip_prefix('@') {
        return (!name.is_empty()).then(|| Target::Abstract(name.as_bytes().to_vec()));
    }
    text.starts_with('/')
        .then(|| Target::Path(PathBuf::from(text)))
}

/// Recommended watchdog ping interval: half of `WATCHDOG_USEC`. Returns `None`
/// when unset/invalid/zero, or when `WATCHDOG_PID` is set to another process.
pub fn watchdog_interval(usec: Option<&str>, pid: Option<&str>, own_pid: u32) -> Option<Duration> {
    let usec: u64 = usec?.trim().parse().ok().filter(|v| *v > 0)?;
    if pid.is_some_and(|p| p.trim().parse::<u32>().ok() != Some(own_pid)) {
        return None;
    }
    Some(Duration::from_micros((usec / 2).max(1)))
}

/// Builds a `STATUS=` message: control characters (including newlines) are
/// removed and the text is truncated to [`STATUS_LIMIT`] bytes on a char boundary.
pub fn format_status(text: &str) -> String {
    let mut out = String::from("STATUS=");
    let limit = "STATUS=".len() + STATUS_LIMIT;
    for c in text.chars().filter(|c| !c.is_control()) {
        if out.len() + c.len_utf8() > limit {
            break;
        }
        out.push(c);
    }
    out
}

/// Notification sender. A default/absent target makes every call a no-op.
#[derive(Clone, Debug, Default)]
pub struct Notifier {
    target: Option<Target>,
}

impl Notifier {
    pub fn from_env() -> Self {
        Self {
            target: parse_notify_socket(std::env::var_os("NOTIFY_SOCKET").as_deref()),
        }
    }

    pub fn with_target(target: Target) -> Self {
        Self {
            target: Some(target),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.target.is_some()
    }

    pub fn ready(&self) -> io::Result<()> {
        self.send("READY=1")
    }

    pub fn watchdog(&self) -> io::Result<()> {
        self.send("WATCHDOG=1")
    }

    pub fn stopping(&self) -> io::Result<()> {
        self.send("STOPPING=1")
    }

    pub fn status(&self, text: &str) -> io::Result<()> {
        self.send(&format_status(text))
    }

    #[cfg(unix)]
    fn send(&self, message: &str) -> io::Result<()> {
        use std::os::unix::net::UnixDatagram;
        let Some(target) = &self.target else {
            return Ok(());
        };
        let socket = UnixDatagram::unbound()?;
        match target {
            Target::Path(path) => socket.send_to(message.as_bytes(), path).map(drop),
            #[cfg(target_os = "linux")]
            Target::Abstract(name) => {
                use std::os::linux::net::SocketAddrExt;
                let address = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
                socket.send_to_addr(message.as_bytes(), &address).map(drop)
            }
            #[cfg(not(target_os = "linux"))]
            Target::Abstract(_) => Ok(()),
        }
    }

    #[cfg(not(unix))]
    fn send(&self, _message: &str) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_notify_socket_values() {
        let p = |s: &str| parse_notify_socket(Some(OsStr::new(s)));
        assert_eq!(
            p("/run/systemd/notify"),
            Some(Target::Path("/run/systemd/notify".into()))
        );
        assert_eq!(p("@abc"), Some(Target::Abstract(b"abc".to_vec())));
        for bad in ["", "@", "relative/path", "notify", "/a\0b"] {
            assert_eq!(p(bad), None, "{bad:?}");
        }
        assert_eq!(parse_notify_socket(None), None);
    }

    #[test]
    fn watchdog_interval_is_half_and_checks_pid() {
        let own = 42;
        let half = Some(Duration::from_secs(15));
        assert_eq!(watchdog_interval(Some("30000000"), None, own), half);
        assert_eq!(watchdog_interval(Some("30000000"), Some("42"), own), half);
        assert_eq!(watchdog_interval(Some("30000000"), Some("43"), own), None);
        assert_eq!(watchdog_interval(Some("30000000"), Some("x"), own), None);
        for bad in [None, Some("0"), Some("abc"), Some("-1"), Some("")] {
            assert_eq!(watchdog_interval(bad, None, own), None);
        }
    }

    #[test]
    fn status_strips_controls_and_limits_length() {
        assert_eq!(format_status("a\nb\r\x00c\x1b[0m"), "STATUS=abc[0m");
        let long = "あ".repeat(500);
        let s = format_status(&long);
        assert!(s.len() <= "STATUS=".len() + STATUS_LIMIT);
        assert!(s.chars().skip(7).all(|c| c == 'あ'));
        assert_eq!(format_status(""), "STATUS=");
    }

    #[test]
    fn disabled_notifier_is_noop() {
        let n = Notifier::default();
        assert!(!n.is_enabled());
        assert!(n.ready().is_ok() && n.watchdog().is_ok() && n.stopping().is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn delivers_datagrams_to_a_real_socket() {
        let dir = std::env::temp_dir().join(format!("sd-notify-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("n.sock");
        let server = std::os::unix::net::UnixDatagram::bind(&path).unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let n = Notifier::with_target(Target::Path(path));
        let mut buf = [0u8; 256];
        for (send, want) in [
            (n.ready(), "READY=1"),
            (n.watchdog(), "WATCHDOG=1"),
            (n.status("hi\nthere"), "STATUS=hithere"),
            (n.stopping(), "STOPPING=1"),
        ] {
            send.unwrap();
            let len = server.recv(&mut buf).unwrap();
            assert_eq!(&buf[..len], want.as_bytes());
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}

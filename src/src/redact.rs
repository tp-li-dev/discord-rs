//! Explicit log-boundary protection. Heuristic redaction is not a secret store.
use std::fmt;

pub const MASK: &str = "[REDACTED]";

/// Install before parsing input or starting an async runtime. Never delegate to
/// the previous hook: payloads, thread names and source paths may contain secrets.
/// Unwinding/abort behavior and the resulting process exit status are unchanged.
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        use std::io::Write;
        let _ = writeln!(std::io::stderr().lock(), "panic [panic]: {MASK}");
    }));
}

/// Preserve typed diagnostics without formatting an untrusted error or walking
/// its source chain. Unknown implementations may execute arbitrary formatting.
pub fn error_diagnostic(error: &(dyn std::error::Error + 'static)) -> String {
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return match error.raw_os_error() {
            Some(code) => format!("io_error kind={:?} os_code={code}", error.kind()),
            None => format!("io_error kind={:?}: {MASK}", error.kind()),
        };
    }
    if let Some(error) = error.downcast_ref::<serde_json::Error>() {
        return format!(
            "json_error category={:?} line={} column={}",
            error.classify(),
            error.line(),
            error.column()
        );
    }
    format!("external_error: {MASK}")
}

/// Formatting never inspects the value, even when T has no formatting traits.
/// Does not zeroize memory; access must be explicit and must not be logged.
pub struct Secret<T>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }
    pub fn expose_secret(&self) -> &T {
        &self.0
    }
    pub fn into_inner(self) -> T {
        self.0
    }
}
impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(MASK)
    }
}
impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(MASK)
    }
}

/// Deferred formatting for text crossing a log boundary.
pub struct Redacted<'a>(pub &'a str);
impl fmt::Display for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&redact(self.0))
    }
}
impl fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

fn word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}
fn stop(b: u8) -> bool {
    b.is_ascii_whitespace() || b"\"'&,;)}]>?#/".contains(&b)
}
fn value_end(b: &[u8], mut i: usize) -> usize {
    if i < b.len() && matches!(b[i], b'[' | b'{') {
        // A credential may be a byte array or nested object. Mask it as one
        // field, including commas, whitespace and quoted nested values.
        let mut depth = 0usize;
        while i < b.len() {
            match b[i] {
                b'\'' | b'"' => {
                    i = value_end(b, i);
                    continue;
                }
                b'[' | b'{' => depth += 1,
                b']' | b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return i + 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        i
    } else if i < b.len() && matches!(b[i], b'\'' | b'"') {
        let quote = b[i];
        i += 1;
        while i < b.len() {
            if b[i] == b'\\' {
                i = (i + 2).min(b.len());
            } else if b[i] == quote {
                return i + 1;
            } else {
                i += 1;
            }
        }
        i
    } else {
        while i < b.len() && !stop(b[i]) {
            i += 1;
        }
        i
    }
}

/// Masks ASCII case-insensitive credential labels, auth schemes, callback and
/// webhook URL path credentials, and common three-part / MFA Discord shapes.
/// Quoted values and PEM private keys may span lines. Unknown unlabelled secrets
/// need Secret<T>; this heuristic must not be used to publish arbitrary input.
pub fn redact(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let b = lower.as_bytes();
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i..].starts_with(b"-----begin ") {
            let label_start = i + b"-----begin ".len();
            if let Some(label_end) = lower[label_start..].find("-----") {
                let label_end = label_start + label_end;
                let label = &lower[label_start..label_end];
                if label.ends_with("private key") || label == "openssh private key" {
                    let footer = format!("-----end {label}-----");
                    let body_start = label_end + 5;
                    let end = lower[body_start..]
                        .find(&footer)
                        .map_or(b.len(), |offset| body_start + offset + footer.len());
                    ranges.push((i, end));
                    i = end;
                    continue;
                }
            }
        }
        let boundary = i == 0 || !word(b[i - 1]);
        if boundary {
            for key in [
                "authorization",
                "proxy-authorization",
                "token",
                "bot_token",
                "interaction_token",
                "custom_id",
                "secret",
                "gateway_secret",
                "manager_secret",
                "client_secret",
                "access_token",
                "refresh_token",
                "discord_token",
                "api_key",
                "api-key",
                "apikey",
                "private_key",
                "private-key",
                "privatekey",
                "signing_key",
                "signingkey",
                "encryption_key",
                "encryptionkey",
                "key",
                "password",
                "passwd",
            ] {
                if b[i..].starts_with(key.as_bytes()) {
                    let mut p = i + key.len();
                    if p < b.len() && matches!(b[p], b'"' | b'\'') {
                        p += 1;
                    }
                    while p < b.len() && b[p].is_ascii_whitespace() {
                        p += 1;
                    }
                    if p < b.len() && matches!(b[p], b':' | b'=') {
                        p += 1;
                        while p < b.len() && b[p].is_ascii_whitespace() {
                            p += 1;
                        }
                        let mut end = value_end(b, p);
                        // An entire Authorization field, including its scheme.
                        if key.ends_with("authorization")
                            && p < b.len()
                            && !matches!(b[p], b'"' | b'\'' | b'[' | b'{')
                        {
                            end = p;
                            loop {
                                while end < b.len() && !b"\r\n\"',;}".contains(&b[end]) {
                                    end += 1;
                                }
                                let mut next = end;
                                if next < b.len() && b[next] == b'\r' {
                                    next += 1;
                                }
                                if next < b.len() && b[next] == b'\n' {
                                    next += 1;
                                    if next < b.len() && matches!(b[next], b' ' | b'\t') {
                                        // Obsolete folded headers are still untrusted input.
                                        end = next + 1;
                                        continue;
                                    }
                                }
                                break;
                            }
                        }
                        if end > p {
                            ranges.push((p, end));
                        }
                    }
                }
            }
            for scheme in ["bot", "bearer"] {
                let p = i + scheme.len();
                if b[i..].starts_with(scheme.as_bytes())
                    && p < b.len()
                    && b[p].is_ascii_whitespace()
                {
                    let mut start = p;
                    while start < b.len() && b[start].is_ascii_whitespace() {
                        start += 1;
                    }
                    let end = value_end(b, start);
                    if end > start {
                        ranges.push((start, end));
                    }
                }
            }
            if word(b[i]) {
                let mut end = i;
                while end < b.len() && (word(b[end]) || b[end] == b'.') {
                    end += 1;
                }
                let candidate = &lower[i..end];
                let parts: Vec<_> = candidate.split('.').collect();
                if (parts.len() == 3
                    && parts[0].len() >= 20
                    && parts[1].len() >= 6
                    && parts[2].len() >= 20)
                    || (candidate.starts_with("mfa.") && candidate.len() >= 24)
                {
                    ranges.push((i, end));
                }
            }
        }
        for prefix in ["/interactions/", "/webhooks/"] {
            if b[i..].starts_with(prefix.as_bytes()) {
                let mut p = i + prefix.len();
                while p < b.len() && b[p].is_ascii_digit() {
                    p += 1;
                }
                if p > i + prefix.len() && p < b.len() && b[p] == b'/' {
                    let start = p + 1;
                    let end = value_end(b, start);
                    if end > start {
                        ranges.push((start, end));
                    }
                }
            }
        }
        i += 1;
    }
    ranges.sort_unstable();
    let mut result = String::new();
    let mut cursor = 0;
    for (start, end) in ranges {
        if start > cursor {
            result.push_str(&input[cursor..start]);
        }
        if end > cursor {
            if start >= cursor {
                result.push_str(MASK);
            }
            cursor = end;
        }
    }
    result.push_str(&input[cursor..]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_keys_and_structured_credentials_are_masked() {
        for label in [
            "PRIVATE KEY",
            "RSA PRIVATE KEY",
            "EC PRIVATE KEY",
            "ENCRYPTED PRIVATE KEY",
            "OPENSSH PRIVATE KEY",
        ] {
            for footer in [format!("\n-----END {label}-----"), String::new()] {
                let input = format!("before -----BEGIN {label}-----\nsynthetic-key-data{footer}");
                assert_eq!(redact(&input), format!("before {MASK}"));
            }
        }
        for label in [
            "key",
            "api_key",
            "private_key",
            "signing_key",
            "encryption_key",
            "password",
            "passwd",
            "client_secret",
            "access_token",
            "refresh_token",
        ] {
            for value in [
                "\"synthetic-only\"",
                "[1, 2, 3, 4]",
                "{\"value\": [\"synthetic-only\", {\"nested\": 7}]}",
                "{\"value\": \"synthetic-only\"",
            ] {
                assert_eq!(
                    redact(&format!("{label}={value}")),
                    format!("{label}={MASK}")
                );
            }
        }
        assert_eq!(
            redact("key_id=public password_policy=strict"),
            "key_id=public password_policy=strict"
        );
        assert_eq!(
            redact(
                "Authorization: Bearer synthetic-first\r\n synthetic-second\n\tsynthetic-third\nstatus=denied"
            ),
            "Authorization: [REDACTED]\nstatus=denied"
        );
        assert_eq!(
            redact("Authorization: [1, 2, 3]"),
            "Authorization: [REDACTED]"
        );
    }

    #[test]
    fn diagnostics_never_format_untrusted_error_or_source() {
        #[derive(Clone, Copy)]
        struct Hostile;
        impl std::fmt::Display for Hostile {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("Display must not be called")
            }
        }
        impl std::fmt::Debug for Hostile {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("Debug must not be called")
            }
        }
        impl std::error::Error for Hostile {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                panic!("source must not be called")
            }
        }
        assert_eq!(
            error_diagnostic(&Hostile),
            format!("external_error: {MASK}")
        );
        let io = std::io::Error::new(std::io::ErrorKind::PermissionDenied, Hostile);
        assert_eq!(
            error_diagnostic(&io),
            format!("io_error kind=PermissionDenied: {MASK}")
        );
        let json = serde_json::from_str::<u64>("\"synthetic-only\"").unwrap_err();
        assert_eq!(
            error_diagnostic(&json),
            "json_error category=Data line=1 column=16"
        );
        let io = std::io::Error::from_raw_os_error(5);
        assert!(error_diagnostic(&io).contains("os_code=5"));
    }

    #[test]
    fn panic_probe() {
        let Some(mode) = std::env::var_os("REDSEC_PANIC_PROBE") else {
            return;
        };
        install_panic_hook();
        // All values are fixtures. A secret-bearing thread name must not leak
        // either, even when RUST_BACKTRACE would make the default hook verbose.
        if mode == "thread" {
            std::thread::Builder::new()
                .name("synthetic-thread-secret".into())
                .spawn(|| panic!("token=synthetic-token secret=synthetic-secret Authorization: Bearer synthetic-auth key=synthetic-key"))
                .unwrap()
                .join()
                .unwrap();
        } else {
            std::panic::panic_any("synthetic-unlabelled-secret");
        }
    }

    #[test]
    fn panic_process_output_never_contains_payload_or_thread_secret() {
        for mode in ["thread", "opaque"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "redact::tests::panic_probe", "--nocapture"])
                .env("REDSEC_PANIC_PROBE", mode)
                .env("RUST_BACKTRACE", "full")
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(101));
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains("panic [panic]: [REDACTED]"));
            for text in [&stdout, &stderr] {
                assert!(!text.contains("synthetic-"));
                assert!(!text.contains("stack backtrace"));
            }
        }
    }
}

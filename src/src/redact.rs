//! Explicit log-boundary protection. Heuristic redaction is not a secret store.
use std::fmt;

pub const MASK: &str = "[REDACTED]";

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
    if i < b.len() && matches!(b[i], b'\'' | b'"') {
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
/// Quoted values may span lines. Unknown unlabelled secrets need Secret<T>.
pub fn redact(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let b = lower.as_bytes();
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < b.len() {
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
                            && !matches!(b[p], b'"' | b'\'')
                        {
                            end = p;
                            while end < b.len() && !b"\r\n\"',;}".contains(&b[end]) {
                                end += 1;
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

use src::worker::diagnostic;
use std::{error::Error, fmt};

struct Untrusted;
impl fmt::Display for Untrusted {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("must not format an untrusted error")
    }
}
impl fmt::Debug for Untrusted {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("must not debug an untrusted error")
    }
}
impl Error for Untrusted {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        panic!("must not traverse an untrusted source")
    }
}

#[test]
fn unknown_errors_are_not_formatted_or_traversed() {
    assert_eq!(diagnostic(&Untrusted), "[REDACTED]");
}

#[test]
fn io_diagnostics_keep_kind_and_os_code_without_custom_payload() {
    let error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, Untrusted);
    assert_eq!(
        diagnostic(&error),
        "IO failure (kind=PermissionDenied, os_code=None)"
    );
    let os = std::io::Error::from_raw_os_error(5);
    let safe = diagnostic(&os);
    assert!(safe.contains("os_code=Some(5)"));
    assert!(safe.contains(&format!("kind={:?}", os.kind())));
}

#[test]
fn json_diagnostics_keep_coordinates_without_input() {
    let error = serde_json::from_str::<bool>("\"tiny-secret\"").unwrap_err();
    assert_eq!(
        diagnostic(&error),
        format!(
            "invalid JSON (category=Data, line={}, column={})",
            error.line(),
            error.column()
        )
    );
    assert!(!diagnostic(&error).contains("tiny-secret"));
}

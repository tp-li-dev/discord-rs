//! Pure Bot reference and isolation checks; no credentials or filesystem are read.
use crate::placement::BotPlacement;
use std::fmt;

/// Maximum UTF-8 byte length of a portable name/path component.
pub const MAX_NAME_BYTES: usize = 64;

/// Errors contain categories and list indices only, never untrusted input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BotRefError {
    InvalidName { index: usize },
    InvalidId { index: usize },
    DuplicateName { first: usize, second: usize },
    DuplicateId { first: usize, second: usize },
    NotFound,
    Ambiguous { indices: Vec<usize> },
    InvalidTokenEnv { index: usize },
    SharedTokenEnv { first: usize, second: usize },
    InvalidStatePath { index: usize },
    OverlappingStatePath { first: usize, second: usize },
}

impl fmt::Display for BotRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidName { .. } => "invalid bot name",
            Self::InvalidId { .. } => "invalid bot id",
            Self::DuplicateName { .. } => "duplicate bot name",
            Self::DuplicateId { .. } => "duplicate bot id",
            Self::NotFound => "bot reference not found",
            Self::Ambiguous { .. } => "ambiguous bot reference",
            Self::InvalidTokenEnv { .. } => "invalid token environment reference",
            Self::SharedTokenEnv { .. } => "token environment reference shared across bots",
            Self::InvalidStatePath { .. } => "invalid bot state path",
            Self::OverlappingStatePath { .. } => "bot state paths overlap",
        })
    }
}
impl std::error::Error for BotRefError {}

/// Unicode letters/numbers/marks are preserved verbatim (no normalization).
/// Rejects controls, whitespace, Windows-invalid characters, dot components,
/// trailing dots, and Windows device names, including names with extensions.
/// This is a component check, not a guarantee against filesystem aliases.
pub fn valid_name(name: &str) -> bool {
    if name.is_empty()
        || name.len() > MAX_NAME_BYTES
        || name == "."
        || name == ".."
        || name.ends_with('.')
        || name
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || "<>:\"/\\|?*".contains(c))
    {
        return false;
    }
    let stem = name.split('.').next().unwrap_or_default().to_uppercase();
    !matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) && !["COM", "LPT"].iter().any(|prefix| {
        stem.strip_prefix(prefix).is_some_and(|suffix| {
            matches!(
                suffix,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        })
    })
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.trim() == id && !id.chars().any(char::is_control)
}

/// Names and IDs are case sensitive and byte exact. Duplicate configuration
/// identities fail even if the query targets an unrelated Bot. Cross-namespace
/// name/ID-prefix collisions are allowed in configuration but resolve ambiguously.
pub fn validate_bots(bots: &[BotPlacement]) -> Result<(), BotRefError> {
    for (index, bot) in bots.iter().enumerate() {
        if !valid_name(&bot.name) {
            return Err(BotRefError::InvalidName { index });
        }
        if !valid_id(&bot.bot_id) {
            return Err(BotRefError::InvalidId { index });
        }
        for (first, other) in bots[..index].iter().enumerate() {
            if other.name == bot.name {
                return Err(BotRefError::DuplicateName {
                    first,
                    second: index,
                });
            }
            if other.bot_id == bot.bot_id {
                return Err(BotRefError::DuplicateId {
                    first,
                    second: index,
                });
            }
        }
    }
    Ok(())
}

/// Resolve name OR ID prefix (including full ID). No exact-match precedence:
/// selecting another Bot by prefix is ambiguous even with an exact name/full ID.
/// A Bot matching both its own name and ID counts once. Empty input never matches.
pub fn resolve<'a>(bots: &'a [BotPlacement], query: &str) -> Result<&'a BotPlacement, BotRefError> {
    validate_bots(bots)?;
    if query.is_empty() {
        return Err(BotRefError::NotFound);
    }
    let indices: Vec<_> = bots
        .iter()
        .enumerate()
        .filter(|(_, bot)| bot.name == query || bot.bot_id.starts_with(query))
        .map(|(index, _)| index)
        .collect();
    match indices.as_slice() {
        [] => Err(BotRefError::NotFound),
        [index] => Ok(&bots[*index]),
        _ => Err(BotRefError::Ambiguous { indices }),
    }
}

/// One entry per Bot, in the same order as the configured Bot list.
/// State directories are relative to ONE trusted shared root, on either OS.
/// Credential values must never be supplied here.
#[derive(Clone, Copy)]
pub struct BotIsolation<'a> {
    pub bot: &'a BotPlacement,
    pub state_dir: &'a str,
}

/// Checks unique identities, portable environment references (case insensitive),
/// and disjoint state namespaces. Both slash styles are recognized on both OSes;
/// absolute paths, traversal, empty components and unsafe components are rejected.
/// Unicode lowercase comparison is conservative, not a filesystem canonicalizer.
/// The caller must prevent symlinks/junctions, short-name aliases, mount aliases,
/// and filesystem-specific Unicode aliases in the trusted root. No ownership,
/// permissions, actual token contents, or runtime isolation is proved here.
pub fn validate_isolation(entries: &[BotIsolation<'_>]) -> Result<(), BotRefError> {
    let bots: Vec<_> = entries.iter().map(|entry| entry.bot.clone()).collect();
    validate_bots(&bots)?;
    let mut paths: Vec<Vec<String>> = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let env = &entry.bot.token_env;
        let mut chars = env.chars();
        if !chars
            .next()
            .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
            || !chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
        {
            return Err(BotRefError::InvalidTokenEnv { index });
        }
        for (first, other) in entries[..index].iter().enumerate() {
            if env.eq_ignore_ascii_case(&other.bot.token_env) {
                return Err(BotRefError::SharedTokenEnv {
                    first,
                    second: index,
                });
            }
        }
        let parts: Vec<_> = entry.state_dir.split(['/', '\\']).collect();
        if parts.iter().any(|part| !valid_name(part)) {
            return Err(BotRefError::InvalidStatePath { index });
        }
        let path: Vec<_> = parts.iter().map(|part| part.to_lowercase()).collect();
        for (first, other) in paths.iter().enumerate() {
            if path.starts_with(other) || other.starts_with(&path) {
                return Err(BotRefError::OverlappingStatePath {
                    first,
                    second: index,
                });
            }
        }
        paths.push(path);
    }
    Ok(())
}

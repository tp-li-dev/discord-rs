//! Transport-neutral argument extraction for command adapters.
//! Prefix tokenization currently accepts whitespace-separated tokens only; quoting and
//! escaping are deliberately rejected until a documented grammar is implemented.
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SlashValue {
    String(String),
    Integer(i64),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgumentError {
    UnsupportedPrefixSyntax,
    Duplicate(String),
    Missing(String),
    Invalid {
        name: String,
        expected: &'static str,
    },
    Unexpected(String),
}
impl fmt::Display for ArgumentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPrefixSyntax => {
                write!(f, "prefix quoting and escapes are not yet supported")
            }
            Self::Duplicate(name) => write!(f, "duplicate argument: {name}"),
            Self::Missing(name) => write!(f, "missing argument: {name}"),
            Self::Invalid { name, expected } => {
                write!(f, "invalid argument {name}: expected {expected}")
            }
            Self::Unexpected(value) => write!(f, "unexpected argument: {value}"),
        }
    }
}
impl std::error::Error for ArgumentError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgumentKind {
    String,
    Integer,
    Boolean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgumentSchema {
    pub name: &'static str,
    pub kind: ArgumentKind,
    pub required: bool,
}

/// Supported primitive types opt into strict conversion from both transports.
pub trait Argument: Sized {
    const EXPECTED: &'static str;
    const KIND: ArgumentKind;
    fn from_prefix(value: &str) -> Option<Self>;
    fn from_slash(value: SlashValue) -> Option<Self>;
}
impl Argument for String {
    const EXPECTED: &'static str = "string";
    const KIND: ArgumentKind = ArgumentKind::String;
    fn from_prefix(value: &str) -> Option<Self> {
        Some(value.to_owned())
    }
    fn from_slash(value: SlashValue) -> Option<Self> {
        if let SlashValue::String(value) = value {
            Some(value)
        } else {
            None
        }
    }
}
impl Argument for i64 {
    const EXPECTED: &'static str = "integer";
    const KIND: ArgumentKind = ArgumentKind::Integer;
    fn from_prefix(value: &str) -> Option<Self> {
        value.parse().ok()
    }
    fn from_slash(value: SlashValue) -> Option<Self> {
        if let SlashValue::Integer(value) = value {
            Some(value)
        } else {
            None
        }
    }
}
impl Argument for bool {
    const EXPECTED: &'static str = "boolean";
    const KIND: ArgumentKind = ArgumentKind::Boolean;
    fn from_prefix(value: &str) -> Option<Self> {
        value.parse().ok()
    }
    fn from_slash(value: SlashValue) -> Option<Self> {
        if let SlashValue::Boolean(value) = value {
            Some(value)
        } else {
            None
        }
    }
}

pub enum Arguments<'a> {
    Prefix(std::str::SplitWhitespace<'a>),
    Slash(BTreeMap<String, SlashValue>),
}
impl<'a> Arguments<'a> {
    pub fn prefix(input: &'a str) -> Result<Self, ArgumentError> {
        if input.contains(['\'', '"', '\\']) {
            return Err(ArgumentError::UnsupportedPrefixSyntax);
        }
        Ok(Self::Prefix(input.split_whitespace()))
    }
    pub fn slash(
        values: impl IntoIterator<Item = (String, SlashValue)>,
    ) -> Result<Self, ArgumentError> {
        let mut map = BTreeMap::new();
        for (name, value) in values {
            if map.insert(name.clone(), value).is_some() {
                return Err(ArgumentError::Duplicate(name));
            }
        }
        Ok(Self::Slash(map))
    }
    pub fn required<T: Argument>(&mut self, name: &str) -> Result<T, ArgumentError> {
        self.optional(name)?
            .ok_or_else(|| ArgumentError::Missing(name.to_owned()))
    }
    pub fn optional<T: Argument>(&mut self, name: &str) -> Result<Option<T>, ArgumentError> {
        let value = match self {
            Self::Prefix(tokens) => tokens.next().map(|value| T::from_prefix(value)),
            Self::Slash(values) => values.remove(name).map(T::from_slash),
        };
        value
            .map(|parsed| {
                parsed.ok_or_else(|| ArgumentError::Invalid {
                    name: name.to_owned(),
                    expected: T::EXPECTED,
                })
            })
            .transpose()
    }
    /// Call after extracting declared options to reject typos or surplus input.
    pub fn finish(mut self) -> Result<(), ArgumentError> {
        self.finish_ref()
    }
    pub fn finish_ref(&mut self) -> Result<(), ArgumentError> {
        let extra = match self {
            Self::Prefix(tokens) => tokens.next().map(str::to_owned),
            Self::Slash(values) => values.keys().next().cloned(),
        };
        match extra {
            Some(value) => Err(ArgumentError::Unexpected(value)),
            None => Ok(()),
        }
    }
}

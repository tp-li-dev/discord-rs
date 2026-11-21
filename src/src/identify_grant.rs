//! Versioned, process-private admission for Discord Identify sends.

use std::{io, time::Duration};

mod client;
pub(crate) mod service;

pub use client::GrantQueue;

pub const GATEWAY_FD_FLAG: &str = "--identify-grant-v1-fd";
const VERSION: u8 = 1;
const REPLY_LEN: usize = 10;
const PERMIT_TTL: Duration = Duration::from_secs(2);
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const UNAVAILABLE_LOG: &str = "Identify send grant unavailable";

#[derive(Clone, Copy)]
enum Reply {
    Permit(u64),
    Wait(u64),
    Unavailable,
}

impl Reply {
    fn encode(self) -> [u8; REPLY_LEN] {
        let (tag, value) = match self {
            Self::Permit(deadline) => (1, deadline),
            Self::Wait(delay) => (2, delay),
            Self::Unavailable => (3, 0),
        };
        let mut bytes = [0; REPLY_LEN];
        bytes[0] = VERSION;
        bytes[1] = tag;
        bytes[2..].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    fn decode(bytes: [u8; REPLY_LEN]) -> Self {
        let value = u64::from_le_bytes(bytes[2..].try_into().expect("fixed reply length"));
        match (bytes[0], bytes[1]) {
            (VERSION, 1) => Self::Permit(value),
            (VERSION, 2) => Self::Wait(value),
            _ => Self::Unavailable,
        }
    }
}

fn boot_now() -> io::Result<Duration> {
    let mut stamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // CLOCK_BOOTTIME includes suspend and has the same origin in both processes.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut stamp) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let seconds = u64::try_from(stamp.tv_sec).map_err(io::Error::other)?;
    let nanos = u32::try_from(stamp.tv_nsec).map_err(io::Error::other)?;
    Ok(Duration::new(seconds, nanos))
}

fn nanos(time: Duration) -> u64 {
    u64::try_from(time.as_nanos()).unwrap_or(u64::MAX)
}

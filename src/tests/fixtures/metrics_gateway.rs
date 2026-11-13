//! Test-only Gateway for the metrics process test: runs until the private stdin
//! carries the shutdown request, drains for a short fixed time, then exits 0.
//! It reads no credential and accepts the extra explicit mock Discord arguments.
use std::{
    io::{BufRead, BufReader, Read},
    sync::mpsc,
    time::{Duration, Instant},
};

fn main() {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        if BufReader::new(std::io::stdin())
            .take(513)
            .read_line(&mut line)
            .is_ok()
        {
            let compact: String = line.chars().filter(|ch| !ch.is_whitespace()).collect();
            if compact == "{\"command\":\"shutdown\"}" {
                let _ = sender.send(());
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if receiver.try_recv().is_ok() {
            std::thread::sleep(Duration::from_millis(150));
            return;
        }
        if Instant::now() >= deadline {
            std::process::exit(84);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

//! Self-tests for the test-only fault injector and local Discord HTTP fixtures.
use src::testkit::{
    Bandwidth, DirectionFaults, FaultConfig, FaultIo, MockDiscord, MockLimits, MockReply,
};
use std::{
    future::Future,
    io,
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    time::{sleep, timeout},
};

const DEADLINE: Duration = Duration::from_secs(3);
const REST: &[u8] =
    b"POST /api/v10/channels/1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}";
const INTERACTION: &[u8] = b"POST /api/v10/interactions/1/fixture/callback HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\n{\"type\":5}";

fn chunk(bytes: usize) -> Option<NonZeroUsize> {
    NonZeroUsize::new(bytes)
}

fn write_faults(write: DirectionFaults) -> FaultConfig {
    FaultConfig {
        write,
        ..Default::default()
    }
}

fn read_faults(read: DirectionFaults) -> FaultConfig {
    FaultConfig {
        read,
        ..Default::default()
    }
}

fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = std::pin::pin!(future);
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn transparent_io_preserves_bytes_flush_and_graceful_shutdown() {
    let (stream, mut peer) = tokio::io::duplex(256);
    let mut stream = FaultIo::new(stream, FaultConfig::default()).unwrap();
    stream.write_all(b"{\"type\":\"health\"}\n").await.unwrap();
    stream.flush().await.unwrap();
    let mut request = [0; 18];
    peer.read_exact(&mut request).await.unwrap();
    assert_eq!(&request, b"{\"type\":\"health\"}\n");
    peer.write_all(b"ready\n").await.unwrap();
    let mut reply = [0; 6];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"ready\n");
    assert_eq!(stream.stats().write_bytes, 18);
    assert_eq!(stream.stats().read_bytes, 6);
    stream.shutdown().await.unwrap();
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn maximum_chunk_fragments_both_directions_without_loss() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let faults = DirectionFaults {
        max_chunk: chunk(3),
        ..Default::default()
    };
    let mut stream = FaultIo::new(
        stream,
        FaultConfig {
            read: faults.clone(),
            write: faults,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(stream.write(b"abcdef").await.unwrap(), 3);
    let mut prefix = [0; 3];
    peer.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix, b"abc");
    peer.write_all(b"abcdef").await.unwrap();
    let mut bytes = [0; 10];
    assert_eq!(stream.read(&mut bytes).await.unwrap(), 3);
    assert_eq!(&bytes[..3], b"abc");
    assert_eq!(stream.read(&mut bytes).await.unwrap(), 3);
    assert_eq!(&bytes[..3], b"def");
}

#[tokio::test]
async fn write_cutoff_delivers_exact_prefix_then_closes_the_transport() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let mut stream = FaultIo::new(
        stream,
        write_faults(DirectionFaults {
            disconnect_after: Some(5),
            max_chunk: chunk(2),
            ..Default::default()
        }),
    )
    .unwrap();
    let error = stream.write_all(b"abcdefgh").await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    let mut received = Vec::new();
    timeout(DEADLINE, peer.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, b"abcde");
    assert_eq!(stream.stats().write_bytes, 5);
    assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(
        stream.write(b"z").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn read_cutoff_caps_transfer_and_closes_both_directions() {
    let (stream, mut peer) = tokio::io::duplex(64);
    peer.write_all(b"abcdefgh").await.unwrap();
    let mut stream = FaultIo::new(
        stream,
        read_faults(DirectionFaults {
            disconnect_after: Some(5),
            ..Default::default()
        }),
    )
    .unwrap();
    let mut received = Vec::new();
    stream.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, b"abcde");
    assert_eq!(stream.stats().read_bytes, 5);
    assert_eq!(
        stream.write(b"z").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn zero_cutoff_is_immediate_and_empty_operations_are_successful() {
    let (stream, _peer) = tokio::io::duplex(64);
    let mut stream = FaultIo::new(
        stream,
        write_faults(DirectionFaults {
            disconnect_after: Some(0),
            latency: Duration::from_secs(60),
            ..Default::default()
        }),
    )
    .unwrap();
    let control = stream.control();
    control.block_reads();
    control.block_writes();
    assert_eq!(stream.write(&[]).await.unwrap(), 0);
    assert_eq!(stream.read(&mut []).await.unwrap(), 0);
    control.unblock_writes();
    assert!(
        matches!(poll_once(stream.write(b"x")), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::BrokenPipe)
    );
    assert_eq!(stream.write(&[]).await.unwrap(), 0);
    assert_eq!(stream.read(&mut []).await.unwrap(), 0);
    assert_eq!(stream.stats().write_bytes, 0);
    assert_eq!(stream.stats().write_delay_samples, 0);
}

#[tokio::test]
async fn gates_register_and_wake_read_and_write_waiters() {
    let (stream, mut peer) = tokio::io::duplex(64);
    peer.write_all(b"r").await.unwrap();
    let mut stream = FaultIo::new(stream, FaultConfig::default()).unwrap();
    let control = stream.control();
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    control.block_reads();
    control.block_writes();
    let mut byte = [0; 1];
    let mut read_buf = ReadBuf::new(&mut byte);
    assert!(
        Pin::new(&mut stream)
            .poll_read(&mut context, &mut read_buf)
            .is_pending()
    );
    assert!(
        Pin::new(&mut stream)
            .poll_write(&mut context, b"w")
            .is_pending()
    );
    assert_eq!(stream.stats().read_delay_samples, 0);
    assert_eq!(stream.stats().write_delay_samples, 0);
    control.unblock_reads();
    control.unblock_writes();
    assert!(wakes.0.load(Ordering::SeqCst) >= 2);
    assert!(matches!(
        Pin::new(&mut stream).poll_read(&mut context, &mut read_buf),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(read_buf.filled(), b"r");
    assert!(matches!(
        Pin::new(&mut stream).poll_write(&mut context, b"w"),
        Poll::Ready(Ok(1))
    ));
}

#[tokio::test]
async fn blocked_read_does_not_block_writes_and_control_disconnect_wakes() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let stream = FaultIo::new(stream, FaultConfig::default()).unwrap();
    let control = stream.control();
    control.block_reads();
    let (mut reader, mut writer) = tokio::io::split(stream);
    writer.write_all(b"heartbeat").await.unwrap();
    let mut heartbeat = [0; 9];
    peer.read_exact(&mut heartbeat).await.unwrap();
    assert_eq!(&heartbeat, b"heartbeat");
    let pending = tokio::spawn(async move { reader.read(&mut [0; 1]).await });
    tokio::task::yield_now().await;
    control.disconnect();
    assert_eq!(
        timeout(DEADLINE, pending).await.unwrap().unwrap().unwrap(),
        0
    );
    assert_eq!(
        writer.write(b"x").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn full_duplex_buffer_applies_backpressure_until_peer_drains() {
    let (stream, mut peer) = tokio::io::duplex(4);
    let mut stream = FaultIo::new(stream, FaultConfig::default()).unwrap();
    stream.write_all(b"full").await.unwrap();
    let pending = tokio::spawn(async move {
        stream.write_all(b"x").await.unwrap();
        stream
    });
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());
    let mut bytes = [0; 4];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"full");
    let stream = timeout(DEADLINE, pending).await.unwrap().unwrap();
    assert_eq!(stream.stats().write_bytes, 5);
    assert_eq!(peer.read_u8().await.unwrap(), b'x');
}

#[tokio::test]
async fn latency_wait_survives_cancellation_without_resampling() {
    let (stream, mut peer) = tokio::io::duplex(64);
    peer.write_all(b"x").await.unwrap();
    let mut stream = FaultIo::new(
        stream,
        read_faults(DirectionFaults {
            latency: Duration::from_millis(100),
            jitter: Duration::from_millis(5),
            ..Default::default()
        }),
    )
    .unwrap();
    let mut byte = [0; 1];
    assert!(
        timeout(Duration::from_millis(5), stream.read(&mut byte))
            .await
            .is_err()
    );
    let first = stream.stats();
    assert_eq!(first.read_delay_samples, 1);
    assert!(
        timeout(Duration::from_millis(5), stream.read(&mut byte))
            .await
            .is_err()
    );
    assert_eq!(stream.stats().read_delay_samples, 1);
    assert_eq!(stream.stats().read_jitter_nanos, first.read_jitter_nanos);
    sleep(Duration::from_millis(160)).await;
    assert!(matches!(
        poll_once(stream.read(&mut byte)),
        Poll::Ready(Ok(1))
    ));
    assert_eq!(byte, [b'x']);
    assert_eq!(stream.stats().read_delay_samples, 1);
}

#[tokio::test]
async fn underlying_pending_does_not_consume_more_delay_samples() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let mut stream = FaultIo::new(stream, FaultConfig::default()).unwrap();
    let mut byte = [0; 1];
    for _ in 0..5 {
        assert!(poll_once(stream.read(&mut byte)).is_pending());
    }
    assert_eq!(stream.stats().read_delay_samples, 1);
    peer.write_all(b"x").await.unwrap();
    stream.read_exact(&mut byte).await.unwrap();
    assert_eq!(stream.stats().read_delay_samples, 1);
}

async fn jitter_samples(seed: u64, interleave_read: bool) -> Vec<u64> {
    let (stream, mut peer) = tokio::io::duplex(128);
    peer.write_all(&[0; 16]).await.unwrap();
    let faults = DirectionFaults {
        jitter: Duration::from_millis(1),
        max_chunk: chunk(1),
        ..Default::default()
    };
    let mut stream = FaultIo::new(
        stream,
        FaultConfig {
            read: faults.clone(),
            write: faults,
            seed,
        },
    )
    .unwrap();
    let mut samples = Vec::new();
    for _ in 0..8 {
        let before = stream.stats().write_jitter_nanos;
        stream.write_all(b"x").await.unwrap();
        samples.push(stream.stats().write_jitter_nanos - before);
        if interleave_read {
            stream.read_exact(&mut [0; 1]).await.unwrap();
        }
    }
    assert_eq!(stream.stats().write_delay_samples, 8);
    samples
}

#[tokio::test]
async fn seeded_jitter_replays_exactly_and_read_write_sequences_are_independent() {
    let first = jitter_samples(17, false).await;
    assert_eq!(first, jitter_samples(17, false).await);
    assert_eq!(first, jitter_samples(17, true).await);
    assert_ne!(first, jitter_samples(18, false).await);
    assert!(first.iter().all(|n| *n <= 1_000_000));
}

#[tokio::test]
async fn bandwidth_caps_burst_then_paces_subsequent_writes() {
    let (stream, _peer) = tokio::io::duplex(64);
    let mut stream = FaultIo::new(
        stream,
        write_faults(DirectionFaults {
            bandwidth: Some(Bandwidth {
                bytes_per_second: NonZeroU64::new(20).unwrap(),
                burst_bytes: NonZeroUsize::new(2).unwrap(),
            }),
            ..Default::default()
        }),
    )
    .unwrap();
    assert_eq!(stream.write(b"abcdef").await.unwrap(), 2);
    assert!(
        timeout(Duration::from_millis(10), stream.write(b"cdef"))
            .await
            .is_err()
    );
    assert_eq!(stream.stats().write_bytes, 2);
    sleep(Duration::from_millis(150)).await;
    assert_eq!(
        timeout(DEADLINE, stream.write(b"cdef"))
            .await
            .unwrap()
            .unwrap(),
        2
    );
    assert_eq!(stream.stats().write_bytes, 4);
}

#[tokio::test]
async fn bandwidth_caps_and_paces_reads_too() {
    let (stream, mut peer) = tokio::io::duplex(64);
    peer.write_all(b"abcdef").await.unwrap();
    let mut stream = FaultIo::new(
        stream,
        read_faults(DirectionFaults {
            bandwidth: Some(Bandwidth {
                bytes_per_second: NonZeroU64::new(20).unwrap(),
                burst_bytes: NonZeroUsize::new(2).unwrap(),
            }),
            ..Default::default()
        }),
    )
    .unwrap();
    let mut bytes = [0; 8];
    assert_eq!(stream.read(&mut bytes).await.unwrap(), 2);
    assert_eq!(&bytes[..2], b"ab");
    assert!(
        timeout(Duration::from_millis(10), stream.read(&mut bytes))
            .await
            .is_err()
    );
    sleep(Duration::from_millis(150)).await;
    assert_eq!(stream.read(&mut bytes).await.unwrap(), 2);
    assert_eq!(&bytes[..2], b"cd");
}

async fn exchange(mock: &MockDiscord, request: &[u8]) -> (Vec<u8>, io::Result<()>) {
    let (mut client, server) = tokio::io::duplex(8192);
    let task = tokio::spawn({
        let mock = mock.clone();
        async move { mock.serve(server).await }
    });
    // Malformed input may be rejected before the client finishes writing.
    let _ = client.write_all(request).await;
    let _ = client.shutdown().await;
    let mut response = Vec::new();
    timeout(DEADLINE, client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let result = timeout(DEADLINE, task).await.unwrap().unwrap();
    (response, result)
}

fn response_parts(bytes: &[u8]) -> (&str, &[u8]) {
    let end = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap();
    let head = std::str::from_utf8(&bytes[..end]).unwrap();
    let body = &bytes[end + 4..];
    let content_length: usize = header(head, "content-length").unwrap().parse().unwrap();
    assert_eq!(content_length, body.len());
    (head, body)
}

fn header<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

#[tokio::test]
async fn mock_scripts_rest_success_rate_limit_server_error_and_interaction_ack() {
    let mock = MockDiscord::new([
        MockReply::json(200, br#"{"id":"1"}"#).unwrap(),
        MockReply::rate_limited(Duration::from_millis(250)),
        MockReply::server_error(503).unwrap(),
        MockReply::interaction_ack(),
    ]);
    for (request, status) in [(REST, 200), (REST, 429), (REST, 503), (INTERACTION, 204)] {
        let (response, result) = exchange(&mock, request).await;
        result.unwrap();
        let (head, body) = response_parts(&response);
        assert!(head.starts_with(&format!("HTTP/1.1 {status} ")));
        assert_eq!(header(head, "connection"), Some("close"));
        if status == 429 {
            let retry: f64 = header(head, "retry-after").unwrap().parse().unwrap();
            assert!((retry - 0.25).abs() < 0.000_001);
            let json: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(json["global"], false);
            assert!((json["retry_after"].as_f64().unwrap() - retry).abs() < 0.000_001);
        } else if status == 204 {
            assert!(body.is_empty());
        }
    }
    let stats = mock.stats();
    assert_eq!(stats.requests, 4);
    assert_eq!(stats.rest_requests, 3);
    assert_eq!(stats.interaction_requests, 1);
    assert_eq!(stats.applied, 2);
    assert_eq!(stats.replies_sent, 4);
    assert_eq!(stats.lost_responses, 0);
    assert_eq!(mock.remaining(), 0);
}

#[tokio::test]
async fn mock_exhaustion_is_visible_and_never_fabricates_success() {
    let mock = MockDiscord::new([]);
    let (response, result) = exchange(&mock, REST).await;
    result.unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 503 "));
    assert_eq!(mock.stats().script_exhausted, 1);
    assert_eq!(mock.stats().applied, 0);
}

#[tokio::test]
async fn mock_delay_allows_client_timeout_after_request_was_applied() {
    let mock =
        MockDiscord::new([MockReply::interaction_ack().with_delay(Duration::from_millis(150))]);
    let (mut client, server) = tokio::io::duplex(8192);
    let task = tokio::spawn({
        let mock = mock.clone();
        async move { mock.serve(server).await }
    });
    client.write_all(INTERACTION).await.unwrap();
    let mut response = Vec::new();
    assert!(
        timeout(Duration::from_millis(20), client.read_to_end(&mut response))
            .await
            .is_err()
    );
    assert!(response.is_empty());
    assert_eq!(mock.stats().requests, 1);
    assert_eq!(mock.stats().applied, 1);
    assert_eq!(mock.stats().replies_sent, 0);
    timeout(DEADLINE, client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 204 "));
    task.await.unwrap().unwrap();
    assert_eq!(mock.stats().replies_sent, 1);
}

#[tokio::test]
async fn response_loss_and_truncation_leave_applied_operations_unknown_to_client() {
    let mock = MockDiscord::new([
        MockReply::json(200, br#"{"id":"1"}"#)
            .unwrap()
            .with_lost_response(),
        MockReply::json(200, br#"{"id":"2"}"#)
            .unwrap()
            .with_truncated_response(12),
    ]);
    let drain = src::response_drain::ResponseDrain::new(2);
    for prefix in [0, 12] {
        let operation = drain.begin().unwrap();
        let (response, result) = exchange(&mock, REST).await;
        result.unwrap();
        assert_eq!(response.len(), prefix);
        assert!(!response.windows(4).any(|part| part == b"\r\n\r\n"));
        operation.unknown();
    }
    drain.start_drain();
    assert_eq!(drain.wait_settled().await.len(), 2);
    assert_eq!(mock.stats().applied, 2);
    assert_eq!(mock.stats().lost_responses, 2);
    assert_eq!(mock.stats().replies_sent, 0);
}

#[tokio::test]
async fn truncation_request_larger_than_response_still_drops_a_byte() {
    let reply = MockReply::json(200, br#"{"id":"1"}"#).unwrap();
    let full = MockDiscord::new([reply.clone()]);
    let (full, result) = exchange(&full, REST).await;
    result.unwrap();
    let mock = MockDiscord::new([reply.with_truncated_response(usize::MAX)]);
    let (truncated, result) = exchange(&mock, REST).await;
    result.unwrap();
    assert_eq!(truncated.len(), full.len() - 1);
    assert_eq!(truncated, full[..full.len() - 1]);
    assert_eq!(mock.stats().replies_sent, 0);
}

#[tokio::test]
async fn fragmented_headers_and_body_work_over_tiny_duplex_buffers() {
    let mock = MockDiscord::new([MockReply::interaction_ack()]);
    let (client, server) = tokio::io::duplex(8);
    let task = tokio::spawn({
        let mock = mock.clone();
        async move { mock.serve(server).await }
    });
    let mut client = FaultIo::new(
        client,
        write_faults(DirectionFaults {
            max_chunk: chunk(1),
            ..Default::default()
        }),
    )
    .unwrap();
    timeout(DEADLINE, client.write_all(INTERACTION))
        .await
        .unwrap()
        .unwrap();
    let mut response = Vec::new();
    timeout(DEADLINE, client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    task.await.unwrap().unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 204 "));
    assert_eq!(mock.stats().interaction_requests, 1);
}

#[tokio::test]
async fn malformed_ambiguous_and_incomplete_requests_do_not_consume_script() {
    let mock = MockDiscord::new([MockReply::interaction_ack()]);
    for request in [
        b"garbage\r\n\r\n".as_slice(),
        b"POST / HTTP/1.1\nContent-Length: 0\n\n",
        b"POST / HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}",
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        b"POST / HTTP/1.1\r\nContent-Length: invalid\r\n\r\n",
        b"POST / HTTP/1.1\r\nContent-Length: 8\r\n\r\n{}",
        b"POST / HTTP/1.1\r\nContent-Length: 184467440737095516160\r\n\r\n",
        b"POST / HTTP/1.1\r\nX-Invalid: before\rafter\r\n\r\n",
        b"POST / HTTP/1.1\r\nX-Invalid\rName: value\r\n\r\n",
    ] {
        let (response, result) = exchange(&mock, request).await;
        assert!(result.is_err());
        assert!(response.is_empty());
        assert_eq!(mock.remaining(), 1);
    }
    assert_eq!(mock.stats().requests, 0);
    assert_eq!(mock.stats().applied, 0);
    assert_eq!(mock.stats().rejected_requests, 9);
}

#[tokio::test]
async fn header_and_body_limits_reject_oversized_input_before_script_consumption() {
    for (limits, request) in [
        (
            MockLimits {
                max_header_bytes: 40,
                ..Default::default()
            },
            REST,
        ),
        (
            MockLimits {
                max_body_bytes: 1,
                ..Default::default()
            },
            REST,
        ),
    ] {
        let mock = MockDiscord::with_limits([MockReply::interaction_ack()], limits).unwrap();
        let (response, result) = exchange(&mock, request).await;
        assert!(result.is_err());
        assert!(response.is_empty());
        assert_eq!(mock.remaining(), 1);
        assert_eq!(mock.stats().rejected_requests, 1);
    }
}

#[tokio::test]
async fn request_timeout_closes_idle_or_partial_clients_without_claiming_application() {
    let mock = MockDiscord::with_limits(
        [MockReply::interaction_ack()],
        MockLimits {
            request_timeout: Duration::from_millis(30),
            ..Default::default()
        },
    )
    .unwrap();
    let (mut client, server) = tokio::io::duplex(64);
    let task = tokio::spawn({
        let mock = mock.clone();
        async move { mock.serve(server).await }
    });
    client.write_all(b"POST / HTTP/1.1\r\n").await.unwrap();
    let error = timeout(DEADLINE, task).await.unwrap().unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(mock.remaining(), 1);
    assert_eq!(mock.stats().applied, 0);
    assert_eq!(mock.stats().rejected_requests, 1);
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
}

#[test]
fn mock_validates_fixtures_and_limits_without_echoing_input() {
    assert!(MockReply::json(99, b"{}").is_err());
    assert!(MockReply::json(600, b"{}").is_err());
    assert!(MockReply::json(200, b"invalid-json").is_err());
    let error = MockReply::json(200, b"invalid-private-fixture")
        .err()
        .unwrap();
    assert!(!error.to_string().contains("invalid-private-fixture"));
    assert!(MockReply::server_error(429).is_err());
    assert!(MockReply::server_error(600).is_err());
    assert!(
        MockDiscord::with_limits(
            [],
            MockLimits {
                max_connections: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        MockDiscord::with_limits(
            [],
            MockLimits {
                max_header_bytes: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[tokio::test]
async fn loopback_tcp_serves_real_http_and_shutdown_closes_pending_connections() {
    let mock = MockDiscord::new([
        MockReply::interaction_ack(),
        MockReply::interaction_ack().with_delay(Duration::from_secs(60)),
    ]);
    let server = mock.spawn_tcp().await.unwrap();
    assert!(server.addr().ip().is_loopback());
    assert_ne!(server.addr().port(), 0);
    let mut client = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    client.write_all(INTERACTION).await.unwrap();
    let mut response = Vec::new();
    timeout(DEADLINE, client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 204 "));
    let mut pending = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    pending.write_all(INTERACTION).await.unwrap();
    timeout(DEADLINE, async {
        while mock.stats().requests != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(mock.stats().applied, 2);
    timeout(DEADLINE, server.shutdown()).await.unwrap().unwrap();
    let result = timeout(DEADLINE, pending.read(&mut [0; 1])).await.unwrap();
    assert!(matches!(result, Ok(0) | Err(_)));
    assert_eq!(mock.stats().replies_sent, 1);
}

#[tokio::test]
async fn idle_tcp_client_does_not_block_another_connection() {
    let mock = MockDiscord::with_limits(
        [MockReply::interaction_ack()],
        MockLimits {
            max_connections: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let server = mock.spawn_tcp().await.unwrap();
    let _idle = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    let mut active = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    active.write_all(INTERACTION).await.unwrap();
    let mut response = Vec::new();
    timeout(DEADLINE, active.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 204 "));
    timeout(DEADLINE, server.shutdown()).await.unwrap().unwrap();
}

#[tokio::test]
async fn exact_request_limits_and_http_10_empty_body_are_supported() {
    let header_length = REST
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap()
        + 4;
    let mock = MockDiscord::with_limits(
        [MockReply::interaction_ack()],
        MockLimits {
            max_header_bytes: header_length,
            max_body_bytes: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let (response, result) = exchange(&mock, REST).await;
    result.unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 204 "));

    let empty = MockDiscord::with_limits(
        [MockReply::json(200, b"{}").unwrap()],
        MockLimits {
            max_body_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let (response, result) = exchange(&empty, b"GET /api/v10/gateway HTTP/1.0\r\n\r\n").await;
    result.unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 200 "));
    assert_eq!(empty.stats().rest_requests, 1);
}

#[tokio::test]
async fn scripts_follow_complete_request_order_and_ignore_incomplete_earlier_request() {
    let mock = MockDiscord::new([
        MockReply::json(200, br#"{"index":1}"#).unwrap(),
        MockReply::json(200, br#"{"index":2}"#).unwrap(),
    ]);
    let (mut incomplete, server) = tokio::io::duplex(8192);
    let task = tokio::spawn({
        let mock = mock.clone();
        async move { mock.serve(server).await }
    });
    incomplete
        .write_all(b"POST / HTTP/1.1\r\nContent-Length: 8\r\n\r\n12")
        .await
        .unwrap();
    let (first, result) = exchange(&mock, REST).await;
    result.unwrap();
    let json: serde_json::Value = serde_json::from_slice(response_parts(&first).1).unwrap();
    assert_eq!(json["index"], 1);
    assert_eq!(mock.remaining(), 1);
    assert_eq!(mock.stats().requests, 1);
    incomplete.write_all(b"345678").await.unwrap();
    let mut second = Vec::new();
    timeout(DEADLINE, incomplete.read_to_end(&mut second))
        .await
        .unwrap()
        .unwrap();
    task.await.unwrap().unwrap();
    let json: serde_json::Value = serde_json::from_slice(response_parts(&second).1).unwrap();
    assert_eq!(json["index"], 2);
    assert_eq!(mock.stats().requests, 2);
}

#[tokio::test]
async fn tcp_concurrency_limit_retains_only_the_configured_active_sessions() {
    let mock = MockDiscord::with_limits(
        [
            MockReply::interaction_ack().with_delay(Duration::from_millis(180)),
            MockReply::interaction_ack(),
        ],
        MockLimits {
            max_connections: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let server = mock.spawn_tcp().await.unwrap();
    let mut first = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    first.write_all(INTERACTION).await.unwrap();
    timeout(DEADLINE, async {
        while mock.stats().requests != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut next = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    next.write_all(INTERACTION).await.unwrap();
    let mut second = Vec::new();
    assert!(
        timeout(Duration::from_millis(20), next.read_to_end(&mut second))
            .await
            .is_err()
    );
    assert_eq!(mock.stats().requests, 1);
    let mut response = Vec::new();
    timeout(DEADLINE, first.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, next.read_to_end(&mut second))
        .await
        .unwrap()
        .unwrap();
    assert!(response_parts(&response).0.starts_with("HTTP/1.1 204 "));
    assert!(response_parts(&second).0.starts_with("HTTP/1.1 204 "));
    assert_eq!(mock.stats().requests, 2);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_tcp_server_cancels_delayed_children() {
    let mock = MockDiscord::new([MockReply::interaction_ack().with_delay(Duration::from_secs(60))]);
    let server = mock.spawn_tcp().await.unwrap();
    let mut client = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    client.write_all(INTERACTION).await.unwrap();
    timeout(DEADLINE, async {
        while mock.stats().requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(server);
    let read = timeout(DEADLINE, client.read(&mut [0; 1])).await.unwrap();
    assert!(matches!(read, Ok(0) | Err(_)));
    assert_eq!(mock.stats().replies_sent, 0);
}

#[test]
fn overflowing_delay_configuration_is_rejected_without_a_runtime() {
    assert!(
        FaultIo::new(
            (),
            read_faults(DirectionFaults {
                jitter: Duration::MAX,
                ..Default::default()
            })
        )
        .is_err()
    );
    assert!(
        FaultIo::new(
            (),
            write_faults(DirectionFaults {
                latency: Duration::MAX,
                jitter: Duration::from_nanos(1),
                ..Default::default()
            })
        )
        .is_err()
    );
}

#[tokio::test]
async fn tcp_fault_cutoff_closes_the_wire_mid_request_without_application() {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (wire, _) = listener.accept().await.unwrap();
    let mock = MockDiscord::new([MockReply::interaction_ack()]);
    let serving = tokio::spawn({
        let mock = mock.clone();
        async move { mock.serve(wire).await }
    });
    let mut client = FaultIo::new(
        client,
        write_faults(DirectionFaults {
            disconnect_after: Some(7),
            max_chunk: chunk(2),
            ..Default::default()
        }),
    )
    .unwrap();
    assert_eq!(
        client.write_all(REST).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(client.stats().write_bytes, 7);
    assert!(timeout(DEADLINE, serving).await.unwrap().unwrap().is_err());
    assert_eq!(mock.stats().requests, 0);
    assert_eq!(mock.stats().rejected_requests, 1);
    assert_eq!(mock.remaining(), 1);
}

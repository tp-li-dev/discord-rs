//! Loopback-only experiment: sequential trials use an authenticated heartbeat stream.
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use src::{
    cluster::{ControlMessage, Heartbeat, NodeId, Transport},
    failover::{Clock, Config, Event, Failover, State},
    redact::Secret,
    tls_transport::{TlsIdentity, TlsTransport, TlsTransportConfig, node_name},
};
use std::time::{Duration, Instant};
const N: usize = 50;
#[derive(Clone)]
struct RealClock(Instant);
impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}
fn report(name: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    let q = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1].as_secs_f64() * 1000.0;
    println!(
        "{name}: n={} p50={:.3}ms p95={:.3}ms max={:.3}ms",
        samples.len(),
        q(50),
        q(95),
        samples.last().unwrap().as_secs_f64() * 1000.0
    );
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let config = |node: NodeId| {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![node_name(node)]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, node_name(node));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
        let id = TlsIdentity {
            certificates: vec![cert.der().clone()],
            private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
        };
        let mut config = TlsTransportConfig::new(node, id, vec![ca.der().clone()]);
        config.allowed_nodes = [NodeId(1), NodeId(2)].into_iter().collect();
        config
    };
    let (a_id, b_id) = (NodeId(1), NodeId(2));
    let mut a = TlsTransport::bind(config(a_id)).await.unwrap();
    let mut b = TlsTransport::bind(config(b_id)).await.unwrap();
    a.set_peer(b_id, b.local_addr()).unwrap();
    let clock = RealClock(Instant::now());
    let cfg = Config::default();
    let mut suspects = Vec::new();
    let mut failures = Vec::new();
    let mut false_positives = 0;
    let mut last_policy = None;
    for trial in 0..N {
        let mut f = Failover::new(clock.clone(), cfg).unwrap();
        f.handle(Event::Quorum(true)).unwrap();
        for beat in 0..2 {
            a.send_confirmed(
                a_id,
                b_id,
                ControlMessage::Heartbeat(Heartbeat {
                    sequence: (trial * 2 + beat) as u64,
                    sent_at: clock.now(),
                    ownership: vec![],
                }),
            )
            .await
            .unwrap();
            let packet = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(p) = b.receive().unwrap() {
                        break p;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(packet.from, a_id);
            if f.handle(Event::Tick).unwrap() != State::Assigned {
                false_positives += 1;
            }
            assert_eq!(f.handle(Event::Heartbeat).unwrap(), State::Assigned);
            if beat == 0 {
                tokio::time::sleep(cfg.heartbeat_interval).await;
            }
        }
        let stopped = Instant::now();
        let mut suspected = None;
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let state = f.handle(Event::Tick).unwrap();
            let elapsed = stopped.elapsed();
            if state == State::Suspected && suspected.is_none() {
                suspected = Some(elapsed);
            }
            if state == State::FailoverPending {
                suspects.push(suspected.expect("missed Suspected"));
                failures.push(elapsed);
                break;
            }
            assert!(elapsed < Duration::from_secs(10), "detection timeout");
        }
        last_policy = Some(f);
    }
    report("Suspected", &mut suspects);
    report("FailoverPending", &mut failures);
    println!("false_positive_observations={false_positives}; trials={N}; poll=5ms");
    let mut policy = last_policy.unwrap();
    let f = &mut policy;
    let attempt = f.attempt();
    assert_eq!(
        f.handle(Event::OwnershipApproved { attempt }).unwrap(),
        State::StandbyReady
    );
    assert_eq!(
        f.handle(Event::StartupComplete { attempt }).unwrap(),
        State::TemporaryActive
    );
    tokio::time::sleep(cfg.cooldown + Duration::from_millis(10)).await;
    assert_eq!(f.handle(Event::Heartbeat).unwrap(), State::FailbackWaiting);
    let attempt = f.attempt();
    let start = Instant::now();
    tokio::time::sleep(cfg.suspect_after + Duration::from_millis(10)).await;
    let state = f.handle(Event::Tick).unwrap();
    let after = f.handle(Event::FailbackComplete { attempt }).unwrap();
    println!(
        "failback instability after {:.3}ms: {state:?}; stale completion: {after:?}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    assert_eq!(state, State::TemporaryActive);
    assert_eq!(after, State::TemporaryActive);
    a.shutdown().await;
    b.shutdown().await;
    println!("failures=0; synthetic approvals; no coordinator, lease, process restart or Discord");
}

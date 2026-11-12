use src::{
    local_control::{ListRequest, ListResponse, serve_list},
    worker::Manager,
};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn authenticated_private_session_lists_only_unknown_ids() {
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    drop(manager.begin_response(0).await.unwrap());
    let secret = [7u8; 32];
    let (client, server) = tokio::io::duplex(8192);
    let task = tokio::spawn(async move { serve_list(server, &secret, &manager).await });
    let mut client = BufReader::new(client);
    let mut data = serde_json::to_vec(&ListRequest {
        secret,
        command: "list_unknown".into(),
    })
    .unwrap();
    data.push(b'\n');
    client.get_mut().write_all(&data).await.unwrap();
    let mut line = String::new();
    client.read_line(&mut line).await.unwrap();
    let result: ListResponse = serde_json::from_str(&line).unwrap();
    assert_eq!(result.unknown, vec![(0, 0)]);
    assert_eq!(result.instance.len(), 32);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn incorrect_secret_is_rejected_without_response() {
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    let (mut client, server) = tokio::io::duplex(8192);
    let task = tokio::spawn(async move { serve_list(server, &[7; 32], &manager).await });
    let mut data = serde_json::to_vec(&ListRequest {
        secret: [8; 32],
        command: "list_unknown".into(),
    })
    .unwrap();
    data.push(b'\n');
    client.write_all(&data).await.unwrap();
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let mut bytes = [0; 1];
    assert_eq!(
        tokio::io::AsyncReadExt::read(&mut client, &mut bytes)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn invalid_frames_fail_closed() {
    for frame in [b"{}\n".to_vec(), vec![b'x'; 4097], b"partial".to_vec()] {
        let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
            .await
            .unwrap();
        let (mut client, server) = tokio::io::duplex(8192);
        let task = tokio::spawn(async move { serve_list(server, &[7; 32], &manager).await });
        client.write_all(&frame).await.unwrap();
        client.shutdown().await.unwrap();
        assert!(task.await.unwrap().is_err());
        let mut reply = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut client, &mut reply)
            .await
            .unwrap();
        assert!(reply.is_empty());
    }
}

#[tokio::test]
async fn bootstrap_bound_gateway_lists_and_disconnect_is_error() {
    use src::local_control::{Bootstrap, GatewayControl, gateway_session};
    let mut host = GatewayControl::bind().await.unwrap();
    let (mut reader, writer) = tokio::io::duplex(4096);
    host.bootstrap(writer).await.unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    let bootstrap: Bootstrap = serde_json::from_slice(&bytes).unwrap();
    // Invalid and idle peers cannot be accepted as the real Gateway.
    let mut invalid = tokio::net::TcpStream::connect(("127.0.0.1", bootstrap.port))
        .await
        .unwrap();
    invalid.write_all(&[0; 32]).await.unwrap();
    let idle = tokio::net::TcpStream::connect(("127.0.0.1", bootstrap.port))
        .await
        .unwrap();
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    drop(manager.begin_response(0).await.unwrap());
    let task = tokio::spawn(gateway_session(bootstrap, manager));
    let reply = host.list().await.unwrap();
    assert_eq!(reply.unknown.len(), 1);
    assert_eq!(reply.unknown[0].0, 0); // worker 0; operation id is process-unique, not fixed
    assert_eq!(host.list().await.unwrap(), reply);
    drop(idle);
    task.abort();
    let _ = task.await;
    assert!(host.list().await.is_err());
}

#[tokio::test]
async fn absent_gateway_is_not_an_empty_list() {
    let mut host = src::local_control::GatewayControl::bind().await.unwrap();
    assert!(host.list().await.is_err());
}

#[tokio::test]
async fn connection_exhaustion_fails_closed_and_recovers() {
    use src::local_control::{Bootstrap, GatewayControl, gateway_session};
    let mut host = GatewayControl::bind().await.unwrap();
    let (mut reader, writer) = tokio::io::duplex(4096);
    host.bootstrap(writer).await.unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    let bootstrap: Bootstrap = serde_json::from_slice(&bytes).unwrap();
    let mut idle = Vec::new();
    for _ in 0..40 {
        idle.push(
            tokio::net::TcpStream::connect(("127.0.0.1", bootstrap.port))
                .await
                .unwrap(),
        );
    }
    assert!(host.list().await.is_err());
    drop(idle);
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    let task = tokio::spawn(gateway_session(bootstrap, manager));
    assert!(host.list().await.unwrap().unknown.is_empty());
    task.abort();
    let _ = task.await;
}

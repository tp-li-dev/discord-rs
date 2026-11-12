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

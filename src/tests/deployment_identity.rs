use src::worker::{DeploymentIdentity, Manager};
use std::process::Command;

fn config() -> DeploymentIdentity {
    DeploymentIdentity::parse(|key| {
        Ok(match key {
            "BOT_ID" => Some("42".into()),
            "SHARD_ID" => Some("2".into()),
            "TOTAL_SHARDS" => Some("4".into()),
            "GENERATION_ID" => Some("100".into()),
            _ => None,
        })
    })
    .unwrap()
}

#[test]
fn deployment_defaults_and_invalid_values() {
    let defaults = DeploymentIdentity::parse(|_| Ok(None)).unwrap();
    assert_eq!(defaults.bot_id.0, 0);
    assert_eq!((defaults.shard.id(), defaults.shard.total()), (0, 1));
    for (key, value) in [
        ("BOT_ID", ""),
        ("BOT_ID", "-1"),
        ("BOT_ID", "18446744073709551616"),
        ("SHARD_ID", "1"),
        ("TOTAL_SHARDS", "0"),
        ("TOTAL_SHARDS", "4294967296"),
        ("GENERATION_ID", "secret-invalid"),
        ("SHARD_ID", " 0"),
    ] {
        let error = DeploymentIdentity::parse(|name| Ok((name == key).then(|| value.into())))
            .err()
            .expect("invalid deployment accepted");
        assert!(!error.to_string().contains("secret-invalid"));
    }
    assert!(DeploymentIdentity::parse(|_| Err("invalid deployment environment")).is_err());
}

#[tokio::test]
async fn manager_passes_deployment_identity_and_rejects_mismatches() {
    let directory =
        std::env::temp_dir().join(format!("deployment-identity-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let fixture = directory.join(format!("good{}", std::env::consts::EXE_SUFFIX));
    assert!(
        Command::new("rustc")
            .arg("--edition=2024")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/deployment_worker.rs")
            )
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success()
    );
    // The fixture verifies the actual child environment and the Manager Hello.
    let manager = Manager::start_pool_with_identity(fixture.clone(), 1, config())
        .await
        .unwrap();
    manager.replace(fixture.clone(), true).await.unwrap();
    let before = manager.describe_workers().await;
    for name in ["bot", "shard", "total", "generation"] {
        let candidate = directory.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(&fixture, &candidate).unwrap();
        assert!(
            manager.replace(candidate, true).await.is_err(),
            "accepted {name} mismatch"
        );
        assert_eq!(manager.describe_workers().await, before);
    }
    // Also exercise the production Worker with non-default Bot/Shard metadata.
    let real = Manager::start_pool_with_identity(env!("CARGO_BIN_EXE_src").into(), 2, config())
        .await
        .unwrap();
    assert!(
        real.dispatch("!ping".into())
            .await
            .unwrap()
            .unwrap()
            .starts_with("Pong!")
    );
}

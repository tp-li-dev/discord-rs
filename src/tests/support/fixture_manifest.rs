//! Create an independent Cargo workspace with the same dependency overrides.
use std::{
    fs,
    path::{Path, PathBuf},
};

pub(super) fn write_fixture_project(
    project: &Path,
    package_name: &str,
    binary_name: &str,
    binary_source: &str,
) -> PathBuf {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fixture_manifest(source_root, package_name, binary_name, binary_source);
    let manifest_path = project.join("Cargo.toml");
    fs::write(
        &manifest_path,
        toml::to_string(&manifest).expect("serialize fixture manifest"),
    )
    .expect("write fixture manifest");

    // Seed the independent workspace with the main workspace's versions.
    // Cargo may update this copy to account for the fixture package.
    fs::copy(source_root.join("Cargo.lock"), project.join("Cargo.lock"))
        .expect("copy workspace dependency lock for fixture");
    manifest_path
}

fn fixture_manifest(
    source_root: &Path,
    package_name: &str,
    binary_name: &str,
    binary_source: &str,
) -> toml::Value {
    let mut manifest: toml::Value = toml::from_str(
        r#"
[package]
name = ""
version = "0.0.0"
edition = "2024"
publish = false
[workspace]
[dependencies]
src = { path = "" }
bot-framework = { path = "" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
[[bin]]
name = ""
path = ""
"#,
    )
    .expect("fixture manifest template");
    manifest["package"]["name"] = toml::Value::String(package_name.to_owned());
    manifest["dependencies"]["src"]["path"] = path_value(source_root);
    manifest["dependencies"]["bot-framework"]["path"] = path_value(&source_root.join("framework"));
    manifest["bin"][0]["name"] = toml::Value::String(binary_name.to_owned());
    manifest["bin"][0]["path"] = path_value(&source_root.join(binary_source));

    // Cargo uses only the current workspace root's overrides. Resolve each
    // path against the manifest that declared it before moving the table.
    let mut source = read_workspace_manifest(source_root);
    let fixture = manifest.as_table_mut().expect("fixture manifest table");
    if let Some(mut patches) = source.remove("patch") {
        for (_, entries) in patches.as_table_mut().expect("[patch] table") {
            rebase_paths(entries, source_root);
        }
        fixture.insert("patch".into(), patches);
    }
    if let Some(mut replacements) = source.remove("replace") {
        rebase_paths(&mut replacements, source_root);
        fixture.insert("replace".into(), replacements);
    }
    manifest
}

fn read_workspace_manifest(source_root: &Path) -> toml::Table {
    let contents = fs::read_to_string(source_root.join("Cargo.toml"))
        .expect("read workspace Cargo.toml for fixture");
    toml::from_str(&contents).expect("parse workspace Cargo.toml for fixture")
}

fn rebase_paths(entries: &mut toml::Value, source_root: &Path) {
    for (_, entry) in entries.as_table_mut().expect("override table") {
        if let Some(path) = entry.get_mut("path") {
            *path = path_value(&source_root.join(path.as_str().expect("override path string")));
        }
    }
}

fn path_value(path: &Path) -> toml::Value {
    toml::Value::String(
        path.to_str()
            .expect("fixture Cargo manifest path must be Unicode")
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_fixture_inherits_workspace_patches() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let source = read_workspace_manifest(root);
        let Some(patches) = source.get("patch") else {
            return;
        };
        let fixture = fixture_manifest(root, "test-fixture", "test-fixture", "src/lib.rs");
        let fixture = fixture
            .get("patch")
            .expect("fixture must include [patch] from src/Cargo.toml");
        for (registry, entries) in patches.as_table().unwrap() {
            let fixture_entries = fixture.get(registry).unwrap_or_else(|| {
                panic!("fixture is missing patches for {registry} from src/Cargo.toml")
            });
            for (name, entry) in entries.as_table().unwrap() {
                let mut expected = entry.clone();
                if let Some(path) = entry.get("path").and_then(toml::Value::as_str) {
                    expected["path"] = path_value(&root.join(path));
                }
                assert_eq!(
                    fixture_entries.get(name),
                    Some(&expected),
                    "fixture must carry the workspace patch for {registry}/{name}"
                );
            }
        }
    }
}

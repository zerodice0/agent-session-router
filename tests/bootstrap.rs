use std::{
    fs::{self, FileTimes, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};

use agent_session_router::{
    bootstrap::{BootstrapAssets, BootstrapError, MANIFEST_FILE},
    onboarding::{BootstrapArtifact, BootstrapManifest, VERSION},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const TARGET: &str = "aarch64-apple-darwin";
const BINARY: &str = "asr-aarch64-apple-darwin";
const ARCHIVE: &str = "agent-session-router-aarch64-apple-darwin.tar.gz";
const BINARY_BYTES: &[u8] = b"verified raw binary bytes";
const ARCHIVE_BYTES: &[u8] = b"verified archive bytes";

struct Bundle {
    root: TempDir,
    directory: PathBuf,
}

impl Bundle {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = root.path().canonicalize().unwrap().join("owned/bootstrap");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(
            directory.parent().unwrap(),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
        let bundle = Self { root, directory };
        bundle.write_contents();
        bundle
    }

    fn write_contents(&self) {
        write_file(&self.directory.join(BINARY), BINARY_BYTES, 0o755);
        write_file(&self.directory.join(ARCHIVE), ARCHIVE_BYTES, 0o644);
        self.write_manifest(&serde_json::to_value(manifest()).unwrap());
    }

    fn write_manifest(&self, value: &Value) {
        write_file(
            &self.directory.join(MANIFEST_FILE),
            &serde_json::to_vec(value).unwrap(),
            0o644,
        );
    }

    fn load(&self) -> BootstrapAssets {
        BootstrapAssets::load(&self.directory)
            .unwrap()
            .expect("present bundle")
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn manifest() -> BootstrapManifest {
    BootstrapManifest {
        version: VERSION,
        asr_version: "0.2.0".into(),
        artifacts: vec![BootstrapArtifact {
            target: TARGET.into(),
            binary_file: BINARY.into(),
            binary_sha256: digest(BINARY_BYTES),
            archive_file: ARCHIVE.into(),
            archive_sha256: digest(ARCHIVE_BYTES),
            binary_bytes: BINARY_BYTES.len() as u64,
            archive_bytes: ARCHIVE_BYTES.len() as u64,
        }],
    }
}

fn write_file(path: &Path, bytes: &[u8], mode: u32) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn assert_invalid(path: &Path) {
    assert!(matches!(BootstrapAssets::load(path), Err(BootstrapError)));
}

fn read_asset(assets: &BootstrapAssets, name: &str) -> Vec<u8> {
    let (mut file, size) = assets.file(name).unwrap().expect("whitelisted file");
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes.len() as u64, size);
    bytes
}

#[test]
fn absent_directory_is_distinct_from_a_present_incomplete_or_invalid_bundle() {
    let bundle = Bundle::new();
    let missing = bundle.directory.join("not-created/child");
    assert!(BootstrapAssets::load(&missing).unwrap().is_none());
    for name in [MANIFEST_FILE, BINARY, ARCHIVE] {
        let bundle = Bundle::new();
        fs::remove_file(bundle.directory.join(name)).unwrap();
        assert_invalid(&bundle.directory);
    }
    assert_invalid(&bundle.directory.join(BINARY));
    let error = BootstrapAssets::load(&bundle.directory.join(BINARY)).unwrap_err();
    assert_eq!(error.to_string(), "bootstrap_assets_invalid");
    assert!(std::error::Error::source(&error).is_none());
}

#[test]
fn valid_bundle_serves_exact_raw_manifest_and_only_whitelisted_assets() {
    let bundle = Bundle::new();
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest()).unwrap();
    manifest_bytes.push(b'\n');
    write_file(
        &bundle.directory.join(MANIFEST_FILE),
        &manifest_bytes,
        0o644,
    );
    write_file(
        &bundle.directory.join("unlisted-secret"),
        b"not public",
        0o600,
    );
    let assets = bundle.load();
    assert_eq!(assets.manifest(), &manifest());
    assert_eq!(assets.manifest_sha256(), digest(&manifest_bytes));
    assert_eq!(read_asset(&assets, MANIFEST_FILE), manifest_bytes);
    assert_eq!(read_asset(&assets, BINARY), BINARY_BYTES);
    assert_eq!(read_asset(&assets, ARCHIVE), ARCHIVE_BYTES);
    for name in [
        "unlisted-secret",
        "../bootstrap/bootstrap-manifest.json",
        "/etc/passwd",
        "./bootstrap-manifest.json",
        "%2e%2e/unlisted-secret",
        "bootstrap-manifest.json/",
        "",
    ] {
        assert!(
            assets.file(name).unwrap().is_none(),
            "unexpected whitelist entry"
        );
    }
}

#[test]
fn separate_downloads_have_independent_offsets_even_under_concurrent_reads() {
    let bundle = Bundle::new();
    let assets = Arc::new(bundle.load());
    for name in [MANIFEST_FILE, BINARY, ARCHIVE] {
        let expected = fs::read(bundle.directory.join(name)).unwrap();
        let (mut first, _) = assets.file(name).unwrap().unwrap();
        let mut prefix = [0_u8; 3];
        first.read_exact(&mut prefix).unwrap();
        assert_eq!(prefix, expected[..3]);
        let other_assets = Arc::clone(&assets);
        let second = thread::spawn(move || read_asset(&other_assets, name));
        let mut rest = Vec::new();
        first.read_to_end(&mut rest).unwrap();
        assert_eq!(second.join().unwrap(), expected);
        assert_eq!(rest, expected[3..]);
        first.seek(SeekFrom::Start(1)).unwrap();
        assert_eq!(read_asset(&assets, name), expected);
    }
}

#[test]
fn manifest_size_limit_is_applied_to_raw_bytes_before_deserialization() {
    let bundle = Bundle::new();
    let mut bytes = serde_json::to_vec(&manifest()).unwrap();
    bytes.resize(128 * 1024, b' ');
    write_file(&bundle.directory.join(MANIFEST_FILE), &bytes, 0o644);
    let assets = bundle.load();
    assert_eq!(assets.manifest_sha256(), digest(&bytes));
    assert_eq!(read_asset(&assets, MANIFEST_FILE), bytes);
    bytes.push(b' ');
    write_file(&bundle.directory.join(MANIFEST_FILE), &bytes, 0o644);
    assert_invalid(&bundle.directory);
}

#[test]
fn startup_enforces_strict_versioned_manifest_and_artifact_contract() {
    let valid = serde_json::to_value(manifest()).unwrap();
    let mut bad_version = valid.clone();
    bad_version["version"] = json!(2);
    let mut unknown_manifest_field = valid.clone();
    unknown_manifest_field["downloadUrl"] = json!("https://untrusted.invalid");
    let mut unknown_artifact_field = valid.clone();
    unknown_artifact_field["artifacts"][0]["command"] = json!("not allowed");
    let mut unsupported_target = valid.clone();
    unsupported_target["artifacts"][0]["target"] = json!("x86_64-pc-windows-msvc");
    let mut duplicate_target = valid.clone();
    duplicate_target["artifacts"]
        .as_array_mut()
        .unwrap()
        .push(valid["artifacts"][0].clone());
    let mut empty_artifacts = valid.clone();
    empty_artifacts["artifacts"] = json!([]);
    let mut empty_version = valid;
    empty_version["asrVersion"] = json!("");
    for invalid in [
        bad_version,
        unknown_manifest_field,
        unknown_artifact_field,
        unsupported_target,
        duplicate_target,
        empty_artifacts,
        empty_version,
    ] {
        let bundle = Bundle::new();
        bundle.write_manifest(&invalid);
        assert_invalid(&bundle.directory);
    }
}

#[test]
fn manifest_cannot_name_paths_or_files_outside_fixed_target_basenames() {
    for (field, name) in [
        ("binaryFile", "../asr-aarch64-apple-darwin"),
        (
            "archiveFile",
            "/agent-session-router-aarch64-apple-darwin.tar.gz",
        ),
        ("binaryFile", "asr-x86_64-apple-darwin"),
        ("archiveFile", "bootstrap-manifest.json"),
    ] {
        let bundle = Bundle::new();
        let mut invalid = serde_json::to_value(manifest()).unwrap();
        invalid["artifacts"][0][field] = json!(name);
        bundle.write_manifest(&invalid);
        assert_invalid(&bundle.directory);
    }
}

#[test]
fn both_binary_and_archive_require_the_declared_digest_and_exact_size() {
    for (name, field) in [(BINARY, "binaryBytes"), (ARCHIVE, "archiveBytes")] {
        let bundle = Bundle::new();
        let path = bundle.directory.join(name);
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        write_file(&path, &bytes, 0o644);
        assert_invalid(&bundle.directory);

        bundle.write_contents();
        let mut invalid = serde_json::to_value(manifest()).unwrap();
        invalid["artifacts"][0][field] = json!(bytes.len() + 1);
        bundle.write_manifest(&invalid);
        assert_invalid(&bundle.directory);
    }
}

#[test]
fn symlinks_in_assets_directory_or_ancestors_are_never_followed() {
    for name in [MANIFEST_FILE, BINARY, ARCHIVE] {
        let bundle = Bundle::new();
        let source = bundle.directory.join(name);
        let outside = bundle.root.path().join("outside");
        fs::rename(&source, &outside).unwrap();
        symlink(&outside, &source).unwrap();
        assert_invalid(&bundle.directory);
    }
    let bundle = Bundle::new();
    let linked = bundle.root.path().join("linked");
    symlink(&bundle.directory, &linked).unwrap();
    assert_invalid(&linked);
    let linked_parent = bundle.root.path().join("linked-parent");
    symlink(bundle.directory.parent().unwrap(), &linked_parent).unwrap();
    assert_invalid(&linked_parent.join("bootstrap"));
    let dangling = bundle.root.path().join("dangling");
    symlink(bundle.root.path().join("absent"), &dangling).unwrap();
    assert_invalid(&dangling);
}

#[test]
fn nonregular_files_and_hardlinks_are_rejected_without_reading() {
    for kind in ["directory", "fifo", "hardlink"] {
        let bundle = Bundle::new();
        let path = bundle.directory.join(BINARY);
        fs::remove_file(&path).unwrap();
        match kind {
            "directory" => fs::create_dir(&path).unwrap(),
            "fifo" => {
                assert!(
                    std::process::Command::new("mkfifo")
                        .arg(&path)
                        .status()
                        .unwrap()
                        .success()
                );
            }
            "hardlink" => {
                let original = bundle.root.path().join("hardlink-original");
                write_file(&original, BINARY_BYTES, 0o755);
                fs::hard_link(original, &path).unwrap();
            }
            _ => unreachable!(),
        }
        assert_invalid(&bundle.directory);
    }
}

#[test]
fn unsafe_permissions_on_files_bundle_or_ancestors_are_rejected() {
    for (name, mode) in [
        (BINARY, 0o775),
        (ARCHIVE, 0o646),
        (MANIFEST_FILE, 0o4644),
        ("", 0o777),
        ("..", 0o775),
    ] {
        let bundle = Bundle::new();
        fs::set_permissions(
            bundle.directory.join(name),
            fs::Permissions::from_mode(mode),
        )
        .unwrap();
        assert_invalid(&bundle.directory);
    }
}

#[test]
fn foreign_ownership_is_rejected_when_privileged_fixtures_are_available() {
    if rustix::process::getuid() != rustix::process::Uid::ROOT {
        // Ordinary users cannot create a foreign-owned, otherwise valid bundle.
        return;
    }
    for name in [MANIFEST_FILE, BINARY, ARCHIVE, "", ".."] {
        let bundle = Bundle::new();
        rustix::fs::chown(
            bundle.directory.join(name),
            Some(rustix::fs::Uid::from_raw(65534)),
            None,
        )
        .unwrap();
        assert_invalid(&bundle.directory);
    }
}

#[test]
fn replacing_any_verified_file_permanently_stops_the_bundle() {
    for name in [MANIFEST_FILE, BINARY, ARCHIVE] {
        let bundle = Bundle::new();
        let assets = bundle.load();
        let path = bundle.directory.join(name);
        let retained = bundle.root.path().join("original");
        fs::rename(&path, &retained).unwrap();
        write_file(&path, &fs::read(&retained).unwrap(), 0o644);
        assert_eq!(assets.file(name).unwrap_err(), BootstrapError);
        fs::remove_file(&path).unwrap();
        fs::rename(&retained, &path).unwrap();
        assert_eq!(assets.file(MANIFEST_FILE).unwrap_err(), BootstrapError);
        assert_eq!(assets.file(ARCHIVE).unwrap_err(), BootstrapError);
    }
}

#[test]
fn replacing_bundle_directory_or_an_ancestor_stops_serving_retained_files() {
    for replace_parent in [false, true] {
        let bundle = Bundle::new();
        let assets = bundle.load();
        let original = if replace_parent {
            bundle.directory.parent().unwrap()
        } else {
            &bundle.directory
        };
        let retained = bundle.root.path().join("retained");
        fs::rename(original, &retained).unwrap();
        fs::create_dir_all(&bundle.directory).unwrap();
        bundle.write_contents();
        assert_eq!(assets.file(BINARY).unwrap_err(), BootstrapError);
    }
}

#[test]
fn in_place_mutation_of_any_asset_stops_all_subsequent_downloads() {
    for name in [MANIFEST_FILE, BINARY, ARCHIVE] {
        let bundle = Bundle::new();
        let path = bundle.directory.join(name);
        let mut writer = OpenOptions::new().write(true).open(&path).unwrap();
        let original_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        writer
            .set_times(FileTimes::new().set_modified(original_time))
            .unwrap();
        let assets = bundle.load();
        writer.write_all(b"X").unwrap();
        writer
            .set_times(FileTimes::new().set_modified(original_time + Duration::from_secs(1)))
            .unwrap();
        assert_eq!(assets.file(BINARY).unwrap_err(), BootstrapError);
        assert_eq!(assets.file(MANIFEST_FILE).unwrap_err(), BootstrapError);
    }
}

#[test]
fn permission_changes_after_startup_disable_serving() {
    let bundle = Bundle::new();
    let assets = bundle.load();
    fs::set_permissions(
        bundle.directory.join(ARCHIVE),
        fs::Permissions::from_mode(0o666),
    )
    .unwrap();
    assert_eq!(assets.file(MANIFEST_FILE).unwrap_err(), BootstrapError);
    let bundle = Bundle::new();
    let assets = bundle.load();
    fs::set_permissions(
        bundle.directory.parent().unwrap(),
        fs::Permissions::from_mode(0o777),
    )
    .unwrap();
    assert_eq!(assets.file(BINARY).unwrap_err(), BootstrapError);
}

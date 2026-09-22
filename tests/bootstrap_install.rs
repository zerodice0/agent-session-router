use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_session_router::{
    bootstrap::{
        install::{BundleInstallError, InstalledBundle, host_target, install_bundle},
        routes::{VerifiedRoute, probe_routes},
    },
    credentials::SecretToken,
    onboarding::{
        BootstrapArtifact, BootstrapManifest, OnboardingInfo, OnboardingRoute, OnboardingTicket,
        RouteKind, TARGETS, VERSION,
    },
    protocol::PROTOCOL_VERSION,
};
use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    response::IntoResponse,
};
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpListener;
use uuid::Uuid;

const ASSETS: [&str; 10] = [
    "omp/index.js",
    "omp/package.json",
    "claude-sdk/bridge.js",
    "claude-sdk/manifest.json",
    "claude-sdk/package.json",
    "claude-plugin/.claude-plugin/marketplace.json",
    "claude-plugin/plugins/asr/.claude-plugin/plugin.json",
    "claude-plugin/plugins/asr/skills/workspace/SKILL.md",
    "codex/skills/asr/SKILL.md",
    "omp/skills/asr/SKILL.md",
];
const ROOT: &str = "share/agent-session-router/integrations";

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn append(
    builder: &mut tar::Builder<Vec<u8>>,
    name: &str,
    kind: tar::EntryType,
    mode: u32,
    bytes: &[u8],
) {
    let mut header = tar::Header::new_ustar();
    header.set_mode(mode);
    header.set_entry_type(kind);
    header.set_size(bytes.len() as u64);
    // Deliberately bypass the builder's safe-path guard: malicious wire bytes
    // must exercise the installer's validation, not fixture construction.
    assert!(name.len() < 100);
    header.as_mut_bytes()[..100].fill(0);
    header.as_mut_bytes()[..name.len()].copy_from_slice(name.as_bytes());
    if kind.is_symlink() || kind.is_hard_link() {
        header.set_link_name("/tmp/asr-outside").unwrap();
    }
    header.set_cksum();
    builder.append(&header, bytes).unwrap();
}

fn archive(binary: &[u8], mode: &str) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for directory in ["bin", "share", "share/agent-session-router", ROOT] {
        append(
            &mut builder,
            directory,
            tar::EntryType::Directory,
            0o755,
            b"",
        );
    }
    let binary_bytes = if mode == "binary-mismatch" {
        b"different executable".as_slice()
    } else {
        binary
    };
    append(
        &mut builder,
        "bin/asr",
        tar::EntryType::Regular,
        if mode == "setuid" { 0o4755 } else { 0o755 },
        binary_bytes,
    );
    if mode != "missing-assets" {
        for asset in ASSETS {
            append(
                &mut builder,
                &format!("{ROOT}/{asset}"),
                tar::EntryType::Regular,
                0o644,
                asset.as_bytes(),
            );
        }
    }
    append_archive_metadata(&mut builder, mode);
    append_malformed_entry(&mut builder, binary, mode);
    let mut tar = builder.into_inner().unwrap();
    if mode == "expanded-limit" {
        // An oversized first header is enough to reject before reading its body.
        let mut header = tar::Header::new_ustar();
        header.set_path("bin/asr").unwrap();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o755);
        header.set_size(2 * 1024 * 1024 * 1024 + 1);
        header.set_cksum();
        tar = header.as_bytes().to_vec();
    }
    let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
    gzip.write_all(&tar).unwrap();
    gzip.finish().unwrap()
}

fn append_archive_metadata(builder: &mut tar::Builder<Vec<u8>>, mode: &str) {
    if matches!(
        mode,
        "gnu-longname" | "pax-path" | "gnu-traversal" | "pax-traversal" | "metadata-limit"
    ) {
        let path = if mode.ends_with("traversal") {
            format!("{ROOT}/../../../../escaped")
        } else {
            format!("{ROOT}/{}", "long-component".repeat(12))
        };
        if mode.starts_with("pax") {
            builder
                .append_pax_extensions([("path", path.as_bytes())])
                .unwrap();
        } else {
            let mut name = if mode == "metadata-limit" {
                vec![b'x'; 64 * 1024]
            } else {
                path.into_bytes()
            };
            name.push(0);
            append(
                builder,
                "././@LongLink",
                tar::EntryType::GNULongName,
                0o644,
                &name,
            );
        }
        append(
            builder,
            &format!("{ROOT}/placeholder"),
            tar::EntryType::Regular,
            0o644,
            b"extended",
        );
    }
}

fn append_malformed_entry(builder: &mut tar::Builder<Vec<u8>>, binary: &[u8], mode: &str) {
    match mode {
        "traversal" => append(
            builder,
            "share/../../escaped",
            tar::EntryType::Regular,
            0o644,
            b"escaped",
        ),
        "absolute" => append(
            builder,
            "/tmp/asr-outside",
            tar::EntryType::Regular,
            0o644,
            b"escaped",
        ),
        "forbidden" => append(
            builder,
            "bin/other",
            tar::EntryType::Regular,
            0o644,
            b"extra",
        ),
        "duplicate" => append(builder, "bin/asr", tar::EntryType::Regular, 0o755, binary),
        "symlink" => append(
            builder,
            &format!("{ROOT}/link"),
            tar::EntryType::Symlink,
            0o777,
            b"",
        ),
        "hardlink" => append(
            builder,
            &format!("{ROOT}/link"),
            tar::EntryType::Link,
            0o644,
            b"",
        ),
        "fifo" => append(
            builder,
            &format!("{ROOT}/fifo"),
            tar::EntryType::Fifo,
            0o644,
            b"",
        ),
        "device" => append(
            builder,
            &format!("{ROOT}/device"),
            tar::EntryType::Char,
            0o644,
            b"",
        ),
        _ => {}
    }
}

#[derive(Clone)]
struct HttpFixture {
    files: Arc<BTreeMap<String, Vec<u8>>>,
    requests: Arc<Mutex<Vec<String>>>,
    mode: String,
}

async fn serve(State(state): State<HttpFixture>, request: Request) -> axum::response::Response {
    let path = request.uri().path().to_owned();
    assert_eq!(request.method(), "GET");
    assert!(request.uri().query().is_none());
    assert!(!request.headers().contains_key("authorization"));
    state.requests.lock().unwrap().push(path.clone());
    if state.mode == "redirect" && path.ends_with("bootstrap-manifest.json") {
        return (StatusCode::FOUND, [("location", "/untrusted")]).into_response();
    }
    if state.mode == "slow-download" && path.ends_with(".tar.gz") {
        tokio::time::sleep(Duration::from_secs(6)).await;
    }
    match state.files.get(&path) {
        Some(bytes) => (StatusCode::OK, bytes.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn release_archive(home: &Path, binary: &[u8]) -> Vec<u8> {
    let distribution = home.join("distribution");
    fs::create_dir_all(distribution.join("bin")).unwrap();
    fs::write(distribution.join("bin/asr"), binary).unwrap();
    fs::set_permissions(
        distribution.join("bin/asr"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    for asset in ASSETS {
        let path = distribution.join(ROOT).join(asset);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, asset.as_bytes()).unwrap();
    }
    // Use exactly the release workflow's native tar layout, not only a Rust
    // builder fixture, so platform tar metadata incompatibilities are visible.
    let archive_file = home.join("distribution.tar.gz");
    let output = Command::new("tar")
        .arg("-C")
        .arg(&distribution)
        .env("COPYFILE_DISABLE", "1")
        .arg("-czf")
        .arg(&archive_file)
        .args(["bin", "share"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::read(archive_file).unwrap()
}

struct DownloadFixture {
    binary: Vec<u8>,
    artifact: BootstrapArtifact,
    manifest: Vec<u8>,
    archive: Vec<u8>,
    manifest_digest: String,
}

fn download_fixture(mode: &str, home: &Path) -> DownloadFixture {
    let release_tar = matches!(
        mode,
        "valid" | "slow-download" | "conflict" | "unsafe-install-path"
    );
    let native = release_tar || matches!(mode, "gnu-longname" | "pax-path");
    let binary = if native {
        fs::read(env!("CARGO_BIN_EXE_asr")).unwrap()
    } else {
        b"#!/bin/sh\ntouch \"$HOME/executed\"\n".to_vec()
    };
    let mut archive = if release_tar {
        release_archive(home, &binary)
    } else {
        archive(&binary, mode)
    };
    let target = if mode == "unsupported-target" {
        *TARGETS
            .iter()
            .find(|target| **target != host_target().unwrap())
            .unwrap()
    } else {
        host_target().unwrap()
    };
    let artifact = BootstrapArtifact {
        target: target.to_owned(),
        binary_file: format!("asr-{target}"),
        binary_sha256: digest(&binary),
        archive_file: format!("agent-session-router-{target}.tar.gz"),
        archive_sha256: digest(&archive),
        binary_bytes: binary.len() as u64,
        archive_bytes: archive.len() as u64,
    };
    let mut manifest = serde_json::to_vec(&BootstrapManifest {
        version: VERSION,
        asr_version: env!("CARGO_PKG_VERSION").to_owned(),
        artifacts: vec![artifact.clone()],
    })
    .unwrap();
    let manifest_digest = digest(&manifest);
    if mode == "manifest-mutation" {
        manifest[0] ^= 1;
    }
    if mode == "archive-mutation" {
        archive[0] ^= 1;
    }
    if mode == "manifest-limit" {
        manifest = vec![b' '; 128 * 1024 + 1];
    }
    if mode == "archive-size" {
        archive.push(0);
    }
    DownloadFixture {
        binary,
        artifact,
        manifest,
        archive,
        manifest_digest,
    }
}

async fn scenario(mode: &str, home: &Path) {
    let DownloadFixture {
        binary,
        artifact,
        manifest,
        archive,
        manifest_digest,
    } = download_fixture(mode, home);
    let server_id = Uuid::new_v4();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let route = OnboardingRoute {
        kind: RouteKind::Local,
        router_url: format!("ws://{}/ws", listener.local_addr().unwrap()),
        ca_pem: None,
    };
    let mut ticket = OnboardingTicket {
        version: VERSION,
        server_id,
        invite_id: Uuid::new_v4(),
        invite_token: SecretToken::parse("A".repeat(43)).unwrap(),
        expires_at: i64::MAX,
        profile_name: "fixture".to_owned(),
        workspace: "fixture-room".parse().unwrap(),
        provider: None,
        routes: vec![route],
        manifest_sha256: manifest_digest.clone(),
        artifacts: vec![artifact.clone()],
    };
    // This mismatch preserves the authenticated manifest digest and its bytes,
    // and therefore specifically exercises exact ticket-entry comparison.
    if mode == "ticket-entry-mismatch" {
        ticket.artifacts[0].binary_sha256 = "f".repeat(64);
    }
    let info = OnboardingInfo {
        version: VERSION,
        server_id,
        protocol_version: PROTOCOL_VERSION,
        manifest_sha256: Some(manifest_digest),
        available_targets: vec![artifact.target.clone()],
    };
    let files = BTreeMap::from([
        (
            "/onboarding/info".to_owned(),
            serde_json::to_vec(&info).unwrap(),
        ),
        (
            "/onboarding/files/bootstrap-manifest.json".to_owned(),
            manifest,
        ),
        (
            format!("/onboarding/files/{}", artifact.archive_file),
            archive,
        ),
    ]);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = HttpFixture {
        files: Arc::new(files),
        requests: requests.clone(),
        mode: mode.to_owned(),
    };
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(serve).with_state(state))
            .await
            .unwrap();
    });
    let verified = probe_routes(
        &ticket.routes,
        server_id,
        Some(&ticket.manifest_sha256),
        &home.join("ca"),
    )
    .await
    .unwrap();
    if mode == "server-mismatch" {
        ticket.server_id = Uuid::new_v4();
    }
    if mode == "unsafe-install-path" {
        fs::create_dir(home.join("outside")).unwrap();
        symlink(home.join("outside"), home.join(".local")).unwrap();
    }
    let result = install_bundle(&ticket, &verified).await;
    if matches!(
        mode,
        "valid" | "slow-download" | "conflict" | "gnu-longname" | "pax-path"
    ) {
        assert_installed(mode, home, &binary, &ticket, &verified, &result.unwrap()).await;
    } else {
        assert_rejected(mode, home, &ticket, result.unwrap_err());
    }
    assert_fixture_cleanup(mode, home, &requests);
    server.abort();
}

async fn assert_installed(
    mode: &str,
    home: &Path,
    binary: &[u8],
    ticket: &OnboardingTicket,
    verified: &VerifiedRoute,
    installed: &InstalledBundle,
) {
    let prefix = home
        .join(".local/share/agent-session-router/versions")
        .join(&ticket.manifest_sha256);
    assert_eq!(installed.executable, prefix.join("bin/asr"));
    assert!(installed.executable.is_absolute());
    assert_eq!(fs::read(&installed.executable).unwrap(), binary);
    for asset in ASSETS {
        assert_eq!(
            fs::read(installed.integrations_dir.join(asset)).unwrap(),
            asset.as_bytes()
        );
    }
    if matches!(mode, "gnu-longname" | "pax-path") {
        assert_eq!(
            fs::read(installed.integrations_dir.join("long-component".repeat(12))).unwrap(),
            b"extended"
        );
        assert!(!installed.integrations_dir.join("placeholder").exists());
    }
    let output = Command::new(&installed.executable)
        .arg("--version")
        .current_dir(home)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains(env!("CARGO_PKG_VERSION")));
    if mode == "valid" {
        let again = install_bundle(ticket, verified).await.unwrap();
        assert_eq!(again.executable, installed.executable);
        assert_eq!(fs::read(&again.executable).unwrap(), binary);
    }
    if mode == "conflict" {
        fs::write(&installed.executable, b"user-owned replacement").unwrap();
        assert_eq!(
            install_bundle(ticket, verified).await.unwrap_err(),
            BundleInstallError::Conflict
        );
        assert_eq!(
            fs::read(&installed.executable).unwrap(),
            b"user-owned replacement"
        );
    }
}

fn assert_rejected(mode: &str, home: &Path, ticket: &OnboardingTicket, error: BundleInstallError) {
    let prefix = home
        .join(".local/share/agent-session-router/versions")
        .join(&ticket.manifest_sha256);
    let expected = match mode {
        "manifest-mutation" | "manifest-limit" | "ticket-entry-mismatch" => {
            BundleInstallError::ManifestMismatch
        }
        "archive-mutation" | "archive-size" => BundleInstallError::ArchiveMismatch,
        "binary-mismatch" => BundleInstallError::BinaryMismatch,
        "missing-assets" => BundleInstallError::InvalidDistribution,
        "unsupported-target" => BundleInstallError::UnsupportedTarget,
        "server-mismatch" => BundleInstallError::ServerMismatch,
        "expanded-limit" | "metadata-limit" => BundleInstallError::ArchiveLimit,
        "unsafe-install-path" => BundleInstallError::UnsafePath,
        "redirect" => BundleInstallError::Redirect,
        _ => BundleInstallError::InvalidArchive,
    };
    assert_eq!(error, expected);
    assert_eq!(error.to_string(), error.code());
    assert!(!format!("{error:?} {error}").contains(ticket.invite_token.expose()));
    assert!(!prefix.join("bin/asr").exists());
    assert!(!home.join("executed").exists());
    assert!(!home.join("escaped").exists());
    if mode == "unsafe-install-path" {
        assert_eq!(fs::read_dir(home.join("outside")).unwrap().count(), 0);
    }
}

fn assert_fixture_cleanup(mode: &str, home: &Path, requests: &Mutex<Vec<String>>) {
    assert!(fs::read_dir(home).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".asr-onboarding-")
    }));
    assert!(!home.join(".config").exists());
    let requests = requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .all(|path| path == "/onboarding/info" || path.starts_with("/onboarding/files/"))
    );
    if matches!(mode, "unsupported-target" | "server-mismatch") {
        assert_eq!(requests.as_slice(), ["/onboarding/info"]);
    }
}

#[test]
fn bootstrap_install_fixture_child() {
    let Ok(mode) = std::env::var("ASR_BOOTSTRAP_INSTALL_FIXTURE") else {
        return;
    };
    let home = PathBuf::from(std::env::var_os("HOME").unwrap());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(scenario(&mode, &home));
}

fn run_case(mode: &str) {
    let root = TempDir::new().unwrap();
    let home = root.path().canonicalize().unwrap();
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bootstrap_install_fixture_child", "--nocapture"])
        .env("ASR_BOOTSTRAP_INSTALL_FIXTURE", mode)
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("ASR_CONFIG_PATH", home.join(".config/asr.json"))
        .current_dir(&home)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "case {mode}:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn verified_archive_installs_and_runs_outside_checkout_and_reinstalls_idempotently() {
    run_case("valid");
}

#[test]
fn archive_download_does_not_inherit_five_second_probe_deadline() {
    run_case("slow-download");
}

#[test]
fn existing_contents_and_unsafe_install_ancestors_are_not_overwritten() {
    for mode in ["conflict", "unsafe-install-path"] {
        run_case(mode);
    }
}

#[test]
fn download_identity_digest_size_and_target_failures_never_execute_or_enroll() {
    for mode in [
        "server-mismatch",
        "unsupported-target",
        "manifest-mutation",
        "manifest-limit",
        "ticket-entry-mismatch",
        "archive-mutation",
        "archive-size",
        "redirect",
    ] {
        run_case(mode);
    }
}

#[test]
fn malicious_archive_entries_and_missing_distribution_assets_never_publish() {
    for mode in [
        "traversal",
        "absolute",
        "forbidden",
        "duplicate",
        "symlink",
        "hardlink",
        "fifo",
        "device",
        "setuid",
        "expanded-limit",
        "binary-mismatch",
        "missing-assets",
    ] {
        run_case(mode);
    }
}

#[test]
fn standard_archive_metadata_is_bounded_and_resolved_paths_remain_confined() {
    for mode in [
        "gnu-longname",
        "pax-path",
        "gnu-traversal",
        "pax-traversal",
        "metadata-limit",
    ] {
        run_case(mode);
    }
}

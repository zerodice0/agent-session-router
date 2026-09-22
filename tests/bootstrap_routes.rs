use std::{
    fs,
    net::{Ipv4Addr, TcpListener as StdTcpListener},
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use agent_session_router::{
    bootstrap::routes::{RouteError, onboarding_url, probe_routes},
    onboarding::{OnboardingInfo, OnboardingRoute, RouteKind, VERSION},
    protocol::PROTOCOL_VERSION,
    router::{RouterConfig, RouterExposure, RouterRuntime},
    store::RouterStore,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use uuid::Uuid;

fn private_directory() -> TempDir {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

fn route(kind: RouteKind, router_url: impl Into<String>) -> OnboardingRoute {
    OnboardingRoute {
        kind,
        router_url: router_url.into(),
        ca_pem: None,
    }
}

fn router_config(data_dir: PathBuf) -> RouterConfig {
    RouterConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        data_dir,
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    }
}

fn identity(data_dir: &Path) -> Uuid {
    let store = RouterStore::open(data_dir).unwrap();
    store.server_id().unwrap()
}

async fn stop(runtime: RouterRuntime) {
    runtime.shutdown().await.unwrap();
    runtime.wait().await.unwrap();
}

fn error(
    result: Result<agent_session_router::bootstrap::routes::VerifiedRoute, RouteError>,
) -> RouteError {
    match result {
        Ok(_) => panic!("route unexpectedly accepted"),
        Err(error) => error,
    }
}

#[test]
fn onboarding_paths_cannot_carry_tokens_or_escape_the_fixed_surface() {
    assert_eq!(
        onboarding_url("wss://router.example:9443/ws", "/onboarding/info")
            .unwrap()
            .as_str(),
        "https://router.example:9443/onboarding/info"
    );
    assert_eq!(
        onboarding_url("ws://127.0.0.1:8787/ws", "/onboarding/enroll")
            .unwrap()
            .as_str(),
        "http://127.0.0.1:8787/onboarding/enroll"
    );
    assert!(
        onboarding_url(
            "wss://router.example/ws",
            "/onboarding/files/asr-aarch64-apple-darwin"
        )
        .is_ok()
    );
    for path in [
        "/ws",
        "/onboarding/files/../../credential.json",
        "/onboarding/files/%2e%2e/secret",
        "/onboarding/files/custom",
        "/onboarding/info?token=sentinel",
        "https://other.example/onboarding/info",
    ] {
        assert_eq!(
            onboarding_url("wss://router.example/ws", path),
            Err(RouteError::InvalidRoutes)
        );
    }
    for url in [
        "https://router.example/ws",
        "wss://router.example/prefix/ws",
        "wss://user:sentinel@router.example/ws",
        "wss://router.example/ws?token=sentinel",
        "wss://router.example/ws#sentinel",
    ] {
        assert_eq!(
            onboarding_url(url, "/onboarding/info"),
            Err(RouteError::InvalidRoutes)
        );
    }
}

#[tokio::test]
async fn native_router_probe_checks_persistent_identity_and_initial_manifest_without_writes() {
    let directory = private_directory();
    let root = directory.path().canonicalize().unwrap();
    let data = root.join("server");
    let server_id = identity(&data);
    let runtime = RouterRuntime::start(router_config(data)).await.unwrap();
    let candidate = route(RouteKind::Local, format!("ws://{}/ws", runtime.address));
    let cache = root.join("client/ca");
    let selected = probe_routes(std::slice::from_ref(&candidate), server_id, None, &cache)
        .await
        .unwrap();
    assert_eq!(selected.info.server_id, server_id);
    assert_ne!(selected.info.server_id, runtime.instance_id);
    assert!(selected.ca_file.is_none());
    let fetched: OnboardingInfo = selected
        .client
        .get(onboarding_url(&selected.route.router_url, "/onboarding/info").unwrap())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched, selected.info);
    assert_eq!(
        error(
            probe_routes(
                std::slice::from_ref(&candidate),
                Uuid::new_v4(),
                None,
                &cache
            )
            .await
        ),
        RouteError::IdentityMismatch
    );
    assert_eq!(
        error(probe_routes(&[candidate], server_id, Some(&"a".repeat(64)), &cache).await),
        RouteError::ManifestMismatch
    );
    assert!(
        !root.join("client").exists(),
        "a tokenless plain probe must not create client state"
    );
    stop(runtime).await;
}

#[tokio::test]
async fn mixed_local_or_invalid_raw_tcp_routes_are_rejected_before_network_or_ca_writes() {
    let directory = private_directory();
    let cache = directory.path().join("ca");
    for candidates in [
        vec![
            route(RouteKind::Local, "ws://127.0.0.1:1/ws"),
            route(RouteKind::Public, "wss://example.invalid/ws"),
        ],
        vec![route(RouteKind::Lan, "ws://192.168.1.2:8787/ws")],
        vec![route(RouteKind::Tailnet, "ws://100.63.1.2:8787/ws")],
        vec![route(RouteKind::Local, "ws://0.0.0.0:8787/ws")],
        vec![route(RouteKind::Public, "wss://169.254.1.2:8787/ws")],
    ] {
        assert_eq!(
            error(probe_routes(&candidates, Uuid::new_v4(), None, &cache).await),
            RouteError::InvalidRoutes
        );
    }
    assert!(!cache.exists());
}

struct Certificates {
    certificate: PathBuf,
    key: PathBuf,
    ca_pem: String,
}

fn certificates(directory: &Path, stem: &str) -> Certificates {
    let mut ca_params = CertificateParams::new(vec![format!("{stem}-ca")]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().unwrap();
    let certificate = params.signed_by(&key, &issuer).unwrap();
    let certificate_file = directory.join(format!("{stem}-chain.pem"));
    let key_file = directory.join(format!("{stem}-key.pem"));
    fs::write(
        &certificate_file,
        format!("{}{}", certificate.pem(), ca.pem()),
    )
    .unwrap();
    fs::write(&key_file, key.serialize_pem()).unwrap();
    fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600)).unwrap();
    Certificates {
        certificate: certificate_file,
        key: key_file,
        ca_pem: ca.pem(),
    }
}

#[test]
fn native_tls_route_uses_private_pinned_ca_and_enforces_hostname_and_trust() {
    run_clean_tls_fixture("native-route");
}

async fn check_native_tls_route_uses_private_pinned_ca_and_enforces_hostname_and_trust() {
    let directory = private_directory();
    let root = directory.path().canonicalize().unwrap();
    let certificates = certificates(&root, "server");
    let data = root.join("server");
    let server_id = identity(&data);
    let port = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut config = router_config(data);
    config.bind.set_port(port);
    config.tls_cert_file = Some(certificates.certificate.clone());
    config.tls_key_file = Some(certificates.key.clone());
    config.public_url = Some(format!("wss://localhost:{port}/ws").parse().unwrap());
    let runtime = RouterRuntime::start(config).await.unwrap();
    let cache = root.join("client/ca");
    let candidate = OnboardingRoute {
        ca_pem: Some(certificates.ca_pem.clone()),
        ..route(RouteKind::Local, format!("wss://localhost:{port}/ws"))
    };
    let selected = probe_routes(std::slice::from_ref(&candidate), server_id, None, &cache)
        .await
        .unwrap();
    let ca_file = selected.ca_file.unwrap();
    assert_eq!(
        ca_file,
        cache.join(format!(
            "{:x}.pem",
            Sha256::digest(certificates.ca_pem.as_bytes())
        ))
    );
    assert_eq!(fs::read_to_string(&ca_file).unwrap(), certificates.ca_pem);
    assert_eq!(fs::metadata(&ca_file).unwrap().mode() & 0o777, 0o600);
    assert_eq!(fs::metadata(&cache).unwrap().mode() & 0o777, 0o700);
    let inode = fs::metadata(&ca_file).unwrap().ino();
    let second = probe_routes(std::slice::from_ref(&candidate), server_id, None, &cache)
        .await
        .unwrap();
    assert_eq!(second.info.server_id, server_id);
    assert_eq!(
        fs::metadata(&ca_file).unwrap().ino(),
        inode,
        "same CA is reused, not overwritten"
    );
    assert_eq!(
        fs::read_dir(&cache).unwrap().count(),
        1,
        "no token, profile, or staging files remain"
    );
    let wrong_hostname = OnboardingRoute {
        router_url: format!("wss://127.0.0.1:{port}/ws"),
        ..candidate.clone()
    };
    assert_eq!(
        error(probe_routes(&[wrong_hostname], server_id, None, &cache).await),
        RouteError::Tls
    );
    let unrelated = self::certificates(&root, "unrelated");
    let wrong_trust = OnboardingRoute {
        ca_pem: Some(unrelated.ca_pem),
        ..candidate
    };
    assert_eq!(
        error(probe_routes(&[wrong_trust], server_id, None, &cache).await),
        RouteError::Tls
    );
    stop(runtime).await;
}

#[test]
fn ca_cache_rejects_symlinks_permissions_hardlinks_and_content_conflicts() {
    run_clean_tls_fixture("ca-cache");
}

async fn check_ca_cache_rejects_symlinks_permissions_hardlinks_and_content_conflicts() {
    let directory = private_directory();
    let root = directory.path().canonicalize().unwrap();
    let certificates = certificates(&root, "ca-storage");
    let candidate = OnboardingRoute {
        ca_pem: Some(certificates.ca_pem.clone()),
        ..route(RouteKind::Local, "wss://localhost:1/ws")
    };
    let digest_name = format!("{:x}.pem", Sha256::digest(certificates.ca_pem.as_bytes()));
    let cache = root.join("ca");
    fs::create_dir(&cache).unwrap();
    fs::set_permissions(&cache, fs::Permissions::from_mode(0o700)).unwrap();
    let target = cache.join(digest_name);
    let outside = root.join("outside.pem");
    fs::write(&outside, &certificates.ca_pem).unwrap();
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o600)).unwrap();
    symlink(&outside, &target).unwrap();
    assert_eq!(
        error(
            probe_routes(
                std::slice::from_ref(&candidate),
                Uuid::new_v4(),
                None,
                &cache
            )
            .await
        ),
        RouteError::InvalidCa
    );
    fs::remove_file(&target).unwrap();
    fs::hard_link(&outside, &target).unwrap();
    assert_eq!(
        error(
            probe_routes(
                std::slice::from_ref(&candidate),
                Uuid::new_v4(),
                None,
                &cache
            )
            .await
        ),
        RouteError::InvalidCa
    );
    fs::remove_file(&target).unwrap();
    fs::write(&target, "conflicting trusted bytes").unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        error(
            probe_routes(
                std::slice::from_ref(&candidate),
                Uuid::new_v4(),
                None,
                &cache
            )
            .await
        ),
        RouteError::InvalidCa
    );
    assert_eq!(
        fs::read_to_string(&target).unwrap(),
        "conflicting trusted bytes"
    );
    fs::write(&target, &certificates.ca_pem).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        error(
            probe_routes(
                std::slice::from_ref(&candidate),
                Uuid::new_v4(),
                None,
                &cache
            )
            .await
        ),
        RouteError::InvalidCa
    );
    let alias = root.join("alias");
    symlink(&cache, &alias).unwrap();
    assert_eq!(
        error(probe_routes(&[candidate], Uuid::new_v4(), None, &alias).await),
        RouteError::InvalidCa
    );
    assert_eq!(fs::read_to_string(outside).unwrap(), certificates.ca_pem);
    let private_key = OnboardingRoute {
        ca_pem: Some(fs::read_to_string(certificates.key).unwrap()),
        ..route(RouteKind::Local, "wss://localhost:1/ws")
    };
    let untouched = root.join("uncreated-ca");
    assert_eq!(
        error(probe_routes(&[private_key], Uuid::new_v4(), None, &untouched).await),
        RouteError::InvalidCa
    );
    assert!(!untouched.exists());
}

fn run_clean_tls_fixture(mode: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "clean_tls_fixture_child", "--nocapture"])
        .env("ASR_CLEAN_TLS_FIXTURE", mode)
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{mode}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn clean_tls_fixture_child() {
    let Ok(mode) = std::env::var("ASR_CLEAN_TLS_FIXTURE") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        match mode.as_str() {
            "native-route" => {
                check_native_tls_route_uses_private_pinned_ca_and_enforces_hostname_and_trust()
                    .await;
            }
            "ca-cache" => {
                check_ca_cache_rejects_symlinks_permissions_hardlinks_and_content_conflicts().await;
            }
            _ => panic!("unexpected clean TLS fixture mode: {mode}"),
        }
    });
}

async fn response_fixture(response: String) -> (OnboardingRoute, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let candidate = route(
        RouteKind::Local,
        format!("ws://{}/ws", listener.local_addr().unwrap()),
    );
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            assert!(request.len() < 4096);
        }
        stream.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(request).unwrap()
    });
    (candidate, task)
}

#[tokio::test]
async fn redirect_is_not_followed_and_probe_never_sends_a_credential_or_enrollment() {
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (candidate, request) = response_fixture(format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{}/onboarding/enroll\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        destination.local_addr().unwrap())).await;
    let directory = private_directory();
    assert_eq!(
        error(probe_routes(&[candidate], Uuid::new_v4(), None, directory.path()).await),
        RouteError::Redirect
    );
    let request = request.await.unwrap().to_ascii_lowercase();
    assert!(request.starts_with("get /onboarding/info http/1.1\r\n"));
    assert!(
        !request.contains("authorization:")
            && !request.contains("cookie:")
            && !request.contains("invite")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), destination.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn info_protocol_digest_and_size_are_fail_closed() {
    let directory = private_directory();
    let server_id = Uuid::new_v4();
    for (document, expected_manifest, expected_error) in [
        (
            json!({"version": VERSION, "serverId": server_id, "protocolVersion": PROTOCOL_VERSION + 1,
            "manifestSha256": null, "availableTargets": []}),
            None,
            RouteError::ProtocolMismatch,
        ),
        (
            json!({"version": VERSION, "serverId": server_id, "protocolVersion": PROTOCOL_VERSION,
            "manifestSha256": "b".repeat(64), "availableTargets": []}),
            Some("a".repeat(64)),
            RouteError::ManifestMismatch,
        ),
        (
            json!({"version": VERSION, "serverId": server_id, "protocolVersion": PROTOCOL_VERSION,
            "manifestSha256": null, "availableTargets": [], "untrusted": "x".repeat(20_000)}),
            None,
            RouteError::InvalidResponse,
        ),
    ] {
        let body = document.to_string();
        let (candidate, request) = response_fixture(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())).await;
        assert_eq!(
            error(
                probe_routes(
                    &[candidate],
                    server_id,
                    expected_manifest.as_deref(),
                    directory.path()
                )
                .await
            ),
            expected_error
        );
        request.await.unwrap();
    }
}

// Environment isolation uses the same child-test pattern as transport_tls.rs;
// no process-global PATH mutation can race another Rust test.
#[test]
fn route_fixture_child() {
    let Ok(mode) = std::env::var("ASR_ROUTE_FIXTURE") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("ASR_ROUTE_FIXTURE_ROOT").unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let candidates = [
            route(RouteKind::Public, "wss://public.invalid/ws"),
            route(RouteKind::Lan, "wss://lan.invalid/ws"),
            route(RouteKind::Tailnet, "ws://100.64.0.2:8787/ws"),
        ];
        let failure =
            error(probe_routes(&candidates, Uuid::new_v4(), None, &root.join("ca")).await);
        if mode == "malformed" {
            assert_eq!(failure, RouteError::TailnetUnverified);
        } else {
            let RouteError::NoReachableRoute { failures } = &failure else {
                panic!("{failure}")
            };
            assert_eq!(
                failures
                    .iter()
                    .map(|failure| failure.kind)
                    .collect::<Vec<_>>(),
                [RouteKind::Tailnet, RouteKind::Lan, RouteKind::Public]
            );
            assert_eq!(
                failures[0].code,
                if mode == "peer-offline" {
                    "tailnet_peer_offline"
                } else {
                    "tailnet_unavailable"
                }
            );
            assert_eq!(failures[1].code, "dns_unavailable");
            assert_eq!(failures[2].code, "dns_unavailable");
        }
        let diagnostic = format!("{failure:?} {failure}");
        assert!(
            !diagnostic.contains("100.64")
                && !diagnostic.contains(".invalid")
                && !diagnostic.contains("sentinel")
        );
        assert!(!root.join("ca").exists());
    });
}

#[test]
fn tailscale_cli_fixture_distinguishes_unavailable_offline_and_malformed_before_fallback() {
    for mode in ["absent", "stopped", "peer-offline", "malformed"] {
        let directory = private_directory();
        let root = directory.path().canonicalize().unwrap();
        if mode != "absent" {
            let status = if mode == "malformed" {
                "{sentinel-private-invalid-json".to_owned()
            } else {
                json!({"BackendState": if mode == "stopped" { "Stopped" } else { "Running" },
                    "Self": {"TailscaleIPs": ["100.64.0.1"]},
                    "Peer": {"offline": {"Online": false, "TailscaleIPs": ["100.64.0.2"]}}})
                .to_string()
            };
            let script = format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{root}/calls'\ncase \"$*\" in\n  'status --json') printf '%s' '{status}' ;;\n  'serve status --json') printf '%s' '{{\"TCP\":{{}}}}' ;;\n  *) exit 64 ;;\nesac\n",
                root = root.display()
            );
            let executable = root.join("tailscale");
            fs::write(&executable, script).unwrap();
            fs::set_permissions(executable, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "route_fixture_child", "--nocapture"])
            .env("ASR_ROUTE_FIXTURE", mode)
            .env("ASR_ROUTE_FIXTURE_ROOT", &root)
            .env("PATH", &root)
            .env_remove("SSL_CERT_FILE")
            .env_remove("SSL_CERT_DIR")
            .env_remove("ASR_CA_FILE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if mode != "absent" {
            assert_eq!(
                fs::read_to_string(root.join("calls")).unwrap(),
                "status --json\nserve status --json\n"
            );
        }
    }
}

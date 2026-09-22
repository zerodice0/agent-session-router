use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use agent_session_router::{
    onboarding::{
        BootstrapArtifact, BootstrapManifest, OnboardingInfo, RouteKind, VERSION,
        routes::prepare_routes,
    },
    process::{RuntimeRecord, RuntimeShareMode},
    router::{RouterConfig, RouterExposure, RouterRuntime},
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn bundle(root: &Path) -> PathBuf {
    let directory = root.join("bootstrap");
    fs::create_dir(&directory).unwrap();
    let binary_file = "asr-aarch64-apple-darwin";
    let archive_file = "agent-session-router-aarch64-apple-darwin.tar.gz";
    fs::write(directory.join(binary_file), b"binary").unwrap();
    fs::write(directory.join(archive_file), b"archive").unwrap();
    let manifest = BootstrapManifest {
        version: VERSION,
        asr_version: env!("CARGO_PKG_VERSION").into(),
        artifacts: vec![BootstrapArtifact {
            target: "aarch64-apple-darwin".into(),
            binary_file: binary_file.into(),
            binary_sha256: digest(b"binary"),
            binary_bytes: 6,
            archive_file: archive_file.into(),
            archive_sha256: digest(b"archive"),
            archive_bytes: 7,
        }],
    };
    fs::write(
        directory.join("bootstrap-manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    directory
}

fn config(root: &Path, name: &str, assets: Option<PathBuf>) -> RouterConfig {
    RouterConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        data_dir: root.join(name),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: assets,
    }
}

fn record(runtime: &RouterRuntime) -> RuntimeRecord {
    RuntimeRecord {
        instance_id: runtime.instance_id,
        control_url: format!("ws://{}/ws", runtime.address),
        advertised_url: None,
        share_mode: RuntimeShareMode::Local,
        owned_serve: None,
    }
}

async fn info(runtime: &RouterRuntime) -> OnboardingInfo {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("http://{}/onboarding/info", runtime.address))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn stop(runtime: RouterRuntime) {
    runtime.shutdown().await.unwrap();
    runtime.wait().await.unwrap();
}

#[tokio::test]
async fn actual_runtime_defaults_and_explicit_alias_replace_other_candidates() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let assets = bundle(&root);
    let runtime = RouterRuntime::start(config(&root, "first", Some(assets.clone())))
        .await
        .unwrap();
    let other = RouterRuntime::start(config(&root, "other", Some(assets)))
        .await
        .unwrap();
    let info = info(&runtime).await;
    let manifest = info.manifest_sha256.as_deref().unwrap();
    let ca_directory = root.join("cas");
    let mut runtime_record = record(&runtime);
    let routes = prepare_routes(
        &runtime_record,
        &[],
        None,
        info.server_id,
        manifest,
        &ca_directory,
    )
    .await
    .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].kind, RouteKind::Local);
    assert_eq!(routes[0].router_url, runtime_record.control_url);

    let alias = format!("ws://localhost:{}/ws", runtime.address.port());
    runtime_record.advertised_url = Some(alias.clone());
    runtime_record.control_url = record(&other).control_url;
    let routes = prepare_routes(
        &runtime_record,
        &[],
        None,
        info.server_id,
        manifest,
        &ca_directory,
    )
    .await
    .unwrap();
    assert_eq!(routes[0].router_url, alias);

    runtime_record.advertised_url = Some(format!("ws://{}/ws", other.address));
    let routes = prepare_routes(
        &runtime_record,
        &[format!("local={alias}")],
        None,
        info.server_id,
        manifest,
        &ca_directory,
    )
    .await
    .unwrap();
    assert_eq!(routes[0].router_url, alias);
    assert_eq!(
        prepare_routes(
            &runtime_record,
            &[],
            None,
            info.server_id,
            manifest,
            &ca_directory
        )
        .await
        .unwrap_err()
        .code(),
        "server_identity_mismatch"
    );
    assert_eq!(
        prepare_routes(
            &record(&runtime),
            &[],
            None,
            info.server_id,
            &"0".repeat(64),
            &ca_directory
        )
        .await
        .unwrap_err()
        .code(),
        "manifest_mismatch"
    );
    stop(runtime).await;
    stop(other).await;
}

#[tokio::test]
async fn absent_assets_and_unsafe_or_ambiguous_candidates_cannot_be_issued() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let runtime = RouterRuntime::start(config(&root, "router", None))
        .await
        .unwrap();
    let info = info(&runtime).await;
    let mut runtime_record = record(&runtime);
    let ca_directory = root.join("cas");
    assert!(
        prepare_routes(
            &runtime_record,
            &[],
            None,
            info.server_id,
            &"a".repeat(64),
            &ca_directory
        )
        .await
        .is_err()
    );
    for endpoints in [
        vec!["local=ws://0.0.0.0:8787/ws".into()],
        vec!["public=wss://[::]:8787/ws".into()],
        vec!["lan=wss://169.254.1.1:8787/ws".into()],
        vec!["lan=wss://[fe80::1]:8787/ws".into()],
        vec!["local=ws://localhost:8787/prefix/ws".into()],
        vec!["local=ws://localhost:8787/ws?secret=must-not-appear".into()],
        vec![
            "local=ws://localhost:8787/ws".into(),
            "public=wss://example.com/ws".into(),
        ],
        vec![
            "lan=wss://10.0.0.1/ws".into(),
            "lan=wss://10.0.0.2/ws".into(),
        ],
        vec!["lan=wss://10.0.0.1/ws".into(); 5],
    ] {
        let error = prepare_routes(
            &runtime_record,
            &endpoints,
            None,
            info.server_id,
            &"a".repeat(64),
            &ca_directory,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "route_invalid");
        assert!(!format!("{error:?}: {error}").contains("must-not-appear"));
    }
    runtime_record.advertised_url = Some("wss://192.0.2.1/ws".into());
    assert_eq!(
        prepare_routes(
            &runtime_record,
            &[],
            None,
            info.server_id,
            &"a".repeat(64),
            &ca_directory
        )
        .await
        .unwrap_err()
        .code(),
        "route_kind_required"
    );
    stop(runtime).await;
}

struct Certificates {
    certificate: PathBuf,
    key: PathBuf,
    ca: PathBuf,
    pem: String,
}

fn certificates(root: &Path) -> Certificates {
    fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
    let mut ca = CertificateParams::new(vec!["route-test-ca".into()]).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let certificate = ca.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca, &ca_key);
    let mut server = CertificateParams::new(vec!["localhost".into()]).unwrap();
    server.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().unwrap();
    let server = server.signed_by(&key, &issuer).unwrap();
    let fixture = Certificates {
        certificate: root.join("server.pem"),
        key: root.join("server.key"),
        ca: root.join("ca.pem"),
        pem: certificate.pem(),
    };
    fs::write(
        &fixture.certificate,
        format!("{}{}", server.pem(), fixture.pem),
    )
    .unwrap();
    fs::write(&fixture.key, key.serialize_pem()).unwrap();
    fs::set_permissions(&fixture.key, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&fixture.ca, &fixture.pem).unwrap();
    fixture
}

async fn assert_ca_environment_precedence(
    runtime_record: &RuntimeRecord,
    certs: &Certificates,
    server_id: Uuid,
    manifest: &str,
    ca_directory: &Path,
) {
    for explicit in [false, true] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "ca_environment_child", "--nocapture"])
            .env(
                "ASR_ROUTE_TEST_RECORD",
                serde_json::to_string(runtime_record).unwrap(),
            )
            .env("ASR_ROUTE_TEST_SERVER", server_id.to_string())
            .env("ASR_ROUTE_TEST_MANIFEST", manifest)
            .env("ASR_ROUTE_TEST_DIRECTORY", ca_directory)
            .env("ASR_ROUTE_TEST_CA", &certs.ca)
            .env(
                "ASR_ROUTE_TEST_EXPLICIT",
                if explicit { "yes" } else { "no" },
            )
            .env("ASR_CA_FILE", if explicit { &certs.key } else { &certs.ca });
        let status = tokio::task::spawn_blocking(move || command.status().unwrap())
            .await
            .unwrap();
        assert!(
            status.success(),
            "explicit CA precedence and environment fallback"
        );
    }
}

#[tokio::test]
async fn actual_tls_alias_requires_the_public_ca_and_matching_hostname() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let assets = bundle(&root);
    let manifest = digest(&fs::read(assets.join("bootstrap-manifest.json")).unwrap());
    let certs = certificates(&root);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut configuration = config(&root, "tls-router", Some(assets));
    configuration.bind.set_port(port);
    configuration.tls_cert_file = Some(certs.certificate.clone());
    configuration.tls_key_file = Some(certs.key.clone());
    configuration.public_url = Some(format!("wss://localhost:{port}/ws").parse().unwrap());
    let runtime = RouterRuntime::start(configuration).await.unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(reqwest::Certificate::from_pem(certs.pem.as_bytes()).unwrap())
        .build()
        .unwrap();
    let info: OnboardingInfo = client
        .get(format!("https://localhost:{port}/onboarding/info"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let runtime_record = RuntimeRecord {
        control_url: format!("wss://localhost:{port}/ws"),
        ..record(&runtime)
    };
    let ca_directory = root.join("cas");
    let routes = prepare_routes(
        &runtime_record,
        &[],
        Some(&certs.ca),
        info.server_id,
        &manifest,
        &ca_directory,
    )
    .await
    .unwrap();
    assert_eq!(routes[0].ca_pem.as_deref(), Some(certs.pem.as_str()));
    assert_ca_environment_precedence(
        &runtime_record,
        &certs,
        info.server_id,
        &manifest,
        &ca_directory,
    )
    .await;
    let wrong_hostname = format!("local=wss://127.0.0.1:{port}/ws");
    assert_eq!(
        prepare_routes(
            &runtime_record,
            &[wrong_hostname],
            Some(&certs.ca),
            info.server_id,
            &manifest,
            &ca_directory
        )
        .await
        .unwrap_err()
        .code(),
        "route_tls_failed"
    );
    assert!(
        prepare_routes(
            &runtime_record,
            &[],
            Some(&certs.key),
            info.server_id,
            &manifest,
            &ca_directory
        )
        .await
        .is_err(),
        "private keys cannot enter tickets"
    );
    let oversized = root.join("oversized.pem");
    fs::write(&oversized, vec![b'x'; 64 * 1024 + 1]).unwrap();
    assert_eq!(
        prepare_routes(
            &runtime_record,
            &[],
            Some(&oversized),
            info.server_id,
            &manifest,
            &ca_directory
        )
        .await
        .unwrap_err()
        .code(),
        "ca_invalid"
    );
    stop(runtime).await;
}

#[test]
fn ca_environment_child() {
    let Ok(serialized) = std::env::var("ASR_ROUTE_TEST_RECORD") else {
        return;
    };
    let runtime_record: RuntimeRecord = serde_json::from_str(&serialized).unwrap();
    let server_id = Uuid::parse_str(&std::env::var("ASR_ROUTE_TEST_SERVER").unwrap()).unwrap();
    let manifest = std::env::var("ASR_ROUTE_TEST_MANIFEST").unwrap();
    let ca_directory = PathBuf::from(std::env::var_os("ASR_ROUTE_TEST_DIRECTORY").unwrap());
    let ca_file = PathBuf::from(std::env::var_os("ASR_ROUTE_TEST_CA").unwrap());
    let explicit = std::env::var("ASR_ROUTE_TEST_EXPLICIT").unwrap() == "yes";
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let routes = executor
        .block_on(prepare_routes(
            &runtime_record,
            &[],
            explicit.then_some(ca_file.as_path()),
            server_id,
            &manifest,
            &ca_directory,
        ))
        .unwrap();
    assert_eq!(
        routes[0].ca_pem.as_deref(),
        Some(fs::read_to_string(ca_file).unwrap().as_str())
    );
}

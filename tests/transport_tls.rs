use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    os::unix::fs::{PermissionsExt, symlink},
    path::PathBuf,
    process::Command,
};

use agent_session_router::{
    client::{ClientConfig, ClientError, ClientRole, RouterClient},
    credentials::read_credential,
    protocol::{ClientMessage, ServerMessage},
    router::{RouterConfig, RouterExposure, RouterRuntime, RouterRuntimeError},
    tls::{TlsError, load_client_config},
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use tempfile::{TempDir, tempdir};
use url::Url;
use uuid::Uuid;

struct CertificateFixture {
    _directory: TempDir,
    certificate_file: PathBuf,
    private_key_file: PathBuf,
    ca_file: PathBuf,
}

fn certificate_fixture() -> CertificateFixture {
    let directory = tempdir().expect("certificate directory");
    let root = directory
        .path()
        .canonicalize()
        .expect("canonical certificate directory");
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
        .expect("protect certificate directory");
    let mut ca_params =
        CertificateParams::new(vec!["asr-test-ca".to_owned()]).expect("CA certificate parameters");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().expect("CA key");
    let ca_certificate = ca_params.self_signed(&ca_key).expect("CA certificate");
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let mut server_params =
        CertificateParams::new(vec!["localhost".to_owned()]).expect("server parameters");
    server_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().expect("server key");
    let server_certificate = server_params
        .signed_by(&server_key, &issuer)
        .expect("server certificate");

    let certificate_file = root.join("server-chain.pem");
    let private_key_file = root.join("server-key.pem");
    let ca_file = root.join("ca.pem");
    fs::write(
        &certificate_file,
        format!("{}{}", server_certificate.pem(), ca_certificate.pem()),
    )
    .expect("write server chain");
    fs::write(&private_key_file, server_key.serialize_pem()).expect("write private key");
    fs::set_permissions(&private_key_file, fs::Permissions::from_mode(0o600))
        .expect("protect private key");
    fs::write(&ca_file, ca_certificate.pem()).expect("write CA certificate");
    CertificateFixture {
        _directory: directory,
        certificate_file,
        private_key_file,
        ca_file,
    }
}

fn reserve_loopback_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve loopback port");
    listener.local_addr().expect("reserved address").port()
}

fn data_dir(directory: &TempDir, name: &str) -> PathBuf {
    directory
        .path()
        .canonicalize()
        .expect("canonical test directory")
        .join(name)
}

fn tls_router_config(data_dir: PathBuf, port: u16, fixture: &CertificateFixture) -> RouterConfig {
    RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        data_dir,
        instance_id: Uuid::new_v4(),
        tls_cert_file: Some(fixture.certificate_file.clone()),
        tls_key_file: Some(fixture.private_key_file.clone()),
        public_url: Some(
            Url::parse(&format!("wss://localhost:{port}/ws")).expect("public WSS URL"),
        ),
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    }
}

#[test]
fn asr_ca_file_child_probe() {
    let Ok(router_url) = std::env::var("ASR_TEST_WSS_URL") else {
        return;
    };
    let credential_file =
        PathBuf::from(std::env::var_os("ASR_TEST_CREDENTIAL").expect("credential path"));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    runtime.block_on(async move {
        let credential = read_credential(&credential_file).expect("read child credential");
        let router_url = Url::parse(&router_url).expect("child router URL");
        let (client, _events) = RouterClient::connect(ClientConfig {
            router_url: router_url.clone(),
            role: ClientRole::Operator { credential },
            ca_file: None,
        })
        .await
        .expect("connect WSS with ASR_CA_FILE");
        assert!(matches!(
            client
                .call(ClientMessage::Ping {
                    request_id: "tls-ping".to_owned(),
                })
                .await
                .expect("WSS ping"),
            ServerMessage::Pong { .. }
        ));

        let mut health_url = router_url;
        health_url.set_scheme("https").expect("replace WSS scheme");
        health_url.set_path("/healthz");
        let shared_tls = load_client_config(None).expect("load ASR shared trust");
        let http = reqwest::Client::builder()
            .tls_backend_preconfigured((*shared_tls).clone())
            .build()
            .expect("build HTTPS client");
        let response = http
            .get(health_url)
            .send()
            .await
            .expect("HTTPS health request");
        assert!(response.status().is_success());
        let health: serde_json::Value = response.json().await.expect("health JSON");
        assert_eq!(health["service"], "agent-session-router");
        assert_eq!(health["status"], "ok");
        client.close().await.expect("close child client");
    });
}

#[test]
fn wss_and_https_use_shared_asr_ca_trust_and_reject_wrong_identity() {
    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "wss_and_https_child", "--nocapture"])
        .env("ASR_TEST_WSS_CHILD", "1")
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR")
        .output()
        .expect("run isolated WSS test");
    assert!(
        output.status.success(),
        "isolated WSS test failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn wss_and_https_child() {
    if std::env::var_os("ASR_TEST_WSS_CHILD").is_none() {
        return;
    }
    let fixture = certificate_fixture();
    let directory = tempdir().expect("router directory");
    let port = reserve_loopback_port();
    let router_data_dir = data_dir(&directory, "tls-data");
    let runtime = RouterRuntime::start(tls_router_config(router_data_dir.clone(), port, &fixture))
        .await
        .expect("start TLS router");
    let router_url = format!("wss://localhost:{port}/ws");
    let credential_file = router_data_dir.join("credentials/admin.json");
    let child = tokio::task::spawn_blocking({
        let ca_file = fixture.ca_file.clone();
        let credential_file = credential_file.clone();
        let router_url = router_url.clone();
        move || {
            Command::new(std::env::current_exe().expect("current test executable"))
                .args(["--exact", "asr_ca_file_child_probe", "--nocapture"])
                .env("ASR_CA_FILE", ca_file)
                .env("ASR_TEST_WSS_URL", router_url)
                .env("ASR_TEST_CREDENTIAL", credential_file)
                .env_remove("SSL_CERT_FILE")
                .env_remove("SSL_CERT_DIR")
                .output()
                .expect("run ASR_CA_FILE child")
        }
    })
    .await
    .expect("join ASR_CA_FILE child");
    assert!(
        child.status.success(),
        "ASR_CA_FILE child failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );

    let credential = read_credential(&credential_file).expect("read admin credential");
    let wrong_ca = tempdir().expect("wrong CA directory");
    let wrong_ca_file = wrong_ca.path().join("wrong-ca.pem");
    let wrong = rcgen::generate_simple_self_signed(vec!["wrong-ca".to_owned()])
        .expect("wrong CA certificate");
    fs::write(&wrong_ca_file, wrong.cert.pem()).expect("write wrong CA");
    let wrong_ca_result = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&router_url).expect("WSS URL"),
        role: ClientRole::Operator {
            credential: credential.clone(),
        },
        ca_file: Some(wrong_ca_file),
    })
    .await;
    assert!(matches!(wrong_ca_result, Err(ClientError::Transport)));

    let wrong_hostname_result = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("wss://127.0.0.1:{port}/ws")).expect("wrong-host WSS URL"),
        role: ClientRole::Operator { credential },
        ca_file: Some(fixture.ca_file.clone()),
    })
    .await;
    assert!(matches!(wrong_hostname_result, Err(ClientError::Transport)));

    runtime.shutdown().await.expect("shutdown TLS router");
    runtime.wait().await.expect("join TLS router");
}

#[test]
fn client_tls_rejects_ssl_certificate_overrides() {
    for variable in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args(["--exact", "ssl_cert_override_child", "--nocapture"])
            .env_remove("SSL_CERT_FILE")
            .env_remove("SSL_CERT_DIR")
            .env(variable, "/nonexistent-asr-test-ca")
            .env("ASR_TEST_SSL_OVERRIDE", variable)
            .output()
            .expect("run isolated SSL override test");
        assert!(
            output.status.success(),
            "{variable}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn ssl_cert_override_child() {
    let Ok(variable) = std::env::var("ASR_TEST_SSL_OVERRIDE") else {
        return;
    };
    assert!(matches!(
        variable.as_str(),
        "SSL_CERT_FILE" | "SSL_CERT_DIR"
    ));
    assert_eq!(
        load_client_config(None).err(),
        Some(TlsError::EnvironmentOverride)
    );
}

fn assert_configuration_failure(config: RouterConfig) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("configuration test runtime");
    let result = runtime.block_on(RouterRuntime::start(config));
    assert!(matches!(result, Err(RouterRuntimeError::Configuration)));
}

#[test]
fn tls_configuration_fails_closed_before_serving() {
    let fixture = certificate_fixture();
    let directory = tempdir().expect("configuration directory");
    let port = reserve_loopback_port();
    let public_url = Url::parse(&format!("wss://localhost:{port}/ws")).expect("public URL");

    assert_configuration_failure(RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        data_dir: data_dir(&directory, "partial"),
        instance_id: Uuid::new_v4(),
        tls_cert_file: Some(fixture.certificate_file.clone()),
        tls_key_file: None,
        public_url: Some(public_url.clone()),
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    });
    assert_configuration_failure(RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        data_dir: data_dir(&directory, "plaintext-nonloopback"),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    });

    let invalid_certificate = directory.path().join("invalid-certificate.pem");
    let mut invalid_pem =
        fs::read(&fixture.certificate_file).expect("read generated certificate chain");
    invalid_pem.extend_from_slice(b"unexpected trailing content\n");
    fs::write(&invalid_certificate, invalid_pem).expect("write invalid certificate");
    assert_configuration_failure(RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        data_dir: data_dir(&directory, "invalid-certificate"),
        instance_id: Uuid::new_v4(),
        tls_cert_file: Some(invalid_certificate),
        tls_key_file: Some(fixture.private_key_file.clone()),
        public_url: Some(public_url.clone()),
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    });

    fs::set_permissions(&fixture.private_key_file, fs::Permissions::from_mode(0o644))
        .expect("make key public");
    assert_configuration_failure(RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        data_dir: data_dir(&directory, "public-key"),
        instance_id: Uuid::new_v4(),
        tls_cert_file: Some(fixture.certificate_file.clone()),
        tls_key_file: Some(fixture.private_key_file.clone()),
        public_url: Some(public_url.clone()),
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    });
    fs::set_permissions(&fixture.private_key_file, fs::Permissions::from_mode(0o600))
        .expect("restore key permissions");

    let symlink_key = directory.path().join("server-key-link.pem");
    symlink(&fixture.private_key_file, &symlink_key).expect("symlink private key");
    assert_configuration_failure(RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        data_dir: data_dir(&directory, "symlink-key"),
        instance_id: Uuid::new_v4(),
        tls_cert_file: Some(fixture.certificate_file.clone()),
        tls_key_file: Some(symlink_key),
        public_url: Some(public_url),
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    });
}

#[test]
fn public_url_must_match_wss_endpoint() {
    let fixture = certificate_fixture();
    let directory = tempdir().expect("public URL directory");
    let port = reserve_loopback_port();
    for (index, public_url) in [
        format!("ws://localhost:{port}/ws"),
        format!("wss://localhost:{port}/wrong"),
        format!("wss://localhost:{}/ws", port.saturating_add(1)),
        format!("wss://192.0.2.1:{port}/ws"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_configuration_failure(RouterConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            data_dir: data_dir(&directory, &format!("url-{index}")),
            instance_id: Uuid::new_v4(),
            tls_cert_file: Some(fixture.certificate_file.clone()),
            tls_key_file: Some(fixture.private_key_file.clone()),
            public_url: Some(Url::parse(&public_url).expect("invalid test public URL parses")),
            exposure: RouterExposure::Direct,
            onboarding_assets_dir: None,
        });
    }
}

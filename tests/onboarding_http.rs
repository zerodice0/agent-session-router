use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use agent_session_router::{
    client::{ClientConfig, ClientRole, RouterClient},
    credentials::{CredentialFile, CredentialRole, read_credential},
    onboarding::{
        BootstrapArtifact, BootstrapManifest, EnrollmentRequest, EnrollmentResponse,
        MAX_ENROLLMENT_BYTES, OnboardingInfo, OnboardingProvider, VERSION, invite_subject,
    },
    protocol::{
        AgentRegistration, ClientMessage, DeliveryMode, PROTOCOL_VERSION, RouterErrorCode,
        ServerMessage, WorkspaceName,
    },
    router::{RouterConfig, RouterExposure, RouterRuntime},
    store::DATABASE_FILE_NAME,
};
use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use uuid::Uuid;

fn config(data_dir: &Path, assets: Option<PathBuf>) -> RouterConfig {
    RouterConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        data_dir: data_dir.to_owned(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: assets,
    }
}

fn http() -> Client {
    Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

fn endpoint(runtime: &RouterRuntime, path: &str) -> String {
    format!("http://{}/onboarding/{path}", runtime.address)
}

async fn admin(runtime: &RouterRuntime, data_dir: &Path) -> RouterClient {
    let credential = read_credential(&data_dir.join("credentials/admin.json")).unwrap();
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url: format!("ws://{}/ws", runtime.address).parse().unwrap(),
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .unwrap();
    client
}

async fn pending(admin: &RouterClient) -> (EnrollmentRequest, CredentialFile, WorkspaceName) {
    let workspace = WorkspaceName::parse("http-enrollment-room").unwrap();
    let ServerMessage::OnboardingInviteIssued {
        server_id,
        invite_id,
        invite_token,
        ..
    } = admin
        .call(ClientMessage::OnboardingInviteIssue {
            request_id: Uuid::new_v4().to_string(),
            workspace: workspace.clone(),
            create_workspace: true,
            provider: Some(OnboardingProvider::Omp),
        })
        .await
        .unwrap()
    else {
        panic!("expected invitation issued by administrator");
    };
    let provider = OnboardingProvider::Omp;
    let (side, client) = provider.identity();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        invite_subject(invite_id),
        Some(side),
        Some(client),
        vec![workspace.clone()],
    )
    .unwrap();
    (
        EnrollmentRequest {
            version: VERSION,
            server_id,
            invite_id,
            invite_token,
            enrollment_id: Uuid::new_v4(),
            provider,
            credential_id: credential.id,
            credential_token: credential.token.clone(),
        },
        credential,
        workspace,
    )
}

async fn response_json(response: Response, status: StatusCode) -> Value {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    assert!(response.headers().get("location").is_none());
    response.json().await.unwrap()
}

async fn assert_error(response: Response, status: StatusCode, code: &str) {
    assert_eq!(response_json(response, status).await, json!({"code": code}));
}

async fn stop(runtime: RouterRuntime) {
    runtime.shutdown().await.unwrap();
    runtime.wait().await.unwrap();
}

#[tokio::test]
async fn info_keeps_persistent_server_identity_without_bootstrap_assets() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let data_dir = root.join("data");
    let client = http();
    let mut first_server_id = None;
    for assets in [None, Some(root.join("absent-bootstrap"))] {
        let runtime = RouterRuntime::start(config(&data_dir, assets))
            .await
            .unwrap();
        let value = response_json(
            client.get(endpoint(&runtime, "info")).send().await.unwrap(),
            StatusCode::OK,
        )
        .await;
        let info: OnboardingInfo = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            value,
            json!({
                "version": VERSION, "serverId": info.server_id,
                "protocolVersion": PROTOCOL_VERSION, "manifestSha256": null, "availableTargets": [],
            })
        );
        assert!(!info.server_id.is_nil());
        assert_ne!(info.server_id, runtime.instance_id);
        if let Some(first) = first_server_id {
            assert_eq!(info.server_id, first);
        }
        first_server_id = Some(info.server_id);
        assert_error(
            client
                .get(endpoint(&runtime, "files/bootstrap-manifest.json"))
                .send()
                .await
                .unwrap(),
            StatusCode::SERVICE_UNAVAILABLE,
            "bootstrap_assets_missing",
        )
        .await;
        let admin = admin(&runtime, &data_dir).await;
        let (request, _, _) = pending(&admin).await;
        assert_eq!(request.server_id, info.server_id);
        admin.close().await.unwrap();
        stop(runtime).await;
    }
}

async fn assert_enrollment_replay(
    client: &Client,
    url: &str,
    request: &EnrollmentRequest,
    credential: &CredentialFile,
    workspace: &WorkspaceName,
) {
    let wrong_server = EnrollmentRequest {
        server_id: Uuid::new_v4(),
        ..request.clone()
    };
    assert_error(
        client.post(url).json(&wrong_server).send().await.unwrap(),
        StatusCode::UNAUTHORIZED,
        "invite_unavailable",
    )
    .await;
    let first = response_json(
        client.post(url).json(request).send().await.unwrap(),
        StatusCode::OK,
    )
    .await;
    let replay = response_json(
        client.post(url).json(request).send().await.unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(first, replay);
    let encoded = first.to_string();
    for secret in [&request.invite_token, &request.credential_token] {
        assert!(
            !encoded.contains(secret.expose()),
            "HTTP success exposed a bearer token"
        );
    }
    let response: EnrollmentResponse = serde_json::from_value(first).unwrap();
    assert_eq!(response.server_id, request.server_id);
    assert_eq!(response.invite_id, request.invite_id);
    assert_eq!(response.claims.id, credential.id);
    assert_eq!(response.claims.workspaces, vec![workspace.clone()]);
    let other_consumer = EnrollmentRequest {
        enrollment_id: Uuid::new_v4(),
        ..request.clone()
    };
    assert_error(
        client.post(url).json(&other_consumer).send().await.unwrap(),
        StatusCode::UNAUTHORIZED,
        "invite_unavailable",
    )
    .await;
}

async fn assert_invited_agent_grants(
    agent: &RouterClient,
    workspace: &WorkspaceName,
    unrelated: WorkspaceName,
) {
    let ServerMessage::Workspaces { workspaces, .. } = agent
        .call(ClientMessage::WorkspaceList {
            request_id: "visible".into(),
            after: None,
            limit: Some(100),
        })
        .await
        .unwrap()
    else {
        panic!("expected granted workspace list");
    };
    assert_eq!(
        workspaces
            .into_iter()
            .map(|item| item.name)
            .collect::<Vec<_>>(),
        vec![workspace.clone()]
    );
    assert!(matches!(
        agent
            .call(ClientMessage::WorkspaceJoin {
                request_id: "hidden".into(),
                name: unrelated,
            })
            .await
            .unwrap(),
        ServerMessage::Error {
            code: RouterErrorCode::WorkspaceNotFound,
            ..
        }
    ));
    assert!(matches!(
        agent
            .call(ClientMessage::WorkspaceCreate {
                request_id: "cannot-create".into(),
                name: WorkspaceName::parse("forbidden-room").unwrap(),
            })
            .await
            .unwrap(),
        ServerMessage::Error {
            code: RouterErrorCode::PermissionDenied,
            ..
        }
    ));
    assert!(matches!(
        agent
            .call(ClientMessage::CredentialIssue {
                request_id: "cannot-issue".into(),
                role: CredentialRole::Operator,
                subject: "forbidden-operator".into(),
                agent_side: None,
                agent_client: None,
                workspaces: vec![workspace.clone()],
            })
            .await
            .unwrap(),
        ServerMessage::Error {
            code: RouterErrorCode::PermissionDenied,
            ..
        }
    ));
    assert_eq!(
        &agent.workspace_join(workspace.clone()).await.unwrap().0,
        workspace
    );
    assert!(matches!(
        agent
            .call(ClientMessage::WorkspacePost {
                request_id: "http-enrolled-post".into(),
                content: "enrolled over HTTP".into(),
            })
            .await
            .unwrap(),
        ServerMessage::WorkspacePosted { .. }
    ));
    assert!(
        agent
            .workspace_history(None, Some(100))
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| event.content.as_deref() == Some("enrolled over HTTP"))
    );
}

#[tokio::test]
async fn http_enrollment_replays_and_registers_only_the_invited_agent_grants() {
    let directory = tempfile::tempdir().unwrap();
    let data_dir = directory.path().canonicalize().unwrap().join("data");
    let runtime = RouterRuntime::start(config(&data_dir, None)).await.unwrap();
    let admin = admin(&runtime, &data_dir).await;
    let unrelated = WorkspaceName::parse("unrelated-private-room").unwrap();
    assert!(matches!(
        admin
            .call(ClientMessage::WorkspaceCreate {
                request_id: "unrelated".into(),
                name: unrelated.clone(),
            })
            .await
            .unwrap(),
        ServerMessage::WorkspaceCreated { .. }
    ));
    let (request, credential, workspace) = pending(&admin).await;
    let client = http();
    let url = endpoint(&runtime, "enroll");
    assert_enrollment_replay(&client, &url, &request, &credential, &workspace).await;

    let (side, provider_client) = OnboardingProvider::Omp.identity();
    let (agent, _events) = RouterClient::connect(ClientConfig {
        router_url: format!("ws://{}/ws", runtime.address).parse().unwrap(),
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: credential.subject.clone(),
                side,
                client: provider_client,
                activity: None,
                delivery_mode: DeliveryMode::Push,
            },
            credential,
            delegation_token: None,
        },
        ca_file: None,
    })
    .await
    .unwrap();
    assert_invited_agent_grants(&agent, &workspace, unrelated).await;
    assert!(matches!(
        admin
            .call(ClientMessage::OnboardingInviteRevoke {
                request_id: "revoke".into(),
                invite_id: request.invite_id,
            })
            .await
            .unwrap(),
        ServerMessage::OnboardingInviteRevoked { .. }
    ));
    assert_error(
        client.post(&url).json(&request).send().await.unwrap(),
        StatusCode::UNAUTHORIZED,
        "invite_unavailable",
    )
    .await;
    agent.close().await.unwrap();
    admin.close().await.unwrap();
    stop(runtime).await;
    for name in [DATABASE_FILE_NAME, "router.sqlite-wal"] {
        let bytes = match fs::read(data_dir.join(name)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => panic!("reading closed database fixture: {error}"),
        };
        for secret in [&request.invite_token, &request.credential_token] {
            assert!(
                !bytes
                    .windows(secret.expose().len())
                    .any(|window| window == secret.expose().as_bytes()),
                "bearer token persisted in database or WAL"
            );
        }
    }
}

#[tokio::test]
async fn enrollment_rejects_non_json_origins_unknown_fields_and_both_oversize_body_modes() {
    let directory = tempfile::tempdir().unwrap();
    let data_dir = directory.path().canonicalize().unwrap().join("data");
    let runtime = RouterRuntime::start(config(&data_dir, None)).await.unwrap();
    let admin = admin(&runtime, &data_dir).await;
    let (request, _, _) = pending(&admin).await;
    let client = http();
    let url = endpoint(&runtime, "enroll");
    for builder in [
        client
            .post(&url)
            .header("content-type", "application/json")
            .body("{"),
        client
            .post(&url)
            .header("content-type", "text/plain")
            .body("{}"),
        client.post(&url).body("{}"),
        client.post(&url).header("origin", "null").json(&request),
    ] {
        assert_error(
            builder.send().await.unwrap(),
            StatusCode::BAD_REQUEST,
            "malformed_request",
        )
        .await;
    }
    let mut unknown = serde_json::to_value(&request).unwrap();
    unknown["role"] = json!("operator");
    assert_error(
        client.post(&url).json(&unknown).send().await.unwrap(),
        StatusCode::BAD_REQUEST,
        "malformed_request",
    )
    .await;
    let malformed = EnrollmentRequest {
        credential_id: Uuid::nil(),
        ..request.clone()
    };
    assert_error(
        client.post(&url).json(&malformed).send().await.unwrap(),
        StatusCode::BAD_REQUEST,
        "malformed_request",
    )
    .await;
    assert_error(
        client
            .post(&url)
            .header("content-type", "application/json")
            .body(vec![b' '; MAX_ENROLLMENT_BYTES + 1])
            .send()
            .await
            .unwrap(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "request_too_large",
    )
    .await;
    let chunks = futures_util::stream::iter([
        Ok::<_, std::io::Error>(vec![b' '; MAX_ENROLLMENT_BYTES]),
        Ok(vec![b' ']),
    ]);
    assert_error(
        client
            .post(&url)
            .header("content-type", "application/json")
            .body(reqwest::Body::wrap_stream(chunks))
            .send()
            .await
            .unwrap(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "request_too_large",
    )
    .await;
    let mut at_limit = serde_json::to_vec(&request).unwrap();
    assert!(at_limit.len() < MAX_ENROLLMENT_BYTES);
    at_limit.resize(MAX_ENROLLMENT_BYTES, b' ');
    response_json(
        client
            .post(&url)
            .header("content-type", "application/json; charset=utf-8")
            .body(at_limit)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    for path in ["enroll", "unknown", "files/a/b", "files/%FF"] {
        assert_error(
            client.get(endpoint(&runtime, path)).send().await.unwrap(),
            StatusCode::BAD_REQUEST,
            "malformed_request",
        )
        .await;
    }
    admin.close().await.unwrap();
    stop(runtime).await;
}

#[tokio::test]
async fn peer_rate_limit_counts_rejected_attempts_and_ignores_forwarding_headers() {
    let directory = tempfile::tempdir().unwrap();
    let data_dir = directory.path().canonicalize().unwrap().join("data");
    let runtime = RouterRuntime::start(config(&data_dir, None)).await.unwrap();
    let client = http();
    for index in 0..11 {
        let response = client
            .post(endpoint(&runtime, "enroll"))
            .header("x-forwarded-for", format!("192.0.2.{}", index + 1))
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .unwrap();
        if index < 10 {
            assert_error(response, StatusCode::BAD_REQUEST, "malformed_request").await;
        } else {
            assert_error(response, StatusCode::TOO_MANY_REQUESTS, "overloaded").await;
        }
    }
    response_json(
        client.get(endpoint(&runtime, "info")).send().await.unwrap(),
        StatusCode::OK,
    )
    .await;
    let admin = admin(&runtime, &data_dir).await;
    pending(&admin).await;
    admin.close().await.unwrap();
    stop(runtime).await;
}

#[tokio::test]
async fn eight_incomplete_json_requests_hold_enrollment_slots_until_their_bodies_finish() {
    let directory = tempfile::tempdir().unwrap();
    let data_dir = directory.path().canonicalize().unwrap().join("data");
    let mut configuration = config(&data_dir, None);
    // This exposure has 32 pre-auth slots, so the ninth request specifically
    // exercises the tighter enrollment bound rather than the direct-IP bound.
    configuration.exposure = RouterExposure::TailscaleServe;
    let runtime = RouterRuntime::start(configuration).await.unwrap();
    let mut sockets = Vec::new();
    for _ in 0..8 {
        let mut socket = TcpStream::connect(runtime.address).await.unwrap();
        socket.write_all(format!(
            "POST /onboarding/enroll HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
            runtime.address,
        ).as_bytes()).await.unwrap();
        let mut interim = [0; 25];
        tokio::time::timeout(Duration::from_secs(2), socket.read_exact(&mut interim))
            .await
            .unwrap()
            .unwrap();
        // Hyper emits this only when the handler starts polling the body.
        assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
        sockets.push(socket);
    }
    let client = http();
    assert_error(
        client
            .post(endpoint(&runtime, "enroll"))
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        StatusCode::TOO_MANY_REQUESTS,
        "overloaded",
    )
    .await;
    for socket in &mut sockets {
        socket.write_all(b"{}").await.unwrap();
    }
    for mut socket in sockets {
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(2), socket.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 400"));
        assert!(
            response
                .to_ascii_lowercase()
                .contains("cache-control: no-store\r\n")
        );
        assert!(response.ends_with("{\"code\":\"malformed_request\"}"));
    }
    assert_error(
        client
            .post(endpoint(&runtime, "enroll"))
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
        "malformed_request",
    )
    .await;
    stop(runtime).await;
}

fn asset_fixture(directory: &Path) -> (BootstrapManifest, Vec<u8>, Vec<u8>) {
    fs::create_dir(directory).unwrap();
    let binary = (0_u8..251).cycle().take(150_000).collect::<Vec<_>>();
    let archive = b"bootstrap archive fixture".to_vec();
    let target = "aarch64-apple-darwin";
    let artifact = BootstrapArtifact {
        target: target.into(),
        binary_file: format!("asr-{target}"),
        binary_sha256: format!("{:x}", Sha256::digest(&binary)),
        archive_file: format!("agent-session-router-{target}.tar.gz"),
        archive_sha256: format!("{:x}", Sha256::digest(&archive)),
        binary_bytes: binary.len() as u64,
        archive_bytes: archive.len() as u64,
    };
    fs::write(directory.join(&artifact.binary_file), &binary).unwrap();
    fs::write(directory.join(&artifact.archive_file), &archive).unwrap();
    let manifest = BootstrapManifest {
        version: VERSION,
        asr_version: env!("CARGO_PKG_VERSION").into(),
        artifacts: vec![artifact],
    };
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    fs::write(directory.join("bootstrap-manifest.json"), &manifest_bytes).unwrap();
    (manifest, binary, manifest_bytes)
}

async fn download(client: &Client, runtime: &RouterRuntime, filename: &str) -> Vec<u8> {
    let response = client
        .get(endpoint(runtime, &format!("files/{filename}")))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    assert!(response.headers().get("location").is_none());
    let length = response.content_length().unwrap();
    let bytes = response.bytes().await.unwrap().to_vec();
    assert_eq!(length, bytes.len() as u64);
    bytes
}

#[tokio::test]
async fn files_stream_only_validated_names_with_independent_positions_and_stop_after_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let assets = root.join("bootstrap");
    let (manifest, binary, manifest_bytes) = asset_fixture(&assets);
    fs::write(assets.join("private-secret.txt"), "must-not-be-served").unwrap();
    let runtime = RouterRuntime::start(config(&root.join("data"), Some(assets.clone())))
        .await
        .unwrap();
    let client = http();
    let info = response_json(
        client.get(endpoint(&runtime, "info")).send().await.unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        info["manifestSha256"],
        format!("{:x}", Sha256::digest(&manifest_bytes))
    );
    assert_eq!(
        info["availableTargets"],
        json!([manifest.artifacts[0].target])
    );
    assert_eq!(
        download(&client, &runtime, "bootstrap-manifest.json").await,
        manifest_bytes
    );
    let filename = &manifest.artifacts[0].binary_file;
    let (first, second) = tokio::join!(
        download(&client, &runtime, filename),
        download(&client, &runtime, filename)
    );
    assert_eq!(first, binary);
    assert_eq!(second, binary);
    for name in [
        "private-secret.txt",
        "missing-file",
        "%2e%2e%2fprivate-secret.txt",
        "%2Fetc%2Fpasswd",
    ] {
        assert_error(
            client
                .get(endpoint(&runtime, &format!("files/{name}")))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
            "malformed_request",
        )
        .await;
    }
    fs::write(assets.join(filename), b"replaced after validation").unwrap();
    assert_error(
        client
            .get(endpoint(&runtime, &format!("files/{filename}")))
            .send()
            .await
            .unwrap(),
        StatusCode::SERVICE_UNAVAILABLE,
        "bootstrap_assets_invalid",
    )
    .await;
    stop(runtime).await;
}

#[tokio::test]
async fn invalid_bootstrap_assets_fail_before_the_actor_creates_server_state() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let assets = root.join("bootstrap");
    let (manifest, _, _) = asset_fixture(&assets);
    fs::write(
        assets.join(&manifest.artifacts[0].binary_file),
        b"wrong digest",
    )
    .unwrap();
    let data_dir = root.join("data");
    let Err(error) = RouterRuntime::start(config(&data_dir, Some(assets))).await else {
        panic!("invalid bootstrap bytes must not start the server");
    };
    assert_eq!(error.to_string(), "bootstrap_assets_invalid");
    assert!(!data_dir.exists());
}

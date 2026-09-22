#![allow(
    dead_code,
    clippy::missing_errors_doc,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]

use std::{
    collections::VecDeque,
    fs,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    os::unix::fs::{PermissionsExt, symlink},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_session_router::{
    client::{ClientConfig, ClientError, ClientRole, RouterClient},
    credentials::read_credential,
    integrations, protocol,
    router::{RouterConfig, RouterExposure, RouterRuntime},
    tasks, tls,
};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Method, Request, Response, StatusCode, Uri},
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tempfile::{TempDir, tempdir};
use tokio::task::JoinHandle;
use uuid::Uuid;

use integrations::{
    ExternalErrorCode, IntegrationAccess, IntegrationCatalog, IntegrationClient,
    IntegrationClientError,
};
use tasks::ExternalProvider;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SECRET: &str = "secret-sentinel-token";
const TEAM_ID: &str = "11111111-1111-4111-8111-111111111111";
const PROJECT_ID: &str = "22222222-2222-4222-8222-222222222222";
const ISSUE_ID: &str = "33333333-3333-4333-8333-333333333333";

#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum ReceiptCommand<'a> {
    Import {
        operation_id: Uuid,
        provider: ExternalProvider,
        external_id: &'a str,
    },
}

struct PrivateConfig {
    _directory: TempDir,
    config_file: PathBuf,
    token_file: PathBuf,
}

impl PrivateConfig {
    fn new(connections: Value) -> Self {
        let directory = tempdir().expect("private config directory");
        let root = directory.path().canonicalize().expect("canonical tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("private tempdir");
        let token_file = root.join("integration.token");
        fs::write(&token_file, format!("{SECRET}\n")).expect("write token");
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o600)).expect("protect token");
        let config_file = root.join("integrations.json");
        fs::write(
            &config_file,
            serde_json::to_vec(&json!({"version": 1, "connections": connections}))
                .expect("serialize config"),
        )
        .expect("write config");
        fs::set_permissions(&config_file, fs::Permissions::from_mode(0o600))
            .expect("protect config");
        Self {
            _directory: directory,
            config_file,
            token_file,
        }
    }

    fn github_connection(&self, workspace: &str, access: &str) -> Value {
        json!({
            "workspace": workspace,
            "provider": "github",
            "repository": "owner/repo",
            "access": access,
            "tokenFile": self.token_file,
        })
    }

    fn linear_connection(&self, workspace: &str, access: &str) -> Value {
        json!({
            "workspace": workspace,
            "provider": "linear",
            "teamId": TEAM_ID,
            "projectId": PROJECT_ID,
            "access": access,
            "tokenFile": self.token_file,
        })
    }

    fn overwrite_connections(&self, connections: Value) {
        fs::write(
            &self.config_file,
            serde_json::to_vec(&json!({"version": 1, "connections": connections}))
                .expect("serialize replacement config"),
        )
        .expect("replace config");
    }
}

#[derive(Clone)]
struct PlannedResponse {
    status: StatusCode,
    body: Vec<u8>,
    delay: Duration,
}

impl PlannedResponse {
    fn json(value: Value) -> Self {
        Self {
            status: StatusCode::OK,
            body: serde_json::to_vec(&value).expect("response JSON"),
            delay: Duration::ZERO,
        }
    }

    fn with_status(status: StatusCode, value: Value) -> Self {
        Self {
            status,
            body: serde_json::to_vec(&value).expect("response JSON"),
            delay: Duration::ZERO,
        }
    }
}

#[derive(Clone, Debug)]
struct ObservedRequest {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Vec<u8>,
}

#[derive(Default)]
struct FixtureState {
    responses: Mutex<VecDeque<PlannedResponse>>,
    requests: Mutex<Vec<ObservedRequest>>,
}

impl FixtureState {
    fn push(&self, response: PlannedResponse) {
        self.responses
            .lock()
            .expect("response lock")
            .push_back(response);
    }

    fn requests(&self) -> Vec<ObservedRequest> {
        self.requests.lock().expect("request lock").clone()
    }

    fn request_count(&self) -> usize {
        self.requests.lock().expect("request lock").len()
    }
}

struct TlsFixture {
    _directory: TempDir,
    address: SocketAddr,
    ca_file: PathBuf,
    state: Arc<FixtureState>,
    handle: axum_server::Handle<SocketAddr>,
    server: JoinHandle<()>,
}

impl TlsFixture {
    fn start() -> Self {
        let directory = tempdir().expect("TLS fixture directory");
        let root = directory.path().canonicalize().expect("canonical TLS root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("private TLS root");

        let mut ca_params =
            CertificateParams::new(vec!["integration-test-ca".to_owned()]).expect("CA params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_key = KeyPair::generate().expect("CA key");
        let ca_certificate = ca_params.self_signed(&ca_key).expect("CA certificate");
        let issuer = Issuer::from_params(&ca_params, &ca_key);
        let mut server_params = CertificateParams::new(vec![
            "api.github.com".to_owned(),
            "api.linear.app".to_owned(),
        ])
        .expect("server params");
        server_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().expect("server key");
        let server_certificate = server_params
            .signed_by(&server_key, &issuer)
            .expect("server certificate");
        let certificate_file = root.join("server.pem");
        let key_file = root.join("server-key.pem");
        let ca_file = root.join("ca.pem");
        fs::write(
            &certificate_file,
            format!("{}{}", server_certificate.pem(), ca_certificate.pem()),
        )
        .expect("write server chain");
        fs::write(&key_file, server_key.serialize_pem()).expect("write server key");
        fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600))
            .expect("protect server key");
        fs::write(&ca_file, ca_certificate.pem()).expect("write CA");

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("fixture bind");
        listener.set_nonblocking(true).expect("nonblocking fixture");
        let address = listener.local_addr().expect("fixture address");
        let state = Arc::new(FixtureState::default());
        let app = Router::new()
            .fallback(fixture_handler)
            .with_state(state.clone());
        let server_config =
            tls::load_server_config(&certificate_file, &key_file).expect("fixture rustls config");
        let handle = axum_server::Handle::new();
        let server_handle = handle.clone();
        let server = tokio::spawn(async move {
            axum_server::from_tcp_rustls(
                listener,
                axum_server::tls_rustls::RustlsConfig::from_config(server_config),
            )
            .expect("create TLS fixture server")
            .handle(server_handle)
            .serve(app.into_make_service())
            .await
            .expect("TLS fixture server");
        });
        Self {
            _directory: directory,
            address,
            ca_file,
            state,
            handle,
            server,
        }
    }

    fn client(&self, timeout: Duration) -> IntegrationClient {
        IntegrationClient::new_for_test(&self.ca_file, self.address, self.address, timeout)
            .expect("fixture client")
    }

    async fn stop(self) {
        self.handle.shutdown();
        self.server.await.expect("fixture server join");
    }
}

async fn fixture_handler(
    State(state): State<Arc<FixtureState>>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, 2 * 1024 * 1024)
        .await
        .expect("fixture request body");
    state
        .requests
        .lock()
        .expect("request lock")
        .push(ObservedRequest {
            method: parts.method,
            uri: parts.uri,
            headers: parts.headers,
            body: bytes.to_vec(),
        });
    let response = state
        .responses
        .lock()
        .expect("response lock")
        .pop_front()
        .expect("planned response");
    if !response.delay.is_zero() {
        tokio::time::sleep(response.delay).await;
    }
    Response::builder()
        .status(response.status)
        .header("content-type", "application/json")
        .body(Body::from(response.body))
        .expect("fixture response")
}

fn github_issue(number: i64, body: Value) -> Value {
    json!({
        "id": 9000 + number,
        "number": number,
        "html_url": format!("https://github.com/owner/repo/issues/{number}"),
        "url": format!("https://api.github.com/repos/owner/repo/issues/{number}"),
        "title": "Fixture issue",
        "body": body,
        "state": "open",
    })
}

fn linear_issue(project_id: &str, description: Value) -> Value {
    json!({
        "id": ISSUE_ID,
        "identifier": "ENG-7",
        "url": "https://linear.app/acme/issue/ENG-7/fixture-issue",
        "title": "Linear fixture",
        "description": description,
        "team": {"id": TEAM_ID},
        "project": {"id": project_id},
        "state": {"name": "Started"},
    })
}

fn write_catalog(include_linear: bool, access: &str) -> (PrivateConfig, IntegrationCatalog) {
    let config = PrivateConfig::new(json!([]));
    let mut connections = vec![config.github_connection("room-a", access)];
    if include_linear {
        connections.push(config.linear_connection("room-b", access));
    }
    config.overwrite_connections(Value::Array(connections));
    let catalog = IntegrationCatalog::load(&config.config_file).expect("load integrations");
    (config, catalog)
}

fn connection<'a>(
    catalog: &'a IntegrationCatalog,
    workspace: &str,
    provider: ExternalProvider,
) -> &'a integrations::IntegrationConnection {
    let workspace = protocol::WorkspaceName::parse(workspace).expect("workspace");
    catalog
        .connection(&workspace, provider)
        .expect("integration connection")
}

#[tokio::test]
async fn private_configuration_is_strict_bounded_and_secret_free() {
    let _guard = TEST_LOCK.lock().await;
    let missing = tempdir().expect("missing config tempdir");
    fs::set_permissions(missing.path(), fs::Permissions::from_mode(0o700))
        .expect("private missing parent");
    let missing_root = missing
        .path()
        .canonicalize()
        .expect("canonical missing parent");
    let empty = IntegrationCatalog::load(&missing_root.join("missing.json"))
        .expect("missing means zero integrations");
    assert!(empty.is_empty());
    assert_eq!(
        IntegrationCatalog::load_explicit(&missing_root.join("missing.json"))
            .expect_err("missing explicit file is invalid")
            .to_string(),
        "integration_configuration_invalid"
    );

    let (config, catalog) = write_catalog(true, "write");
    assert_eq!(catalog.len(), 2);
    let public = catalog.public();
    assert_eq!(public[0].target, "owner/repo");
    assert_eq!(public[0].access, IntegrationAccess::Write);
    assert_eq!(public[1].target, format!("{TEAM_ID}/{PROJECT_ID}"));
    let rendered = format!(
        "{catalog:?} {}",
        serde_json::to_string(&public).expect("public JSON")
    );
    assert!(!rendered.contains(SECRET));
    assert!(!rendered.contains(config.token_file.to_string_lossy().as_ref()));
    fs::set_permissions(&config.config_file, fs::Permissions::from_mode(0o644))
        .expect("loosen config");
    IntegrationCatalog::load(&config.config_file).expect_err("public config rejected");
    fs::set_permissions(&config.config_file, fs::Permissions::from_mode(0o600))
        .expect("restore config");
    let config_symlink = config.config_file.with_file_name("integrations-link.json");
    symlink(&config.config_file, &config_symlink).expect("config symlink");
    IntegrationCatalog::load(&config_symlink).expect_err("symlink config rejected");

    config.overwrite_connections(json!([{
        "workspace": "room-a", "provider": "github", "repository": "owner/repo",
        "tokenFile": config.token_file, "unexpected": true
    }]));
    let error = IntegrationCatalog::load(&config.config_file).expect_err("unknown field rejected");
    assert_eq!(error.to_string(), "integration_configuration_invalid");
    assert!(!format!("{error:?}").contains(SECRET));

    config.overwrite_connections(json!([
        config.github_connection("room-a", "read"),
        config.github_connection("room-a", "write")
    ]));
    IntegrationCatalog::load(&config.config_file).expect_err("duplicate room/provider rejected");

    config.overwrite_connections(json!([
        config.github_connection("room-a", "read"),
        config.github_connection("room-b", "read")
    ]));
    IntegrationCatalog::load(&config.config_file).expect_err("duplicate target rejected");

    let mut invalid_repository = config.github_connection("room-a", "read");
    invalid_repository["repository"] = json!("https://github.com/owner/repo");
    config.overwrite_connections(json!([invalid_repository]));
    IntegrationCatalog::load(&config.config_file).expect_err("repository URL rejected");

    config.overwrite_connections(json!([config.github_connection("room-a", "read")]));
    fs::set_permissions(&config.token_file, fs::Permissions::from_mode(0o644))
        .expect("loosen token");
    IntegrationCatalog::load(&config.config_file).expect_err("public token rejected");
    fs::set_permissions(&config.token_file, fs::Permissions::from_mode(0o600))
        .expect("restore token");
    let symlink_file = config.token_file.with_file_name("token-link");
    symlink(&config.token_file, &symlink_file).expect("token symlink");
    let mut symlink_connection = config.github_connection("room-a", "read");
    symlink_connection["tokenFile"] = json!(symlink_file);
    config.overwrite_connections(json!([symlink_connection]));
    IntegrationCatalog::load(&config.config_file).expect_err("symlink token rejected");
    config.overwrite_connections(json!([config.github_connection("room-a", "read")]));
    fs::write(&config.token_file, vec![b'x'; 8 * 1024 + 1]).expect("oversize token");
    IntegrationCatalog::load(&config.config_file).expect_err("oversize token rejected");
    fs::write(&config.config_file, vec![b' '; 256 * 1024 + 1]).expect("oversize config");
    IntegrationCatalog::load(&config.config_file).expect_err("oversize config rejected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_reloads_lists_and_restores_durable_bindings() {
    let _guard = TEST_LOCK.lock().await;
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("router-data");
    let runtime = RouterRuntime::start(RouterConfig {
        bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        data_dir: data_dir.clone(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    })
    .await
    .expect("start router");
    let credential =
        read_credential(&data_dir.join("credentials/admin.json")).expect("bootstrap credential");
    let router_url = url::Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url,
        role: ClientRole::Operator {
            credential: credential.clone(),
        },
        ca_file: None,
    })
    .await
    .expect("connect operator");
    let workspace = protocol::WorkspaceName::parse("integration-room").expect("workspace");
    client
        .call(protocol::ClientMessage::WorkspaceCreate {
            request_id: "create-integration-room".to_owned(),
            name: workspace.clone(),
        })
        .await
        .expect("create workspace");
    client
        .workspace_join(workspace.clone())
        .await
        .expect("join workspace");

    let token_file = data_dir.join("github.token");
    fs::write(&token_file, format!("{SECRET}\n")).expect("write token");
    fs::set_permissions(&token_file, fs::Permissions::from_mode(0o600)).expect("protect token");
    let config_file = data_dir.join("integrations.json");
    let valid_config = json!({
        "version": 1,
        "connections": [{
            "workspace": workspace.as_str(),
            "provider": "github",
            "repository": "owner/repo",
            "tokenFile": token_file,
            "access": "write"
        }]
    });
    fs::write(
        &config_file,
        serde_json::to_vec(&valid_config).expect("encode configuration"),
    )
    .expect("write configuration");
    fs::set_permissions(&config_file, fs::Permissions::from_mode(0o600))
        .expect("protect configuration");

    let reloaded = client
        .integration_reload()
        .await
        .expect("reload integration");
    assert_eq!(reloaded.len(), 1);
    assert_eq!(reloaded[0].provider, ExternalProvider::Github);
    assert_eq!(reloaded[0].workspace, workspace);
    assert_eq!(reloaded[0].target, "owner/repo");
    assert_eq!(reloaded[0].access, IntegrationAccess::Write);
    assert!(reloaded[0].available);

    fs::write(&config_file, br#"{"version":1,"connections":"invalid"}"#)
        .expect("write invalid configuration");
    let error = client
        .integration_reload()
        .await
        .expect_err("bad reload must fail atomically");
    assert!(matches!(
        error,
        ClientError::Router(protocol::RouterErrorCode::IntegrationConfigurationInvalid)
    ));
    let still_active = client
        .integration_list(workspace.clone())
        .await
        .expect("last good binding remains active");
    let created_task = client
        .task_mutation(protocol::ClientMessage::TaskCreate {
            request_id: "create-linked-task".to_owned(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            title: "Task with external state".to_owned(),
            description: "External links and receipts survive restart".to_owned(),
        })
        .await
        .expect("create task");
    let task_id = created_task.task.summary.id;

    assert_eq!(still_active, reloaded);

    fs::write(
        &config_file,
        serde_json::to_vec(&valid_config).expect("encode restored configuration"),
    )
    .expect("restore configuration");
    client.close().await.expect("close client");
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");

    let recovered_operation_id = Uuid::new_v4();
    let replayed_operation_id = Uuid::new_v4();
    let replay_request = serde_json::to_vec(&ReceiptCommand::Import {
        operation_id: replayed_operation_id,
        provider: ExternalProvider::Github,
        external_id: "123",
    })
    .expect("encode replay request");
    let linked_operation_id = Uuid::new_v4();
    let replay_hash: [u8; 32] = Sha256::digest(replay_request).into();
    let database =
        rusqlite::Connection::open(data_dir.join(agent_session_router::store::DATABASE_FILE_NAME))
            .expect("open router database");
    database
        .execute(
            "INSERT INTO external_operations(workspace,operation_id,actor_id,provider,namespace,kind,task_id,source_version,request_hash,payload_json,phase,status,external_id,url,error,created_at,updated_at) VALUES(?1,?2,'operator:admin','github','owner/repo','publish_issue',NULL,NULL,?3,?4,'dispatched','running',NULL,NULL,NULL,1,1)",
            rusqlite::params![
                workspace.as_str(),
                recovered_operation_id.to_string(),
                vec![0_u8; 32],
                json!({
                    "kind": "publish_issue",
                    "creation_id": Uuid::new_v4(),
                    "title": "Recovered operation",
                    "body": "body",
                    "marker": "[asr recovery]"
                })
                .to_string()
            ],
        )
        .expect("seed dispatched operation");
    database
        .execute(
            "INSERT INTO external_operations(workspace,operation_id,actor_id,provider,namespace,kind,task_id,source_version,request_hash,payload_json,phase,status,external_id,url,error,created_at,updated_at) VALUES(?1,?2,'operator:admin','github','owner/repo','import',NULL,NULL,?3,?4,'terminal','failed',NULL,NULL,'not_applied',1,1)",
            rusqlite::params![
                workspace.as_str(),
                replayed_operation_id.to_string(),
                replay_hash.as_slice(),
                json!({"kind": "import", "external_id": "123"}).to_string()
            ],
        )
        .expect("seed replayable receipt");
    database
        .execute(
            "INSERT INTO external_links(workspace,task_id,provider,namespace,external_id,url,linked_at) VALUES(?1,?2,'github','owner/repo','44','https://github.com/owner/repo/issues/44',1)",
            rusqlite::params![workspace.as_str(), task_id],
        )
        .expect("seed durable external link");
    database
        .execute(
            "INSERT INTO external_operations(workspace,operation_id,actor_id,provider,namespace,kind,task_id,source_version,request_hash,payload_json,phase,status,external_id,url,error,created_at,updated_at) VALUES(?1,?2,'operator:admin','github','owner/repo','link',?3,?4,?5,?6,'terminal','succeeded','44','https://github.com/owner/repo/issues/44',NULL,1,1)",
            rusqlite::params![
                workspace.as_str(),
                linked_operation_id.to_string(),
                task_id,
                created_task.applied_version,
                vec![1_u8; 32],
                json!({
                    "kind": "link",
                    "external_id": "44",
                    "expected_version": created_task.applied_version,
                    "replace": false
                })
                .to_string()
            ],
        )
        .expect("seed durable external operation");
    drop(database);

    let runtime = RouterRuntime::start(RouterConfig {
        bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        data_dir,
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    })
    .await
    .expect("restart router");
    let router_url = url::Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url,
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("reconnect operator");
    client
        .workspace_join(workspace.clone())
        .await
        .expect("rejoin workspace");
    let restored = client
        .integration_list(workspace.clone())
        .await
        .expect("list restored binding");
    let durable_task = client
        .task_get(workspace.clone(), task_id)
        .await
        .expect("read task external detail");
    assert_eq!(durable_task.links.len(), 1);
    assert_eq!(durable_task.links[0].external_id, "44");
    assert!(
        durable_task
            .external_operations
            .iter()
            .any(|operation| operation.id == linked_operation_id)
    );
    assert_eq!(restored, reloaded);
    let (recovered, resolution) = client
        .task_external_status(workspace.clone(), recovered_operation_id)
        .await
        .expect("read recovered operation");
    assert_eq!(
        recovered.status,
        tasks::ExternalOperationStatus::Unconfirmed
    );
    assert_eq!(recovered.error.as_deref(), Some("external_unconfirmed"));
    assert!(resolution.is_none());
    let history = client
        .workspace_history(Some(0), Some(100))
        .await
        .expect("read recovery event");
    assert!(history.events.iter().any(|event| {
        event.kind == protocol::WorkspaceEventKind::Integration
            && event
                .content
                .as_deref()
                .is_some_and(|content| content.contains(&recovered_operation_id.to_string()))
    }));
    let replayed = client
        .task_import(
            workspace.clone(),
            ExternalProvider::Github,
            "123".to_owned(),
            replayed_operation_id,
        )
        .await
        .expect("replay exact operation receipt");
    assert_eq!(replayed.id, replayed_operation_id);
    assert_eq!(replayed.status, tasks::ExternalOperationStatus::Failed);
    let conflict = client
        .task_import(
            workspace.clone(),
            ExternalProvider::Github,
            "different".to_owned(),
            replayed_operation_id,
        )
        .await
        .expect_err("changed arguments must conflict");
    assert!(matches!(
        conflict,
        ClientError::Router(protocol::RouterErrorCode::RequestConflict)
    ));

    let resolution_id = Uuid::new_v4();
    let (resolved_operation, resolution) = client
        .task_external_resolve(
            workspace.clone(),
            recovered_operation_id,
            resolution_id,
            tasks::ExternalResolutionOutcome::NotApplied,
            None,
            "Verified absent in the provider".to_owned(),
        )
        .await
        .expect("resolve unconfirmed operation");
    assert_eq!(
        resolved_operation.status,
        tasks::ExternalOperationStatus::Failed
    );
    assert_eq!(
        resolved_operation.error.as_deref(),
        Some("operator_confirmed_not_applied")
    );
    assert_eq!(resolution.id, resolution_id);
    let replayed_resolution = client
        .task_external_resolve(
            workspace.clone(),
            recovered_operation_id,
            resolution_id,
            tasks::ExternalResolutionOutcome::NotApplied,
            None,
            "Verified absent in the provider".to_owned(),
        )
        .await
        .expect("replay exact resolution");
    assert_eq!(replayed_resolution, (resolved_operation, resolution));
    let resolution_conflict = client
        .task_external_resolve(
            workspace,
            recovered_operation_id,
            resolution_id,
            tasks::ExternalResolutionOutcome::NotApplied,
            None,
            "Changed resolution evidence".to_owned(),
        )
        .await
        .expect_err("changed resolution arguments must conflict");
    assert!(matches!(
        resolution_conflict,
        ClientError::Router(protocol::RouterErrorCode::RequestConflict)
    ));
    client.close().await.expect("close client");
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn github_adapter_validates_identity_headers_statuses_and_never_retries_mutations() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = TlsFixture::start();
    let (_config, catalog) = write_catalog(false, "write");
    let connection = connection(&catalog, "room-a", ExternalProvider::Github);
    let client = fixture.client(Duration::from_millis(100));

    fixture.state.push(PlannedResponse::json(json!({
        "full_name": "owner/repo", "html_url": "https://github.com/owner/repo", "has_issues": true
    })));
    let check = client.check(connection).await.expect("GitHub check");
    assert_eq!(check.target, "owner/repo");

    fixture
        .state
        .push(PlannedResponse::json(github_issue(7, Value::Null)));
    let issue = client
        .get_issue(connection, "7")
        .await
        .expect("GitHub issue");
    assert_eq!(issue.external_id, "7");
    assert_eq!(issue.description, "");

    fixture.state.push(PlannedResponse::with_status(
        StatusCode::CREATED,
        github_issue(8, json!("published body")),
    ));
    let created = client
        .create_issue(
            connection,
            Uuid::new_v4(),
            "Published title",
            "published body",
        )
        .await
        .expect("GitHub issue create");
    assert_eq!(created.external_id, "8");

    fixture
        .state
        .push(PlannedResponse::json(github_issue(7, json!("body"))));
    fixture.state.push(PlannedResponse::with_status(
        StatusCode::CREATED,
        json!({
            "id": 8123,
            "html_url": "https://github.com/owner/repo/issues/7#issuecomment-8123",
            "issue_url": "https://api.github.com/repos/owner/repo/issues/7"
        }),
    ));
    let comment = client
        .create_comment(connection, "7", Uuid::new_v4(), "selected report")
        .await
        .expect("GitHub comment create");
    assert_eq!(comment.external_id, "8123");

    let marker = "[asr workspace:room-a task:1 version:1 actor:operator:admin operation:test]";
    fixture.state.push(PlannedResponse::json(json!({
        "id": 8123,
        "html_url": "https://github.com/owner/repo/issues/7#issuecomment-8123",
        "issue_url": "https://api.github.com/repos/owner/repo/issues/7",
        "body": format!("selected report\n\n{marker}")
    })));
    let verified = client
        .verify_comment_marker(connection, "7", "8123", marker)
        .await
        .expect("verify exact GitHub marker");
    assert_eq!(verified, comment);

    fixture.state.push(PlannedResponse::json(json!({
        "id": 99,
        "html_url": "https://github.com/owner/repo/issues/7#issuecomment-99",
        "issue_url": "https://api.github.com/repos/owner/repo/issues/7",
        "body": marker
    })));
    assert_eq!(
        client
            .verify_comment_marker(connection, "7", "8123", marker)
            .await
            .expect_err("wrong comment identity rejected"),
        IntegrationClientError::Failed(ExternalErrorCode::ScopeMismatch)
    );

    let successful = fixture.state.requests();
    assert_eq!(successful[0].method, Method::GET);
    for request in &successful {
        assert_eq!(
            request.headers["authorization"]
                .to_str()
                .expect("authorization"),
            format!("Bearer {SECRET}")
        );
        assert_eq!(request.headers["accept"], "application/vnd.github+json");
        assert_eq!(request.headers["x-github-api-version"], "2026-03-10");
        assert_eq!(request.headers["user-agent"], "agent-session-router");
    }
    let issue_payload: Value = serde_json::from_slice(&successful[2].body).expect("issue payload");
    assert_eq!(
        issue_payload,
        json!({"title": "Published title", "body": "published body"})
    );
    let comment_payload: Value =
        serde_json::from_slice(&successful[4].body).expect("comment payload");
    assert_eq!(comment_payload, json!({"body": "selected report"}));

    fixture.state.push(PlannedResponse::with_status(
        StatusCode::FORBIDDEN,
        json!({"message": SECRET}),
    ));
    let forbidden = client
        .get_issue(connection, "9")
        .await
        .expect_err("403 rejected");
    assert_eq!(
        forbidden,
        IntegrationClientError::Failed(ExternalErrorCode::PermissionDenied)
    );
    assert!(!format!("{forbidden:?} {forbidden}").contains(SECRET));

    fixture.state.push(PlannedResponse::with_status(
        StatusCode::FOUND,
        json!({"message": SECRET}),
    ));
    assert_eq!(
        client
            .get_issue(connection, "9")
            .await
            .expect_err("redirect rejected"),
        IntegrationClientError::Failed(ExternalErrorCode::RebindRequired)
    );

    let before_mutation = fixture.state.request_count();
    fixture.state.push(PlannedResponse::with_status(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({"message": SECRET}),
    ));
    let server_error = client
        .create_issue(connection, Uuid::new_v4(), "one attempt", "body")
        .await
        .expect_err("mutation 5xx unconfirmed");
    assert_eq!(
        server_error,
        IntegrationClientError::Unconfirmed(ExternalErrorCode::ApiError)
    );
    assert_eq!(fixture.state.request_count(), before_mutation + 1);

    fixture.state.push(PlannedResponse {
        status: StatusCode::OK,
        body: serde_json::to_vec(&github_issue(9, json!("body"))).expect("delayed body"),
        delay: Duration::from_millis(250),
    });
    assert_eq!(
        client
            .get_issue(connection, "9")
            .await
            .expect_err("timeout"),
        IntegrationClientError::Failed(ExternalErrorCode::Timeout)
    );

    fixture.state.push(PlannedResponse {
        status: StatusCode::OK,
        body: vec![b'x'; 1024 * 1024 + 1],
        delay: Duration::ZERO,
    });
    assert_eq!(
        client
            .get_issue(connection, "9")
            .await
            .expect_err("oversize"),
        IntegrationClientError::Failed(ExternalErrorCode::ResponseTooLarge)
    );

    fixture.state.push(PlannedResponse {
        status: StatusCode::OK,
        body: b"not-json".to_vec(),
        delay: Duration::ZERO,
    });
    assert_eq!(
        client
            .get_issue(connection, "9")
            .await
            .expect_err("malformed"),
        IntegrationClientError::Failed(ExternalErrorCode::ApiError)
    );

    let mut wrong_scope = github_issue(9, json!("body"));
    wrong_scope["html_url"] = json!("https://github.com/other/repo/issues/9");
    fixture.state.push(PlannedResponse::json(wrong_scope));
    assert_eq!(
        client
            .get_issue(connection, "9")
            .await
            .expect_err("scope mismatch"),
        IntegrationClientError::Failed(ExternalErrorCode::ScopeMismatch)
    );

    let mut pull_request = github_issue(9, json!("body"));
    pull_request["pull_request"] = json!({"url": "https://api.github.com/pulls/9"});
    fixture.state.push(PlannedResponse::json(pull_request));
    assert_eq!(
        client
            .get_issue(connection, "9")
            .await
            .expect_err("PR rejected"),
        IntegrationClientError::Failed(ExternalErrorCode::ScopeMismatch)
    );
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linear_adapter_validates_graphql_identity_scope_and_single_mutation_attempt() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = TlsFixture::start();
    let (_config, catalog) = write_catalog(true, "write");
    let connection = connection(&catalog, "room-b", ExternalProvider::Linear);
    let client = fixture.client(Duration::from_secs(2));

    fixture.state.push(PlannedResponse::json(json!({"data": {
        "team": {"id": TEAM_ID}, "project": {"id": PROJECT_ID}
    }})));
    client.check(connection).await.expect("Linear check");

    fixture.state.push(PlannedResponse::json(json!({
        "data": {"issue": linear_issue(PROJECT_ID, Value::Null)}
    })));
    let issue = client
        .get_issue(connection, "ENG-7")
        .await
        .expect("Linear issue");
    assert_eq!(issue.external_id, ISSUE_ID);
    assert_eq!(issue.description, "");

    let creation_id = Uuid::new_v4();
    fixture
        .state
        .push(PlannedResponse::json(json!({"data": {"issueCreate": {
            "success": true,
            "issue": {
                "id": creation_id,
                "identifier": "ENG-8",
                "url": "https://linear.app/acme/issue/ENG-8/published",
                "team": {"id": TEAM_ID},
                "project": {"id": PROJECT_ID}
            }
        }}})));
    let created = client
        .create_issue(connection, creation_id, "Linear title", "Linear body")
        .await
        .expect("Linear create issue");
    assert_eq!(created.external_id, creation_id.to_string());

    fixture.state.push(PlannedResponse::json(json!({
        "data": {"issue": linear_issue(PROJECT_ID, json!("body"))}
    })));
    let comment_id = Uuid::new_v4();
    fixture
        .state
        .push(PlannedResponse::json(json!({"data": {"commentCreate": {
            "success": true,
            "comment": {
                "id": comment_id,
                "url": format!("https://linear.app/acme/issue/ENG-7/fixture#comment-{comment_id}"),
                "issue": {
                    "id": ISSUE_ID,
                    "team": {"id": TEAM_ID},
                    "project": {"id": PROJECT_ID}
                }
            }
        }}})));
    client
        .create_comment(connection, ISSUE_ID, comment_id, "selected report")
        .await
        .expect("Linear create comment");

    let marker = "[asr workspace:room-b task:1 version:1 actor:operator:admin operation:test]";
    fixture.state.push(PlannedResponse::json(json!({
        "data": {"issue": linear_issue(PROJECT_ID, json!("body"))}
    })));
    fixture
        .state
        .push(PlannedResponse::json(json!({"data": {"comment": {
            "id": comment_id,
            "url": format!("https://linear.app/acme/issue/ENG-7/fixture#comment-{comment_id}"),
            "body": format!("selected report\n\n{marker}"),
            "issue": {
                "id": ISSUE_ID,
                "team": {"id": TEAM_ID},
                "project": {"id": PROJECT_ID}
            }
        }}})));
    let verified = client
        .verify_comment_marker(connection, ISSUE_ID, &comment_id.to_string(), marker)
        .await
        .expect("verify exact Linear marker");
    assert_eq!(verified.external_id, comment_id.to_string());

    let successful = fixture.state.requests();
    for request in &successful {
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.uri.path(), "/graphql");
        assert_eq!(request.headers["authorization"], SECRET);
        assert!(
            request.headers["content-type"]
                .to_str()
                .expect("content type")
                .starts_with("application/json")
        );
    }
    let check_body: Value = serde_json::from_slice(&successful[0].body).expect("check body");
    assert_eq!(
        check_body["query"],
        "query($team:String!,$project:String!){team(id:$team){id} project(id:$project){id}}"
    );
    assert_eq!(check_body["variables"]["team"], TEAM_ID);
    assert_eq!(check_body["variables"]["project"], PROJECT_ID);
    let create_body: Value = serde_json::from_slice(&successful[2].body).expect("create body");
    assert_eq!(
        create_body["variables"]["input"]["id"],
        creation_id.to_string()
    );
    assert_eq!(create_body["variables"]["input"]["teamId"], TEAM_ID);
    assert_eq!(create_body["variables"]["input"]["projectId"], PROJECT_ID);

    fixture.state.push(PlannedResponse::json(json!({
        "errors": [{"message": SECRET}], "data": {"issue": null}
    })));
    let graph_error = client
        .get_issue(connection, ISSUE_ID)
        .await
        .expect_err("GraphQL errors rejected");
    assert_eq!(
        graph_error,
        IntegrationClientError::Failed(ExternalErrorCode::ApiError)
    );
    assert!(!format!("{graph_error:?} {graph_error}").contains(SECRET));

    fixture.state.push(PlannedResponse::json(json!({"data": {
        "issue": linear_issue("44444444-4444-4444-8444-444444444444", json!("body"))
    }})));
    assert_eq!(
        client
            .get_issue(connection, ISSUE_ID)
            .await
            .expect_err("wrong project rejected"),
        IntegrationClientError::Failed(ExternalErrorCode::ScopeMismatch)
    );

    let failed_creation = Uuid::new_v4();
    let before = fixture.state.request_count();
    fixture
        .state
        .push(PlannedResponse::json(json!({"data": {"issueCreate": {
            "success": false, "issue": null
        }}})));
    assert_eq!(
        client
            .create_issue(connection, failed_creation, "title", "body")
            .await
            .expect_err("success false unconfirmed"),
        IntegrationClientError::Unconfirmed(ExternalErrorCode::ApiError)
    );
    assert_eq!(fixture.state.request_count(), before + 1);

    let mismatch_id = Uuid::new_v4();
    fixture
        .state
        .push(PlannedResponse::json(json!({"data": {"issueCreate": {
            "success": true,
            "issue": {
                "id": Uuid::new_v4(), "identifier": "ENG-9",
                "url": "https://linear.app/acme/issue/ENG-9/wrong-id",
                "team": {"id": TEAM_ID}, "project": {"id": PROJECT_ID}
            }
        }}})));
    assert_eq!(
        client
            .create_issue(connection, mismatch_id, "title", "body")
            .await
            .expect_err("creation UUID mismatch"),
        IntegrationClientError::Unconfirmed(ExternalErrorCode::ScopeMismatch)
    );
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_is_global_four_slot_and_fail_fast() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = TlsFixture::start();
    let (_config, catalog) = write_catalog(false, "read");
    let catalog = Arc::new(catalog);
    let client = Arc::new(fixture.client(Duration::from_secs(2)));
    for _ in 0..4 {
        let mut response = PlannedResponse::json(json!({
            "full_name": "owner/repo",
            "html_url": "https://github.com/owner/repo",
            "has_issues": true
        }));
        response.delay = Duration::from_millis(250);
        fixture.state.push(response);
    }

    let mut calls = Vec::new();
    for _ in 0..4 {
        let client = client.clone();
        let catalog = catalog.clone();
        calls.push(tokio::spawn(async move {
            let connection = connection(&catalog, "room-a", ExternalProvider::Github);
            client.check(connection).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.state.request_count() < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("four admitted requests reached fixture");
    let fifth = client
        .check(connection(&catalog, "room-a", ExternalProvider::Github))
        .await
        .expect_err("fifth slot rejected");
    assert_eq!(fifth, IntegrationClientError::Busy);
    assert_eq!(fixture.state.request_count(), 4);
    for call in calls {
        call.await
            .expect("admitted call join")
            .expect("admitted call");
    }
    fixture.stop().await;
}

use std::{
    fs,
    io::Write,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use agent_session_router::{
    client::{ClientConfig, ClientRole, RouterClient},
    credentials::{CredentialFile, CredentialRole, read_credential, write_credential_exclusive},
    onboarding::{
        BootstrapArtifact, BootstrapManifest, EnrollmentRequest, EnrollmentResponse,
        OnboardingInfo, OnboardingProvider, OnboardingTicket, RouteKind, VERSION, invite_subject,
        issue::{PromptOptions, issue_prompt},
    },
    process::{RuntimeRecord, RuntimeShareMode, RuntimeStore},
    protocol::{ClientMessage, ServerMessage, WorkspaceName},
    router::{RouterConfig, RouterExposure, RouterRuntime},
    store::DATABASE_FILE_NAME,
};
use reqwest::StatusCode;
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

// Issuance verifies bounded bytes, not executable behavior. Shell execution and
// full release extraction have their own tests; this fixture never runs a stub.
fn bundle(directory: &Path) -> BootstrapManifest {
    fs::create_dir_all(directory).unwrap();
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    let binary = b"bounded issuance binary fixture";
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut archive = tar::Builder::new(encoder);
    let mut header = tar::Header::new_gnu();
    header.set_size(binary.len() as u64);
    header.set_mode(0o755);
    header.set_cksum();
    archive
        .append_data(&mut header, "bin/asr", &binary[..])
        .unwrap();
    let archive = archive.into_inner().unwrap().finish().unwrap();
    let target = "aarch64-apple-darwin";
    let artifact = BootstrapArtifact {
        target: target.into(),
        binary_file: format!("asr-{target}"),
        binary_sha256: digest(binary),
        binary_bytes: binary.len() as u64,
        archive_file: format!("agent-session-router-{target}.tar.gz"),
        archive_sha256: digest(&archive),
        archive_bytes: archive.len() as u64,
    };
    fs::write(directory.join(&artifact.binary_file), binary).unwrap();
    fs::write(directory.join(&artifact.archive_file), archive).unwrap();
    let manifest = BootstrapManifest {
        version: VERSION,
        asr_version: env!("CARGO_PKG_VERSION").into(),
        artifacts: vec![artifact],
    };
    fs::write(
        directory.join("bootstrap-manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    manifest
}

fn config(data_dir: &Path, assets: &Path) -> RouterConfig {
    RouterConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        data_dir: data_dir.into(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: Some(assets.into()),
    }
}

fn record(runtime: &RouterRuntime) -> RuntimeRecord {
    RuntimeRecord {
        instance_id: runtime.instance_id,
        control_url: format!("ws://{}/ws", runtime.address),
        share_mode: RuntimeShareMode::Local,
        advertised_url: None,
        owned_serve: None,
    }
}

async fn admin(runtime: &RouterRuntime, data_dir: &Path) -> RouterClient {
    RouterClient::connect(ClientConfig {
        router_url: record(runtime).control_url.parse().unwrap(),
        role: ClientRole::Operator {
            credential: read_credential(&data_dir.join("credentials/admin.json")).unwrap(),
        },
        ca_file: None,
    })
    .await
    .unwrap()
    .0
}

async fn workspaces(client: &RouterClient) -> Vec<WorkspaceName> {
    match client
        .call(ClientMessage::WorkspaceList {
            request_id: Uuid::new_v4().to_string(),
            after: None,
            limit: Some(100),
        })
        .await
        .unwrap()
    {
        ServerMessage::Workspaces {
            workspaces,
            has_more: false,
            ..
        } => workspaces.into_iter().map(|w| w.name).collect(),
        _ => panic!("expected complete workspace page"),
    }
}

async fn stop(runtime: RouterRuntime) {
    runtime.shutdown().await.unwrap();
    runtime.wait().await.unwrap();
}

fn invitation_counts(data_dir: &Path) -> (i64, i64) {
    // Independent post-shutdown audit: issuance has no public invitation-list API.
    let connection = Connection::open_with_flags(
        data_dir.join(DATABASE_FILE_NAME),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    connection
        .query_row(
            "SELECT count(*), count(redeemed_at) FROM onboarding_invites",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

fn options() -> PromptOptions {
    PromptOptions {
        workspace: WorkspaceName::parse("requested-room").unwrap(),
        create_workspace: true,
        name: Some("office".into()),
        provider: Some(OnboardingProvider::Omp),
        endpoints: Vec::new(),
        ca_file: None,
    }
}

fn replace_admin(data_dir: &Path, credential: &CredentialFile) {
    let path = data_dir.join("credentials/admin.json");
    fs::remove_file(&path).unwrap();
    write_credential_exclusive(&path, credential).unwrap();
}

fn reject_prompt_input(
    mode: &str,
    home: &Path,
    assets: &Path,
    manifest: Option<BootstrapManifest>,
    request: &mut PromptOptions,
) -> &'static str {
    match mode {
        "missing-assets" => "bootstrap_assets_missing",
        "mutated-binary" | "mutated-archive" => {
            let artifact = &manifest.as_ref().unwrap().artifacts[0];
            let path = assets.join(if mode == "mutated-binary" {
                &artifact.binary_file
            } else {
                &artifact.archive_file
            });
            let mut bytes = fs::read(&path).unwrap();
            bytes[0] ^= 1;
            fs::write(path, bytes).unwrap();
            "bootstrap_assets_invalid"
        }
        "changed-manifest" => {
            let mut manifest = manifest.unwrap();
            manifest.asr_version.push_str("-changed");
            fs::write(
                assets.join("bootstrap-manifest.json"),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            "bootstrap_manifest_mismatch"
        }
        "reserved-name" | "invalid-name" => {
            request.name = Some(
                if mode == "reserved-name" {
                    "local"
                } else {
                    "../office"
                }
                .into(),
            );
            "invalid_profile"
        }
        "invalid-route" => {
            request.endpoints = vec!["local=ws://0.0.0.0:8787/ws".into()];
            "route_invalid"
        }
        "private-key-ca" => {
            let path = home.join("not-a-ca.pem");
            fs::write(
                &path,
                "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
            )
            .unwrap();
            request.ca_file = Some(path);
            "ca_invalid"
        }
        "missing-workspace" => {
            request.create_workspace = false;
            "workspace_not_found"
        }
        _ => panic!("unknown prompt input scenario"),
    }
}

fn reject_runtime(
    mode: &str,
    home: &Path,
    data_dir: &Path,
    store: &RuntimeStore,
    runtime: &RouterRuntime,
) -> &'static str {
    match mode {
        "absent-runtime" => {
            fs::remove_file(store.runtime_path()).unwrap();
            "router_not_running"
        }
        "runtime-instance-mismatch" => {
            let mut stale = record(runtime);
            stale.instance_id = Uuid::new_v4();
            store.write(&stale).unwrap();
            "owned_router_unreachable"
        }
        "unowned-directory" => {
            fs::set_permissions(data_dir, fs::Permissions::from_mode(0o755)).unwrap();
            "owned_runtime_invalid"
        }
        "unowned-record" => {
            fs::set_permissions(store.runtime_path(), fs::Permissions::from_mode(0o644)).unwrap();
            "owned_runtime_invalid"
        }
        "symlink-record" => {
            let path = store.runtime_path();
            let target = home.join("runtime-copy.json");
            fs::rename(&path, &target).unwrap();
            symlink(target, path).unwrap();
            "owned_runtime_invalid"
        }
        _ => panic!("unknown runtime ownership scenario"),
    }
}

async fn reject_authority(
    mode: &str,
    home: &Path,
    data_dir: &Path,
    assets: &Path,
    observer: &RouterClient,
    request: &mut PromptOptions,
) -> (&'static str, Option<RouterRuntime>) {
    match mode {
        "foreign-route" | "foreign-admin" => {
            let foreign = RouterRuntime::start(config(&home.join("foreign"), assets))
                .await
                .unwrap();
            let expected = if mode == "foreign-route" {
                request.endpoints = vec![format!("local={}", record(&foreign).control_url)];
                "server_identity_mismatch"
            } else {
                let credential =
                    read_credential(&home.join("foreign/credentials/admin.json")).unwrap();
                replace_admin(data_dir, &credential);
                "admin_connection_failed"
            };
            (expected, Some(foreign))
        }
        "unowned-admin" => {
            fs::set_permissions(
                data_dir.join("credentials/admin.json"),
                fs::Permissions::from_mode(0o644),
            )
            .unwrap();
            ("admin_credential_invalid", None)
        }
        "non-admin" | "forged-admin-claims" => {
            let ServerMessage::CredentialIssued { mut credential, .. } = observer
                .call(ClientMessage::CredentialIssue {
                    request_id: Uuid::new_v4().to_string(),
                    role: CredentialRole::Operator,
                    subject: "ordinary-operator".into(),
                    agent_side: None,
                    agent_client: None,
                    workspaces: vec![],
                })
                .await
                .unwrap()
            else {
                panic!("expected operator credential");
            };
            if mode == "forged-admin-claims" {
                credential.subject = "admin".into();
            }
            replace_admin(data_dir, &credential);
            ("permission_denied", None)
        }
        _ => panic!("unknown authority scenario"),
    }
}

async fn rejected(mode: &str, home: &Path) {
    let data_dir = home.join("data");
    let assets = data_dir.join("bootstrap");
    // Let RouterRuntime create its private data directory in the missing case.
    let manifest = if mode == "missing-assets" {
        None
    } else {
        Some(bundle(&assets))
    };
    if manifest.is_some() {
        fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let runtime = RouterRuntime::start(config(&data_dir, &assets))
        .await
        .unwrap();
    let store = RuntimeStore::new(data_dir.clone()).unwrap();
    store.write(&record(&runtime)).unwrap();
    let observer = admin(&runtime, &data_dir).await;
    assert!(workspaces(&observer).await.is_empty());
    let mut request = options();
    let (expected, other) = match mode {
        "absent-runtime"
        | "runtime-instance-mismatch"
        | "unowned-directory"
        | "unowned-record"
        | "symlink-record" => (
            reject_runtime(mode, home, &data_dir, &store, &runtime),
            None,
        ),
        "foreign-route"
        | "foreign-admin"
        | "unowned-admin"
        | "non-admin"
        | "forged-admin-claims" => {
            reject_authority(mode, home, &data_dir, &assets, &observer, &mut request).await
        }
        _ => (
            reject_prompt_input(mode, home, &assets, manifest, &mut request),
            None,
        ),
    };
    let Err(error) = issue_prompt(&data_dir, request).await else {
        panic!("issuance must fail for {mode}");
    };
    assert_eq!(error.0, expected, "case {mode}");
    // The independent authenticated connection observes the actor, not a mock.
    assert!(
        workspaces(&observer).await.is_empty(),
        "failed issuance created a workspace"
    );
    observer.close().await.unwrap();
    stop(runtime).await;
    assert_eq!(
        invitation_counts(&data_dir),
        (0, 0),
        "failed issuance issued or consumed an invitation"
    );
    if let Some(foreign) = other {
        let observer = admin(&foreign, &home.join("foreign")).await;
        assert!(workspaces(&observer).await.is_empty());
        observer.close().await.unwrap();
        stop(foreign).await;
        assert_eq!(invitation_counts(&home.join("foreign")), (0, 0));
    }
}

fn prompt_ticket(text: &str) -> OnboardingTicket {
    text.lines()
        .find_map(|line| serde_json::from_str::<OnboardingTicket>(line).ok())
        .expect("ticket JSON in executable prompt")
}

fn assert_prompt_shell_syntax(text: &str) {
    // Parse the actual copy/paste shell as POSIX sh; do not replace the server or
    // downloaded installer with a shell stub to claim issuance coverage.
    let shell = text
        .split_once("```sh\n")
        .unwrap()
        .1
        .split_once("\n```")
        .unwrap()
        .0;
    let mut parser = Command::new("sh")
        .arg("-n")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    parser
        .stdin
        .take()
        .unwrap()
        .write_all(shell.as_bytes())
        .unwrap();
    assert!(parser.wait_with_output().unwrap().status.success());
}

async fn assert_provider_bound_redemption(
    http: &reqwest::Client,
    runtime: &RouterRuntime,
    ticket: &OnboardingTicket,
) {
    let provider = OnboardingProvider::Omp;
    let (side, client) = provider.identity();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        invite_subject(ticket.invite_id),
        Some(side),
        Some(client),
        vec![ticket.workspace.clone()],
    )
    .unwrap();
    let mut enrollment = EnrollmentRequest {
        version: VERSION,
        server_id: ticket.server_id,
        invite_id: ticket.invite_id,
        invite_token: ticket.invite_token.clone(),
        enrollment_id: Uuid::new_v4(),
        provider: OnboardingProvider::CodexCli,
        credential_id: credential.id,
        credential_token: credential.token.clone(),
    };
    let url = format!("http://{}/onboarding/enroll", runtime.address);
    assert_eq!(
        http.post(&url)
            .json(&enrollment)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    enrollment.provider = provider;
    let response = http.post(&url).json(&enrollment).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: EnrollmentResponse = response.json().await.unwrap();
    assert_eq!(response.server_id, ticket.server_id);
    assert_eq!(response.invite_id, ticket.invite_id);
    assert_eq!(response.claims, credential.public_claims());
}

async fn succeeds(home: &Path, relative_assets: bool) {
    let data_dir = home.join("data");
    let assets = if relative_assets {
        home.join("bundle")
    } else {
        data_dir.join("bootstrap")
    };
    let manifest = bundle(&assets);
    if !relative_assets {
        fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let runtime = RouterRuntime::start(config(&data_dir, &assets))
        .await
        .unwrap();
    let store = RuntimeStore::new(data_dir.clone()).unwrap();
    store.write(&record(&runtime)).unwrap();
    let observer = admin(&runtime, &data_dir).await;
    let admin_credential = read_credential(&data_dir.join("credentials/admin.json")).unwrap();
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let info: OnboardingInfo = http
        .get(format!("http://{}/onboarding/info", runtime.address))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut request = options();
    request.name = None;
    let issued = issue_prompt(&data_dir, request).await.unwrap();
    let ticket = prompt_ticket(&issued.text);
    ticket.validate().unwrap();
    assert_eq!(ticket.server_id, info.server_id);
    assert_ne!(ticket.server_id, runtime.instance_id);
    assert_eq!(ticket.invite_id, issued.invite_id);
    assert_eq!(ticket.expires_at, issued.expires_at);
    assert_eq!(ticket.provider, Some(OnboardingProvider::Omp));
    assert_eq!(ticket.workspace, options().workspace);
    assert_eq!(issued.workspace, ticket.workspace);
    assert_eq!(issued.provider, ticket.provider);
    assert_eq!(
        ticket.profile_name,
        format!("asr-{:08x}", info.server_id.as_fields().0)
    );
    assert_ne!(ticket.profile_name, "local");
    assert_eq!(Some(ticket.manifest_sha256.clone()), info.manifest_sha256);
    assert_eq!(ticket.artifacts, manifest.artifacts);
    assert_eq!(ticket.routes.len(), 1);
    assert_eq!(ticket.routes[0].kind, RouteKind::Local);
    assert_eq!(ticket.routes[0].router_url, record(&runtime).control_url);
    assert!(issued.text.contains(ticket.invite_token.expose()));
    assert!(!issued.text.contains(admin_credential.token.expose()));
    assert!(!issued.text.contains(&admin_credential.id.to_string()));
    assert!(
        !issued
            .text
            .contains(&serde_json::to_string(&admin_credential).unwrap())
    );
    assert_eq!(workspaces(&observer).await, vec![ticket.workspace.clone()]);

    assert_prompt_shell_syntax(&issued.text);

    // An existing workspace is reusable, without a duplicate workspace mutation.
    let mut existing = options();
    existing.create_workspace = false;
    let second = issue_prompt(&data_dir, existing).await.unwrap();
    assert_ne!(second.invite_id, ticket.invite_id);
    assert_eq!(prompt_ticket(&second.text).profile_name, "office");
    assert_eq!(workspaces(&observer).await, vec![ticket.workspace.clone()]);

    assert_provider_bound_redemption(&http, &runtime, &ticket).await;
    observer.close().await.unwrap();
    stop(runtime).await;
    assert_eq!(invitation_counts(&data_dir), (2, 1));

    // The ticket pins durable server identity, never the launcher instance UUID.
    let restarted = RouterRuntime::start(config(&data_dir, &assets))
        .await
        .unwrap();
    let info: OnboardingInfo = http
        .get(format!("http://{}/onboarding/info", restarted.address))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(info.server_id, ticket.server_id);
    stop(restarted).await;
}

#[test]
fn onboarding_issue_fixture_child() {
    let Ok(mode) = std::env::var("ASR_ISSUE_FIXTURE") else {
        return;
    };
    let home = PathBuf::from(std::env::var_os("HOME").unwrap());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            if mode == "success" || mode == "relative-assets" {
                succeeds(&home, mode == "relative-assets").await;
            } else {
                rejected(&mode, &home).await;
            }
        });
}

fn run_case(mode: &str) {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().canonicalize().unwrap();
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "onboarding_issue_fixture_child", "--nocapture"])
        .env("ASR_ISSUE_FIXTURE", mode)
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("ASR_CONFIG_PATH", home.join(".config/asr.json"))
        .env("ASR_DATA_DIR", home.join("data"))
        .env_remove("ASR_CA_FILE")
        .env_remove("ASR_BOOTSTRAP_DIR")
        .env("ROUTER_URL", "ws://127.0.0.1:1/ws")
        .current_dir(&home);
    if mode == "relative-assets" {
        command.env("ASR_BOOTSTRAP_DIR", "bundle");
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "case {mode}:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn verified_owned_issuance_creates_bound_redeemable_tickets_without_admin_secrets() {
    run_case("success");
    run_case("relative-assets");
}

#[test]
fn unavailable_or_changed_assets_fail_before_workspace_and_invitation_mutations() {
    for mode in [
        "missing-assets",
        "mutated-binary",
        "mutated-archive",
        "changed-manifest",
    ] {
        run_case(mode);
    }
}

#[test]
fn invalid_prompt_inputs_and_foreign_routes_fail_before_issuance() {
    for mode in [
        "reserved-name",
        "invalid-name",
        "invalid-route",
        "private-key-ca",
        "foreign-route",
        "missing-workspace",
    ] {
        run_case(mode);
    }
}

#[test]
fn runtime_ownership_and_instance_must_be_verified_before_issuance() {
    for mode in [
        "absent-runtime",
        "runtime-instance-mismatch",
        "unowned-directory",
        "unowned-record",
        "symlink-record",
    ] {
        run_case(mode);
    }
}

#[test]
fn admin_file_must_be_owned_and_match_server_authority() {
    for mode in [
        "unowned-admin",
        "foreign-admin",
        "non-admin",
        "forged-admin-claims",
    ] {
        run_case(mode);
    }
}

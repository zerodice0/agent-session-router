use std::{
    fs,
    io::{BufRead, BufReader},
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use agent_session_router::{
    bootstrap::routes::{VerifiedRoute, probe_routes},
    client::{ClientConfig, ClientRole, RouterClient},
    config::{ConfigFile, Profile, load_config, save_config},
    credentials::{
        CredentialFile, CredentialRole, ensure_private_directory, read_credential,
        write_credential_atomic_no_replace, write_credential_exclusive,
    },
    onboarding::{
        BootstrapArtifact, EnrollmentRequest, EnrollmentResponse, OnboardingProvider,
        OnboardingRoute, OnboardingTicket, RouteKind, VERSION, invite_subject,
        journal::{
            ActionStatus, ConfigurationLock, InstallAction, Journal, JournalError, JournalState,
            Stage,
        },
    },
    protocol::{ClientMessage, ServerMessage, WorkspaceName},
    router::{RouterConfig, RouterExposure, RouterRuntime},
    store::{RouterStore, now_millis},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use uuid::Uuid;

const PROVIDER: OnboardingProvider = OnboardingProvider::Omp;

struct Fixture {
    _temporary: TempDir,
    root: PathBuf,
    config: PathBuf,
    executable: PathBuf,
    data: PathBuf,
    ticket: OnboardingTicket,
    runtime: RouterRuntime,
}

impl Fixture {
    async fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let data = root.join("server");
        let mut store = RouterStore::open(&data).unwrap();
        let invite = store
            .issue_onboarding_invite(
                &WorkspaceName::parse("journal-room").unwrap(),
                true,
                Some(PROVIDER),
                now_millis().unwrap(),
            )
            .unwrap();
        store.close().unwrap();
        let runtime = RouterRuntime::start(RouterConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            data_dir: data.clone(),
            instance_id: Uuid::new_v4(),
            tls_cert_file: None,
            tls_key_file: None,
            public_url: None,
            exposure: RouterExposure::Direct,
            onboarding_assets_dir: None,
        })
        .await
        .unwrap();
        let executable = root.join("asr");
        fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let target = "aarch64-apple-darwin";
        let ticket = OnboardingTicket {
            version: VERSION,
            server_id: invite.server_id,
            invite_id: invite.invite_id,
            invite_token: invite.invite_token,
            expires_at: invite.expires_at,
            profile_name: "office".to_owned(),
            workspace: invite.workspace,
            provider: invite.provider,
            routes: vec![OnboardingRoute {
                kind: RouteKind::Local,
                router_url: format!("ws://{}/ws", runtime.address),
                ca_pem: None,
            }],
            manifest_sha256: "1".repeat(64),
            artifacts: vec![BootstrapArtifact {
                target: target.to_owned(),
                binary_file: format!("asr-{target}"),
                binary_sha256: "2".repeat(64),
                archive_file: format!("agent-session-router-{target}.tar.gz"),
                archive_sha256: "3".repeat(64),
                binary_bytes: 1,
                archive_bytes: 1,
            }],
        };
        Self {
            _temporary: temporary,
            config: root.join("client/config.json"),
            root,
            executable,
            data,
            ticket,
            runtime,
        }
    }

    fn state_path(&self) -> PathBuf {
        self.config
            .parent()
            .unwrap()
            .join("onboarding")
            .join(self.ticket.invite_id.to_string())
            .join(PROVIDER.as_str())
            .join("state.json")
    }

    fn prepare(&self) -> Journal {
        Journal::prepare(&self.config, &self.ticket, PROVIDER, &self.executable).unwrap()
    }

    fn load(&self) -> Journal {
        Journal::load(&self.config, self.ticket.invite_id, PROVIDER).unwrap()
    }

    async fn route(&self) -> VerifiedRoute {
        probe_routes(
            &self.ticket.routes,
            self.ticket.server_id,
            None,
            &self.root.join("probe-ca"),
        )
        .await
        .unwrap()
    }

    async fn stop(self) {
        self.runtime.shutdown().await.unwrap();
        self.runtime.wait().await.unwrap();
        let store = RouterStore::open(&self.data).unwrap();
        assert_eq!(
            store.credential_count().unwrap(),
            2,
            "one admin and one enrolled identity"
        );
    }
}

#[derive(Clone, Copy)]
enum Fault {
    LostResponse,
    ClaimsMismatch,
    PublishConflict,
    OversizedResponse,
}

struct Proxy {
    task: JoinHandle<()>,
    posts: Arc<AtomicUsize>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// Forward to the real router, changing only the first exchange response. Every
// POST checks that the credential and enrollment UUID were already on disk.
async fn proxy(fixture: &mut Fixture, fault: Fault) -> Proxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", fixture.runtime.address);
    fixture.ticket.routes[0].router_url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let state_path = fixture.state_path();
    let config_path = fixture.config.clone();
    let posts = Arc::new(AtomicUsize::new(0));
    let counter = posts.clone();
    let task = tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (header, body) = request(&mut socket).await;
            let method = header.split_whitespace().next().unwrap();
            let path = header.split_whitespace().nth(1).unwrap();
            assert!(matches!(path, "/onboarding/info" | "/onboarding/enroll"));
            assert!(!header.to_ascii_lowercase().contains("\r\norigin:"));
            let index = if method == "POST" {
                let enrollment: EnrollmentRequest = serde_json::from_slice(&body).unwrap();
                let state: JournalState =
                    serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
                let credential = read_credential(&state.credential_file).unwrap();
                assert_eq!(enrollment.enrollment_id, state.enrollment_id);
                assert_eq!(enrollment.credential_id, credential.id);
                assert!(enrollment.credential_token == credential.token);
                assert!(!header.contains(enrollment.invite_token.expose()));
                assert!(!header.contains(enrollment.credential_token.expose()));
                counter.fetch_add(1, Ordering::SeqCst)
            } else {
                usize::MAX
            };
            let response = client
                .request(method.parse().unwrap(), format!("{upstream}{path}"))
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let mut bytes = response.bytes().await.unwrap().to_vec();
            if index == 0 {
                assert_eq!(status, reqwest::StatusCode::OK);
                match fault {
                    Fault::LostResponse => continue,
                    Fault::ClaimsMismatch => {
                        let mut envelope: EnrollmentResponse =
                            serde_json::from_slice(&bytes).unwrap();
                        envelope.claims.workspaces.clear();
                        bytes = serde_json::to_vec(&envelope).unwrap();
                    }
                    Fault::PublishConflict => {
                        let mut config = ConfigFile {
                            version: 2,
                            ..ConfigFile::default()
                        };
                        config.profiles.insert(
                            "office".to_owned(),
                            Profile::manual("ws://127.0.0.1:1/ws".to_owned()),
                        );
                        save_config(&config_path, &config).unwrap();
                    }
                    Fault::OversizedResponse => bytes = vec![b' '; 4097],
                }
            }
            let response_header = format!(
                "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                status.as_u16(),
                status.canonical_reason().unwrap_or("response"),
                bytes.len()
            );
            socket.write_all(response_header.as_bytes()).await.unwrap();
            let _ = socket.write_all(&bytes).await;
            let _ = socket.shutdown().await;
        }
    });
    Proxy { task, posts }
}

async fn request(socket: &mut TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break end + 4;
        }
        assert!(bytes.len() < 16384);
    };
    let header = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let length = header
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + length {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    (header, bytes[header_end..header_end + length].to_vec())
}

#[tokio::test]
async fn exchange_replay_and_configured_state_preserve_one_identity_and_strip_the_invite() {
    let fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let credential = journal.credential().unwrap();
    let enrollment_id = journal.state().enrollment_id;
    let route = fixture.route().await;
    assert_eq!(
        journal.enroll(&route).await.unwrap(),
        credential.public_claims()
    );
    assert_eq!(journal.state().stage, Stage::Enrolled);
    let saved = load_config(&fixture.config).unwrap();
    let profile = &saved.profiles["office"];
    assert_eq!(profile.server_id, Some(fixture.ticket.server_id));
    assert_eq!(
        profile.bindings[&PROVIDER].credential_file,
        journal.state().credential_file
    );
    assert_eq!(
        profile.bindings[&PROVIDER].workspace,
        fixture.ticket.workspace
    );
    let config_bytes = fs::read(&fixture.config).unwrap();
    for token in [&fixture.ticket.invite_token, &credential.token] {
        assert!(
            !config_bytes
                .windows(token.expose().len())
                .any(|bytes| bytes == token.expose().as_bytes())
        );
    }
    drop(journal);
    let mut resumed = fixture.load();
    assert_eq!(resumed.state().enrollment_id, enrollment_id);
    assert_eq!(
        resumed.enroll(&route).await.unwrap(),
        credential.public_claims()
    );
    let action = InstallAction::File {
        path: fixture.root.join("skill"),
        sha256: "4".repeat(64),
    };
    let index = resumed.plan(action.clone()).unwrap();
    assert_eq!(resumed.plan(action).unwrap(), index);
    assert_eq!(
        fixture.load().state().actions[index].status,
        ActionStatus::Planned
    );
    assert_eq!(resumed.mark_configured(), Err(JournalError::Conflict));
    resumed.mark_applied(index).unwrap();
    resumed.mark_configured().unwrap();
    let configured = fixture.load();
    assert_eq!(configured.state().stage, Stage::Configured);
    assert!(configured.state().ticket.is_none());
    let state_bytes = fs::read(fixture.state_path()).unwrap();
    assert!(
        !state_bytes
            .windows(fixture.ticket.invite_token.expose().len())
            .any(|bytes| bytes == fixture.ticket.invite_token.expose().as_bytes())
    );
    assert_eq!(configured.credential().unwrap().id, credential.id);
    assert_eq!(fixture.prepare().state().stage, Stage::Configured);
    fixture.stop().await;
}

#[tokio::test]
async fn lost_http_response_resumes_the_durable_pending_credential_without_consuming_twice() {
    let mut fixture = Fixture::new().await;
    let proxy = proxy(&mut fixture, Fault::LostResponse).await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let credential = journal.credential().unwrap();
    let enrollment_id = journal.state().enrollment_id;
    let route = fixture.route().await;
    assert_eq!(journal.enroll(&route).await, Err(JournalError::Unavailable));
    assert_eq!(fixture.load().state().stage, Stage::Prepared);
    assert!(!fixture.config.exists());
    drop(journal);
    let mut resumed = fixture.load();
    assert_eq!(resumed.state().enrollment_id, enrollment_id);
    assert!(resumed.credential().unwrap() == credential);
    assert_eq!(
        resumed.enroll(&route).await.unwrap(),
        credential.public_claims()
    );
    assert_eq!(proxy.posts.load(Ordering::SeqCst), 2);
    fixture.stop().await;
}

#[tokio::test]
async fn claims_mismatch_never_publishes_profile_and_a_valid_replay_recovers() {
    let mut fixture = Fixture::new().await;
    let proxy = proxy(&mut fixture, Fault::ClaimsMismatch).await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let route = fixture.route().await;
    assert_eq!(
        journal.enroll(&route).await,
        Err(JournalError::ClaimsMismatch)
    );
    assert!(!fixture.config.exists());
    let mut resumed = fixture.load();
    assert_eq!(resumed.state().stage, Stage::Prepared);
    assert_eq!(
        resumed.enroll(&route).await.unwrap(),
        resumed.credential().unwrap().public_claims()
    );
    assert_eq!(proxy.posts.load(Ordering::SeqCst), 2);
    fixture.stop().await;
}

#[tokio::test]
async fn successful_exchange_is_persisted_before_profile_publication_can_fail() {
    let mut fixture = Fixture::new().await;
    let _proxy = proxy(&mut fixture, Fault::PublishConflict).await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let route = fixture.route().await;
    let enrollment_id = journal.state().enrollment_id;
    assert_eq!(
        journal.enroll(&route).await,
        Err(JournalError::ProfileConflict)
    );
    let mut resumed = fixture.load();
    assert_eq!(resumed.state().stage, Stage::Enrolled);
    assert_eq!(resumed.state().enrollment_id, enrollment_id);
    assert!(
        load_config(&fixture.config).unwrap().profiles["office"]
            .bindings
            .is_empty()
    );
    fs::remove_file(&fixture.config).unwrap();
    resumed.enroll(&route).await.unwrap();
    assert_eq!(
        load_config(&fixture.config).unwrap().profiles["office"].bindings[&PROVIDER]
            .credential_file,
        resumed.state().credential_file
    );
    fixture.stop().await;
}

#[tokio::test]
async fn enrollment_rejects_large_responses_and_checks_route_and_profile_before_posting() {
    let mut fixture = Fixture::new().await;
    let proxy = proxy(&mut fixture, Fault::OversizedResponse).await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let mut route = fixture.route().await;
    route.info.server_id = Uuid::new_v4();
    assert_eq!(
        journal.enroll(&route).await,
        Err(JournalError::IdentityMismatch)
    );
    route.info.server_id = fixture.ticket.server_id;
    let original = route.route.router_url.clone();
    route.route.router_url = "ws://127.0.0.1:1/ws".to_owned();
    assert_eq!(
        journal.enroll(&route).await,
        Err(JournalError::InvalidRoute)
    );
    route.route.router_url = original;
    let mut config = ConfigFile {
        version: 2,
        ..ConfigFile::default()
    };
    config.profiles.insert(
        "office".to_owned(),
        Profile::manual("ws://127.0.0.1:1/ws".to_owned()),
    );
    save_config(&fixture.config, &config).unwrap();
    assert_eq!(
        journal.enroll(&route).await,
        Err(JournalError::ProfileConflict)
    );
    assert_eq!(proxy.posts.load(Ordering::SeqCst), 0);
    fs::remove_file(&fixture.config).unwrap();
    assert_eq!(
        journal.enroll(&route).await,
        Err(JournalError::InvalidResponse)
    );
    assert!(!fixture.config.exists());
    assert_eq!(fixture.load().state().stage, Stage::Prepared);
    journal.enroll(&route).await.unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn enrolled_resume_does_not_bypass_server_revocation() {
    let fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let route = fixture.route().await;
    journal.enroll(&route).await.unwrap();
    let (admin, _events) = RouterClient::connect(ClientConfig {
        router_url: fixture.ticket.routes[0].router_url.parse().unwrap(),
        role: ClientRole::Operator {
            credential: read_credential(&fixture.data.join("credentials/admin.json")).unwrap(),
        },
        ca_file: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        admin
            .call(ClientMessage::OnboardingInviteRevoke {
                request_id: Uuid::new_v4().to_string(),
                invite_id: fixture.ticket.invite_id,
            })
            .await
            .unwrap(),
        ServerMessage::OnboardingInviteRevoked { .. }
    ));
    let mut resumed = fixture.load();
    assert_eq!(
        resumed.enroll(&route).await,
        Err(JournalError::InviteUnavailable)
    );
    assert_eq!(resumed.state().stage, Stage::Enrolled);
    admin.close().await.unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn orphan_credential_is_adopted_only_for_the_exact_invited_claims() {
    let fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let directory = fixture.state_path().parent().unwrap().to_owned();
    ensure_private_directory(&directory, true).unwrap();
    let (side, client) = PROVIDER.identity();
    let orphan = CredentialFile::generate(
        CredentialRole::Agent,
        invite_subject(fixture.ticket.invite_id),
        Some(side),
        Some(client),
        vec![fixture.ticket.workspace.clone()],
    )
    .unwrap();
    write_credential_atomic_no_replace(&directory, "credential.json", &orphan).unwrap();
    let mut journal = fixture.prepare();
    assert!(journal.credential().unwrap() == orphan);
    assert_eq!(
        journal.enroll(&fixture.route().await).await.unwrap(),
        orphan.public_claims()
    );
    // A missing state cannot justify a fresh credential, even after enrollment.
    fs::remove_file(fixture.state_path()).unwrap();
    let mut recreated = fixture.prepare();
    assert!(recreated.credential().unwrap() == orphan);
    assert_eq!(
        recreated.enroll(&fixture.route().await).await,
        Err(JournalError::InviteUnavailable)
    );
    fixture.stop().await;
}

#[tokio::test]
async fn mismatched_orphan_and_unknown_journal_fields_are_not_overwritten() {
    let fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let directory = fixture.state_path().parent().unwrap().to_owned();
    ensure_private_directory(&directory, true).unwrap();
    let (side, client) = PROVIDER.identity();
    let wrong = CredentialFile::generate(
        CredentialRole::Agent,
        "wrong-agent".to_owned(),
        Some(side),
        Some(client),
        vec![fixture.ticket.workspace.clone()],
    )
    .unwrap();
    let path = write_credential_atomic_no_replace(&directory, "credential.json", &wrong).unwrap();
    let temporary_link = directory.join(format!(".credential-{}.tmp", Uuid::new_v4()));
    fs::hard_link(&path, &temporary_link).unwrap();
    let before = fs::read(&path).unwrap();
    assert_eq!(
        Journal::prepare(
            &fixture.config,
            &fixture.ticket,
            PROVIDER,
            &fixture.executable
        )
        .unwrap_err(),
        JournalError::ClaimsMismatch
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(fs::metadata(&path).unwrap().nlink(), 2);
    assert!(
        temporary_link.exists(),
        "mismatched claims must not authorize link recovery"
    );
    fs::remove_file(temporary_link).unwrap();
    fs::remove_file(path).unwrap();
    let journal = fixture.prepare();
    let mut value = serde_json::to_value(journal.state()).unwrap();
    value["unexpectedAuthority"] = serde_json::json!("admin");
    fs::write(fixture.state_path(), serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(
        Journal::load(&fixture.config, fixture.ticket.invite_id, PROVIDER).unwrap_err(),
        JournalError::Invalid
    );
    assert_eq!(
        Journal::prepare(
            &fixture.config,
            &fixture.ticket,
            PROVIDER,
            &fixture.executable
        )
        .unwrap_err(),
        JournalError::Invalid
    );
    fixture.runtime.shutdown().await.unwrap();
    fixture.runtime.wait().await.unwrap();
}

#[tokio::test]
async fn journal_modes_symlinks_and_stale_writers_fail_closed_without_secret_debug() {
    let fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let credential = journal.credential().unwrap();
    for path in [
        fixture.config.parent().unwrap().to_owned(),
        fixture.state_path().parent().unwrap().to_owned(),
    ] {
        assert_eq!(fs::metadata(path).unwrap().mode() & 0o777, 0o700);
    }
    for path in [
        fixture.state_path(),
        journal.state().credential_file.clone(),
    ] {
        assert_eq!(fs::metadata(path).unwrap().mode() & 0o777, 0o600);
    }
    let command = InstallAction::Command {
        argv: vec![
            "provider".to_owned(),
            "register".to_owned(),
            "--profile".to_owned(),
            "office".to_owned(),
        ],
    };
    let mut stale = fixture.load();
    let index = journal.plan(command.clone()).unwrap();
    assert_eq!(journal.plan(command).unwrap(), index);
    assert_eq!(
        stale.plan(InstallAction::File {
            path: fixture.root.join("skill"),
            sha256: "5".repeat(64)
        }),
        Err(JournalError::Conflict)
    );
    let skill = fixture.root.join("skill");
    journal
        .plan(InstallAction::File {
            path: skill.clone(),
            sha256: "5".repeat(64),
        })
        .unwrap();
    assert_eq!(
        journal.plan(InstallAction::File {
            path: skill,
            sha256: "6".repeat(64)
        }),
        Err(JournalError::Conflict)
    );
    for secret in [
        fixture.ticket.invite_token.expose(),
        credential.token.expose(),
    ] {
        assert_eq!(
            journal.plan(InstallAction::Command {
                argv: vec!["provider".to_owned(), secret.to_owned()]
            }),
            Err(JournalError::Invalid)
        );
        assert!(!format!("{journal:?}").contains(secret));
        assert!(!format!("{:?}", journal.state()).contains(secret));
    }
    let journal_path = fixture.state_path();
    fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        Journal::load(&fixture.config, fixture.ticket.invite_id, PROVIDER).unwrap_err(),
        JournalError::Permissions
    );
    fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o600)).unwrap();
    let saved = journal_path.with_file_name("saved.json");
    fs::rename(&journal_path, &saved).unwrap();
    symlink(&saved, &journal_path).unwrap();
    assert_eq!(
        Journal::load(&fixture.config, fixture.ticket.invite_id, PROVIDER).unwrap_err(),
        JournalError::Permissions
    );
    fs::remove_file(&journal_path).unwrap();
    fs::rename(&saved, &journal_path).unwrap();
    let credential_path = journal.state().credential_file.clone();
    let saved_credential = credential_path.with_file_name("saved-credential.json");
    fs::rename(&credential_path, &saved_credential).unwrap();
    symlink(&saved_credential, &credential_path).unwrap();
    assert_eq!(
        Journal::load(&fixture.config, fixture.ticket.invite_id, PROVIDER).unwrap_err(),
        JournalError::Permissions
    );
    let directory = journal_path.parent().unwrap();
    let saved_directory = directory.with_file_name("saved-provider");
    fs::rename(directory, &saved_directory).unwrap();
    symlink(&saved_directory, directory).unwrap();
    assert_eq!(
        Journal::load(&fixture.config, fixture.ticket.invite_id, PROVIDER).unwrap_err(),
        JournalError::Permissions
    );
    fixture.runtime.shutdown().await.unwrap();
    fixture.runtime.wait().await.unwrap();
}

#[tokio::test]
async fn exact_enrollment_replay_survives_server_restart_and_expired_invitation() {
    let mut fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    // The client never uses the ticket timestamp as the authority for replay.
    fixture.ticket.expires_at = 1;
    let mut journal = fixture.prepare();
    let claims = journal.enroll(&fixture.route().await).await.unwrap();
    let enrollment_id = journal.state().enrollment_id;
    let address = fixture.runtime.address;
    fixture.runtime.shutdown().await.unwrap();
    fixture.runtime.wait().await.unwrap();
    let database = rusqlite::Connection::open(fixture.data.join("router.sqlite")).unwrap();
    database.execute(
        "UPDATE onboarding_invites SET created_at=created_at-1200000, expires_at=expires_at-1200000, redeemed_at=redeemed_at-1200000 WHERE id=?1",
        [fixture.ticket.invite_id.to_string()],
    ).unwrap();
    database.close().unwrap();
    fixture.runtime = RouterRuntime::start(RouterConfig {
        bind: address,
        data_dir: fixture.data.clone(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    })
    .await
    .unwrap();
    let mut resumed = fixture.load();
    assert_eq!(resumed.state().enrollment_id, enrollment_id);
    assert_eq!(
        resumed.enroll(&fixture.route().await).await.unwrap(),
        claims
    );
    fixture.stop().await;
}

#[tokio::test]
async fn failed_persistence_never_exposes_an_intent_or_transition_on_same_process_retry() {
    let fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let mut journal = fixture.prepare();
    let before = fs::read(fixture.state_path()).unwrap();
    let oversized = InstallAction::Command {
        argv: vec!["provider".to_owned(), "x".repeat(1024 * 1024)],
    };
    for _ in 0..2 {
        assert_eq!(journal.plan(oversized.clone()), Err(JournalError::Invalid));
        assert!(journal.state().actions.is_empty());
        assert_eq!(fs::read(fixture.state_path()).unwrap(), before);
    }
    let action = InstallAction::Command {
        argv: vec!["provider".to_owned(), "register".to_owned()],
    };
    let index = journal.plan(action.clone()).unwrap();
    assert_eq!(fixture.load().state().actions[index].intent, action);
    journal.enroll(&fixture.route().await).await.unwrap();

    // Permission validation is deterministic even if this test is run as root.
    // A failed Applied or Configured write must keep the last committed state.
    let state_path = fixture.state_path();
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o400)).unwrap();
    assert_eq!(journal.mark_applied(index), Err(JournalError::Permissions));
    assert_eq!(journal.state().actions[index].status, ActionStatus::Planned);
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        fixture.load().state().actions[index].status,
        ActionStatus::Planned
    );
    journal.mark_applied(index).unwrap();
    assert_eq!(
        fixture.load().state().actions[index].status,
        ActionStatus::Applied
    );
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o400)).unwrap();
    assert_eq!(journal.mark_configured(), Err(JournalError::Permissions));
    assert_eq!(journal.state().stage, Stage::Enrolled);
    assert!(journal.state().ticket.is_some());
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(fixture.load().state().stage, Stage::Enrolled);
    journal.mark_configured().unwrap();
    assert_eq!(fixture.load().state().stage, Stage::Configured);
    assert!(fixture.load().state().ticket.is_none());
    fixture.stop().await;
}

#[tokio::test]
async fn crash_between_credential_link_and_unlink_recovers_only_the_valid_writer_link() {
    let fixture = Fixture::new().await;
    let _lock = ConfigurationLock::acquire(&fixture.config).unwrap();
    let directory = fixture.state_path().parent().unwrap().to_owned();
    ensure_private_directory(&directory, true).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "credential_publication_child", "--nocapture"])
        .env("ASR_JOURNAL_PUBLICATION_DIRECTORY", &directory)
        .env(
            "ASR_JOURNAL_PUBLICATION_INVITE",
            fixture.ticket.invite_id.to_string(),
        )
        .env(
            "ASR_JOURNAL_PUBLICATION_WORKSPACE",
            fixture.ticket.workspace.as_str(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "publication child exited early"
        );
        if line.contains("credential-link-published") {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let path = directory.join("credential.json");
    assert_eq!(fs::metadata(&path).unwrap().nlink(), 2);
    let credential = read_credential(&path).unwrap();
    let mut journal = fixture.prepare();
    assert!(journal.credential().unwrap() == credential);
    assert_eq!(fs::metadata(&path).unwrap().nlink(), 1);
    assert_eq!(fs::metadata(fixture.state_path()).unwrap().nlink(), 1);
    assert_eq!(
        journal.enroll(&fixture.route().await).await.unwrap(),
        credential.public_claims()
    );

    // An external hard link is not the credential writer's private temporary
    // publication name and must never be unlinked or silently accepted.
    let external = fixture.root.join("unrelated-credential-copy");
    fs::hard_link(&path, &external).unwrap();
    assert_eq!(
        Journal::load(&fixture.config, fixture.ticket.invite_id, PROVIDER).unwrap_err(),
        JournalError::Permissions
    );
    assert!(external.exists());
    assert_eq!(fs::metadata(&path).unwrap().nlink(), 2);
    fs::remove_file(external).unwrap();
    assert!(fixture.load().credential().unwrap() == credential);
    fixture.stop().await;
}

#[test]
fn credential_publication_child() {
    let Some(directory) = std::env::var_os("ASR_JOURNAL_PUBLICATION_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let invite_id =
        Uuid::parse_str(&std::env::var("ASR_JOURNAL_PUBLICATION_INVITE").unwrap()).unwrap();
    let workspace =
        WorkspaceName::parse(std::env::var("ASR_JOURNAL_PUBLICATION_WORKSPACE").unwrap()).unwrap();
    let (side, client) = PROVIDER.identity();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        invite_subject(invite_id),
        Some(side),
        Some(client),
        vec![workspace],
    )
    .unwrap();
    // Execute the existing writer's exact syscall prefix, then let the parent
    // kill this process in its real link-before-unlink crash window.
    let temporary = directory.join(format!(".credential-{}.tmp", Uuid::new_v4()));
    write_credential_exclusive(&temporary, &credential).unwrap();
    fs::hard_link(&temporary, directory.join("credential.json")).unwrap();
    fs::File::open(&directory).unwrap().sync_all().unwrap();
    println!("credential-link-published");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
    std::thread::sleep(Duration::from_secs(30));
}

#[test]
fn configuration_lock_is_nonblocking_private_and_released_by_process_death() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let config = root.join("client/config.json");
    let lock = ConfigurationLock::acquire(&config).unwrap();
    assert!(matches!(
        ConfigurationLock::acquire(&config),
        Err(JournalError::ConfigurationBusy)
    ));
    drop(lock);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lock_child", "--nocapture"])
        .env("ASR_JOURNAL_LOCK_CHILD", &config)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "lock child exited early"
        );
        if line.contains("journal-lock-ready") {
            break;
        }
    }
    assert!(matches!(
        ConfigurationLock::acquire(&config),
        Err(JournalError::ConfigurationBusy)
    ));
    child.kill().unwrap();
    child.wait().unwrap();
    let lock = ConfigurationLock::acquire(&config).unwrap();
    let path = config.parent().unwrap().join("onboarding.lock");
    assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
    drop(lock);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        ConfigurationLock::acquire(&config),
        Err(JournalError::Permissions)
    ));
    fs::remove_file(&path).unwrap();
    let sentinel = root.join("sentinel");
    fs::write(&sentinel, b"untouched").unwrap();
    symlink(&sentinel, &path).unwrap();
    assert!(matches!(
        ConfigurationLock::acquire(&config),
        Err(JournalError::Permissions)
    ));
    assert_eq!(fs::read(&sentinel).unwrap(), b"untouched");
}

#[test]
fn lock_child() {
    let Some(config) = std::env::var_os("ASR_JOURNAL_LOCK_CHILD") else {
        return;
    };
    let _lock = ConfigurationLock::acquire(Path::new(&config)).unwrap();
    println!("journal-lock-ready");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
    std::thread::sleep(Duration::from_secs(30));
}

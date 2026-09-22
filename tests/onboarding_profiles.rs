use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::Path,
    process::Command,
};

use agent_session_router::{
    config::{self, ConfigError, ConfigFile, Profile},
    credentials::SecretToken,
    onboarding::{
        BootstrapArtifact, OnboardingProvider, OnboardingRoute, OnboardingTicket, RouteKind,
    },
    protocol::WorkspaceName,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use uuid::Uuid;

fn ticket() -> OnboardingTicket {
    OnboardingTicket {
        version: 1,
        server_id: Uuid::new_v4(),
        invite_id: Uuid::new_v4(),
        invite_token: SecretToken::parse("A".repeat(43)).unwrap(),
        expires_at: 600_000,
        profile_name: "office".into(),
        workspace: WorkspaceName::parse("team-room").unwrap(),
        provider: None,
        routes: vec![OnboardingRoute {
            kind: RouteKind::Local,
            router_url: "ws://127.0.0.1:8877/ws".into(),
            ca_pem: None,
        }],
        manifest_sha256: "a".repeat(64),
        artifacts: vec![BootstrapArtifact {
            target: "aarch64-apple-darwin".into(),
            binary_file: "asr-aarch64-apple-darwin".into(),
            binary_sha256: "b".repeat(64),
            binary_bytes: 1,
            archive_file: "agent-session-router-aarch64-apple-darwin.tar.gz".into(),
            archive_sha256: "c".repeat(64),
            archive_bytes: 1,
        }],
    }
}

fn publish(
    path: &Path,
    ticket: &OnboardingTicket,
    provider: OnboardingProvider,
    credential: &Path,
) -> Result<(), ConfigError> {
    let routes =
        config::store_onboarding_routes(&ticket.routes, &path.parent().unwrap().join("cas"))?;
    config::publish_onboarding_binding(
        path,
        ticket,
        provider,
        credential,
        &ticket.routes[0].router_url,
        routes,
    )
}

fn fixture(mode: &str) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "profile_fixture_child", "--nocapture"])
        .env("ASR_PROFILE_FIXTURE", mode)
        .env("ASR_CONFIG_PATH", root.join("config.json"))
        .env("HOME", &root)
        .env("XDG_CONFIG_HOME", root.join(".config"))
        .env_remove("ROUTER_URL")
        .env_remove("ASR_CREDENTIAL_FILE")
        .env_remove("ASR_CA_FILE")
        .current_dir(&root);
    match mode {
        "environment-url" | "explicit-profile" => {
            command.env("ROUTER_URL", "wss://override.example/ws");
        }
        "environment-credential" | "explicit-credential" => {
            command.env("ASR_CREDENTIAL_FILE", root.join("environment.json"));
        }
        "explicit-ca" => {
            command.env("ASR_CA_FILE", "~/override.pem");
        }
        _ => {}
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{mode}: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn version_one_migration_preserves_operator_selection_and_local_default() {
    fixture("migration");
}

#[test]
fn provider_selection_respects_address_credentials_and_ca_precedence() {
    for mode in [
        "default-binding",
        "environment-url",
        "explicit-profile",
        "environment-credential",
        "explicit-credential",
        "explicit-ca",
    ] {
        fixture(mode);
    }
}

#[test]
fn publication_is_idempotent_and_rejects_identity_replacement() {
    fixture("publication");
}

#[test]
fn stored_routes_preserve_public_ca_files_without_pem_in_config() {
    fixture("ca-storage");
}

#[test]
fn invalid_binding_routes_and_unsafe_paths_are_rejected() {
    fixture("invalid-config");
}

#[test]
fn atomic_config_publication_rejects_symlink_and_hardlink_targets() {
    fixture("unsafe-publication");
}

#[test]
fn profile_fixture_child() {
    let Ok(mode) = std::env::var("ASR_PROFILE_FIXTURE") else {
        return;
    };
    let path = config::config_path().unwrap();
    match mode.as_str() {
        "migration" => migration_fixture(&path),
        "publication" => publication_fixture(&path),
        "ca-storage" => ca_storage_fixture(&path),
        "invalid-config" => invalid_config_fixture(&path),
        "unsafe-publication" => unsafe_publication_fixture(&path),
        _ => selection_fixture(&mode, &path),
    }
}

fn migration_fixture(path: &Path) {
    let selected = config::select(None, None).unwrap();
    assert_eq!(selected.profile.as_deref(), Some("local"));
    assert_eq!(selected.router_url.as_str(), config::DEFAULT_ROUTER_URL);
    fs::write(path, r#"{"version":1,"defaultProfile":"legacy","profiles":{"legacy":{"routerUrl":"wss://legacy.example/ws"}}}"#).unwrap();
    let loaded = config::load_config(path).unwrap();
    let before = config::select(None, None).unwrap();
    assert_eq!(before.router_url.as_str(), "wss://legacy.example/ws");
    assert_eq!(before.profile.as_deref(), Some("legacy"));
    let provider = config::select_provider(None, None, OnboardingProvider::CodexCli).unwrap();
    assert!(provider.selection.credential_file.is_none());
    assert!(provider.routes.is_empty());
    config::save_config(path, &loaded).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(saved["version"], 2);
    assert_eq!(saved["defaultProfile"], "legacy");
    assert_eq!(
        config::select(None, None).unwrap().router_url,
        before.router_url
    );
    let local = config::select(Some("local"), None).unwrap();
    assert_eq!(local.router_url.as_str(), config::DEFAULT_ROUTER_URL);
    let mut invalid = loaded;
    invalid.profiles.insert(
        "local".into(),
        Profile::manual("wss://other.example/ws".into()),
    );
    let previous = fs::read(path).unwrap();
    assert!(config::save_config(path, &invalid).is_err());
    assert_eq!(fs::read(path).unwrap(), previous);
}

fn publication_fixture(path: &Path) {
    let root = path.parent().unwrap();
    let ticket = ticket();
    let credential = root.join("credential.json");
    fs::write(&credential, "LONG_LIVED_CREDENTIAL_SENTINEL").unwrap();
    publish(path, &ticket, OnboardingProvider::CodexCli, &credential).unwrap();
    let original = fs::read(path).unwrap();
    publish(path, &ticket, OnboardingProvider::CodexCli, &credential).unwrap();
    assert_eq!(fs::read(path).unwrap(), original);
    let another_credential = root.join("another.json");
    assert!(matches!(
        publish(
            path,
            &ticket,
            OnboardingProvider::CodexCli,
            &another_credential
        ),
        Err(ConfigError::BindingConflict)
    ));
    assert_eq!(fs::read(path).unwrap(), original);
    let mut changed = ticket.clone();
    changed.workspace = WorkspaceName::parse("other-room").unwrap();
    assert!(matches!(
        publish(path, &changed, OnboardingProvider::CodexCli, &credential),
        Err(ConfigError::BindingConflict)
    ));
    changed = ticket.clone();
    changed.server_id = Uuid::new_v4();
    assert!(matches!(
        publish(path, &changed, OnboardingProvider::CodexCli, &credential),
        Err(ConfigError::ProfileConflict)
    ));
    changed = ticket.clone();
    changed.provider = Some(OnboardingProvider::Omp);
    assert!(matches!(
        publish(path, &changed, OnboardingProvider::CodexCli, &credential),
        Err(ConfigError::BindingConflict)
    ));
    assert_eq!(fs::read(path).unwrap(), original);
    publish(path, &ticket, OnboardingProvider::Omp, &another_credential).unwrap();
    let saved = config::load_config(path).unwrap();
    let bindings = &saved.profiles["office"].bindings;
    assert_eq!(
        bindings[&OnboardingProvider::CodexCli].credential_file,
        credential
    );
    assert_eq!(
        bindings[&OnboardingProvider::Omp].credential_file,
        another_credential
    );
    let text = fs::read_to_string(path).unwrap();
    assert!(!text.contains(ticket.invite_token.expose()));
    assert!(!text.contains("LONG_LIVED_CREDENTIAL_SENTINEL"));
    let mut manual = saved;
    manual.profiles.insert(
        "office".into(),
        Profile::manual(ticket.routes[0].router_url.clone()),
    );
    config::save_config(path, &manual).unwrap();
    let original = fs::read(path).unwrap();
    assert!(matches!(
        publish(path, &ticket, OnboardingProvider::CodexCli, &credential),
        Err(ConfigError::ProfileConflict)
    ));
    assert_eq!(fs::read(path).unwrap(), original);
}

fn ca_storage_fixture(path: &Path) {
    let root = path.parent().unwrap();
    let mut ticket = ticket();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let key = KeyPair::generate().unwrap();
    let pem = ca.self_signed(&key).unwrap().pem();
    ticket.routes = vec![
        OnboardingRoute {
            kind: RouteKind::Lan,
            router_url: "wss://192.168.2.5/ws".into(),
            ca_pem: Some(pem.clone()),
        },
        OnboardingRoute {
            kind: RouteKind::Public,
            router_url: "wss://router.example/ws".into(),
            ca_pem: Some(pem.clone()),
        },
    ];
    publish(
        path,
        &ticket,
        OnboardingProvider::CodexCli,
        &root.join("credential.json"),
    )
    .unwrap();
    let saved = config::load_config(path).unwrap();
    for route in &saved.profiles["office"].routes {
        let ca_path = route.ca_file.as_ref().unwrap();
        assert_eq!(fs::read_to_string(ca_path).unwrap(), pem);
        assert_eq!(fs::metadata(ca_path).unwrap().mode() & 0o777, 0o600);
    }
    let config_text = fs::read_to_string(path).unwrap();
    assert!(!config_text.contains("BEGIN CERTIFICATE"));
    assert!(!config_text.contains(ticket.invite_token.expose()));
    let selection =
        config::select_provider(Some("office"), None, OnboardingProvider::CodexCli).unwrap();
    assert_eq!(
        selection.ca_file,
        saved.profiles["office"].routes[0].ca_file
    );
    let previous = fs::read(path).unwrap();
    ticket.routes[1].ca_pem = Some(key.serialize_pem());
    assert!(config::store_onboarding_routes(&ticket.routes, &root.join("bad-cas")).is_err());
    assert!(!root.join("bad-cas").exists());
    assert_eq!(fs::read(path).unwrap(), previous);
}

fn invalid_config_fixture(path: &Path) {
    let root = path.parent().unwrap();
    let ticket = ticket();
    publish(
        path,
        &ticket,
        OnboardingProvider::CodexCli,
        &root.join("credential.json"),
    )
    .unwrap();
    let valid: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let mut cases = Vec::new();
    let mut changed = valid.clone();
    changed["profiles"]["office"]["serverId"] = serde_json::json!(Uuid::nil());
    cases.push(changed);
    let mut changed = valid.clone();
    changed["profiles"]["office"]["bindings"]["unknown"] =
        changed["profiles"]["office"]["bindings"]["codex-cli"].clone();
    cases.push(changed);
    let mut changed = valid.clone();
    changed["profiles"]["office"]["bindings"]["codex-cli"]["credentialFile"] =
        serde_json::json!("../credential.json");
    cases.push(changed);
    let mut changed = valid.clone();
    changed["profiles"]["office"]["routes"][0]["routerUrl"] = serde_json::json!("ws://10.1.2.3/ws");
    cases.push(changed);
    let mut changed = valid.clone();
    changed["profiles"]["office"]["inviteToken"] = serde_json::json!("not-a-profile-field");
    cases.push(changed);
    let target = root.join("real-credential.json");
    fs::write(&target, "private").unwrap();
    let link = root.join("linked-credential.json");
    symlink(&target, &link).unwrap();
    let mut changed = valid;
    changed["profiles"]["office"]["bindings"]["codex-cli"]["credentialFile"] =
        serde_json::json!(link);
    cases.push(changed);
    for invalid in cases {
        fs::write(path, serde_json::to_vec(&invalid).unwrap()).unwrap();
        assert!(config::load_config(path).is_err());
    }
}

fn unsafe_publication_fixture(path: &Path) {
    let root = path.parent().unwrap();
    let config = ConfigFile {
        version: 2,
        default_profile: None,
        profiles: BTreeMap::new(),
    };
    let target = root.join("untouched.json");
    fs::write(&target, b"do not overwrite").unwrap();
    symlink(&target, path).unwrap();
    assert!(config::save_config(path, &config).is_err());
    assert!(config::load_config(path).is_err());
    assert_eq!(fs::read(&target).unwrap(), b"do not overwrite");
    fs::remove_file(path).unwrap();
    fs::hard_link(&target, path).unwrap();
    assert!(config::save_config(path, &config).is_err());
    assert_eq!(fs::read(&target).unwrap(), b"do not overwrite");
    fs::remove_file(path).unwrap();
    let real = root.join("real");
    fs::create_dir(&real).unwrap();
    let linked = root.join("linked");
    symlink(&real, &linked).unwrap();
    assert!(config::save_config(&linked.join("config.json"), &config).is_err());
    assert!(!real.join("config.json").exists());
    config::save_config(path, &config).unwrap();
    assert_eq!(fs::metadata(path).unwrap().mode() & 0o777, 0o600);
}

fn selection_fixture(mode: &str, path: &Path) {
    let root = path.parent().unwrap();
    let mut ticket = ticket();
    if matches!(mode, "explicit-ca" | "environment-url") {
        ticket.routes = vec![OnboardingRoute {
            kind: RouteKind::Public,
            router_url: "wss://router.example/ws".into(),
            ca_pem: None,
        }];
    }
    let credential = root.join("binding.json");
    publish(path, &ticket, OnboardingProvider::CodexCli, &credential).unwrap();
    let mut saved = config::load_config(path).unwrap();
    saved.default_profile = Some("office".into());
    if matches!(mode, "explicit-ca" | "environment-url") {
        saved.profiles.get_mut("office").unwrap().routes[0].ca_file = Some(root.join("stored.pem"));
    }
    config::save_config(path, &saved).unwrap();
    let explicit = (mode == "explicit-credential").then(|| root.join("explicit.json"));
    let explicit_profile = (mode == "explicit-profile").then_some("office");
    let selection = config::select_provider(
        explicit_profile,
        explicit.as_deref(),
        OnboardingProvider::CodexCli,
    )
    .unwrap();
    if mode == "environment-url" {
        assert_eq!(
            selection.selection.router_url.as_str(),
            "wss://override.example/ws"
        );
        assert!(selection.selection.profile.is_none());
        assert!(selection.selection.credential_file.is_none());
        assert!(selection.initial_workspace.is_none());
        assert!(selection.expected_server_id.is_none());
        assert!(selection.routes.is_empty());
        assert!(selection.ca_file.is_none());
    } else {
        assert_eq!(
            selection.selection.router_url.as_str(),
            ticket.routes[0].router_url
        );
        assert_eq!(selection.initial_workspace, Some(ticket.workspace));
        assert_eq!(selection.expected_server_id, Some(ticket.server_id));
        let expected_credential = match mode {
            "environment-credential" => root.join("environment.json"),
            "explicit-credential" => root.join("explicit.json"),
            _ => credential,
        };
        assert_eq!(
            selection.selection.credential_file,
            Some(expected_credential)
        );
        assert_eq!(selection.routes, saved.profiles["office"].routes);
    }
    if mode == "explicit-ca" {
        assert_eq!(selection.ca_file, Some(root.join("override.pem")));
    }
    let operator = config::select(explicit_profile, None).unwrap();
    let environment_credential = matches!(mode, "environment-credential" | "explicit-credential")
        .then(|| root.join("environment.json"));
    assert_eq!(operator.credential_file, environment_credential);
    let other =
        config::select_provider(Some("office"), None, OnboardingProvider::ClaudeCode).unwrap();
    assert!(other.initial_workspace.is_none());
    if !matches!(mode, "environment-credential" | "explicit-credential") {
        assert!(other.selection.credential_file.is_none());
    }
}

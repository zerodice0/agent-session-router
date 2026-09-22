use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Barrier, Mutex},
    thread,
};

use agent_session_router::{
    credentials::{CredentialFile, CredentialRole, SecretToken},
    onboarding::{
        EnrollmentError, EnrollmentRequest, INVITE_LIFETIME_MS, IssuedInvite, OnboardingProvider,
        VERSION, invite_subject,
    },
    protocol::{AgentClient, AgentSide, WorkspaceName},
    store::{DATABASE_FILE_NAME, RouterStore, StoreError},
    tasks::{CallerContext, CallerRole, TaskCommand, apply_task, get_task},
};
use rusqlite::{Connection, params};
use tempfile::{TempDir, tempdir};
use uuid::Uuid;

fn persistent_store() -> (TempDir, PathBuf, RouterStore) {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("data");
    let store = RouterStore::open(&data_dir).expect("open store");
    (directory, data_dir, store)
}

fn workspace() -> WorkspaceName {
    WorkspaceName::parse("enrollment-room").expect("workspace name")
}

fn pending(
    invite: &IssuedInvite,
    provider: OnboardingProvider,
) -> (EnrollmentRequest, CredentialFile) {
    let (side, client) = provider.identity();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        invite_subject(invite.invite_id),
        Some(side),
        Some(client),
        vec![invite.workspace.clone()],
    )
    .expect("client-generated credential");
    let request = EnrollmentRequest {
        version: VERSION,
        server_id: invite.server_id,
        invite_id: invite.invite_id,
        invite_token: invite.invite_token.clone(),
        enrollment_id: Uuid::new_v4(),
        provider,
        credential_id: credential.id,
        credential_token: credential.token.clone(),
    };
    (request, credential)
}

fn alter_closed_database(data_dir: &Path, sql: &str) {
    let connection = Connection::open(data_dir.join(DATABASE_FILE_NAME)).expect("open fixture DB");
    connection.execute_batch(sql).expect("configure fixture DB");
    connection.close().expect("close fixture DB");
}

fn assert_no_persisted_tokens(data_dir: &Path, tokens: &[&SecretToken]) {
    for name in [DATABASE_FILE_NAME, "router.sqlite-wal"] {
        let bytes = match fs::read(data_dir.join(name)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => panic!("read database bytes: {error}"),
        };
        for token in tokens {
            assert!(
                !bytes
                    .windows(token.expose().len())
                    .any(|window| window == token.expose().as_bytes()),
                "bearer token persisted in database or WAL"
            );
        }
    }
}

#[test]
fn enrollment_fixes_provider_identity_and_grants_only_the_invited_workspace() {
    for (provider, side, client) in [
        (
            OnboardingProvider::ClaudeCode,
            AgentSide::Claude,
            AgentClient::ClaudeCode,
        ),
        (
            OnboardingProvider::CodexCli,
            AgentSide::Codex,
            AgentClient::CodexCli,
        ),
        (
            OnboardingProvider::Omp,
            AgentSide::Generic,
            AgentClient::Omp,
        ),
    ] {
        let mut store = RouterStore::open_memory().expect("open store");
        let unrelated = WorkspaceName::parse("unrelated-room").expect("workspace name");
        store.create_workspace(&unrelated).expect("unrelated room");
        let invite = store
            .issue_onboarding_invite(&workspace(), true, Some(provider), 1_000)
            .expect("issue invite");
        let (request, credential) = pending(&invite, provider);
        let claims = store.redeem_onboarding(&request, 1_001).expect("enroll");
        assert_eq!(claims.role, CredentialRole::Agent);
        assert_eq!(claims.agent_side, Some(side));
        assert_eq!(claims.agent_client, Some(client));
        assert_eq!(claims.subject, format!("asr-{}", invite.invite_id.simple()));
        assert_eq!(claims.workspaces, vec![workspace()]);
        assert_eq!(claims.id, credential.id);
        let authenticated = store
            .credential_by_hash(&credential.token_hash())
            .expect("credential lookup")
            .expect("credential registered");
        assert_eq!(authenticated.claims, claims);
        assert_eq!(authenticated.revoked_at, None);
        let (visible, _, _) = store
            .list_workspace_rows(Some(&claims.workspaces), None, 100)
            .expect("granted workspace listing");
        assert_eq!(
            visible
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec![workspace()]
        );
    }
}

#[test]
fn exact_replay_survives_restart_and_expiry_without_creating_another_credential() {
    let (_directory, data_dir, mut store) = persistent_store();
    let invite = store
        .issue_onboarding_invite(&workspace(), true, None, 1_000)
        .expect("issue invite");
    let (request, credential) = pending(&invite, OnboardingProvider::Omp);
    let claims = store.redeem_onboarding(&request, 1_001).expect("enroll");
    assert_eq!(store.redeem_onboarding(&request, 1_002), Ok(claims.clone()));
    assert_no_persisted_tokens(&data_dir, &[&invite.invite_token, &credential.token]);
    store.close().expect("close store");

    let mut store = RouterStore::open(&data_dir).expect("restart store");
    assert_eq!(store.server_id().expect("server ID"), invite.server_id);
    assert_eq!(
        store.redeem_onboarding(&request, invite.expires_at + 1),
        Ok(claims)
    );
    let (other, _) = pending(&invite, OnboardingProvider::Omp);
    for altered in [
        EnrollmentRequest {
            enrollment_id: other.enrollment_id,
            ..request.clone()
        },
        EnrollmentRequest {
            credential_id: other.credential_id,
            ..request.clone()
        },
        EnrollmentRequest {
            credential_token: other.credential_token,
            ..request.clone()
        },
        EnrollmentRequest {
            provider: OnboardingProvider::ClaudeCode,
            ..request.clone()
        },
    ] {
        assert_eq!(
            store.redeem_onboarding(&altered, invite.expires_at + 2),
            Err(EnrollmentError::InviteUnavailable)
        );
    }
    assert_eq!(store.credential_count().expect("credential count"), 1);
    assert_no_persisted_tokens(&data_dir, &[&invite.invite_token, &credential.token]);
}

#[test]
fn wrong_invitation_identity_or_provider_does_not_consume_it_and_expiry_is_exclusive() {
    let mut store = RouterStore::open_memory().expect("open store");
    let invite = store
        .issue_onboarding_invite(&workspace(), true, Some(OnboardingProvider::Omp), 1_000)
        .expect("issue invite");
    let (request, _) = pending(&invite, OnboardingProvider::Omp);
    let (_, other) = pending(&invite, OnboardingProvider::Omp);
    for altered in [
        EnrollmentRequest {
            server_id: Uuid::new_v4(),
            ..request.clone()
        },
        EnrollmentRequest {
            invite_id: Uuid::new_v4(),
            ..request.clone()
        },
        EnrollmentRequest {
            invite_token: other.token,
            ..request.clone()
        },
        EnrollmentRequest {
            provider: OnboardingProvider::CodexCli,
            ..request.clone()
        },
    ] {
        assert_eq!(
            store.redeem_onboarding(&altered, 1_001),
            Err(EnrollmentError::InviteUnavailable)
        );
    }
    assert_eq!(store.credential_count().expect("credential count"), 0);
    assert!(
        store
            .redeem_onboarding(&request, invite.expires_at - 1)
            .is_ok()
    );

    let expired = store
        .issue_onboarding_invite(&workspace(), false, None, 1_000)
        .expect("second invite");
    let (request, _) = pending(&expired, OnboardingProvider::Omp);
    assert_eq!(expired.expires_at, 1_000 + INVITE_LIFETIME_MS);
    assert_eq!(
        store.redeem_onboarding(&request, expired.expires_at),
        Err(EnrollmentError::InviteUnavailable)
    );
    assert_eq!(store.credential_count().expect("credential count"), 1);
}

#[test]
fn malformed_ids_and_noncanonical_tokens_are_rejected_before_consumption() {
    let mut store = RouterStore::open_memory().expect("open store");
    let invite = store
        .issue_onboarding_invite(&workspace(), true, None, 1_000)
        .expect("issue invite");
    let (request, _) = pending(&invite, OnboardingProvider::Omp);
    let encoded = serde_json::to_value(&request).expect("encode request");
    let noncanonical = format!("{}B", "A".repeat(42));
    for (field, value) in [
        ("version", serde_json::json!(0)),
        ("serverId", serde_json::json!(Uuid::nil())),
        ("inviteId", serde_json::json!(Uuid::nil())),
        ("enrollmentId", serde_json::json!(Uuid::nil())),
        ("credentialId", serde_json::json!(Uuid::nil())),
        ("inviteToken", serde_json::json!(noncanonical)),
        ("credentialToken", serde_json::json!(noncanonical)),
        ("credentialToken", serde_json::json!("too-short")),
    ] {
        let mut value_request = encoded.clone();
        value_request[field] = value;
        let altered: EnrollmentRequest =
            serde_json::from_value(value_request).expect("deserialize request fields");
        let error = store
            .redeem_onboarding(&altered, 1_001)
            .expect_err("malformed");
        assert_eq!(error, EnrollmentError::Malformed);
        for token in [&request.invite_token, &request.credential_token] {
            assert!(!error.to_string().contains(token.expose()));
            assert!(!format!("{error:?}").contains(token.expose()));
        }
    }
    assert_eq!(store.credential_count().expect("credential count"), 0);
    assert!(store.redeem_onboarding(&request, 1_002).is_ok());
}

#[test]
fn invite_and_credential_revocations_fence_replays_independently() {
    let mut store = RouterStore::open_memory().expect("open store");
    let unused = store
        .issue_onboarding_invite(&workspace(), true, None, 1_000)
        .expect("unused invite");
    let (unused_request, _) = pending(&unused, OnboardingProvider::Omp);
    store
        .revoke_onboarding_invite(unused.invite_id, 1_001)
        .expect("revoke");
    store
        .revoke_onboarding_invite(unused.invite_id, 1_002)
        .expect("idempotent revoke");
    assert_eq!(
        store.redeem_onboarding(&unused_request, 1_003),
        Err(EnrollmentError::InviteUnavailable)
    );
    assert_eq!(store.credential_count().expect("credential count"), 0);
    assert!(matches!(
        store.revoke_onboarding_invite(Uuid::new_v4(), 1_004),
        Err(StoreError::RequestNotFound)
    ));

    let used = store
        .issue_onboarding_invite(&workspace(), false, None, 1_000)
        .expect("used invite");
    let (used_request, credential) = pending(&used, OnboardingProvider::Omp);
    let claims = store
        .redeem_onboarding(&used_request, 1_001)
        .expect("enroll");
    store
        .revoke_onboarding_invite(used.invite_id, 1_002)
        .expect("revoke used invite");
    assert_eq!(
        store.redeem_onboarding(&used_request, used.expires_at + 1),
        Err(EnrollmentError::InviteUnavailable)
    );
    let stored = store
        .credential_by_hash(&credential.token_hash())
        .expect("lookup credential")
        .expect("credential survives invite revoke");
    assert_eq!(stored.claims, claims);
    assert_eq!(stored.revoked_at, None);

    let active = store
        .issue_onboarding_invite(&workspace(), false, None, 1_000)
        .expect("active invite");
    let (active_request, credential) = pending(&active, OnboardingProvider::Omp);
    store
        .redeem_onboarding(&active_request, 1_001)
        .expect("enroll");
    assert!(
        store
            .revoke_credential(credential.id)
            .expect("revoke credential")
    );
    assert_eq!(
        store.redeem_onboarding(&active_request, active.expires_at + 1),
        Err(EnrollmentError::InviteUnavailable)
    );
    assert_eq!(store.credential_count().expect("credential count"), 2);
}

#[test]
fn duplicate_credential_id_or_hash_rolls_back_without_consuming_the_invite() {
    let mut store = RouterStore::open_memory().expect("open store");
    let invite = store
        .issue_onboarding_invite(&workspace(), true, None, 1_000)
        .expect("issue invite");
    let (_, existing) = pending(&invite, OnboardingProvider::Omp);
    store
        .insert_credential(&existing)
        .expect("existing credential");
    let (request, credential) = pending(&invite, OnboardingProvider::Omp);
    for altered in [
        EnrollmentRequest {
            credential_id: existing.id,
            ..request.clone()
        },
        EnrollmentRequest {
            credential_token: existing.token.clone(),
            ..request.clone()
        },
    ] {
        assert_eq!(
            store.redeem_onboarding(&altered, 1_001),
            Err(EnrollmentError::InviteUnavailable)
        );
        assert_eq!(store.credential_count().expect("credential count"), 1);
    }
    assert_eq!(
        store.redeem_onboarding(&request, 1_002),
        Ok(credential.public_claims())
    );
    assert_eq!(store.credential_count().expect("credential count"), 2);
}

#[test]
fn concurrent_consumers_have_one_winner_and_only_its_exact_replay_succeeds() {
    let mut store = RouterStore::open_memory().expect("open store");
    let invite = store
        .issue_onboarding_invite(&workspace(), true, None, 1_000)
        .expect("issue invite");
    let requests = [
        pending(&invite, OnboardingProvider::Omp).0,
        pending(&invite, OnboardingProvider::Omp).0,
    ];
    // RouterStore deliberately has one writer; the mutex models competing actor callers.
    let store = Arc::new(Mutex::new(store));
    let barrier = Arc::new(Barrier::new(3));
    let results = thread::scope(|scope| {
        let handles = requests
            .iter()
            .map(|request| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    store
                        .lock()
                        .expect("store lock")
                        .redeem_onboarding(request, 1_001)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("consumer thread"))
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let mut store = store.lock().expect("store lock");
    for (request, result) in requests.iter().zip(results) {
        if let Ok(claims) = result {
            assert_eq!(
                store.redeem_onboarding(request, invite.expires_at + 1),
                Ok(claims)
            );
        } else {
            assert_eq!(result, Err(EnrollmentError::InviteUnavailable));
            assert_eq!(
                store.redeem_onboarding(request, invite.expires_at + 1),
                Err(EnrollmentError::InviteUnavailable)
            );
        }
    }
    assert_eq!(store.credential_count().expect("credential count"), 1);
}

#[test]
fn failures_roll_back_workspace_creation_and_credential_insertion() {
    let (_directory, data_dir, mut store) = persistent_store();
    assert!(matches!(
        store.issue_onboarding_invite(&workspace(), false, None, 1_000),
        Err(StoreError::WorkspaceNotFound)
    ));
    store.close().expect("close store");
    alter_closed_database(
        &data_dir,
        "CREATE TRIGGER reject_invite BEFORE INSERT ON onboarding_invites BEGIN SELECT RAISE(ABORT,'fixture'); END;",
    );
    let mut store = RouterStore::open(&data_dir).expect("reopen store");
    assert!(
        store
            .issue_onboarding_invite(&workspace(), true, None, 1_000)
            .is_err()
    );
    assert!(
        !store
            .workspace_exists(&workspace())
            .expect("workspace lookup")
    );
    store.close().expect("close store");
    alter_closed_database(&data_dir, "DROP TRIGGER reject_invite;");
    let mut store = RouterStore::open(&data_dir).expect("reopen store");
    let invite = store
        .issue_onboarding_invite(&workspace(), true, None, 1_000)
        .expect("issue invite after failed transaction");
    let created_at = store
        .workspace_created_at(&workspace())
        .expect("workspace creation time");
    store
        .issue_onboarding_invite(&workspace(), true, None, 2_000)
        .expect("reuse existing workspace");
    assert_eq!(
        store
            .workspace_created_at(&workspace())
            .expect("creation time"),
        created_at
    );
    store.close().expect("close store");
    alter_closed_database(
        &data_dir,
        "CREATE TRIGGER reject_consume BEFORE UPDATE OF enrollment_id ON onboarding_invites BEGIN SELECT RAISE(ABORT,'fixture'); END;",
    );
    let mut store = RouterStore::open(&data_dir).expect("reopen store");
    let (request, credential) = pending(&invite, OnboardingProvider::Omp);
    assert_eq!(
        store.redeem_onboarding(&request, 2_001),
        Err(EnrollmentError::Unavailable)
    );
    assert_eq!(store.credential_count().expect("credential count"), 0);
    assert!(
        store
            .credential_by_id(credential.id)
            .expect("credential lookup")
            .is_none()
    );
    store.close().expect("close store");
    alter_closed_database(&data_dir, "DROP TRIGGER reject_consume;");
    let mut store = RouterStore::open(&data_dir).expect("reopen store");
    assert_eq!(
        store.redeem_onboarding(&request, 2_002),
        Ok(credential.public_claims())
    );
    assert_eq!(store.credential_count().expect("credential count"), 1);
}

#[test]
fn version_one_migration_preserves_credentials_events_tasks_and_operation_receipts() {
    let (_directory, data_dir, mut store) = persistent_store();
    store
        .create_workspace(&workspace())
        .expect("legacy workspace");
    let admin = CredentialFile::generate(
        CredentialRole::Operator,
        "admin".to_owned(),
        None,
        None,
        vec![workspace()],
    )
    .expect("legacy credential");
    store
        .insert_credential(&admin)
        .expect("insert legacy credential");
    let caller = CallerContext {
        actor_id: "operator:admin".to_owned(),
        role: CallerRole::Admin,
        workspace: workspace(),
        agent_id: None,
        credential_id: admin.id,
        session_id: None,
        connection_generation: 1,
        reservation: None,
    };
    let command = TaskCommand::Create {
        operation_id: Uuid::new_v4(),
        title: "Preserved task".to_owned(),
        description: "Preserved description".to_owned(),
    };
    let task = apply_task(&mut store, &caller, &command).expect("create legacy task");
    store
        .append_chat_idempotent(
            &workspace(),
            "operator:admin",
            "legacy-chat",
            "durable content",
        )
        .expect("legacy chat");
    let history = serde_json::to_value(
        store
            .history(&workspace(), 0, 100)
            .expect("legacy history")
            .0,
    )
    .expect("serialize history");
    let task_before = serde_json::to_value(&task.mutation.task).expect("serialize task");
    let created_at = store
        .workspace_created_at(&workspace())
        .expect("creation time");
    store.close().expect("close store");
    // Version 2 leaves every version-1 table unchanged. Remove only its new tables
    // to construct the complete prior schema, including real durable records.
    alter_closed_database(
        &data_dir,
        "DROP TABLE onboarding_invites; DROP TABLE server_metadata; PRAGMA user_version=1;",
    );

    let mut store = RouterStore::open(&data_dir).expect("migrate version one");
    let server_id = store.server_id().expect("migration server ID");
    assert!(!server_id.is_nil());
    assert_eq!(
        store
            .workspace_created_at(&workspace())
            .expect("creation time"),
        created_at
    );
    assert_eq!(
        store
            .credential_by_hash(&admin.token_hash())
            .expect("credential lookup")
            .expect("legacy credential")
            .claims,
        admin.public_claims()
    );
    assert_eq!(
        serde_json::to_value(
            store
                .history(&workspace(), 0, 100)
                .expect("migrated history")
                .0
        )
        .expect("serialize history"),
        history
    );
    assert_eq!(
        serde_json::to_value(
            get_task(&store, &workspace(), task.mutation.task.summary.id)
                .expect("task lookup")
                .expect("legacy task")
        )
        .expect("serialize task"),
        task_before
    );
    let replay = apply_task(&mut store, &caller, &command).expect("legacy operation replay");
    assert_eq!(
        replay.mutation.task.summary.id,
        task.mutation.task.summary.id
    );
    assert_eq!(store.latest_seq(&workspace()).expect("latest seq"), Some(2));
    assert_migrated_store_accepts_enrollment_and_restarts(store, &data_dir, server_id);
}

fn assert_migrated_store_accepts_enrollment_and_restarts(
    mut store: RouterStore,
    data_dir: &Path,
    server_id: Uuid,
) {
    let invite = store
        .issue_onboarding_invite(&workspace(), false, None, 1_000)
        .expect("invite on migrated store");
    let (request, _) = pending(&invite, OnboardingProvider::Omp);
    store
        .redeem_onboarding(&request, 1_001)
        .expect("enroll on migrated store");
    store.close().expect("close migrated store");
    let connection =
        Connection::open(data_dir.join(DATABASE_FILE_NAME)).expect("read migrated schema");
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .expect("schema version"),
        2
    );
    connection.close().expect("close schema read");
    let store = RouterStore::open(data_dir).expect("second restart");
    assert_eq!(store.server_id().expect("persistent server ID"), server_id);
    assert_eq!(store.credential_count().expect("credential count"), 2);
}

#[test]
fn schema_rejects_partial_consumption_and_invalid_invitation_records() {
    let (_directory, data_dir, mut store) = persistent_store();
    let invite = store
        .issue_onboarding_invite(&workspace(), true, None, 1_000)
        .expect("issue invite");
    let (request, _) = pending(&invite, OnboardingProvider::Omp);
    let claims = store.redeem_onboarding(&request, 1_001).expect("enroll");
    store.close().expect("close store");
    let connection = Connection::open(data_dir.join(DATABASE_FILE_NAME)).expect("open fixture DB");
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .expect("enable foreign keys");
    for update in [
        "enrollment_id=NULL",
        "credential_id=NULL",
        "redeemed_at=NULL",
        "provider='unsupported-provider'",
        "token_hash=zeroblob(31)",
        "workspace='missing-workspace'",
        "expires_at=created_at+1",
    ] {
        let error = connection
            .execute(
                &format!("UPDATE onboarding_invites SET {update} WHERE id=?1"),
                params![invite.invite_id.to_string()],
            )
            .expect_err("invalid invite row rejected");
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation)
        );
    }
    connection.close().expect("close fixture DB");
    let mut store = RouterStore::open(&data_dir).expect("reopen valid store");
    assert_eq!(
        store.redeem_onboarding(&request, invite.expires_at),
        Ok(claims)
    );
}

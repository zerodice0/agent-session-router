use agent_session_router::{
    credentials::{CredentialFile, CredentialRole},
    protocol::{AgentClient, AgentSide, WorkspaceEventKind, WorkspaceName},
    store::{EventInsert, RouterStore, StoreError},
    tasks::{
        CallerContext, CallerRole, ReservationFence, StopEvidence, TaskChange, TaskCommand,
        TaskEvent, TaskState, apply_task, get_task,
    },
};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn data_directory_has_single_writer_ownership() {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("data");
    let store = RouterStore::open(&data_dir).expect("first store");
    let second = RouterStore::open(&data_dir);
    assert!(matches!(second, Err(StoreError::InUse)));
    store.close().expect("close first store");
    RouterStore::open(&data_dir)
        .expect("reopen after release")
        .close()
        .expect("close reopened store");
}

#[test]
fn routed_request_ids_remain_unique_after_restart() {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("data");
    let workspace = WorkspaceName::parse("request-ids").expect("valid workspace");
    let other = WorkspaceName::parse("other-request-ids").expect("valid workspace");
    let mut store = RouterStore::open(&data_dir).expect("open store");
    store
        .create_workspace(&workspace)
        .expect("create workspace");
    store
        .create_workspace(&other)
        .expect("create other workspace");
    store
        .append_request(&EventInsert {
            workspace: &workspace,
            kind: WorkspaceEventKind::Request,
            actor_id: "worker-1",
            request_id: Some("request-1"),
            target_id: Some("worker-2"),
            task_id: None,
            content: Some("first payload"),
            ok: None,
            error: None,
        })
        .expect("append request");
    store.close().expect("close store");

    let mut store = RouterStore::open(&data_dir).expect("reopen store");
    let duplicate = store.append_request(&EventInsert {
        workspace: &workspace,
        kind: WorkspaceEventKind::Request,
        actor_id: "worker-1",
        request_id: Some("request-1"),
        target_id: Some("worker-2"),
        task_id: None,
        content: Some("second payload"),
        ok: None,
        error: None,
    });
    assert!(matches!(duplicate, Err(StoreError::Conflict)));
    store
        .append_request(&EventInsert {
            workspace: &other,
            kind: WorkspaceEventKind::Request,
            actor_id: "worker-1",
            request_id: Some("request-1"),
            target_id: Some("worker-2"),
            task_id: None,
            content: Some("second payload"),
            ok: None,
            error: None,
        })
        .expect("same request id in another workspace");
    let (events, _, _) = store.history(&workspace, 0, 100).expect("history");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == WorkspaceEventKind::Request)
            .count(),
        1
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn restart_recovers_running_attempt_once_with_a_durable_event() {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("data");
    let workspace = WorkspaceName::parse("recovery-tests").expect("valid workspace");
    let mut store = RouterStore::open(&data_dir).expect("open store");
    store
        .create_workspace(&workspace)
        .expect("create workspace");
    let admin = CredentialFile::generate(
        CredentialRole::Operator,
        "admin".to_owned(),
        None,
        None,
        vec![workspace.clone()],
    )
    .expect("admin credential");
    let agent = CredentialFile::generate(
        CredentialRole::Agent,
        "worker-1".to_owned(),
        Some(AgentSide::Generic),
        Some(AgentClient::Omp),
        vec![workspace.clone()],
    )
    .expect("agent credential");
    store.insert_credential(&admin).expect("insert admin");
    store.insert_credential(&agent).expect("insert agent");

    let admin_caller = CallerContext {
        actor_id: "operator:admin".to_owned(),
        role: CallerRole::Admin,
        workspace: workspace.clone(),
        agent_id: None,
        credential_id: admin.id,
        session_id: None,
        connection_generation: 1,
        reservation: None,
    };
    let created = apply_task(
        &mut store,
        &admin_caller,
        &TaskCommand::Create {
            operation_id: Uuid::new_v4(),
            title: "Recover me".to_owned(),
            description: String::new(),
        },
    )
    .expect("create task");
    let task_id = created.mutation.task.summary.id;
    let assigned = apply_task(
        &mut store,
        &admin_caller,
        &TaskCommand::Assign {
            operation_id: Uuid::new_v4(),
            task_id,
            expected_version: created.mutation.applied_version,
            agent_id: Some(agent.subject.clone()),
        },
    )
    .expect("assign task");
    let work_request_id = "recovery-work".to_owned();
    let agent_caller = CallerContext {
        actor_id: agent.subject.clone(),
        role: CallerRole::Agent,
        workspace: workspace.clone(),
        agent_id: Some(agent.subject.clone()),
        credential_id: agent.id,
        session_id: Some(Uuid::new_v4()),
        connection_generation: 4,
        reservation: Some(ReservationFence {
            work_request_id: work_request_id.clone(),
            task_id,
        }),
    };
    apply_task(
        &mut store,
        &agent_caller,
        &TaskCommand::Begin {
            operation_id: Uuid::new_v4(),
            task_id,
            work_request_id,
            expected_version: assigned.mutation.applied_version,
            last_checkpoint_id: None,
            resume_note: "begin".to_owned(),
        },
    )
    .expect("begin task");
    store.close().expect("simulate clean process close");

    let recovered = RouterStore::open(&data_dir).expect("recover store");
    let task = get_task(&recovered, &workspace, task_id)
        .expect("load recovered task")
        .expect("task exists");
    assert_eq!(task.summary.state, TaskState::Paused);
    assert_eq!(task.summary.stop_evidence, Some(StopEvidence::Unknown));
    let (events, _, _) = recovered.history(&workspace, 0, 100).expect("history");
    let recovery_events = events
        .iter()
        .filter(|event| event.kind == WorkspaceEventKind::Task)
        .filter_map(|event| event.content.as_deref())
        .filter_map(|content| serde_json::from_str::<TaskEvent>(content).ok())
        .filter(|event| event.change == TaskChange::Interrupted)
        .count();
    assert_eq!(recovery_events, 1);
    recovered.close().expect("close recovered store");

    let reopened = RouterStore::open(&data_dir).expect("reopen recovered store");
    let (events, _, _) = reopened.history(&workspace, 0, 100).expect("history");
    let recovery_events = events
        .iter()
        .filter(|event| event.kind == WorkspaceEventKind::Task)
        .filter_map(|event| event.content.as_deref())
        .filter_map(|content| serde_json::from_str::<TaskEvent>(content).ok())
        .filter(|event| event.change == TaskChange::Interrupted)
        .count();
    assert_eq!(recovery_events, 1);
}

use agent_session_router::{
    credentials::{CredentialFile, CredentialRole},
    protocol::{AgentClient, AgentSide, RouterErrorCode, WorkspaceName},
    store::RouterStore,
    tasks::{
        CallerContext, CallerRole, ReservationFence, StopEvidence, TaskCheckpoint, TaskCommand,
        TaskState, agent_has_execution_barrier, apply_task, get_task,
    },
};
use uuid::Uuid;

fn checkpoint(summary: &str) -> TaskCheckpoint {
    TaskCheckpoint {
        summary: summary.to_owned(),
        next_steps: "continue safely".to_owned(),
        artifacts: vec!["artifact://checkpoint".to_owned()],
        risks: String::new(),
    }
}

fn setup() -> (RouterStore, WorkspaceName, CallerContext, CredentialFile) {
    let workspace = WorkspaceName::parse("task-tests").expect("valid workspace");
    let mut store = RouterStore::open_memory().expect("open store");
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
    let caller = CallerContext {
        actor_id: "operator:admin".to_owned(),
        role: CallerRole::Admin,
        workspace: workspace.clone(),
        agent_id: None,
        credential_id: admin.id,
        session_id: None,
        connection_generation: 1,
        reservation: None,
    };
    (store, workspace, caller, agent)
}

#[test]
#[allow(clippy::too_many_lines)]
fn task_mutations_are_idempotent_fenced_and_release_execution() {
    let (mut store, workspace, admin, agent) = setup();
    let create = TaskCommand::Create {
        operation_id: Uuid::new_v4(),
        title: "Repair the router".to_owned(),
        description: "Exercise the durable task lifecycle".to_owned(),
    };
    let created = apply_task(&mut store, &admin, &create).expect("create task");
    let replayed = apply_task(&mut store, &admin, &create).expect("replay create");
    assert_eq!(
        created.mutation.task.summary.id,
        replayed.mutation.task.summary.id
    );
    assert_eq!(
        created.mutation.applied_version,
        replayed.mutation.applied_version
    );

    let task_id = created.mutation.task.summary.id;
    let assigned = apply_task(
        &mut store,
        &admin,
        &TaskCommand::Assign {
            operation_id: Uuid::new_v4(),
            task_id,
            expected_version: created.mutation.applied_version,
            agent_id: Some(agent.subject.clone()),
        },
    )
    .expect("assign task");

    let session_id = Uuid::new_v4();
    let work_request_id = "work-1".to_owned();
    let mut agent_caller = CallerContext {
        actor_id: agent.subject.clone(),
        role: CallerRole::Agent,
        workspace: workspace.clone(),
        agent_id: Some(agent.subject.clone()),
        credential_id: agent.id,
        session_id: Some(session_id),
        connection_generation: 7,
        reservation: Some(ReservationFence {
            work_request_id: work_request_id.clone(),
            task_id,
        }),
    };
    let begun = apply_task(
        &mut store,
        &agent_caller,
        &TaskCommand::Begin {
            operation_id: Uuid::new_v4(),
            task_id,
            work_request_id,
            expected_version: assigned.mutation.applied_version,
            last_checkpoint_id: None,
            resume_note: "Starting from the assignment".to_owned(),
        },
    )
    .expect("begin task");
    let attempt_id = begun
        .mutation
        .task
        .summary
        .current_attempt_id
        .expect("running attempt");
    assert!(agent_has_execution_barrier(&store, &agent.subject).expect("barrier query"));

    agent_caller.session_id = Some(Uuid::new_v4());
    let stale = apply_task(
        &mut store,
        &agent_caller,
        &TaskCommand::Checkpoint {
            operation_id: Uuid::new_v4(),
            task_id,
            attempt_id,
            expected_version: begun.mutation.applied_version,
            checkpoint: checkpoint("must not apply"),
        },
    )
    .expect_err("foreign session must be rejected");
    assert_eq!(stale.code, RouterErrorCode::TaskStaleAttempt);

    agent_caller.session_id = Some(session_id);
    let checkpointed = apply_task(
        &mut store,
        &agent_caller,
        &TaskCommand::Checkpoint {
            operation_id: Uuid::new_v4(),
            task_id,
            attempt_id,
            expected_version: begun.mutation.applied_version,
            checkpoint: checkpoint("durable checkpoint"),
        },
    )
    .expect("checkpoint task");
    let paused = apply_task(
        &mut store,
        &agent_caller,
        &TaskCommand::Pause {
            operation_id: Uuid::new_v4(),
            task_id,
            attempt_id,
            expected_version: checkpointed.mutation.applied_version,
            checkpoint: checkpoint("released checkpoint"),
            blocked: false,
        },
    )
    .expect("pause task");
    assert_eq!(paused.mutation.task.summary.state, TaskState::Paused);
    assert_eq!(
        paused.mutation.task.summary.stop_evidence,
        Some(StopEvidence::Released)
    );
    assert!(!agent_has_execution_barrier(&store, &agent.subject).expect("barrier query"));

    let persisted = get_task(&store, &workspace, task_id)
        .expect("load task")
        .expect("task exists");
    assert_eq!(persisted.summary.state, TaskState::Paused);
    assert_eq!(
        persisted.checkpoint.expect("checkpoint").body,
        paused
            .mutation
            .task
            .checkpoint
            .expect("pause checkpoint")
            .body
    );
}

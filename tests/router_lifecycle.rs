use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use agent_session_router::{
    client::{ClientConfig, ClientEvent, ClientEvents, ClientRole, RouterClient},
    credentials::{CredentialFile, CredentialRole, read_credential},
    protocol::{
        AgentClient, AgentRegistration, AgentSide, ClientMessage, DeliveryMode, RouterErrorCode,
        ServerMessage, WorkspaceEventKind, WorkspaceName,
    },
    router::{RouterConfig, RouterExposure, RouterRuntime},
    store::RouterStore,
    tasks::{PauseReason, StopEvidence, TaskState},
};
use tempfile::{TempDir, tempdir};
use url::Url;
use uuid::Uuid;

fn config(data_dir: &std::path::Path) -> RouterConfig {
    RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        data_dir: data_dir.to_owned(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
    }
}

fn registration() -> AgentRegistration {
    AgentRegistration {
        agent_id: "worker-1".to_owned(),
        side: AgentSide::Generic,
        client: AgentClient::Omp,
        activity: None,
        delivery_mode: DeliveryMode::Push,
    }
}

async fn prepared_router() -> (
    TempDir,
    RouterRuntime,
    Url,
    WorkspaceName,
    CredentialFile,
    CredentialFile,
    CredentialFile,
) {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("data");
    let bootstrap = RouterRuntime::start(config(&data_dir))
        .await
        .expect("bootstrap router");
    let admin = read_credential(&data_dir.join("credentials/admin.json"))
        .expect("bootstrap admin credential");
    bootstrap.shutdown().await.expect("stop bootstrap router");
    bootstrap.wait().await.expect("join bootstrap router");

    let workspace = WorkspaceName::parse("lifecycle").expect("valid workspace");
    let first = CredentialFile::generate(
        CredentialRole::Agent,
        "worker-1".to_owned(),
        Some(AgentSide::Generic),
        Some(AgentClient::Omp),
        vec![workspace.clone()],
    )
    .expect("first agent credential");
    let second = CredentialFile::generate(
        CredentialRole::Agent,
        "worker-1".to_owned(),
        Some(AgentSide::Generic),
        Some(AgentClient::Omp),
        vec![workspace.clone()],
    )
    .expect("second agent credential");
    let mut store = RouterStore::open(&data_dir).expect("open prepared store");
    store
        .create_workspace(&workspace)
        .expect("create lifecycle workspace");
    store
        .insert_credential(&first)
        .expect("insert first agent credential");
    store
        .insert_credential(&second)
        .expect("insert second agent credential");
    store.close().expect("close prepared store");

    let runtime = RouterRuntime::start(config(&data_dir))
        .await
        .expect("start prepared router");
    let url = Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    (directory, runtime, url, workspace, admin, first, second)
}

async fn connect_operator(url: &Url, credential: CredentialFile) -> (RouterClient, ClientEvents) {
    RouterClient::connect(ClientConfig {
        router_url: url.clone(),
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("connect operator")
}

async fn connect_agent(
    url: &Url,
    credential: CredentialFile,
    delegation_token: Option<agent_session_router::credentials::SecretToken>,
) -> (RouterClient, ClientEvents) {
    RouterClient::connect(ClientConfig {
        router_url: url.clone(),
        role: ClientRole::Primary {
            agent: registration(),
            credential,
            delegation_token,
        },
        ca_file: None,
    })
    .await
    .expect("connect agent")
}

async fn wait_for_delivery(events: &mut ClientEvents) -> (String, i64, i64) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = events.recv().await.expect("agent event stream");
            if let ClientEvent::Delivery {
                request_id,
                task: Some(task),
                ..
            } = item.event
            {
                return (request_id, task.id, task.expected_version);
            }
        }
    })
    .await
    .expect("delivery timeout")
}

async fn wait_for_closed(events: &mut ClientEvents) -> RouterErrorCode {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = events.recv().await.expect("client event stream");
            if let ClientEvent::Closed(code) = item.event {
                return code;
            }
        }
    })
    .await
    .expect("terminal close timeout")
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn revoke_closes_agent_delegate_and_pending_and_fences_late_reply() {
    let (_directory, runtime, url, workspace, admin_credential, first, second) =
        prepared_router().await;
    let (admin, _admin_events) = connect_operator(&url, admin_credential).await;
    admin
        .workspace_join(workspace.clone())
        .await
        .expect("operator joins workspace");

    let delegation_token = second.token.clone();
    let (agent, mut agent_events) =
        connect_agent(&url, first.clone(), Some(delegation_token.clone())).await;
    agent
        .workspace_join(workspace.clone())
        .await
        .expect("agent joins workspace");
    let (_events, _, live) = agent
        .workspace_subscribe(0)
        .await
        .expect("agent subscribes");
    assert!(live);
    let (delegate, mut delegate_events) = RouterClient::connect(ClientConfig {
        router_url: url.clone(),
        role: ClientRole::Delegate {
            owner_id: "worker-1".to_owned(),
            delegation_token,
        },
        ca_file: None,
    })
    .await
    .expect("connect delegate");

    let created = admin
        .task_mutation(ClientMessage::TaskCreate {
            request_id: "create-task".to_owned(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            title: "Revocation task".to_owned(),
            description: "Exercise revocation fencing".to_owned(),
        })
        .await
        .expect("create task");
    let task_id = created.task.summary.id;
    let assigned = admin
        .task_mutation(ClientMessage::TaskAssign {
            request_id: "assign-task".to_owned(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            expected_version: created.applied_version,
            agent_id: Some("worker-1".to_owned()),
        })
        .await
        .expect("assign task");
    let requested = delegate
        .call(ClientMessage::TaskRequest {
            request_id: "work-1".to_owned(),
            workspace: workspace.clone(),
            task_id,
            expected_version: assigned.applied_version,
            message: Some("Start the revocation task".to_owned()),
            timeout_ms: Some(10_000),
        })
        .await
        .expect("request task");
    match requested {
        ServerMessage::Accepted { .. } => {}
        ServerMessage::Error { code, .. } => panic!("task request failed: {code}"),
        _ => panic!("unexpected task request response"),
    }
    let (work_request_id, delivered_task_id, delivered_version) =
        wait_for_delivery(&mut agent_events).await;
    assert_eq!(delivered_task_id, task_id);
    let begun = agent
        .task_mutation(ClientMessage::TaskBegin {
            request_id: "begin-task".to_owned(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            work_request_id: work_request_id.clone(),
            expected_version: delivered_version,
            last_checkpoint_id: None,
            resume_note: "Starting the assigned task".to_owned(),
        })
        .await
        .expect("begin task");
    assert_eq!(begun.task.summary.state, TaskState::InProgress);

    assert!(matches!(
        agent
            .call(ClientMessage::WorkspaceLeave {
                request_id: "leave-running".to_owned(),
            })
            .await
            .expect("running leave response"),
        ServerMessage::Error {
            code: RouterErrorCode::WorkspaceBusy,
            ..
        }
    ));

    assert!(
        runtime
            .revoke_credential(first.id)
            .await
            .expect("revoke credential")
    );
    assert_eq!(
        wait_for_closed(&mut agent_events).await,
        RouterErrorCode::Unauthorized
    );
    assert_eq!(
        wait_for_closed(&mut delegate_events).await,
        RouterErrorCode::Unauthorized
    );

    let interrupted = admin
        .task_get(workspace.clone(), task_id)
        .await
        .expect("read interrupted task");
    assert_eq!(interrupted.summary.state, TaskState::Paused);
    assert_eq!(
        interrupted.summary.pause_reason,
        Some(PauseReason::CredentialRevoked)
    );
    assert_eq!(
        interrupted.summary.stop_evidence,
        Some(StopEvidence::Unknown)
    );
    assert!(interrupted.summary.current_attempt_id.is_none());

    let (replacement, _replacement_events) = connect_agent(&url, second, None).await;
    replacement
        .workspace_join(workspace.clone())
        .await
        .expect("replacement agent joins");
    assert!(matches!(
        replacement
            .call(ClientMessage::Reply {
                request_id: work_request_id,
                ok: true,
                content: Some("late result".to_owned()),
                error: None,
            })
            .await
            .expect("late reply response"),
        ServerMessage::Error {
            code: RouterErrorCode::RequestNotFound,
            ..
        }
    ));
    assert!(matches!(
        replacement
            .call(ClientMessage::WorkspaceLeave {
                request_id: "leave-unconfirmed".to_owned(),
            })
            .await
            .expect("unconfirmed leave response"),
        ServerMessage::Error {
            code: RouterErrorCode::LeaveUnconfirmed,
            ..
        }
    ));

    let history = admin
        .workspace_history(Some(0), Some(100))
        .await
        .expect("read workspace history");
    let results = history
        .events
        .iter()
        .filter(|event| {
            event.kind == WorkspaceEventKind::Result
                && event.request_id.as_deref() == Some("work-1")
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].error,
        Some(RouterErrorCode::RequesterDisconnected)
    );
    let after_late_reply = admin
        .task_get(workspace.clone(), task_id)
        .await
        .expect("read task after late reply");
    assert_eq!(
        after_late_reply.summary.version,
        interrupted.summary.version
    );
    assert_eq!(
        after_late_reply.summary.pause_reason,
        Some(PauseReason::CredentialRevoked)
    );

    replacement.close().await.expect("close replacement agent");
    admin.close().await.expect("close operator");
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn primary_disconnect_durably_interrupts_running_attempt() {
    let (_directory, runtime, url, workspace, admin_credential, first, second) =
        prepared_router().await;
    let (admin, _admin_events) = connect_operator(&url, admin_credential).await;
    admin
        .workspace_join(workspace.clone())
        .await
        .expect("operator joins workspace");
    let delegation_token = second.token.clone();
    let (agent, mut agent_events) =
        connect_agent(&url, first, Some(delegation_token.clone())).await;
    agent
        .workspace_join(workspace.clone())
        .await
        .expect("agent joins workspace");
    let (delegate, _delegate_events) = RouterClient::connect(ClientConfig {
        router_url: url.clone(),
        role: ClientRole::Delegate {
            owner_id: "worker-1".to_owned(),
            delegation_token,
        },
        ca_file: None,
    })
    .await
    .expect("connect delegate");
    let created = admin
        .task_mutation(ClientMessage::TaskCreate {
            request_id: "disconnect-create".to_owned(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            title: "Disconnect task".to_owned(),
            description: "Exercise durable transport interruption".to_owned(),
        })
        .await
        .expect("create disconnect task");
    let task_id = created.task.summary.id;
    let assigned = admin
        .task_mutation(ClientMessage::TaskAssign {
            request_id: "disconnect-assign".to_owned(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            expected_version: created.applied_version,
            agent_id: Some("worker-1".to_owned()),
        })
        .await
        .expect("assign disconnect task");
    assert!(matches!(
        delegate
            .call(ClientMessage::TaskRequest {
                request_id: "disconnect-work".to_owned(),
                workspace: workspace.clone(),
                task_id,
                expected_version: assigned.applied_version,
                message: Some("Start the disconnect task".to_owned()),
                timeout_ms: Some(10_000),
            })
            .await
            .expect("request disconnect task"),
        ServerMessage::Accepted { .. }
    ));
    let (work_request_id, _, delivered_version) = wait_for_delivery(&mut agent_events).await;
    agent
        .task_mutation(ClientMessage::TaskBegin {
            request_id: "disconnect-begin".to_owned(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            work_request_id,
            expected_version: delivered_version,
            last_checkpoint_id: None,
            resume_note: "Starting before transport loss".to_owned(),
        })
        .await
        .expect("begin disconnect task");

    agent.close().await.expect("close primary transport");
    let interrupted = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let task = admin
                .task_get(workspace.clone(), task_id)
                .await
                .expect("read task after disconnect");
            if task.summary.state == TaskState::Paused {
                return task;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("disconnect interruption timeout");
    assert_eq!(
        interrupted.summary.pause_reason,
        Some(PauseReason::TransportLost)
    );
    assert_eq!(
        interrupted.summary.stop_evidence,
        Some(StopEvidence::Unknown)
    );
    assert!(interrupted.summary.current_attempt_id.is_none());
    let history = admin
        .workspace_history(Some(0), Some(100))
        .await
        .expect("read disconnect history");
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| {
                event.kind == WorkspaceEventKind::Result
                    && event.request_id.as_deref() == Some("disconnect-work")
            })
            .count(),
        1
    );

    admin.close().await.expect("close operator");
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");
}

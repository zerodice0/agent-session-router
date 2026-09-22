use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    time::Duration,
};

use agent_session_router::{
    client::{ClientConfig, ClientError, ClientEvent, ClientEvents, ClientRole, RouterClient},
    credentials::{CredentialFile, CredentialRole, SecretToken},
    protocol::{
        AgentClient, AgentRegistration, AgentSide, AgentStatus, ClientMessage, DeliveryMode,
        RouterErrorCode, ServerMessage, TaskExecutionEvidence, TaskFence, WorkspaceEventKind,
        WorkspaceName,
    },
    router::{RouterConfig, RouterExposure, RouterRuntime},
    store::RouterStore,
    tasks::{AttemptStatus, PauseReason, TaskChange},
};
use tempfile::{TempDir, tempdir};
use url::Url;
use uuid::Uuid;

fn config(data_dir: &Path) -> RouterConfig {
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

fn workspace(name: &str) -> WorkspaceName {
    WorkspaceName::parse(name).expect("valid workspace")
}

fn agent_credential(agent_id: &str, grants: Vec<WorkspaceName>) -> CredentialFile {
    CredentialFile::generate(
        CredentialRole::Agent,
        agent_id.to_owned(),
        Some(AgentSide::Generic),
        Some(AgentClient::Omp),
        grants,
    )
    .expect("generate agent credential")
}

fn operator_credential(subject: &str, grants: Vec<WorkspaceName>) -> CredentialFile {
    CredentialFile::generate(
        CredentialRole::Operator,
        subject.to_owned(),
        None,
        None,
        grants,
    )
    .expect("generate operator credential")
}

fn registration(agent_id: &str) -> AgentRegistration {
    AgentRegistration {
        agent_id: agent_id.to_owned(),
        side: AgentSide::Generic,
        client: AgentClient::Omp,
        activity: None,
        delivery_mode: DeliveryMode::Push,
    }
}

async fn connect_agent(
    url: &Url,
    agent_id: &str,
    credential: CredentialFile,
    delegation_token: Option<SecretToken>,
) -> (RouterClient, ClientEvents) {
    RouterClient::connect(ClientConfig {
        router_url: url.clone(),
        role: ClientRole::Primary {
            agent: registration(agent_id),
            credential,
            delegation_token,
        },
        ca_file: None,
    })
    .await
    .expect("connect agent")
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

async fn prepare() -> (
    TempDir,
    RouterRuntime,
    Url,
    WorkspaceName,
    WorkspaceName,
    CredentialFile,
    CredentialFile,
    CredentialFile,
    CredentialFile,
) {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("router-data");
    let bootstrap = RouterRuntime::start(config(&data_dir))
        .await
        .expect("bootstrap router");
    bootstrap.shutdown().await.expect("stop bootstrap router");
    bootstrap.wait().await.expect("join bootstrap router");

    let red = workspace("red");
    let blue = workspace("blue");
    let agent_a = agent_credential("agent-a", vec![red.clone()]);
    let agent_b = agent_credential("agent-b", vec![red.clone()]);
    let agent_c = agent_credential("agent-c", vec![blue.clone()]);
    let red_operator = operator_credential("red-operator", vec![red.clone()]);
    let mut store = RouterStore::open(&data_dir).expect("open prepared store");
    store.create_workspace(&red).expect("create red workspace");
    store
        .create_workspace(&blue)
        .expect("create blue workspace");
    for credential in [&agent_a, &agent_b, &agent_c, &red_operator] {
        store
            .insert_credential(credential)
            .expect("insert credential");
    }
    store.close().expect("close prepared store");

    let runtime = RouterRuntime::start(config(&data_dir))
        .await
        .expect("start prepared router");
    let url = Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    (
        directory,
        runtime,
        url,
        red,
        blue,
        agent_a,
        agent_b,
        agent_c,
        red_operator,
    )
}

async fn next_delivery(events: &mut ClientEvents) -> (String, u64) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = events.recv().await.expect("client event stream");
            if let ClientEvent::Delivery {
                request_id,
                timeout_ms,
                ..
            } = item.event
            {
                return (request_id, timeout_ms);
            }
        }
    })
    .await
    .expect("delivery timeout")
}

async fn next_cancellation(
    events: &mut ClientEvents,
) -> (WorkspaceName, String, RouterErrorCode, Option<TaskFence>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = events.recv().await.expect("client event stream");
            if let ClientEvent::WorkCancelled {
                workspace,
                request_id,
                reason,
                task,
            } = item.event
            {
                return (workspace, request_id, reason, task);
            }
        }
    })
    .await
    .expect("cancellation timeout")
}

async fn close_all(clients: Vec<RouterClient>, runtime: RouterRuntime) {
    for client in clients {
        let _ = client.close().await;
    }
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::similar_names, clippy::too_many_lines)]
async fn rooms_roles_delegation_and_identity_are_isolated() {
    let (
        _directory,
        runtime,
        url,
        red,
        blue,
        agent_a_file,
        agent_b_file,
        agent_c_file,
        operator_file,
    ) = prepare().await;
    let delegate_capability = agent_credential("delegate-capability", vec![red.clone()]).token;
    let (agent_a, _agent_a_events) = connect_agent(
        &url,
        "agent-a",
        agent_a_file.clone(),
        Some(delegate_capability.clone()),
    )
    .await;
    let (agent_b, _agent_b_events) = connect_agent(&url, "agent-b", agent_b_file, None).await;
    let (agent_c, _agent_c_events) = connect_agent(&url, "agent-c", agent_c_file, None).await;
    let (operator, _operator_events) = connect_operator(&url, operator_file).await;

    agent_a
        .workspace_join(red.clone())
        .await
        .expect("A joins red");
    agent_b
        .workspace_join(red.clone())
        .await
        .expect("B joins red");
    agent_c
        .workspace_join(blue.clone())
        .await
        .expect("C joins blue");
    operator
        .workspace_join(red.clone())
        .await
        .expect("operator joins red");
    let (delegate, _delegate_events) = RouterClient::connect(ClientConfig {
        router_url: url.clone(),
        role: ClientRole::Delegate {
            owner_id: "agent-a".to_owned(),
            delegation_token: delegate_capability,
        },
        ca_file: None,
    })
    .await
    .expect("connect delegate");

    let listed = agent_a
        .call(ClientMessage::List {
            request_id: "list-red".to_owned(),
        })
        .await
        .expect("list red members");
    let ServerMessage::Agents { agents, .. } = listed else {
        panic!("unexpected member response");
    };
    assert_eq!(
        agents
            .iter()
            .map(|agent| agent.agent_id.as_str())
            .collect::<Vec<_>>(),
        vec!["agent-a", "agent-b"]
    );

    for target in ["agent-c", "missing-agent"] {
        assert!(matches!(
            agent_a
                .call(ClientMessage::Send {
                    request_id: format!("cross-{target}"),
                    to: target.to_owned(),
                    content: "room-private routing check".to_owned(),
                    timeout_ms: Some(1_000),
                })
                .await
                .expect("cross-room response"),
            ServerMessage::Error {
                code: RouterErrorCode::TargetOffline,
                ..
            }
        ));
    }
    assert!(matches!(
        operator
            .call(ClientMessage::WorkspaceJoin {
                request_id: "operator-blue".to_owned(),
                name: blue.clone(),
            })
            .await
            .expect("operator blue response"),
        ServerMessage::Error {
            code: RouterErrorCode::WorkspaceNotFound,
            ..
        }
    ));
    assert!(matches!(
        agent_a
            .call(ClientMessage::WorkspaceCreate {
                request_id: "agent-create".to_owned(),
                name: workspace("forbidden"),
            })
            .await
            .expect("agent create response"),
        ServerMessage::Error {
            code: RouterErrorCode::PermissionDenied,
            ..
        }
    ));
    assert!(matches!(
        delegate
            .call(ClientMessage::WorkspaceSubscribe {
                request_id: "delegate-subscribe".to_owned(),
                after: 0,
            })
            .await
            .expect("delegate subscribe response"),
        ServerMessage::Error {
            code: RouterErrorCode::PermissionDenied,
            ..
        }
    ));

    let delegated = delegate
        .call(ClientMessage::WorkspacePost {
            request_id: "delegated-post".to_owned(),
            content: "posted through delegate".to_owned(),
        })
        .await
        .expect("delegate post");
    assert!(matches!(delegated, ServerMessage::WorkspacePosted { .. }));
    let red_post = agent_a
        .call(ClientMessage::WorkspacePost {
            request_id: "same-chat".to_owned(),
            content: "red content".to_owned(),
        })
        .await
        .expect("red post");
    let blue_post = agent_c
        .call(ClientMessage::WorkspacePost {
            request_id: "same-chat".to_owned(),
            content: "blue content".to_owned(),
        })
        .await
        .expect("blue post");
    assert!(matches!(red_post, ServerMessage::WorkspacePosted { .. }));
    assert!(matches!(blue_post, ServerMessage::WorkspacePosted { .. }));
    assert!(matches!(
        agent_a
            .call(ClientMessage::WorkspacePost {
                request_id: "same-chat".to_owned(),
                content: "different red content".to_owned(),
            })
            .await
            .expect("conflicting red post"),
        ServerMessage::Error {
            code: RouterErrorCode::RequestConflict,
            ..
        }
    ));

    let red_history = agent_a
        .workspace_history(Some(0), Some(100))
        .await
        .expect("red history");
    assert!(
        red_history
            .events
            .iter()
            .all(|event| event.workspace == red)
    );
    assert!(red_history.events.iter().any(|event| {
        event.kind == WorkspaceEventKind::Chat
            && event.actor_id == "agent-a"
            && event.content.as_deref() == Some("posted through delegate")
    }));
    assert!(!red_history.events.iter().any(|event| {
        event.actor_id == "operator:red-operator"
            || event.content.as_deref() == Some("blue content")
    }));
    assert_eq!(
        red_history
            .events
            .iter()
            .filter(|event| event.request_id.as_deref() == Some("same-chat"))
            .count(),
        1
    );

    let duplicate = RouterClient::connect(ClientConfig {
        router_url: url,
        role: ClientRole::Primary {
            agent: registration("agent-a"),
            credential: agent_a_file,
            delegation_token: None,
        },
        ca_file: None,
    })
    .await;
    assert!(matches!(
        duplicate,
        Err(ClientError::Router(RouterErrorCode::AgentConflict))
    ));

    assert_eq!(
        delegate
            .workspace_leave()
            .await
            .expect("delegate leaves for owner"),
        Some(red.clone())
    );
    assert!(matches!(
        agent_a
            .call(ClientMessage::List {
                request_id: "owner-after-delegate-leave".to_owned(),
            })
            .await
            .expect("owner state after delegated leave"),
        ServerMessage::Error {
            code: RouterErrorCode::WorkspaceRequired,
            ..
        }
    ));
    let remaining = agent_b
        .call(ClientMessage::List {
            request_id: "members-after-delegate-leave".to_owned(),
        })
        .await
        .expect("members after delegated leave");
    let ServerMessage::Agents { agents, .. } = remaining else {
        panic!("unexpected member response");
    };
    assert_eq!(
        agents
            .iter()
            .map(|descriptor| descriptor.agent_id.as_str())
            .collect::<Vec<_>>(),
        vec!["agent-b"]
    );

    close_all(vec![delegate, operator, agent_c, agent_b, agent_a], runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::similar_names, clippy::too_many_lines)]
async fn nested_requests_share_parent_deadline_and_busy_sessions_reject_cycles() {
    let (_directory, runtime, url, red, _blue, agent_a_file, agent_b_file, _agent_c, operator_file) =
        prepare().await;
    let (agent_a, mut agent_a_events) = connect_agent(&url, "agent-a", agent_a_file, None).await;
    let (agent_b, mut agent_b_events) = connect_agent(&url, "agent-b", agent_b_file, None).await;
    let (operator, _operator_events) = connect_operator(&url, operator_file).await;
    agent_a
        .workspace_join(red.clone())
        .await
        .expect("A joins red");
    agent_b
        .workspace_join(red.clone())
        .await
        .expect("B joins red");
    operator
        .workspace_join(red)
        .await
        .expect("operator joins red");

    assert!(matches!(
        operator
            .call(ClientMessage::Send {
                request_id: "parent-work".to_owned(),
                to: "agent-a".to_owned(),
                content: "parent".to_owned(),
                timeout_ms: Some(250),
            })
            .await
            .expect("parent accepted"),
        ServerMessage::Accepted { .. }
    ));
    let (parent_request, _) = next_delivery(&mut agent_a_events).await;
    assert_eq!(parent_request, "parent-work");

    assert!(matches!(
        agent_a
            .call(ClientMessage::Send {
                request_id: "nested-work".to_owned(),
                to: "agent-b".to_owned(),
                content: "nested".to_owned(),
                timeout_ms: Some(10_000),
            })
            .await
            .expect("nested accepted"),
        ServerMessage::Accepted { .. }
    ));
    let (nested_request, nested_timeout) = next_delivery(&mut agent_b_events).await;
    assert_eq!(nested_request, "nested-work");
    assert!(nested_timeout <= 250);

    assert!(matches!(
        agent_b
            .call(ClientMessage::Send {
                request_id: "busy-cycle".to_owned(),
                to: "agent-a".to_owned(),
                content: "cycle".to_owned(),
                timeout_ms: Some(1_000),
            })
            .await
            .expect("busy cycle response"),
        ServerMessage::Error {
            code: RouterErrorCode::SessionBusy,
            ..
        }
    ));

    let (a_workspace, a_request, a_reason, a_task) = next_cancellation(&mut agent_a_events).await;
    let (b_workspace, b_request, b_reason, b_task) = next_cancellation(&mut agent_b_events).await;
    assert_eq!(a_request, "parent-work");
    assert_eq!(b_request, "nested-work");
    assert_eq!(a_reason, RouterErrorCode::RequestTimeout);
    assert_eq!(b_reason, RouterErrorCode::RequestTimeout);

    let listed = operator
        .call(ClientMessage::List {
            request_id: "list-during-cancellation".to_owned(),
        })
        .await
        .expect("list during cancellation");
    let ServerMessage::Agents { agents, .. } = listed else {
        panic!("unexpected member response");
    };
    assert!(
        agents
            .iter()
            .all(|agent| { agent.status == AgentStatus::Busy && !agent.ready })
    );
    assert!(matches!(
        agent_b
            .call(ClientMessage::Send {
                request_id: "still-busy".to_owned(),
                to: "agent-a".to_owned(),
                content: "not idle yet".to_owned(),
                timeout_ms: Some(1_000),
            })
            .await
            .expect("busy response after terminal result"),
        ServerMessage::Error {
            code: RouterErrorCode::SessionBusy,
            ..
        }
    ));

    assert!(matches!(
        agent_b
            .work_idle(a_workspace.clone(), a_request.clone(), a_task.clone(), true,)
            .await,
        Err(ClientError::Router(RouterErrorCode::PermissionDenied))
    ));

    agent_a
        .work_idle(a_workspace, a_request, a_task, true)
        .await
        .expect("A confirms provider idle");
    agent_b
        .work_idle(b_workspace, b_request, b_task, true)
        .await
        .expect("B confirms provider idle");
    let listed = operator
        .call(ClientMessage::List {
            request_id: "list-after-idle".to_owned(),
        })
        .await
        .expect("list after idle");
    let ServerMessage::Agents { agents, .. } = listed else {
        panic!("unexpected member response");
    };
    assert!(
        agents
            .iter()
            .all(|agent| { agent.status == AgentStatus::Idle && agent.ready })
    );

    close_all(vec![operator, agent_b, agent_a], runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn task_interrupt_requires_exact_idle_and_stop_confirmations() {
    let (_directory, runtime, url, red, _blue, agent_a_file, _agent_b, _agent_c, operator_file) =
        prepare().await;
    let (agent, mut agent_events) = connect_agent(&url, "agent-a", agent_a_file, None).await;
    let (operator, _operator_events) = connect_operator(&url, operator_file).await;
    agent
        .workspace_join(red.clone())
        .await
        .expect("agent joins workspace");
    operator
        .workspace_join(red.clone())
        .await
        .expect("operator joins workspace");
    let session_id = match operator
        .call(ClientMessage::List {
            request_id: "list-for-session-fence".to_owned(),
        })
        .await
        .expect("list session fence")
    {
        ServerMessage::Agents { agents, .. } => {
            agents
                .into_iter()
                .find(|descriptor| descriptor.agent_id == "agent-a")
                .expect("agent descriptor")
                .session_id
        }
        _ => panic!("unexpected member response"),
    };

    let created = operator
        .task_mutation(ClientMessage::TaskCreate {
            request_id: "create-interrupt-task".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            title: "Interrupt lifecycle".to_owned(),
            description: "Verify both cancellation fences".to_owned(),
        })
        .await
        .expect("create task");
    let task_id = created.task.summary.id;
    let assigned = operator
        .task_mutation(ClientMessage::TaskAssign {
            request_id: "assign-interrupt-task".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            expected_version: created.applied_version,
            agent_id: Some("agent-a".to_owned()),
        })
        .await
        .expect("assign task");
    assert!(matches!(
        operator
            .call(ClientMessage::TaskRequest {
                request_id: "interrupt-work".to_owned(),
                workspace: red.clone(),
                task_id,
                expected_version: assigned.applied_version,
                message: Some("begin".to_owned()),
                timeout_ms: Some(10_000),
            })
            .await
            .expect("request task"),
        ServerMessage::Accepted { .. }
    ));
    let dispatch = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = agent_events.recv().await.expect("agent event stream");
            if let ClientEvent::Delivery {
                request_id, task, ..
            } = item.event
                && request_id == "interrupt-work"
            {
                return task.expect("task delivery");
            }
        }
    })
    .await
    .expect("task delivery timeout");
    let begun = agent
        .task_mutation(ClientMessage::TaskBegin {
            request_id: "begin-interrupt-task".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            work_request_id: "interrupt-work".to_owned(),
            expected_version: dispatch.expected_version,
            last_checkpoint_id: None,
            resume_note: "starting".to_owned(),
        })
        .await
        .expect("begin task");
    let begun_barrier = agent
        .execution_barrier()
        .await
        .expect("fence begun attempt");
    assert_eq!(begun_barrier.session_id, session_id);
    assert_eq!(
        begun_barrier.current,
        begun
            .task
            .current_attempt
            .as_ref()
            .map(|attempt| TaskFence {
                task_id,
                attempt_id: attempt.id,
            })
    );
    assert_eq!(begun_barrier.stop_pending, None);
    let begun_attempt_id = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = agent_events.recv().await.expect("agent event stream");
            if let ClientEvent::TaskAttemptChanged {
                workspace,
                task_id: changed_task_id,
                attempt: Some(attempt),
                closed_attempt_id,
                current: Some(current),
                stop_pending,
            } = item.event
                && changed_task_id == task_id
                && attempt.status == AttemptStatus::Running
            {
                assert_eq!(workspace, red);
                assert_eq!(attempt.task_id, task_id);
                assert_eq!(closed_attempt_id, None);
                assert_eq!(current.attempt_id, attempt.id);
                assert_eq!(stop_pending, None);
                break attempt.id;
            }
        }
    })
    .await
    .expect("begin notification timeout");
    assert_eq!(
        begun.task.summary.current_attempt_id,
        Some(begun_attempt_id)
    );
    operator
        .task_mutation(ClientMessage::TaskInterrupt {
            request_id: "interrupt-task".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            expected_version: begun.applied_version,
            note: "stop requested".to_owned(),
        })
        .await
        .expect("interrupt task");
    let (cancel_workspace, cancel_request, reason, task) =
        next_cancellation(&mut agent_events).await;
    assert_eq!(cancel_request, "interrupt-work");
    assert_eq!(reason, RouterErrorCode::TaskInterrupted);
    let fence = task.expect("task cancellation fence");
    assert_eq!(fence.task_id, task_id);

    let stale = agent
        .work_idle(
            cancel_workspace.clone(),
            "stale-work".to_owned(),
            Some(fence.clone()),
            true,
        )
        .await;
    assert!(
        matches!(
            stale,
            Err(ClientError::Router(RouterErrorCode::RequestNotFound))
        ),
        "unexpected stale idle response: {stale:?}"
    );
    agent
        .work_idle(cancel_workspace, cancel_request, Some(fence.clone()), true)
        .await
        .expect("confirm provider idle");
    assert!(matches!(
        operator
            .call(ClientMessage::Send {
                request_id: "blocked-until-stopped".to_owned(),
                to: "agent-a".to_owned(),
                content: "must remain blocked".to_owned(),
                timeout_ms: Some(1_000),
            })
            .await
            .expect("stop-pending send response"),
        ServerMessage::Error {
            code: RouterErrorCode::SessionBusy,
            ..
        }
    ));
    assert!(matches!(
        operator
            .call(ClientMessage::TaskExecutionStopped {
                request_id: "operator-cannot-report-provider-stop".to_owned(),
                workspace: red.clone(),
                task_id,
                attempt_id: fence.attempt_id,
                ended_session_id: session_id,
                evidence: TaskExecutionEvidence::ProviderTerminal,
                reason: PauseReason::OperatorInterrupt,
            })
            .await
            .expect("operator lifecycle response"),
        ServerMessage::Error {
            code: RouterErrorCode::PermissionDenied,
            ..
        }
    ));
    assert!(matches!(
        agent
            .task_execution_stopped(
                red.clone(),
                task_id,
                fence.attempt_id,
                Uuid::new_v4(),
                TaskExecutionEvidence::ProviderTerminal,
                PauseReason::OperatorInterrupt,
            )
            .await,
        Err(ClientError::Router(RouterErrorCode::TaskStaleAttempt))
    ));
    agent
        .task_execution_stopped(
            red.clone(),
            task_id,
            fence.attempt_id,
            session_id,
            TaskExecutionEvidence::ProviderTerminal,
            PauseReason::OperatorInterrupt,
        )
        .await
        .expect("confirm execution stopped");
    let stopped_barrier = agent
        .execution_barrier()
        .await
        .expect("fence stopped attempt");
    assert_eq!(stopped_barrier.session_id, session_id);
    assert_eq!(stopped_barrier.current, None);
    assert_eq!(stopped_barrier.stop_pending, None);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = agent_events.recv().await.expect("agent event stream");
            if let ClientEvent::TaskAttemptChanged {
                workspace,
                task_id: changed_task_id,
                attempt: Some(attempt),
                closed_attempt_id: Some(closed_attempt_id),
                current,
                stop_pending,
            } = item.event
                && changed_task_id == task_id
                && stop_pending.is_none()
            {
                assert_eq!(workspace, red);
                assert_eq!(attempt.id, fence.attempt_id);
                assert_eq!(closed_attempt_id, fence.attempt_id);
                assert_eq!(current, None);
                break;
            }
        }
    })
    .await
    .expect("stop notification timeout");
    agent
        .task_execution_stopped(
            red,
            task_id,
            fence.attempt_id,
            session_id,
            TaskExecutionEvidence::HostIdle,
            PauseReason::SessionEnded,
        )
        .await
        .expect("repeat stopped evidence is idempotent");
    let listed = operator
        .call(ClientMessage::List {
            request_id: "list-after-stop-confirmation".to_owned(),
        })
        .await
        .expect("list after stop confirmation");
    let ServerMessage::Agents { agents, .. } = listed else {
        panic!("unexpected member response");
    };
    let descriptor = agents
        .iter()
        .find(|descriptor| descriptor.agent_id == "agent-a")
        .expect("agent descriptor");
    assert_eq!(descriptor.status, AgentStatus::Idle);
    assert!(descriptor.ready);

    close_all(vec![operator, agent], runtime).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::similar_names)]
async fn subscription_handoff_is_ordered_and_future_cursors_do_not_harm_health() {
    let (_directory, runtime, url, red, _blue, agent_a_file, agent_b_file, _agent_c, _operator) =
        prepare().await;
    let (agent_a, mut agent_a_events) = connect_agent(&url, "agent-a", agent_a_file, None).await;
    let (agent_b, _agent_b_events) = connect_agent(&url, "agent-b", agent_b_file, None).await;
    agent_a
        .workspace_join(red.clone())
        .await
        .expect("A joins red");
    agent_b.workspace_join(red).await.expect("B joins red");

    let mut posted_seq = 0;
    for (index, content) in ["first", "second", "third"].into_iter().enumerate() {
        let response = agent_b
            .call(ClientMessage::WorkspacePost {
                request_id: format!("ordered-{index}"),
                content: content.to_owned(),
            })
            .await
            .expect("post ordered message");
        let ServerMessage::WorkspacePosted { seq, .. } = response else {
            panic!("unexpected post response");
        };
        posted_seq = seq;
    }
    let (events, next_cursor, live) = agent_a
        .workspace_subscribe(0)
        .await
        .expect("subscribe from beginning");
    assert!(live);
    assert_eq!(next_cursor, posted_seq);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == WorkspaceEventKind::Chat)
            .filter_map(|event| event.content.as_deref())
            .collect::<Vec<_>>(),
        vec!["first", "second", "third"]
    );

    agent_b
        .call(ClientMessage::WorkspacePost {
            request_id: "live-post".to_owned(),
            content: "live".to_owned(),
        })
        .await
        .expect("post live message");
    let live_event = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = agent_a_events
                .recv()
                .await
                .expect("subscriber event stream");
            if let ClientEvent::WorkspaceEvent(event) = item.event
                && event.request_id.as_deref() == Some("live-post")
            {
                return event;
            }
        }
    })
    .await
    .expect("live handoff timeout");
    assert_eq!(live_event.content.as_deref(), Some("live"));
    agent_a
        .ack_event(live_event.workspace.clone(), live_event.seq)
        .expect("ack live event");

    assert!(matches!(
        agent_a
            .call(ClientMessage::WorkspaceHistory {
                request_id: "future-history".to_owned(),
                after: Some(live_event.seq + 1),
                limit: Some(10),
            })
            .await
            .expect("future cursor response"),
        ServerMessage::Error {
            code: RouterErrorCode::InvalidMessage,
            ..
        }
    ));
    assert!(matches!(
        agent_a
            .call(ClientMessage::Ping {
                request_id: "healthy-after-cursor".to_owned(),
            })
            .await
            .expect("router remains healthy"),
        ServerMessage::Pong { .. }
    ));

    close_all(vec![agent_b, agent_a], runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_history_is_typed_filtered_bounded_and_rejects_future_cursors() {
    let (_directory, runtime, url, red, blue, _agent_a, _agent_b, _agent_c, operator_file) =
        prepare().await;
    let (operator, _operator_events) = connect_operator(&url, operator_file).await;
    operator
        .workspace_join(red.clone())
        .await
        .expect("operator joins task history workspace");
    let created = operator
        .task_mutation(ClientMessage::TaskCreate {
            request_id: "history-create".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            title: "History task".to_owned(),
            description: "Typed task history".to_owned(),
        })
        .await
        .expect("create history task");
    let task_id = created.task.summary.id;
    operator
        .task_mutation(ClientMessage::TaskNote {
            request_id: "history-note".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            text: "history note".to_owned(),
        })
        .await
        .expect("note history task");
    operator
        .task_mutation(ClientMessage::TaskCreate {
            request_id: "history-other".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            title: "Other task".to_owned(),
            description: "Must not leak into the requested task history".to_owned(),
        })
        .await
        .expect("create other task");

    let first = operator
        .task_history(red.clone(), task_id, Some(0), Some(1))
        .await
        .expect("first task history page");
    assert_eq!(first.workspace, red);
    assert_eq!(first.task_id, task_id);
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].event.change, TaskChange::Created);
    assert!(first.has_more);
    let second = operator
        .task_history(red.clone(), task_id, Some(first.next_cursor), Some(100))
        .await
        .expect("continued task history page");
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].event.change, TaskChange::Noted);
    assert_eq!(second.events[0].event.task.id, task_id);
    assert!(!second.has_more);

    assert!(matches!(
        operator
            .task_history(
                red.clone(),
                task_id,
                Some(agent_session_router::tasks::MAX_SAFE_INTEGER),
                Some(10),
            )
            .await,
        Err(ClientError::Router(RouterErrorCode::InvalidMessage))
    ));
    assert!(matches!(
        operator
            .task_history(blue, task_id, Some(0), Some(10))
            .await,
        Err(ClientError::Router(RouterErrorCode::WorkspaceMismatch))
    ));

    close_all(vec![operator], runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_reply_completes_router_request_without_reply_ack() {
    let (_directory, runtime, url, red, _blue, agent_file, _agent_b, _agent_c, operator_file) =
        prepare().await;
    let (agent, mut agent_events) = connect_agent(&url, "agent-a", agent_file, None).await;
    let (operator, mut operator_events) = connect_operator(&url, operator_file).await;
    agent
        .workspace_join(red.clone())
        .await
        .expect("agent joins reply workspace");
    operator
        .workspace_join(red)
        .await
        .expect("operator joins reply workspace");
    let response = operator
        .call(ClientMessage::Send {
            request_id: "typed-reply".to_owned(),
            to: "agent-a".to_owned(),
            content: "request".to_owned(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("send routed request");
    assert!(matches!(response, ServerMessage::Accepted { .. }));
    let (request_id, _) = next_delivery(&mut agent_events).await;
    agent
        .reply(request_id, true, Some("typed response".to_owned()), None)
        .await
        .expect("write typed reply");
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = operator_events.recv().await.expect("operator event stream");
            if let ClientEvent::SendResult(result) = item.event {
                return result;
            }
        }
    })
    .await
    .expect("typed result timeout");
    assert!(result.ok);
    assert_eq!(result.content.as_deref(), Some("typed response"));

    close_all(vec![operator, agent], runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enriched_task_history_bounds_report_bodies_to_one_response_page() {
    let (_directory, runtime, url, red, _blue, _agent_a, _agent_b, _agent_c, operator_file) =
        prepare().await;
    let (operator, _operator_events) = connect_operator(&url, operator_file).await;
    operator
        .workspace_join(red.clone())
        .await
        .expect("join task history workspace");
    let created = operator
        .task_mutation(ClientMessage::TaskCreate {
            request_id: "history-bound-create".to_owned(),
            workspace: red.clone(),
            operation_id: Uuid::new_v4(),
            title: "large history".to_owned(),
            description: String::new(),
        })
        .await
        .expect("create history task");
    let task_id = created.task.summary.id;
    let note = "x".repeat(16 * 1024);
    for index in 0..20 {
        operator
            .task_mutation(ClientMessage::TaskNote {
                request_id: format!("history-bound-note-{index}"),
                workspace: red.clone(),
                operation_id: Uuid::new_v4(),
                task_id,
                text: note.clone(),
            })
            .await
            .expect("append large history note");
    }

    let page = operator
        .task_history(red.clone(), task_id, Some(0), Some(100))
        .await
        .expect("bounded enriched task history");
    assert!(page.has_more);
    assert!(page.events.len() < 21);
    assert!(
        page.events
            .iter()
            .filter(|event| event.event.change == TaskChange::Noted)
            .all(|event| event.report.is_some())
    );
    let encoded = serde_json::to_vec(&ServerMessage::TaskHistory {
        request_id: "rpc:00000000-0000-4000-8000-000000000000".to_owned(),
        page,
    })
    .expect("serialize bounded history response");
    assert!(encoded.len() <= agent_session_router::protocol::MAX_RESPONSE_BYTES);

    close_all(vec![operator], runtime).await;
}

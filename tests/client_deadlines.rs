use std::time::Duration;

use agent_session_router::{
    client::{
        ClientConfig, ClientConnectionState, ClientError, ClientEvent, ClientRole, RouterClient,
    },
    credentials::{CredentialFile, CredentialRole},
    protocol::{
        AgentClient, AgentDescriptor, AgentRegistration, AgentSide, AgentStatus, ClientMessage,
        DeliveryMode, PROTOCOL_VERSION, RegistrationRole, RouterErrorCode, ServerMessage,
        TaskFence, WorkspaceName, parse_client_message,
    },
    tasks::{AttemptStatus, StopEvidence, TaskAttempt},
};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use url::Url;
use uuid::Uuid;

async fn recv_client<S>(socket: &mut WebSocketStream<S>) -> ClientMessage
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let message = socket
        .next()
        .await
        .expect("client websocket message")
        .expect("valid websocket message");
    let Message::Text(text) = message else {
        panic!("expected client text message");
    };
    parse_client_message(text.as_str()).expect("valid client protocol message")
}

async fn send_server<S>(socket: &mut WebSocketStream<S>, message: &ServerMessage)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .send(Message::Text(
            serde_json::to_string(message)
                .expect("serialize server message")
                .into(),
        ))
        .await
        .expect("send server message");
}

async fn register_operator<S>(socket: &mut WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    assert!(matches!(
        recv_client(socket).await,
        ClientMessage::RegisterOperator { .. }
    ));
    send_server(
        socket,
        &ServerMessage::RegisteredOperator {
            protocol_version: PROTOCOL_VERSION,
            subject: "deadline-test".to_owned(),
            admin: false,
            workspace: None,
            cursor: 0,
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn deadlines_and_cancellation_clear_pending_without_replay() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let address = listener.local_addr().expect("test server address");
    let (cancel_seen_tx, cancel_seen_rx) = tokio::sync::oneshot::channel();
    let (reconnected_tx, reconnected_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("first connection");
        let mut socket = accept_async(stream).await.expect("first websocket");
        register_operator(&mut socket).await;

        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "deadline-1"
        ));
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "deadline-1"
        ));
        send_server(
            &mut socket,
            &ServerMessage::Pong {
                request_id: "deadline-1".to_owned(),
            },
        )
        .await;

        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "cancel-1"
        ));
        cancel_seen_tx.send(()).expect("signal cancelled call");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "cancel-1"
        ));
        send_server(
            &mut socket,
            &ServerMessage::Pong {
                request_id: "cancel-1".to_owned(),
            },
        )
        .await;
        socket.close(None).await.expect("close first websocket");
        drop(socket);

        let (stream, _) = listener.accept().await.expect("reconnected client");
        let mut socket = accept_async(stream).await.expect("reconnected websocket");
        register_operator(&mut socket).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), socket.next())
                .await
                .is_err(),
            "an uncertain request was replayed after reconnect"
        );
        reconnected_tx.send(()).expect("signal clean reconnect");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "after-reconnect"
        ));
        send_server(
            &mut socket,
            &ServerMessage::Pong {
                request_id: "after-reconnect".to_owned(),
            },
        )
        .await;
        let _ = socket.next().await;
    });

    let credential = CredentialFile::generate(
        CredentialRole::Operator,
        "deadline-test".to_owned(),
        None,
        None,
        Vec::new(),
    )
    .expect("operator credential");
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).expect("test URL"),
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("connect client");

    let timeout = client
        .call_with_deadline(
            ClientMessage::Ping {
                request_id: "deadline-1".to_owned(),
            },
            tokio::time::Instant::now() + Duration::from_millis(40),
        )
        .await;
    assert!(matches!(
        timeout,
        Err(ClientError::Router(RouterErrorCode::RequestTimeout))
    ));
    assert!(matches!(
        client
            .call_with_deadline(
                ClientMessage::Ping {
                    request_id: "deadline-1".to_owned(),
                },
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .expect("reuse timed-out request id"),
        ServerMessage::Pong { request_id } if request_id == "deadline-1"
    ));

    let cancelled_client = client.clone();
    let cancelled = tokio::spawn(async move {
        cancelled_client
            .call_with_deadline(
                ClientMessage::Ping {
                    request_id: "cancel-1".to_owned(),
                },
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .await
    });
    cancel_seen_rx.await.expect("cancelled call reached server");
    cancelled.abort();
    assert!(matches!(
        cancelled.await,
        Err(error) if error.is_cancelled()
    ));
    assert!(matches!(
        client
            .call_with_deadline(
                ClientMessage::Ping {
                    request_id: "cancel-1".to_owned(),
                },
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .expect("reuse cancelled request id"),
        ServerMessage::Pong { request_id } if request_id == "cancel-1"
    ));

    reconnected_rx
        .await
        .expect("client reconnected without replay");
    assert!(matches!(
        client
            .call_with_deadline(
                ClientMessage::Ping {
                    request_id: "after-reconnect".to_owned(),
                },
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .expect("call after reconnect"),
        ServerMessage::Pong { request_id } if request_id == "after-reconnect"
    ));

    client.close().await.expect("close client");
    server.await.expect("join test server");
}
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn reconnect_restores_membership_subscription_and_acknowledged_cursor() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind restore server");
    let address = listener.local_addr().expect("restore server address");
    let workspace = WorkspaceName::parse("restore-room").expect("valid workspace");
    let server_workspace = workspace.clone();
    let (restored_tx, restored_rx) = tokio::sync::oneshot::channel();
    let (unsubscribe_seen_tx, unsubscribe_seen_rx) = tokio::sync::oneshot::channel();
    let closed_attempts = [Uuid::new_v4(), Uuid::new_v4()];
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("first restore connection");
        let mut socket = accept_async(stream).await.expect("first restore websocket");
        register_operator(&mut socket).await;
        let ClientMessage::WorkspaceJoin { request_id, name } = recv_client(&mut socket).await
        else {
            panic!("expected initial workspace join");
        };
        assert_eq!(name, server_workspace);
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceJoined {
                request_id,
                workspace: server_workspace.clone(),
                cursor: 3,
            },
        )
        .await;
        let ClientMessage::WorkspaceSubscribe { request_id, after } =
            recv_client(&mut socket).await
        else {
            panic!("expected initial workspace subscription");
        };
        assert_eq!(after, 0);
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceSubscription {
                request_id,
                workspace: server_workspace.clone(),
                events: Vec::new(),
                next_cursor: 3,
                live: true,
            },
        )
        .await;
        let ClientMessage::WorkspaceUnsubscribe {
            request_id: old_unsubscribe,
        } = recv_client(&mut socket).await
        else {
            panic!("expected pending unsubscribe");
        };
        unsubscribe_seen_tx
            .send(())
            .expect("signal pending unsubscribe");
        let ClientMessage::WorkspaceSubscribe { request_id, after } =
            recv_client(&mut socket).await
        else {
            panic!("expected newer subscription");
        };
        assert_eq!(after, 0);
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceSubscription {
                request_id,
                workspace: server_workspace.clone(),
                events: Vec::new(),
                next_cursor: 3,
                live: true,
            },
        )
        .await;
        let stale_unsubscribe = ServerMessage::WorkspaceUnsubscribed {
            request_id: old_unsubscribe,
            workspace: server_workspace.clone(),
        };
        // Neither the older pending operation nor its unmatched duplicate may erase
        // the newer subscription, even though both name the same workspace.
        send_server(&mut socket, &stale_unsubscribe).await;
        send_server(&mut socket, &stale_unsubscribe).await;
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "cursor-fence"
        ));
        send_server(
            &mut socket,
            &ServerMessage::Pong {
                request_id: "cursor-fence".to_owned(),
            },
        )
        .await;
        socket.close(None).await.expect("close restore websocket");
        drop(socket);

        let (stream, _) = listener.accept().await.expect("restored connection");
        let mut socket = accept_async(stream).await.expect("restored websocket");
        register_operator(&mut socket).await;
        let ClientMessage::WorkspaceJoin { request_id, name } = recv_client(&mut socket).await
        else {
            panic!("expected restored workspace join");
        };
        assert_eq!(name, server_workspace);
        send_server(
            &mut socket,
            &ServerMessage::TaskAttemptChanged {
                workspace: server_workspace.clone(),
                task_id: 81,
                attempt: None,
                closed_attempt_id: Some(closed_attempts[0]),
                current: None,
                stop_pending: None,
            },
        )
        .await;
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceJoined {
                request_id,
                workspace: server_workspace.clone(),
                cursor: 9,
            },
        )
        .await;
        let ClientMessage::WorkspaceSubscribe { request_id, after } =
            recv_client(&mut socket).await
        else {
            panic!("expected restored workspace subscription");
        };
        assert_eq!(after, 7);
        send_server(
            &mut socket,
            &ServerMessage::TaskAttemptChanged {
                workspace: server_workspace.clone(),
                task_id: 82,
                attempt: None,
                closed_attempt_id: Some(closed_attempts[1]),
                current: None,
                stop_pending: None,
            },
        )
        .await;
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceSubscription {
                request_id,
                workspace: server_workspace,
                events: Vec::new(),
                next_cursor: 7,
                live: true,
            },
        )
        .await;
        restored_tx.send(()).expect("signal restored state");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "restored-call"
        ));
        send_server(
            &mut socket,
            &ServerMessage::Pong {
                request_id: "restored-call".to_owned(),
            },
        )
        .await;
        let _ = socket.next().await;
    });

    let credential = CredentialFile::generate(
        CredentialRole::Operator,
        "admin".to_owned(),
        None,
        None,
        Vec::new(),
    )
    .expect("restore credential");
    let (client, mut events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).expect("restore URL"),
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("connect restore client");
    let mut state = client.connection_state();
    assert_eq!(
        *state.borrow_and_update(),
        ClientConnectionState::Connected { epoch: 1 }
    );
    assert_eq!(client.session_id(), None);
    assert_eq!(
        client.operator_is_admin(),
        Some(false),
        "local admin-shaped claims must not override authenticated registration",
    );
    client
        .workspace_join(workspace.clone())
        .await
        .expect("initial workspace join");
    let (_, _, live) = client
        .workspace_subscribe(0)
        .await
        .expect("initial subscription");
    assert!(live);
    let unsubscribe_client = client.clone();
    let unsubscribe = tokio::spawn(async move {
        unsubscribe_client
            .call(ClientMessage::WorkspaceUnsubscribe {
                request_id: "older-unsubscribe".to_owned(),
            })
            .await
    });
    unsubscribe_seen_rx
        .await
        .expect("unsubscribe reached server");
    assert!(
        client
            .workspace_subscribe(0)
            .await
            .expect("newer subscription")
            .2
    );
    assert!(matches!(
        unsubscribe
            .await
            .expect("unsubscribe task")
            .expect("unsubscribe response"),
        ServerMessage::WorkspaceUnsubscribed { .. }
    ));
    client.ack_event(workspace, 7).expect("ack event cursor");
    assert!(matches!(
        client
            .call(ClientMessage::Ping {
                request_id: "cursor-fence".to_owned(),
            })
            .await
            .expect("cursor fence"),
        ServerMessage::Pong { .. }
    ));
    tokio::time::timeout(Duration::from_secs(3), restored_rx)
        .await
        .expect("restore timeout")
        .expect("restore signal");
    tokio::time::timeout(
        Duration::from_secs(3),
        state.wait_for(|value| *value == (ClientConnectionState::Connected { epoch: 2 })),
    )
    .await
    .expect("connected epoch deadline")
    .expect("connected epoch sender");
    for (task_id, closed_attempt_id) in [(81, closed_attempts[0]), (82, closed_attempts[1])] {
        let item = tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .expect("restored attempt deadline")
            .expect("restored attempt event");
        assert!(matches!(
            item.event,
            ClientEvent::TaskAttemptChanged {
                task_id: received,
                attempt: None,
                closed_attempt_id: Some(closed),
                current: None,
                stop_pending: None,
                ..
            } if received == task_id && closed == closed_attempt_id
        ));
    }
    assert!(matches!(
        client
            .call(ClientMessage::Ping {
                request_id: "restored-call".to_owned(),
            })
            .await
            .expect("call after state restoration"),
        ServerMessage::Pong { .. }
    ));
    client.close().await.expect("close restore client");
    tokio::time::timeout(
        Duration::from_secs(3),
        state.wait_for(|value| *value == (ClientConnectionState::Closed { reason: None })),
    )
    .await
    .expect("closed state deadline")
    .expect("closed state sender");
    assert_eq!(client.operator_is_admin(), None);
    server.await.expect("join restore server");
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn uncertain_leave_disconnects_owner_but_not_delegate() {
    let owner_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind owner leave server");
    let owner_address = owner_listener.local_addr().expect("owner server address");
    let workspace = WorkspaceName::parse("leave-room").expect("valid leave workspace");
    let owner_workspace = workspace.clone();
    let (owner_reconnected_tx, owner_reconnected_rx) = tokio::sync::oneshot::channel();
    let owner_server = tokio::spawn(async move {
        let (stream, _) = owner_listener.accept().await.expect("owner connection");
        let mut socket = accept_async(stream).await.expect("owner websocket");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::RegisterOperator { .. }
        ));
        send_server(
            &mut socket,
            &ServerMessage::RegisteredOperator {
                protocol_version: PROTOCOL_VERSION,
                subject: "leave-owner".to_owned(),
                admin: false,
                workspace: Some(owner_workspace),
                cursor: 4,
            },
        )
        .await;
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::WorkspaceLeave { request_id } if request_id == "owner-leave"
        ));
        let _ = socket.next().await;
        drop(socket);

        let (stream, _) = owner_listener.accept().await.expect("owner reconnect");
        let mut socket = accept_async(stream)
            .await
            .expect("owner reconnect websocket");
        register_operator(&mut socket).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), socket.next())
                .await
                .is_err(),
            "timed-out owner leave replayed membership"
        );
        owner_reconnected_tx
            .send(())
            .expect("signal owner reconnect");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "owner-after-leave"
        ));
        send_server(
            &mut socket,
            &ServerMessage::Pong {
                request_id: "owner-after-leave".to_owned(),
            },
        )
        .await;
        let _ = socket.next().await;
    });
    let owner_credential = CredentialFile::generate(
        CredentialRole::Operator,
        "leave-owner".to_owned(),
        None,
        None,
        Vec::new(),
    )
    .expect("owner credential");
    let (owner, _owner_events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{owner_address}")).expect("owner URL"),
        role: ClientRole::Operator {
            credential: owner_credential,
        },
        ca_file: None,
    })
    .await
    .expect("connect owner");
    assert!(matches!(
        owner
            .call_with_deadline(
                ClientMessage::WorkspaceLeave {
                    request_id: "owner-leave".to_owned(),
                },
                tokio::time::Instant::now() + Duration::from_millis(40),
            )
            .await,
        Err(ClientError::Router(RouterErrorCode::RequestTimeout))
    ));
    tokio::time::timeout(Duration::from_secs(3), owner_reconnected_rx)
        .await
        .expect("owner reconnect timeout")
        .expect("owner reconnect signal");
    owner
        .call(ClientMessage::Ping {
            request_id: "owner-after-leave".to_owned(),
        })
        .await
        .expect("owner call after leave timeout");
    owner.close().await.expect("close owner");
    owner_server.await.expect("join owner server");

    let delegate_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind delegate leave server");
    let delegate_address = delegate_listener
        .local_addr()
        .expect("delegate server address");
    let delegate_workspace = workspace;
    let delegate_server = tokio::spawn(async move {
        let (stream, _) = delegate_listener
            .accept()
            .await
            .expect("delegate connection");
        let mut socket = accept_async(stream).await.expect("delegate websocket");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::RegisterDelegate { .. }
        ));
        send_server(
            &mut socket,
            &ServerMessage::Registered {
                protocol_version: PROTOCOL_VERSION,
                agent: AgentDescriptor {
                    agent_id: "delegate-owner".to_owned(),
                    side: AgentSide::Generic,
                    client: AgentClient::Omp,
                    activity: None,
                    status: AgentStatus::Idle,
                    delivery_mode: DeliveryMode::Push,
                    ready: true,
                    session_id: Uuid::new_v4(),
                },
                role: RegistrationRole::Delegate,
                workspace: Some(delegate_workspace),
                cursor: 4,
            },
        )
        .await;
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::WorkspaceLeave { request_id } if request_id == "delegate-leave"
        ));
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Ping { request_id } if request_id == "delegate-after-leave"
        ));
        send_server(
            &mut socket,
            &ServerMessage::Pong {
                request_id: "delegate-after-leave".to_owned(),
            },
        )
        .await;
        let _ = socket.next().await;
    });
    let token_source = CredentialFile::generate(
        CredentialRole::Operator,
        "delegate-token".to_owned(),
        None,
        None,
        Vec::new(),
    )
    .expect("delegation token source");
    let (delegate, _delegate_events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{delegate_address}")).expect("delegate URL"),
        role: ClientRole::Delegate {
            owner_id: "delegate-owner".to_owned(),
            delegation_token: token_source.token,
        },
        ca_file: None,
    })
    .await
    .expect("connect delegate");
    assert!(matches!(
        delegate
            .call_with_deadline(
                ClientMessage::WorkspaceLeave {
                    request_id: "delegate-leave".to_owned(),
                },
                tokio::time::Instant::now() + Duration::from_millis(40),
            )
            .await,
        Err(ClientError::Router(RouterErrorCode::LeaveUnconfirmed))
    ));
    delegate
        .call(ClientMessage::Ping {
            request_id: "delegate-after-leave".to_owned(),
        })
        .await
        .expect("delegate remains connected");
    delegate.close().await.expect("close delegate");
    delegate_server.await.expect("join delegate server");
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn execution_barrier_orders_full_attempt_notification_on_the_live_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind barrier server");
    let address = listener.local_addr().expect("barrier server address");
    let session_id = Uuid::new_v4();
    let attempt_id = Uuid::new_v4();
    let workspace = WorkspaceName::parse("barrier-room").expect("workspace");
    let attempt = TaskAttempt {
        id: attempt_id,
        task_id: 9,
        agent_id: "barrier-agent".to_owned(),
        session_id,
        work_request_id: "barrier-work".to_owned(),
        resumed_from_checkpoint_id: None,
        status: AttemptStatus::Running,
        stop_evidence: StopEvidence::Unknown,
        reason: None,
        started_at: 1,
        ended_at: None,
        stopped_at: None,
    };
    let server_workspace = workspace.clone();
    let server_attempt = attempt.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("barrier connection");
        let mut socket = accept_async(stream).await.expect("barrier websocket");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Register { .. }
        ));
        send_server(
            &mut socket,
            &ServerMessage::Registered {
                protocol_version: PROTOCOL_VERSION,
                agent: AgentDescriptor {
                    agent_id: "barrier-agent".to_owned(),
                    side: AgentSide::Generic,
                    client: AgentClient::Omp,
                    activity: None,
                    status: AgentStatus::Idle,
                    delivery_mode: DeliveryMode::Pull,
                    ready: false,
                    session_id,
                },
                role: RegistrationRole::Agent,
                workspace: None,
                cursor: 0,
            },
        )
        .await;
        send_server(
            &mut socket,
            &ServerMessage::TaskAttemptChanged {
                workspace: server_workspace,
                task_id: 9,
                attempt: Some(server_attempt),
                closed_attempt_id: None,
                current: Some(TaskFence {
                    task_id: 9,
                    attempt_id,
                }),
                stop_pending: None,
            },
        )
        .await;
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::Readiness { ready: true }
        ));
        let ClientMessage::Ping { request_id } = recv_client(&mut socket).await else {
            panic!("expected barrier ping");
        };
        send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
        let _ = socket.next().await;
    });

    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        "barrier-agent".to_owned(),
        Some(AgentSide::Generic),
        Some(AgentClient::Omp),
        vec![workspace.clone()],
    )
    .expect("agent credential");
    let (client, mut events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).expect("barrier URL"),
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: "barrier-agent".to_owned(),
                side: AgentSide::Generic,
                client: AgentClient::Omp,
                activity: None,
                delivery_mode: DeliveryMode::Pull,
            },
            credential,
            delegation_token: None,
        },
        ca_file: None,
    })
    .await
    .expect("connect barrier client");

    assert_eq!(client.session_id(), Some(session_id));
    client.set_ready(true).await.expect("set ready");
    let barrier = client.execution_barrier().await.expect("execution barrier");
    assert_eq!(barrier.session_id, session_id);
    assert_eq!(
        barrier.current,
        Some(TaskFence {
            task_id: 9,
            attempt_id,
        })
    );
    assert_eq!(barrier.stop_pending, None);

    let event = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("attempt event timeout")
        .expect("attempt event");
    match event.event {
        ClientEvent::TaskAttemptChanged {
            workspace: event_workspace,
            task_id,
            attempt: Some(event_attempt),
            closed_attempt_id,
            current,
            stop_pending,
        } => {
            assert_eq!(event_workspace, workspace);
            assert_eq!(task_id, 9);
            assert_eq!(event_attempt, attempt);
            assert_eq!(closed_attempt_id, None);
            assert_eq!(
                current,
                Some(TaskFence {
                    task_id: 9,
                    attempt_id,
                })
            );
            assert_eq!(stop_pending, None);
        }
        _ => panic!("unexpected client event"),
    }

    client.close().await.expect("close barrier client");
    server.await.expect("join barrier server");
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_reply_completes_after_same_socket_write_without_server_ack() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind reply server");
    let address = listener.local_addr().expect("reply server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("reply connection");
        let mut socket = accept_async(stream).await.expect("reply websocket");
        register_operator(&mut socket).await;
        let reply = recv_client(&mut socket).await;
        let _ = socket.next().await;
        reply
    });
    let credential = CredentialFile::generate(
        CredentialRole::Operator,
        "deadline-test".to_owned(),
        None,
        None,
        Vec::new(),
    )
    .expect("operator credential");
    let (client, _) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).expect("reply URL"),
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("connect reply client");

    tokio::time::timeout(
        Duration::from_secs(1),
        client.reply(
            "inbound-request".to_owned(),
            false,
            None,
            Some(RouterErrorCode::ProviderError),
        ),
    )
    .await
    .expect("reply must not wait for an acknowledgement")
    .expect("reply write");
    client.close().await.expect("close reply client");
    assert!(matches!(
        server.await.expect("join reply server"),
        ClientMessage::Reply {
            request_id,
            ok: false,
            content: None,
            error: Some(RouterErrorCode::ProviderError),
        } if request_id == "inbound-request"
    ));
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn unmatched_or_wrong_workspace_subscription_cannot_resurrect_after_unsubscribe() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind subscription server");
    let address = listener.local_addr().expect("subscription server address");
    let workspace = WorkspaceName::parse("subscription-room").expect("workspace");
    let server_workspace = workspace.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("initial connection");
        let mut socket = accept_async(stream).await.expect("initial websocket");
        register_operator(&mut socket).await;
        let ClientMessage::WorkspaceJoin { request_id, .. } = recv_client(&mut socket).await else {
            panic!("expected workspace join");
        };
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceJoined {
                request_id,
                workspace: server_workspace.clone(),
                cursor: 0,
            },
        )
        .await;
        let ClientMessage::WorkspaceSubscribe { request_id, .. } = recv_client(&mut socket).await
        else {
            panic!("expected initial subscription");
        };
        let old_subscription = ServerMessage::WorkspaceSubscription {
            request_id,
            workspace: server_workspace.clone(),
            events: Vec::new(),
            next_cursor: 0,
            live: true,
        };
        send_server(&mut socket, &old_subscription).await;
        let ClientMessage::WorkspaceUnsubscribe { request_id } = recv_client(&mut socket).await
        else {
            panic!("expected confirmed unsubscribe");
        };
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceUnsubscribed {
                request_id,
                workspace: server_workspace.clone(),
            },
        )
        .await;
        let ClientMessage::WorkspaceSubscribe { request_id, .. } = recv_client(&mut socket).await
        else {
            panic!("expected subscription for current workspace");
        };
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceSubscription {
                request_id,
                workspace: WorkspaceName::parse("other-room").expect("other workspace"),
                events: Vec::new(),
                next_cursor: 0,
                live: true,
            },
        )
        .await;
        // An already-completed request is unmatched, even if its workspace is current.
        send_server(&mut socket, &old_subscription).await;
        let ClientMessage::Ping { request_id } = recv_client(&mut socket).await else {
            panic!("expected association fence");
        };
        // Matching a pending ID alone is insufficient: this request was not Subscribe.
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceSubscription {
                request_id,
                workspace: server_workspace.clone(),
                events: Vec::new(),
                next_cursor: 0,
                live: true,
            },
        )
        .await;
        socket
            .close(None)
            .await
            .expect("cut unsubscribed connection");
        drop(socket);

        let (stream, _) = listener.accept().await.expect("reconnected connection");
        let mut socket = accept_async(stream).await.expect("reconnected websocket");
        register_operator(&mut socket).await;
        let ClientMessage::WorkspaceJoin { request_id, name } = recv_client(&mut socket).await
        else {
            panic!("expected retained membership");
        };
        assert_eq!(name, server_workspace);
        send_server(
            &mut socket,
            &ServerMessage::WorkspaceJoined {
                request_id,
                workspace: server_workspace,
                cursor: 0,
            },
        )
        .await;
        let next = tokio::time::timeout(Duration::from_secs(3), recv_client(&mut socket))
            .await
            .expect("next command after membership restoration");
        let ClientMessage::Ping { request_id } = next else {
            panic!("confirmed unsubscribe must not restore a subscription");
        };
        assert_eq!(request_id, "still-unsubscribed");
        send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
        let _ = socket.next().await;
    });
    let credential = CredentialFile::generate(
        CredentialRole::Operator,
        "subscription-test".to_owned(),
        None,
        None,
        Vec::new(),
    )
    .expect("operator credential");
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).expect("server URL"),
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("connect operator");
    let mut state = client.connection_state();
    client
        .workspace_join(workspace)
        .await
        .expect("join workspace");
    client
        .workspace_subscribe(0)
        .await
        .expect("initial subscribe");
    assert!(matches!(
        client
            .call(ClientMessage::WorkspaceUnsubscribe {
                request_id: "confirmed-unsubscribe".to_owned(),
            })
            .await
            .expect("unsubscribe response"),
        ServerMessage::WorkspaceUnsubscribed { .. }
    ));
    client
        .workspace_subscribe(0)
        .await
        .expect("wrong-workspace response");
    client
        .call(ClientMessage::Ping {
            request_id: "association-fence".to_owned(),
        })
        .await
        .expect("wrong-effect response");
    tokio::time::timeout(
        Duration::from_secs(3),
        state.wait_for(|value| *value == (ClientConnectionState::Connected { epoch: 2 })),
    )
    .await
    .expect("membership-only reconnect deadline")
    .expect("state sender");
    assert!(matches!(
        client
            .call_with_deadline(
                ClientMessage::Ping {
                    request_id: "still-unsubscribed".to_owned()
                },
                tokio::time::Instant::now() + Duration::from_secs(3),
            )
            .await
            .expect("call after membership-only restoration"),
        ServerMessage::Pong { .. }
    ));
    client.close().await.expect("close operator");
    server.await.expect("subscription server task");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_initial_connect_closes_stalled_registration_transport() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled registration server");
    let address = listener.local_addr().expect("stalled registration address");
    let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("registration connection");
        let mut socket = accept_async(stream).await.expect("registration websocket");
        assert!(matches!(
            recv_client(&mut socket).await,
            ClientMessage::RegisterOperator { .. }
        ));
        registered_tx.send(()).expect("registration reached server");
        match tokio::time::timeout(Duration::from_secs(1), socket.next()).await {
            Ok(None | Some(Err(_) | Ok(Message::Close(_)))) => {}
            _ => panic!("cancelled connect must promptly release its registration transport"),
        }
    });
    let credential = CredentialFile::generate(
        CredentialRole::Operator,
        "cancelled-registration".to_owned(),
        None,
        None,
        Vec::new(),
    )
    .expect("registration credential");
    let connecting = tokio::spawn(RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).expect("registration URL"),
        role: ClientRole::Operator { credential },
        ca_file: None,
    }));
    tokio::time::timeout(Duration::from_secs(3), registered_rx)
        .await
        .expect("registration request deadline")
        .expect("registration request signal");
    connecting.abort();
    assert!(connecting.await.is_err_and(|error| error.is_cancelled()));
    server.await.expect("cancelled registration server");
}

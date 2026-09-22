use std::{net::SocketAddr, time::Duration};

use agent_session_router::{
    client::{
        ClientConfig, ClientConnectionState, ClientEvent, ClientEvents, ClientRole, RouterClient,
    },
    credentials::read_credential,
    protocol::{
        ClientMessage, ServerMessage, WorkspaceName, parse_client_message, parse_server_message,
    },
    router::{CREDENTIAL_MESSAGES_PER_SECOND, RouterConfig, RouterExposure, RouterRuntime},
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{oneshot, watch};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use url::Url;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NotificationPlacement {
    BeforeJoined,
    BeforeSubscription,
    AfterReplay,
    HoldSubscription,
}

struct FaultEvidence {
    placement: NotificationPlacement,
    workspace: WorkspaceName,
    cursor: i64,
}

struct FaultProxy {
    url: Url,
    cut: oneshot::Sender<()>,
    resume: oneshot::Sender<()>,
    fault: oneshot::Receiver<FaultEvidence>,
    restored_after: oneshot::Receiver<i64>,
    task: tokio::task::JoinHandle<()>,
}

impl FaultProxy {
    #[allow(clippy::too_many_lines)]
    async fn start(router: Url, placement: NotificationPlacement) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fault proxy");
        let url = Url::parse(&format!(
            "ws://{}/ws",
            listener.local_addr().expect("fault proxy address")
        ))
        .expect("fault proxy URL");
        let (cut, mut cut_rx) = oneshot::channel();
        let (resume, resume_rx) = oneshot::channel();
        let (fault_tx, fault) = oneshot::channel();
        let (after_tx, restored_after) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("initial proxy connection");
            let mut downstream = accept_async(stream).await.expect("initial downstream");
            let (mut upstream, _) = connect_async(router.as_str())
                .await
                .expect("initial real router connection");
            loop {
                tokio::select! {
                    _ = &mut cut_rx => break,
                    message = downstream.next() => {
                        let Some(Ok(message)) = message else { return };
                        if upstream.send(message).await.is_err() { return; }
                    }
                    message = upstream.next() => {
                        let Some(Ok(message)) = message else { return };
                        if downstream.send(message).await.is_err() { return; }
                    }
                }
            }
            // Drop both TCP transports, rather than asking the router to change membership.
            drop(downstream);
            drop(upstream);
            let (stream, _) = listener.accept().await.expect("reconnect proxy connection");
            let mut downstream = accept_async(stream).await.expect("reconnect downstream");
            // Bulk setup takes longer than the registration deadline at the real
            // credential rate limit, so that case gates join instead of registration.
            let mut resume_rx = Some(resume_rx);
            if placement != NotificationPlacement::AfterReplay {
                resume_rx
                    .take()
                    .expect("registration gate")
                    .await
                    .expect("resume reconnect");
            }
            let (mut upstream, _) = connect_async(router.as_str())
                .await
                .expect("reconnect real router connection");
            let mut held_join = None;
            let mut held_membership = None;
            let mut fault_tx = Some(fault_tx);
            let mut after_tx = Some(after_tx);
            loop {
                tokio::select! {
                    message = downstream.next() => {
                        let Some(Ok(message)) = message else { break };
                        if let Message::Text(text) = &message {
                            match parse_client_message(text.as_str()).expect("valid forwarded request") {
                                ClientMessage::WorkspaceJoin { .. } => {
                                    if let Some(resume_rx) = resume_rx.take() {
                                        resume_rx.await.expect("resume membership restoration");
                                    }
                                }
                                ClientMessage::WorkspaceSubscribe { after, .. } => {
                                    if let Some(after_tx) = after_tx.take() {
                                        let _ = after_tx.send(after);
                                    }
                                }
                                _ => {}
                            }
                        }
                        if upstream.send(message).await.is_err() { break; }
                    }
                    message = upstream.next() => {
                        let Some(Ok(message)) = message else { break };
                        let parsed = match &message {
                            Message::Text(text) => Some(
                                parse_server_message(text.as_str()).expect("valid real router response")
                            ),
                            _ => None,
                        };
                        match parsed {
                            Some(ServerMessage::WorkspaceJoined { .. })
                                if placement == NotificationPlacement::BeforeJoined && fault_tx.is_some() =>
                            {
                                held_join = Some(message);
                                continue;
                            }
                            Some(ServerMessage::WorkspaceChanged { workspace: Some(workspace), cursor })
                                if fault_tx.is_some() =>
                            {
                                let evidence = FaultEvidence { placement, workspace, cursor };
                                if placement != NotificationPlacement::BeforeJoined {
                                    held_membership = Some((message, evidence));
                                    continue;
                                }
                                // Reorder the router's own valid notification ahead of its join reply.
                                if downstream.send(message).await.is_err() { break; }
                                let _ = fault_tx.take().expect("one join fault").send(evidence);
                                let joined = held_join.take().expect("router sent join before membership");
                                if downstream.send(joined).await.is_err() { break; }
                                continue;
                            }
                            Some(ServerMessage::WorkspaceSubscription { .. })
                                if placement == NotificationPlacement::BeforeSubscription && fault_tx.is_some() =>
                            {
                                let (membership, evidence) = held_membership
                                    .take()
                                    .expect("router membership before subscription reply");
                                if downstream.send(membership).await.is_err() { break; }
                                let _ = fault_tx.take().expect("one subscription fault").send(evidence);
                            }
                            Some(ServerMessage::WorkspaceSubscription { .. })
                                if placement == NotificationPlacement::HoldSubscription =>
                            {
                                // Keep the real reply off the wire until the client closes.
                                continue;
                            }
                            Some(ServerMessage::WorkspaceSubscription { live: true, .. })
                                if placement == NotificationPlacement::AfterReplay && fault_tx.is_some() =>
                            {
                                // Isolate backpressure from the independent membership interleaving defect.
                                if downstream.send(message).await.is_err() { break; }
                                let (membership, evidence) = held_membership
                                    .take()
                                    .expect("membership retained until live restoration");
                                if downstream.send(membership).await.is_err() { break; }
                                let _ = fault_tx.take().expect("one delayed notification").send(evidence);
                                continue;
                            }
                            _ => {}
                        }
                        if downstream.send(message).await.is_err() { break; }
                    }
                }
            }
        });
        Self {
            url,
            cut,
            resume,
            fault,
            restored_after,
            task,
        }
    }
}

async fn wait_state(
    state: &mut watch::Receiver<ClientConnectionState>,
    expected: ClientConnectionState,
) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let actual = state.borrow_and_update().clone();
            if actual == expected {
                break;
            }
            assert!(
                !matches!(actual, ClientConnectionState::Closed { .. }),
                "connection closed while waiting for {expected:?}: {actual:?}"
            );
            state.changed().await.expect("connection state sender");
        }
    })
    .await
    .expect("connection state deadline");
}

async fn post(client: &RouterClient, content: &str) -> i64 {
    match client
        .call(ClientMessage::WorkspacePost {
            request_id: Uuid::new_v4().to_string(),
            content: content.to_owned(),
        })
        .await
        .expect("post through real router")
    {
        ServerMessage::WorkspacePosted { seq, .. } => seq,
        ServerMessage::Error { code, .. } => panic!("real-router post failed: {code}"),
        _ => panic!("unexpected post response"),
    }
}

struct RestoreFixture {
    directory: tempfile::TempDir,
    runtime: RouterRuntime,
    publisher: RouterClient,
    client: RouterClient,
    events: ClientEvents,
    proxy: FaultProxy,
    workspace: WorkspaceName,
    acknowledged: i64,
}

#[allow(clippy::too_many_lines)]
async fn start_subscribed_operator(placement: NotificationPlacement) -> RestoreFixture {
    let directory = tempfile::tempdir().expect("private router directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical directory")
        .join("data");
    let runtime = RouterRuntime::start(RouterConfig {
        bind: "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("loopback address"),
        data_dir: data_dir.clone(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    })
    .await
    .expect("start real router");
    let credential = read_credential(&data_dir.join("credentials/admin.json"))
        .expect("read private bootstrap credential");
    let router_url = Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    let config = ClientConfig {
        router_url: router_url.clone(),
        role: ClientRole::Operator { credential },
        ca_file: None,
    };
    let (publisher, _publisher_events) = RouterClient::connect(config.clone())
        .await
        .expect("connect direct publisher");
    let workspace = WorkspaceName::parse("reconnect-room").expect("workspace name");
    assert!(matches!(
        publisher
            .call(ClientMessage::WorkspaceCreate {
                request_id: "create-room".to_owned(),
                name: workspace.clone(),
            })
            .await
            .expect("create workspace"),
        ServerMessage::WorkspaceCreated { .. }
    ));
    publisher
        .workspace_join(workspace.clone())
        .await
        .expect("join publisher");
    let acknowledged = post(&publisher, "acknowledged before disconnect").await;
    let proxy = FaultProxy::start(router_url, placement).await;
    let (client, mut events) = RouterClient::connect(ClientConfig {
        router_url: proxy.url.clone(),
        ..config
    })
    .await
    .expect("connect operator through bidirectional proxy");
    assert_eq!(
        *client.connection_state().borrow(),
        ClientConnectionState::Connected { epoch: 1 },
    );
    assert_eq!(
        client.session_id(),
        None,
        "operators have no agent session ID"
    );
    assert_eq!(client.operator_is_admin(), Some(true));
    client
        .workspace_join(workspace.clone())
        .await
        .expect("join proxied operator");
    let (initial, cursor, live) = client
        .workspace_subscribe(0)
        .await
        .expect("subscribe operator");
    assert!(live);
    assert_eq!(cursor, acknowledged);
    assert_eq!(
        initial.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![acknowledged]
    );
    client
        .ack_event(workspace.clone(), acknowledged)
        .expect("ack initial event");
    client
        .call(ClientMessage::Ping {
            request_id: "ack-fence".to_owned(),
        })
        .await
        .expect("same socket ack fence");
    let membership = tokio::time::timeout(DEADLINE, events.recv())
        .await
        .expect("initial membership deadline")
        .expect("initial membership event");
    assert!(
        matches!(&membership.event, ClientEvent::MembershipChanged { workspace: Some(name), .. } if name == &workspace)
    );
    drop(membership);
    RestoreFixture {
        directory,
        runtime,
        publisher,
        client,
        events,
        proxy,
        workspace,
        acknowledged,
    }
}

#[allow(clippy::too_many_lines)]
async fn reconnect_with_membership_notification(placement: NotificationPlacement) {
    let RestoreFixture {
        directory: _directory,
        runtime,
        publisher,
        client,
        mut events,
        proxy,
        workspace,
        acknowledged,
    } = start_subscribed_operator(placement).await;
    let mut state = client.connection_state();

    proxy.cut.send(()).expect("cut established transport");
    wait_state(&mut state, ClientConnectionState::Reconnecting).await;
    assert_eq!(client.operator_is_admin(), None);
    let missed = post(&publisher, "persisted while operator disconnected").await;
    proxy
        .resume
        .send(())
        .expect("release reconnect after offline post");
    let evidence = tokio::time::timeout(DEADLINE, proxy.fault)
        .await
        .expect("notification fault deadline")
        .expect("notification was forwarded");
    assert_eq!(evidence.placement, placement);
    assert_eq!(evidence.workspace, workspace);
    assert_eq!(evidence.cursor, missed);
    eprintln!(
        "real-router fault forwarded: {placement:?}, membership cursor {}",
        evidence.cursor
    );

    let restored = tokio::time::timeout(DEADLINE, async {
        loop {
            let actual = state.borrow_and_update().clone();
            if actual != ClientConnectionState::Reconnecting {
                break actual;
            }
            state.changed().await.expect("restoration state sender");
        }
    })
    .await
    .expect("restoration must finish or report a terminal reason");
    if restored != (ClientConnectionState::Connected { epoch: 2 }) {
        // Keep the intentionally red reproduction's failure about client behavior, not cleanup.
        proxy.task.abort();
        let _ = proxy.task.await;
        let _ = client.close().await;
        publisher
            .close()
            .await
            .expect("close direct publisher after failed restore");
        runtime
            .shutdown()
            .await
            .expect("shutdown failed-restore router");
        runtime.wait().await.expect("join failed-restore router");
        panic!("valid WorkspaceChanged {placement:?} must restore epoch 2; got {restored:?}");
    }
    assert_eq!(client.session_id(), None);
    assert_eq!(
        tokio::time::timeout(DEADLINE, proxy.restored_after)
            .await
            .expect("restored subscription deadline")
            .expect("restored subscription request"),
        acknowledged
    );

    let mut membership_seen = false;
    let replay = tokio::time::timeout(DEADLINE, async {
        loop {
            let item = events.recv().await.expect("recovered event stream");
            match item.event {
                ClientEvent::MembershipChanged {
                    workspace: Some(name),
                    cursor,
                } => {
                    assert_eq!(name, workspace);
                    assert_eq!(cursor, missed);
                    membership_seen = true;
                }
                ClientEvent::WorkspaceEvent(event) => break event,
                event => panic!("unexpected restoration event: {event:?}"),
            }
        }
    })
    .await
    .expect("replay deadline");
    assert!(
        membership_seen,
        "restoration must publish membership before replay"
    );
    assert_eq!(replay.workspace, workspace);
    assert_eq!(replay.seq, missed, "acked event must not replay");
    assert_eq!(
        replay.content.as_deref(),
        Some("persisted while operator disconnected")
    );
    client
        .ack_event(workspace.clone(), replay.seq)
        .expect("ack restored event");
    let page = client
        .workspace_history(Some(acknowledged), Some(100))
        .await
        .expect("recovered real membership permits history");
    assert_eq!(page.workspace, workspace);
    assert_eq!(
        page.events
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![missed]
    );

    let live_seq = post(&publisher, "live after restoration").await;
    let live = tokio::time::timeout(DEADLINE, events.recv())
        .await
        .expect("restored live subscription deadline")
        .expect("restored live event");
    assert!(matches!(&live.event, ClientEvent::WorkspaceEvent(event)
        if event.seq == live_seq && event.content.as_deref() == Some("live after restoration")));
    drop(live);
    client.close().await.expect("close restored operator");
    wait_state(&mut state, ClientConnectionState::Closed { reason: None }).await;
    assert_eq!(client.operator_is_admin(), None);
    proxy.task.abort();
    let _ = proxy.task.await;
    publisher.close().await.expect("close publisher");
    runtime.shutdown().await.expect("shutdown real router");
    runtime.wait().await.expect("join real router");
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_accepts_workspace_changed_before_joined() {
    reconnect_with_membership_notification(NotificationPlacement::BeforeJoined).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_accepts_workspace_changed_before_subscription() {
    reconnect_with_membership_notification(NotificationPlacement::BeforeSubscription).await;
}

// A single-thread runtime makes the old synchronous 100-event enqueue exhaust its
// 64 slots before the consumer can run, independent of machine scheduling.
#[tokio::test]
async fn reconnect_replays_multiple_pages_through_bounded_queue_with_consumer_acks() {
    let RestoreFixture {
        directory: _directory,
        runtime,
        publisher,
        client,
        mut events,
        proxy,
        workspace,
        acknowledged,
    } = start_subscribed_operator(NotificationPlacement::AfterReplay).await;
    let mut state = client.connection_state();
    proxy.cut.send(()).expect("cut subscribed operator");
    wait_state(&mut state, ClientConnectionState::Reconnecting).await;
    let mut expected = Vec::new();
    let mut posts = tokio::time::interval(Duration::from_millis(
        1000 / CREDENTIAL_MESSAGES_PER_SECOND + 1,
    ));
    posts.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    for index in 0..130 {
        posts.tick().await;
        expected.push(post(&publisher, &format!("offline replay {index}")).await);
    }
    proxy.resume.send(()).expect("resume multipage restoration");
    assert_eq!(
        tokio::time::timeout(DEADLINE, proxy.restored_after)
            .await
            .expect("restore request deadline")
            .expect("restore request"),
        acknowledged,
    );
    let mut applied = Vec::new();
    let outcome = tokio::time::timeout(DEADLINE, async {
        loop {
            if applied == expected
                && *state.borrow() == (ClientConnectionState::Connected { epoch: 2 })
            {
                break;
            }
            tokio::select! {
                item = events.recv() => {
                    let item = item.expect("replay event stream");
                    match item.event {
                        ClientEvent::WorkspaceEvent(event) => {
                            assert_eq!(event.workspace, workspace);
                            assert_eq!(event.seq, expected[applied.len()], "replay must have no duplicate or gap");
                            applied.push(event.seq);
                            client.ack_event(workspace.clone(), event.seq).expect("ack only applied event");
                        }
                        ClientEvent::MembershipChanged { .. } => {}
                        event => panic!("unexpected replay event: {event:?}"),
                    }
                }
                changed = state.changed() => {
                    changed.expect("replay connection state sender");
                }
            }
        }
    }).await;
    let observed_state = state.borrow().clone();
    proxy.task.abort();
    let _ = proxy.task.await;
    client.close().await.expect("close multipage operator");
    publisher.close().await.expect("close multipage publisher");
    runtime.shutdown().await.expect("shutdown multipage router");
    runtime.wait().await.expect("join multipage router");
    assert!(
        outcome.is_ok(),
        "one restoration must deliver all 130 events while draining ACKs; applied {}, state {observed_state:?}",
        applied.len(),
    );
    assert_eq!(applied, expected);
    assert_eq!(
        observed_state,
        ClientConnectionState::Connected { epoch: 2 }
    );
}

#[tokio::test]
async fn close_during_subscription_restoration_does_not_wait_for_server_reply() {
    let RestoreFixture {
        directory: _directory,
        runtime,
        publisher,
        client,
        events: _events,
        proxy,
        workspace: _workspace,
        acknowledged,
    } = start_subscribed_operator(NotificationPlacement::HoldSubscription).await;
    let mut state = client.connection_state();
    proxy.cut.send(()).expect("cut before stalled restoration");
    wait_state(&mut state, ClientConnectionState::Reconnecting).await;
    proxy
        .resume
        .send(())
        .expect("resume into stalled restoration");
    assert_eq!(
        tokio::time::timeout(DEADLINE, proxy.restored_after)
            .await
            .expect("stalled subscription deadline")
            .expect("stalled subscription request"),
        acknowledged,
    );
    assert_eq!(*state.borrow(), ClientConnectionState::Reconnecting);
    let outcome = tokio::time::timeout(Duration::from_secs(1), client.close()).await;
    let observed_state = state.borrow().clone();
    // Abort the proxy only after measuring close: this must not be what unblocks it.
    proxy.task.abort();
    let _ = proxy.task.await;
    wait_state(&mut state, ClientConnectionState::Closed { reason: None }).await;
    publisher
        .close()
        .await
        .expect("close stalled-restore publisher");
    runtime
        .shutdown()
        .await
        .expect("shutdown stalled-restore router");
    runtime.wait().await.expect("join stalled-restore router");
    assert!(
        matches!(outcome, Ok(Ok(()))),
        "close must cancel restoration without waiting for its 60-second response timeout",
    );
    assert_eq!(
        observed_state,
        ClientConnectionState::Closed { reason: None }
    );
    assert_eq!(client.operator_is_admin(), None);
}
